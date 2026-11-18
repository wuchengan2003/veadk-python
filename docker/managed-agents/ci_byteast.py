"""Run ByteAST image scans and fail CI on sensitive information findings."""

import json
import os
from pathlib import Path
import re
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import HTTPRedirectHandler, Request, build_opener

from ci_registry import settings


API_URL = os.environ.get("BYTEAST_API_URL", "").strip().rstrip("/")
REPORT_URL = os.environ.get("BYTEAST_REPORT_URL", "").strip().rstrip("/")
RESULT_FILE = Path(".byteast-scan.json")
SCAN_TIMEOUT_SECONDS = 1800
POLL_SECONDS = 15
PLUGINS = ["cve", "sensitive_info", "malware", "benchmark", "unexpected_file"]
STATUS_NAMES = {
    1: "ready",
    2: "queuing",
    3: "scheduling",
    4: "running",
    5: "completed",
    6: "cancelled",
    7: "failed",
    8: "timed out",
    9: "awaiting approval",
}


class ScanError(Exception):
    """An error message that is safe to print in CI logs."""


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise ScanError("ByteAST API returned an unexpected redirect")


def http_failure(error, path):
    message = f"ByteAST API returned HTTP {error.code} for {path.split('?')[0]}"
    request_id = error.headers.get("x-tt-logid", "")
    if re.fullmatch(r"[A-Fa-f0-9]{16,64}", request_id):
        message += f" (gateway request ID: {request_id})"
    if error.code in (401, 403) and error.fp is not None:
        try:
            with error:
                body = json.loads(error.read(4096))
        except (OSError, ValueError):
            return message
        detail = body.get("error") if isinstance(body, dict) else None
        if isinstance(detail, str):
            if "不允许个人账号访问" in detail:
                message += (
                    "; ByteAST requires a service account, not a personal account"
                )
            elif "开通了API调用审批" in detail:
                message += (
                    "; the gateway found no effective API approval for this caller"
                )
    return message


class Client:
    def __init__(self, token):
        self.token = token
        self.opener = build_opener(NoRedirect())

    def request(self, path, payload=None, timeout=60):
        body = json.dumps(payload).encode() if payload is not None else None
        request = Request(
            API_URL + path,
            data=body,
            headers={
                "Authorization": f"Bearer {self.token}",
                "Domain": "byteast",
                "Content-Type": "application/json",
            },
        )
        try:
            with self.opener.open(request, timeout=timeout) as response:
                result = json.load(response)
        except HTTPError as error:
            raise ScanError(http_failure(error, path)) from None
        except (URLError, TimeoutError):
            raise ScanError("ByteAST API connection failed or timed out") from None
        except (ValueError, UnicodeError):
            raise ScanError("ByteAST API returned invalid JSON") from None
        if not isinstance(result, dict) or type(result.get("error_code")) is not int:
            raise ScanError("ByteAST API response is missing its result code")
        if result["error_code"] != 0:
            # Do not echo API messages or bodies: they can contain credentials/findings.
            raise ScanError(f"ByteAST API error code {result['error_code']}")
        return result


def configuration():
    if not API_URL or not REPORT_URL:
        raise ScanError(
            "Set BYTEAST_API_URL and BYTEAST_REPORT_URL in CI configuration"
        )
    image, sha, _ = settings()
    token = os.environ.get("BYTEAST_TOKEN", "").strip()
    if not token or "${{" in token:
        raise ScanError("Missing BYTEAST_TOKEN CI secret")
    return f"{image}:{sha}", token


def payload_for(image):
    return {
        "title": f"Worker image {image.rsplit(':', 1)[-1]}",
        "workload": {
            "target": [
                {
                    "image": {
                        "image_info_url": image,
                        "image_type": 0,
                        "username": os.environ["VOLC_CR_USERNAME"],
                        "password": os.environ["VOLC_CR_PASSWORD"],
                        "forbid_ssl": False,
                    }
                }
            ]
        },
        "engine_config": {
            "ast_id": 1005,
            "asset_type": 2,
            "plugin": {"range": {"keys": PLUGINS}},
        },
        "cron": {"task_type": 5, "scan_now": True},
    }


def wait_for_scan(client, scheduler_id, timeout=SCAN_TIMEOUT_SECONDS):
    deadline = time.monotonic() + timeout
    last_status = None
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise ScanError("ByteAST scan exceeded the CI wait timeout")
        result = client.request(
            "/ui/report/info?" + urlencode({"scheduler_id": scheduler_id}),
            timeout=min(60, remaining),
        )
        data = result.get("data")
        status = data.get("status") if isinstance(data, dict) else None
        if type(status) is not int or status not in STATUS_NAMES:
            raise ScanError("ByteAST returned an unknown scan status")
        if status != last_status:
            print(f"ByteAST scan: {STATUS_NAMES[status]}", flush=True)
            last_status = status
        if status == 5:
            return
        if status in (6, 7, 8):
            raise ScanError(f"ByteAST scan {STATUS_NAMES[status]}")
        remaining = deadline - time.monotonic()
        if remaining > 0:
            time.sleep(min(POLL_SECONDS, remaining))


def statistics_from(result):
    statistics = result.get("statistic")
    if not isinstance(statistics, dict):
        raise ScanError("ByteAST report is missing risk statistics")
    counts = {}
    for group, keys in {
        "cve_count": ["level_severe", "level_high", "level_middle", "level_low"],
        "plugin_result_count": PLUGINS[1:],
    }.items():
        values = statistics.get(group, {})
        if not isinstance(values, dict):
            raise ScanError("ByteAST report contains invalid risk statistics")
        for key in keys:
            if key in values:
                value = values[key]
                if type(value) is not int or value < 0:
                    raise ScanError("ByteAST report contains invalid risk counts")
                counts[key] = value
    return counts


def report_counts(client, scheduler_id):
    def result_for(result_type):
        return client.request(
            "/ui/report/image_result",
            {"scheduler_id": scheduler_id, "result_type": result_type},
        )

    def count_entries(result, field, key):
        total = result.get("total")
        if (
            not isinstance(result.get("data"), dict)
            or type(total) is not int
            or total < 0
        ):
            raise ScanError(f"ByteAST report contains no valid total for {key}")
        entries = result["data"].get(field)
        if entries is not None and not isinstance(entries, list):
            raise ScanError(f"ByteAST report contains invalid entries for {key}")
        return max(total, len(entries or []))

    vulnerabilities = result_for(0)
    sensitive_info = count_entries(result_for(1), "sensitiveInfo", "sensitive_info")
    if vulnerabilities.get("statistic") is not None:
        counts = statistics_from(vulnerabilities)
        counts["sensitive_info"] = max(counts.get("sensitive_info", 0), sensitive_info)
        return counts

    # Some reports omit statistics and return total=0 even when entries exist.
    counts = {
        "vulnerability_total": count_entries(
            vulnerabilities, "vulnerability", "vulnerability_total"
        ),
        "sensitive_info": sensitive_info,
        "malware": count_entries(result_for(2), "malware", "malware"),
        "benchmark": count_entries(result_for(3), "benchmark", "benchmark"),
        "unexpected_file": count_entries(
            result_for(4), "unexpectedFile", "unexpected_file"
        ),
    }
    print("ByteAST 未返回详细统计，以下按各类别的返回条目数和总数汇总")
    return counts


def print_temporary_report(client, scheduler_id):
    share_token = client.request(
        "/ui/auth/temp_link", {"scheduler_id": scheduler_id}
    ).get("data")
    if not isinstance(share_token, str) or not share_token.strip():
        raise ScanError("ByteAST returned no temporary report token")
    query = urlencode(
        {"asset_type": 2, "tab": "vulnerability", "temp_token": share_token}
    )
    line = f"ByteAST 临时报告（7 天有效）：{REPORT_URL}{scheduler_id}?{query}"
    print(line, flush=True)
    print(f"::add-message ::{line}", flush=True)


def scan():
    RESULT_FILE.unlink(missing_ok=True)
    image, token = configuration()
    client = Client(token)
    # Do not retry creation: a lost response may still have created a scan task.
    created = client.request("/ui/task/create", payload_for(image)).get("data")
    scheduler_id = created.get("scheduler_id") if isinstance(created, dict) else None
    if not isinstance(scheduler_id, str) or not re.fullmatch(
        r"[A-Za-z0-9_-]{1,128}", scheduler_id
    ):
        raise ScanError("ByteAST task creation returned no valid scheduler ID")
    report_url = f"{REPORT_URL}/{scheduler_id}"
    print(f"ByteAST 报告：{report_url}", flush=True)
    print(f"::add-message ::ByteAST 报告：{report_url}", flush=True)
    wait_for_scan(client, scheduler_id)
    try:
        counts = report_counts(client, scheduler_id)
        print("ByteAST 风险统计：" + json.dumps(counts, ensure_ascii=False))
        if counts["sensitive_info"]:
            raise ScanError(
                f"ByteAST detected {counts['sensitive_info']} sensitive information "
                f"finding(s) in report {scheduler_id}"
            )
    except ScanError:
        # Bits skips the summary step when the scan step fails.
        try:
            print_temporary_report(client, scheduler_id)
        except ScanError as error:
            print(f"ByteAST temporary report unavailable: {error}", file=sys.stderr)
        raise
    RESULT_FILE.write_text(
        json.dumps({"image": image, "report_url": report_url, "counts": counts})
    )
    print("ByteAST 扫描完成：未发现敏感信息")


def summary():
    image, token = configuration()
    result = json.loads(RESULT_FILE.read_text())
    report_url = result.get("report_url", "")
    if result.get("image") != image or not re.fullmatch(
        re.escape(REPORT_URL) + r"[A-Za-z0-9_-]{1,128}", report_url
    ):
        raise ScanError("ByteAST summary does not match the current image")
    print(f"镜像：{image}")
    print(f"::add-message ::镜像：{image}")
    print_temporary_report(Client(token), report_url.removeprefix(REPORT_URL))


if __name__ == "__main__":
    try:
        {"check": configuration, "scan": scan, "summary": summary}[sys.argv[1]]()
    except ScanError as error:
        print(f"ByteAST step failed: {error}", file=sys.stderr)
        sys.exit(1)
    except Exception as error:
        print(f"ByteAST step failed: {type(error).__name__}", file=sys.stderr)
        sys.exit(1)
