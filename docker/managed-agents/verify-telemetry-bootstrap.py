"""Offline startup verification. Run using python -S in the worker image."""

import json
import os
import runpy
from pathlib import Path


def main():
    os.environ.update(
        {
            "OTEL_TRACES_EXPORTER": "otlp",
            "OTEL_METRICS_EXPORTER": "otlp",
            "OTEL_LOGS_EXPORTER": "otlp",
            "OTEL_EXPORTER_OTLP_ENDPOINT": "http://localhost:4318",
            "OTEL_EXPORTER_OTLP_PROTOCOL": "http/protobuf",
            "OTEL_SERVICE_NAME": "offline-runtime.telemetry",
            "OTEL_RESOURCE_ATTRIBUTES": "apmplus.business_carrier=agentkit_runtime",
            "ENABLE_APMPLUS": "true",
        }
    )
    bootstrap = runpy.run_path(str(Path(__file__).with_name("configure-telemetry.py")))
    disabled = bootstrap["disabled_exporters"](os.environ)
    assert len(disabled) == 3
    for key in disabled:
        os.environ[key] = "none"

    # -S avoids auto site initialization. Add the image's installed packages only
    # after filtering, then execute the same SDK configurator used by the injected
    # OpenTelemetry distro. No installed dependency is replaced.
    import sys
    import sysconfig

    sys.path.append(sysconfig.get_path("purelib"))
    sys.path.append(
        str(
            Path(sys.executable).parent.parent
            / f"lib/python{sys.version_info.major}.{sys.version_info.minor}/site-packages"
        )
    )
    from opentelemetry.sdk._configuration import _initialize_components
    from opentelemetry import _logs, metrics, trace

    _initialize_components()
    global_trace = trace.get_tracer_provider()
    global_meter = metrics.get_meter_provider()
    global_logs = _logs.get_logger_provider()
    assert not global_trace._active_span_processor._span_processors
    assert not global_meter._sdk_config.metric_readers
    assert not global_logs._multi_log_record_processor._log_record_processors

    from veadk.tracing.telemetry import apmplus_logs, portal_metrics
    from veadk.tracing.telemetry.exporters.apmplus_exporter import (
        APMPlusExporter,
        APMPlusExporterConfig,
        shutdown_apmplus_meter_provider,
    )
    from veadk.tracing.telemetry.opentelemetry_tracer import OpentelemetryTracer

    try:
        tracer = OpentelemetryTracer(
            exporters=[
                APMPlusExporter(
                    config=APMPlusExporterConfig(
                        endpoint="http://127.0.0.1:4317",
                        app_key="offline-placeholder",
                    )
                )
            ]
        )
        assert len(global_trace._active_span_processor._span_processors) == 2
        assert not tracer.apmplus_managed_externally
        assert portal_metrics.portal_metric_recorder.provider is not global_meter
        for resource in (
            global_trace.resource,
            portal_metrics.portal_metric_recorder.provider._sdk_config.resource,
            apmplus_logs._provider.resource,
        ):
            assert resource.attributes["service.name"] == "offline-runtime.telemetry"
            assert resource.attributes["apmplus.business_carrier"] == "agentkit_runtime"
        assert os.getenv("OTEL_SDK_DISABLED", "false").lower() != "true"
        assert os.environ["ENABLE_APMPLUS"] == "true"
        print(
            json.dumps(
                {
                    "bootstrap": "passed",
                    "empty_providers_taken_over": True,
                    "service_identity_preserved": True,
                    "sdk_enabled": True,
                }
            )
        )
    finally:
        apmplus_logs.shutdown_apmplus_logs()
        shutdown_apmplus_meter_provider()
        global_trace.shutdown()
        global_meter.shutdown()
        global_logs.shutdown()


if __name__ == "__main__":
    main()
