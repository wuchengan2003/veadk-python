# Copyright (c) 2025 Beijing Volcano Engine Technology Co., Ltd. and/or its affiliates.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""兼容 VeADK 1.1.14 的 BytePlus APMPlus OpenAPI 地址。"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from veadk.utils.cloud_provider import CloudProvider


def _apmplus_openapi_host(provider: "CloudProvider") -> str:
    from veadk.utils.cloud_provider import apmplus_openapi_host

    # BytePlus 统一使用新加坡 OpenAPI 入口，签名区域仍由 VeADK 按原配置选择。
    if provider == "byteplus":
        return "apmplus-server.ap-southeast-1.byteplusapi.com"
    return apmplus_openapi_host(provider)


def configure_apmplus_endpoint() -> None:
    """在创建 Agent 前安装地址适配，保留 VeADK 的凭据、签名和异常处理。"""
    from veadk.auth.veauth import apmplus_veauth

    # VeADK 在此模块按名称导入 resolver；只改 utils 中的函数不会生效。
    # 待固定依赖版本支持区域入口后移除此兼容适配。
    apmplus_veauth.apmplus_openapi_host = _apmplus_openapi_host
