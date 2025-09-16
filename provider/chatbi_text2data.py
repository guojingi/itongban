from typing import Any
import requests
from dify_plugin import ToolProvider
from dify_plugin.errors.tool import ToolProviderCredentialValidationError


class Text2DataProvider(ToolProvider):
    
    def _validate_credentials(self, credentials: dict[str, Any]) -> None:
        """
                验证提供的 api key 是否有效。
                尝试调用接口验证。
                如果验证失败，应抛出 ToolProviderCredentialValidationError 异常。
                """
        base_url = credentials.get("base_url")
        access_token = credentials.get("access_token")
        if not base_url:
            raise ToolProviderCredentialValidationError("BaseURL不能为空。")
        if not access_token:
            raise ToolProviderCredentialValidationError("访问令牌不能为空。")

        try:
            params = {
                "access_token": access_token,
            }
            api_url = base_url + "/api/chat/query/verify"
            response = requests.post(api_url,
                                     headers={'Authorization': f'Bearer {access_token}', 'Content-Type': 'application/json'},
                                     json=params,
                                     timeout=600)
            if response.status_code == 200:
                result = response.json()
                if result.get("code") == 200:
                    pass
                else:
                    raise ToolProviderCredentialValidationError(result)
            else:
                print(f"错误信息: {response.text}")
                raise ToolProviderCredentialValidationError(response.text)
        except Exception as e:
            # 如果 API 调用失败，说明凭证很可能无效
            raise ToolProviderCredentialValidationError(f"凭证验证失败：{e}")
