from collections.abc import Generator
import logging
from typing import Any
import sqlparse
import requests
from dify_plugin import Tool
from dify_plugin.entities.tool import ToolInvokeMessage

from utils.db_util import DbUtil


class Text2DataTool(Tool):
    def _invoke(self, tool_parameters: dict[str, Any]) -> Generator[ToolInvokeMessage]:
        """
                根据输入的标题和内容，创建一个新的 Telegraph 页面。

                Args:
                    tool_parameters: 一个包含工具输入参数的字典:
                        - assistant_name (str): 助理名称。
                        - query (str): 查询语句。
                        - db_type (str): 数据库类型。
                        - db_host (str): 数据库地址。
                        - db_port (number): 数据库端口号。
                        - db_username (str): 数据库用户名。
                        - db_password (str): 数据库用户密码。
                        - db_name (str): 数据库库名。
                        - query_sql (str): 查询通过id查询dify用户姓名或邮箱的sql。

                Yields:
                    ToolInvokeMessage: 包含调用助理问数返回的消息。

                Raises:
                    Exception: 如果调用失败，则抛出包含错误信息的异常。
                """
        # 1. 从运行时获取凭证
        try:
            base_url = self.runtime.credentials["base_url"]
            access_token = self.runtime.credentials["access_token"]
        except KeyError:
            raise Exception("插件授权未配置或无效。")
        # 查询用户属性：用户姓名或者邮箱
        user_property = ""
        # 数据库连接配置 - 请根据您的环境修改这些值
        db_type = tool_parameters.get("db_type", "")
        if not db_type:
            raise ValueError("Please select the database type")
        db_host = tool_parameters.get("db_host", "")
        if not db_host:
            raise ValueError("Please fill in the database host")
        db_port = tool_parameters.get("db_port", "")
        if db_port is not None:
            db_port = str(db_port)
        db_username = tool_parameters.get("db_username", "")
        if not db_username:
            raise ValueError("Please fill in the database username")
        db_password = tool_parameters.get("db_password", "")
        if not db_password:
            raise ValueError("Please fill in the database password")
        db_name = tool_parameters.get("db_name", "")
        db_properties = tool_parameters.get("db_properties", "")
        query_sql = tool_parameters.get("query_sql", "")
        if not query_sql:
            raise ValueError("Please fill in the query SQL,for example: select email from accounts where name = 'admin'")
        statements = sqlparse.parse(query_sql)
        if len(statements) != 1:
            raise ValueError("Only a single query SQL can be filled")
        statement = statements[0]
        if statement.get_type() != 'SELECT':
            raise ValueError("Query SQL can only be a single SELECT statement")
        try:
            with DbUtil(db_type=db_type,
                        username=db_username,
                        password=db_password,
                        host=db_host,
                        port=db_port,
                        database=db_name,
                        properties=db_properties) as db:
                records = db.run_query(query_sql)
                if records:
                    user_property = next(iter(records[0].values()))
        except Exception as e:
            logging.exception("SQL query execution failed: %s", str(e))
            raise RuntimeError(f"Error executing SQL: {e}") from e
        # 2. 获取工具输入参数，使用 .get 提供默认值
        # 助理名称
        assistant_name = tool_parameters.get("assistant_name", "")
        # 查询语句
        query = tool_parameters.get("query", "")
        if not assistant_name:
            raise Exception("助理名称不能为空。")
        if not query:
            raise Exception("查询语句不能为空。")
        # 3. 调用接口执行操作
        try:
            params = {
                "agentName": assistant_name,
                "queryText": query,
                "userName": user_property,
            }
            api_url = base_url + "/api/chat/dify/executeByParams"
            response = requests.post(api_url,
                                     headers={'Authorization': f'Bearer {access_token}', 'Content-Type': 'application/json'},
                                     json=params,
                                     timeout=600)
            if response.status_code == 200:
                result = response.json()
                # 打印解析后的Python对象
                result_data = result.get('data')
                return_data = ""
                if result_data:
                    return_data = result_data.get('textResult')
                # 4. 返回结果
                # 文本输出
                yield self.create_text_message(f"{return_data}")
                # JSON输出
                yield self.create_json_message(result_data)
                # 变量输出 (用于工作流)
                yield self.create_variable_message("status", result.get('msg'))
            else:
                print(f"错误信息: {response.text}")
                # 错误处理
                yield self.create_text_message(f"Error: {response.text}")
                return response.text
        except Exception as e:
            # 如果接口调用失败，抛出异常
            raise Exception(f"调用API失败: {e}")

