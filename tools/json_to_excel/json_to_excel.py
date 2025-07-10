from collections.abc import Generator
from typing import Any
import json
import pandas as pd
from io import BytesIO

from dify_plugin import Tool
from dify_plugin.entities.tool import ToolInvokeMessage
from tools.utils.file_utils import get_meta_data
from tools.utils.mimetype_utils import MimeType


class JsonToExcelTool(Tool):
    def _invoke(self, tool_parameters: dict[str, Any]) -> Generator[ToolInvokeMessage]:

        json_string = tool_parameters.get("json_str", None)
        file_name = tool_parameters.get("file_name", None)

        if not json_string:
            raise Exception({"error": "No JSON data provided."})

        # 尝试解析 JSON 字符串
        try:
            data = json.loads(json_string)
        except json.JSONDecodeError:
            # 如果第一次失败，再尝试去除双重转义
            try:
                intermediate = json.loads(f'"{json_string}"')
                data = json.loads(intermediate)
            except Exception as e:
                raise ValueError(f"无法解析 JSON 字符串: {e}")

        if not isinstance(data, list) or not all(isinstance(item, dict) for item in data):
            raise Exception({"error": "JSON data must be a list of dictionaries."})

        try:
            # 转换为 DataFrame
            df = pd.DataFrame(data)

            # 写入 BytesIO 为 Excel 文件（.xlsx）
            excel_buffer = BytesIO()
            with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                df.to_excel(writer, index=False)

            excel_buffer.seek(0)  # 重置指针以供读取
            
            # 返回 blob 文件
            yield self.create_blob_message(
                blob=excel_buffer.read(),
                meta=get_meta_data(
                    mime_type=MimeType.XLSX,
                    output_filename=file_name if file_name else "结果.xlsx"
                ),
            )
        except Exception as e:
            raise Exception({"error": f"Failed to generate Excel: {str(e)}"})
