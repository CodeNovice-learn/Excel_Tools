import json
from collections.abc import Generator
from typing import Any
import json
import warnings
import requests
from io import BytesIO

import pandas as pd
from dify_plugin import Tool
from dify_plugin.entities.tool import ToolInvokeMessage


class ReadExcelTool(Tool):
    def _invoke(self, tool_parameters: dict[str, Any]) -> Generator[ToolInvokeMessage, None, None]:
        
        # 忽略 openpyxl 样式警告
        warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")
        
        # 获取文件信息
        file_url = tool_parameters.get("file_url")
        sheet_name = tool_parameters.get("sheet_name")  # 可选参数

        try:
            # 下载文件
            response = requests.get(file_url, timeout=10)
            response.raise_for_status()
            file_bytes = BytesIO(response.content)
            # 获取 sheet 列表
            excel_file = pd.ExcelFile(file_bytes)
            all_sheets = excel_file.sheet_names

            # 构建大小写不敏感映射
            sheet_name_lut = {s.lower(): s for s in all_sheets}
            # sheet_name 为空默认第一个；大小写不敏感匹配
            if not sheet_name:
                sheet_name = all_sheets[0]
            elif sheet_name.lower() not in sheet_name_lut:
                yield self.create_text_message("")
                return
            # 读取数据
            df = pd.read_excel(excel_file, sheet_name=sheet_name)

            # 替换 NaN 为 None
            df = df.fillna("")

            # 将每行转为 dict，并移除值为 None 的字段
            json_data = [
                {k: v for k, v in row.items() if v is not None}
                for row in df.to_dict(orient="records")
            ]

            yield self.create_text_message(str(json.dumps(json_data, ensure_ascii=False)))
        except Exception as e:
            yield self.create_text_message(
                f"Failed to read XLSX file, error: {str(e)}")
            return
