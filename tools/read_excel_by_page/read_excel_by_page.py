import json
import warnings
import requests
from io import BytesIO
from collections import OrderedDict
from typing import Any
from collections.abc import Generator

import pandas as pd
from dify_plugin import Tool
from dify_plugin.entities.tool import ToolInvokeMessage


class ReadExcelByPageTool(Tool):
    def _invoke(self, tool_parameters: dict[str, Any]) -> Generator[ToolInvokeMessage, None, None]:

        # 忽略 openpyxl 样式警告
        warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")

        # 获取文件信息
        file_url = tool_parameters.get("file_url")
        sheet_name = tool_parameters.get("sheet_name")  # 可选参数
        page_num = tool_parameters.get("page_num", 1)  # 默认为 1
        page_size = tool_parameters.get("page_size", 10)  # 默认为 10

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
                yield self.create_text_message("指定的 sheet 名不存在")
                return
            else:
                sheet_name = sheet_name_lut[sheet_name.lower()]

            # 读取数据（保留原始表头顺序）
            df = pd.read_excel(excel_file, sheet_name=sheet_name)
            df = df.fillna("")  # 替换 NaN

            # 分页处理
            total_rows = len(df)
            start = (page_num - 1) * page_size
            end = start + page_size

            if start >= total_rows:
                yield self.create_text_message(f"页码超出范围，当前总行数为 {total_rows}，请求起始行为 {start + 1}")
                return

            paged_df = df.iloc[start:end]

            # 使用 OrderedDict 保证输出字段顺序一致（与原始表头一致）
            json_data = [
                OrderedDict((col, row[col]) for col in df.columns)
                for _, row in paged_df.iterrows()
            ]

            yield self.create_variable_message('data', json_data)
            yield self.create_text_message(json.dumps(json_data, ensure_ascii=False, indent=2))

        except Exception as e:
            yield self.create_text_message(f"Failed to read XLSX file, error: {str(e)}")
            return
