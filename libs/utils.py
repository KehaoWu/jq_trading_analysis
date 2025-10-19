"""
工具函数模块

该模块提供了各种通用的工具函数，包括日期解析等功能。
"""

from datetime import datetime


def parse_date_string(date_str: str) -> str:
    """
    解析日期字符串为统一格式(YYYY-MM-DD)
    
    Args:
        date_str: 日期字符串，可能是YYYYMMDD或YYYY-MM-DD格式
        
    Returns:
        str: 格式化后的日期字符串(YYYY-MM-DD)
    """
    # 尝试解析YYYYMMDD格式
    if len(date_str) == 8 and date_str.isdigit():
        return f"{date_str[:4]}-{date_str[4:6]}-{date_str[6:8]}"
    
    # 尝试解析YYYY-MM-DD格式
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        pass
    
    # 尝试解析其他可能的格式
    try:
        dt = datetime.strptime(date_str, "%Y%m%d")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        pass
    
    # 如果无法解析，返回原始字符串
    return date_str