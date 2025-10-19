"""
格式转换模块

该模块提供了各种数据格式转换的功能，包括聚宽回测格式、持仓格式等。
"""

import json
import os
from datetime import datetime
from typing import Dict, List, Optional

import numpy as np
import pandas as pd

from .data_loader import load_backtest_data


def export_data_to_csv(
    data: List[Dict],
    output_file: str,
    date_column: str = "date",
    sort_by_date: bool = True
) -> None:
    """
    将数据导出为CSV文件
    
    Args:
        data: 包含数据的字典列表
        output_file: 输出CSV文件路径
        date_column: 日期列名，用于排序
        sort_by_date: 是否按日期排序
        
    Raises:
        ValueError: 输入参数无效
        FileNotFoundError: 输出目录不存在
    """
    # 验证参数
    if not data:
        raise ValueError("数据不能为空")
    
    if not output_file:
        raise ValueError("输出文件路径不能为空")
    
    # 确保输出目录存在
    output_dir = os.path.dirname(output_file)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)
    
    # 转换为DataFrame
    df = pd.DataFrame(data)
    
    # 按日期排序
    if sort_by_date and date_column in df.columns:
        df = df.sort_values(date_column)
    
    # 导出为CSV
    df.to_csv(output_file, index=False, encoding='utf-8')


def generate_hedge_backtest_format(hedge_data: Dict, output_file: Optional[str] = None) -> List[Dict]:
    """
    生成聚宽回测格式的对冲数据
    
    Args:
        hedge_data: 对冲数据
        output_file: 输出文件路径(可选)
        
    Returns:
        List[Dict]: 聚宽回测格式的对冲数据列表
    """
    if "data" not in hedge_data:
        raise ValueError("对冲数据格式错误，缺少data字段")
    
    # 转换为聚宽回测格式
    backtest_format_data = []
    
    for item in hedge_data["data"]:
        date_str = item["date"].replace("-", "")
        
        # 计算累积收益率
        if backtest_format_data:
            prev_cumulative_return = backtest_format_data[-1]["data"]["overallReturn"]["records"][0]["value"]
            daily_return = item["hedge_return"]
            cumulative_return = prev_cumulative_return * (1 + daily_return / 100) - prev_cumulative_return + prev_cumulative_return
        else:
            cumulative_return = 0
        
        backtest_format_data.append({
            "type": "daily_data",
            "date": date_str,
            "data": {
                "benchmark": {
                    "count": 1,
                    "records": [
                        {
                            "timestamp": int(datetime.strptime(item["date"], "%Y-%m-%d").timestamp() * 1000),
                            "date_string": f"{date_str} 16:00:00",
                            "value": cumulative_return
                        }
                    ]
                },
                "gains": {
                    "count": 2,
                    "records": [
                        {
                            "timestamp": int(datetime.strptime(item["date"], "%Y-%m-%d").timestamp() * 1000),
                            "date_string": f"{date_str} 16:00:00",
                            "value": max(0, item["hedge_return"]),
                            "sub_field": "earn"
                        },
                        {
                            "timestamp": int(datetime.strptime(item["date"], "%Y-%m-%d").timestamp() * 1000),
                            "date_string": f"{date_str} 16:00:00",
                            "value": min(0, item["hedge_return"]),
                            "sub_field": "lose"
                        }
                    ]
                },
                "orders": {
                    "count": 2,
                    "records": [
                        {
                            "timestamp": int(datetime.strptime(item["date"], "%Y-%m-%d").timestamp() * 1000),
                            "date_string": f"{date_str} 16:00:00",
                            "value": 0,
                            "sub_field": "buy"
                        },
                        {
                            "timestamp": int(datetime.strptime(item["date"], "%Y-%m-%d").timestamp() * 1000),
                            "date_string": f"{date_str} 16:00:00",
                            "value": 0,
                            "sub_field": "sell"
                        }
                    ]
                },
                "overallReturn": {
                    "count": 1,
                    "records": [
                        {
                            "timestamp": int(datetime.strptime(item["date"], "%Y-%m-%d").timestamp() * 1000),
                            "date_string": f"{date_str} 16:00:00",
                            "value": cumulative_return
                        }
                    ]
                }
            },
            "metadata": hedge_data.get("metadata", {})
        })
    
    # 如果指定了输出文件，则保存结果
    if output_file:
        with open(output_file, 'w', encoding='utf-8') as f:
            for item in backtest_format_data:
                f.write(json.dumps(item, ensure_ascii=False) + '\n')
    
    return backtest_format_data





def generate_hedge_position_format(hedge_data: Dict, output_file: Optional[str] = None) -> Dict:
    """
    生成聚宽持仓格式的对冲数据
    
    Args:
        hedge_data: 对冲数据
        output_file: 输出文件路径(可选)
        
    Returns:
        Dict: 聚宽持仓格式的对冲数据
    """
    if "data" not in hedge_data:
        raise ValueError("对冲数据格式错误，缺少data字段")
    
    # 转换为聚宽持仓格式
    position_format_data = {
        "backtest_id": hedge_data.get("metadata", {}).get("backtest_file", "").split("/")[-1].split(".")[0],
        "balances": []
    }
    
    for item in hedge_data["data"]:
        position_format_data["balances"].append({
            "time": f"{item['date']} 16:00:00",
            "aval_cash": item["hedge_cash"],
            "total_value": item["hedge_total_value"],
            "cash": item["hedge_cash"],
            "net_value": item["hedge_net_value"],
            "position_ratio": item["position_ratio"]
        })
    
    # 如果指定了输出文件，则保存结果
    if output_file:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(position_format_data, f, ensure_ascii=False, indent=2)
    
    return position_format_data