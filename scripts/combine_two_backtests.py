"""
合并两个回测数据脚本

该脚本用于合并两个不同时间段的回测数据，计算合并后的日收益率和累积收益率。
"""

import json
import os
import sys
from datetime import datetime
from typing import List, Dict, Tuple

import pandas as pd

# 添加项目根目录到系统路径，以便导入libs中的模块
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from libs.data_loader import load_backtest_data
from libs.returns_calculator import calculate_daily_returns, calculate_cumulative_returns


def extract_cumulative_returns(backtest_data: List[Dict]) -> List[Dict]:
    """
    从回测数据中提取累积收益率
    
    Args:
        backtest_data: 回测数据列表
        
    Returns:
        List[Dict]: 包含日期和累积收益率的数据列表
    """
    result = []
    
    for item in backtest_data:
        date = item.get('date')
        if not date:
            continue
            
        # 提取overallReturn中的累积收益率
        overall_return = item.get('data', {}).get('overallReturn', {})
        records = overall_return.get('records', [])
        
        if records:
            cumulative_return = records[0].get('value', 0)
            result.append({
                'date': date,
                'cumulative_return': cumulative_return
            })
    
    # 按日期排序
    result.sort(key=lambda x: x['date'])
    return result


def calculate_daily_returns_from_cumulative(cumulative_data: List[Dict]) -> List[Dict]:
    """
    根据累积收益率计算日收益率
    
    Args:
        cumulative_data: 包含日期和累积收益率的数据列表
        
    Returns:
        List[Dict]: 包含日期、累积收益率和日收益率的数据列表
    """
    if not cumulative_data:
        return []
    
    # 使用已有的calculate_daily_returns函数
    daily_returns_data = calculate_daily_returns(cumulative_data)
    
    return daily_returns_data


def merge_daily_returns(data1: List[Dict], data2: List[Dict]) -> List[Dict]:
    """
    合并两个日收益率数据
    
    Args:
        data1: 第一个日收益率数据列表
        data2: 第二个日收益率数据列表
        
    Returns:
        List[Dict]: 合并后的日收益率数据列表
    """
    # 转换为DataFrame
    df1 = pd.DataFrame(data1)
    df2 = pd.DataFrame(data2)
    
    # 合并两个DataFrame，按日期对齐
    merged_df = pd.concat([df1, df2], ignore_index=True)
    
    # 按日期排序
    merged_df = merged_df.sort_values('date')
    
    # 去重（保留每个日期的最新数据）
    merged_df = merged_df.drop_duplicates(subset=['date'], keep='last')
    
    return merged_df.to_dict('records')


def calculate_merged_cumulative_returns(daily_returns_data: List[Dict]) -> List[Dict]:
    """
    根据合并后的日收益率计算累积收益率
    
    Args:
        daily_returns_data: 合并后的日收益率数据列表
        
    Returns:
        List[Dict]: 包含日期、日收益率和累积收益率的数据列表
    """
    if not daily_returns_data:
        return []
    
    # 提取日收益率列表
    daily_returns = [item.get('daily_return', 0) for item in daily_returns_data]
    
    # 计算累积收益率
    cumulative_values = calculate_cumulative_returns(daily_returns)
    
    # 将累积收益率添加到数据中
    for i, item in enumerate(daily_returns_data):
        item['merged_cumulative_return'] = cumulative_values[i] - 100  # 转换为百分比形式
    
    return daily_returns_data


def save_to_jq_format(data: List[Dict], output_file: str) -> None:
    """
    将合并后的数据保存为聚宽回测数据格式
    
    Args:
        data: 包含日期、日收益率和累积收益率的数据列表
        output_file: 输出文件路径
    """
    # 确保输出目录存在
    output_dir = os.path.dirname(output_file)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)
    
    # 转换为聚宽格式
    jq_data = []
    
    for item in data:
        date = item.get('date')
        cumulative_return = item.get('merged_cumulative_return', 0)
        daily_return = item.get('daily_return', 0)
        
        # 创建聚宽格式的数据
        jq_item = {
            "type": "daily_data",
            "date": date,
            "data": {
                "overallReturn": {
                    "count": 1,
                    "records": [
                        {
                            "timestamp": int(datetime.strptime(date, "%Y%m%d").timestamp() * 1000),
                            "date_string": f"{date} 16:00:00",
                            "value": cumulative_return
                        }
                    ]
                }
            },
            "metadata": {
                "backtest_id": "merged_backtest",
                "backtest_name": "合并回测",
                "download_time": datetime.now().isoformat(),
                "source_note": "由两个回测数据合并而成",
                "data_fields": ["overallReturn"]
            }
        }
        
        jq_data.append(jq_item)
    
    # 写入文件
    with open(output_file, 'w', encoding='utf-8') as f:
        for item in jq_data:
            f.write(json.dumps(item, ensure_ascii=False) + '\n')


def combine_two_backtests(file1: str, file2: str, output_file: str) -> None:
    """
    合并两个回测数据文件
    
    Args:
        file1: 第一个回测数据文件路径
        file2: 第二个回测数据文件路径
        output_file: 输出文件路径
    """
    print(f"开始合并回测数据: {file1} 和 {file2}")
    
    # 加载回测数据
    print("加载回测数据...")
    backtest_data1 = load_backtest_data(file1)
    backtest_data2 = load_backtest_data(file2)
    
    # 提取累积收益率
    print("提取累积收益率...")
    cumulative_data1 = extract_cumulative_returns(backtest_data1)
    cumulative_data2 = extract_cumulative_returns(backtest_data2)
    
    # 计算日收益率
    print("计算日收益率...")
    daily_returns_data1 = calculate_daily_returns_from_cumulative(cumulative_data1)
    daily_returns_data2 = calculate_daily_returns_from_cumulative(cumulative_data2)
    
    # 合并日收益率数据
    print("合并日收益率数据...")
    merged_daily_returns = merge_daily_returns(daily_returns_data1, daily_returns_data2)
    
    # 计算合并后的累积收益率
    print("计算合并后的累积收益率...")
    merged_data = calculate_merged_cumulative_returns(merged_daily_returns)
    
    # 保存为聚宽格式
    print(f"保存合并后的数据到: {output_file}")
    save_to_jq_format(merged_data, output_file)
    
    print("合并完成!")


def main():
    """
    主函数，用于通过main.py调用
    """
    # 示例用法
    file1 = "/home/wukehao/Projects/jq_trading_analysis/data/day1_topk200_200101-250721/模拟盘-T1卖出_349b6bd77de4ad7afbca3e74b13e8876_20090105_20191231_daily.jsonl"
    file2 = "/home/wukehao/Projects/jq_trading_analysis/data/day1_topk200_200101-250721/模拟盘-T1卖出_e73d3c750a6b305b220c0da6fdab5363_20200102_20250721_daily.jsonl"
    output_file = "/home/wukehao/Projects/jq_trading_analysis/output/merged_backtest.jsonl"
    
    combine_two_backtests(file1, file2, output_file)


if __name__ == "__main__":
    # 示例用法
    file1 = "/home/wukehao/Projects/jq_trading_analysis/data/day1_topk200_200101-250721/模拟盘-T1卖出_349b6bd77de4ad7afbca3e74b13e8876_20090105_20191231_daily.jsonl"
    file2 = "/home/wukehao/Projects/jq_trading_analysis/data/day1_topk200_200101-250721/模拟盘-T1卖出_e73d3c750a6b305b220c0da6fdab5363_20200102_20250721_daily.jsonl"
    output_file = "/home/wukehao/Projects/jq_trading_analysis/output/merged_backtest.jsonl"
    
    combine_two_backtests(file1, file2, output_file)