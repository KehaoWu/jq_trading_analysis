"""
对冲数据计算模块

该模块提供了根据聚宽回测数据和指数数据计算对冲数据的功能。
对冲收益率的计算方法为：对冲日收益率 = 回测日收益率 - 对冲持仓比例 * 指数日收益率
"""

import json
import os
from datetime import datetime
from typing import Dict, List, Tuple, Optional, Union

import numpy as np
import pandas as pd

# 导入拆分出去的模块
from .data_loader import load_backtest_data, load_position_data, load_index_data
from .format_converter import (
    generate_hedge_backtest_format,
    generate_hedge_position_format
)
from .returns_calculator import calculate_daily_returns_to_csv, calculate_daily_returns
from .utils import parse_date_string


def calculate_hedge_data(
    backtest_file: str,
    position_file: Optional[str] = None,
    index_file: str = "",
    output_file: Optional[str] = None
) -> Dict:
    """
    计算对冲数据
    
    Args:
        backtest_file: 回测数据文件路径
        position_file: 持仓数据文件路径(可选)
        index_file: 指数数据文件路径
        hedge_ratio: 对冲比例，默认为1.0
        output_file: 输出文件路径(可选)
        
    Returns:
        Dict: 计算得到的对冲数据
        
    Raises:
        ValueError: 输入参数无效
        FileNotFoundError: 文件不存在
    """
    # 验证参数
    if not os.path.exists(backtest_file):
        raise FileNotFoundError(f"回测数据文件不存在: {backtest_file}")
    
    if not os.path.exists(index_file):
        raise FileNotFoundError(f"指数数据文件不存在: {index_file}")
    
    if position_file and not os.path.exists(position_file):
        raise FileNotFoundError(f"持仓数据文件不存在: {position_file}")
    
    # 加载数据
    backtest_data = load_backtest_data(backtest_file)
    index_data = load_index_data(index_file)
    
    # 如果提供了持仓数据文件，则加载持仓数据
    position_data = None
    if position_file:
        position_data = load_position_data(position_file)
    
    # 将回测数据转换为DataFrame，方便处理
    backtest_records = []
    for item in backtest_data:
        date = parse_date_string(item.get('date', ''))
        overall_return = item.get('data', {}).get('overallReturn', {}).get('records', [{}])[0].get('value', 0)
        backtest_records.append({
            'date': date,
            'cumulative_return': overall_return
        })
    
    backtest_df = pd.DataFrame(backtest_records)
    
    # 计算日收益率（使用标准化方法从累积收益率计算）
    backtest_df = backtest_df.sort_values('date')
    
    # 使用returns_calculator模块的标准化方法计算日收益率
    backtest_records_with_daily_returns = calculate_daily_returns(
        backtest_df.to_dict('records'),
        date_column='date',
        cumulative_return_column='cumulative_return'
    )
    
    # 更新DataFrame
    backtest_df = pd.DataFrame(backtest_records_with_daily_returns)
    backtest_df['return'] = backtest_df['daily_return']
    
    # 将指数数据转换为DataFrame
    index_df = pd.DataFrame([
        {
            'date': parse_date_string(item.get('date', '')),
            'return': item.get('pctChg', 0)
        }
        for item in index_data
    ])
    
    # 合并数据
    merged_df = pd.merge(backtest_df, index_df, on='date', how='inner', suffixes=('_backtest', '_index'))
    
    # 如果有持仓数据，则合并持仓数据
    if position_data and 'balances' in position_data:
        position_df = pd.DataFrame([
            {
                'date': parse_date_string(item.get('time', '').split(' ')[0]),
                'position_ratio': item.get('position_ratio', 0),
                'cash': item.get('cash', 0),
                'total_value': item.get('total_value', 0),
                'net_value': item.get('net_value', 0)
            }
            for item in position_data.get('balances', [])
        ])
        merged_df = pd.merge(merged_df, position_df, on='date', how='left')
        
    
    # 计算对冲收益率: 对冲日收益率 = 回测日收益率 - 回测持仓比例 * 指数收益率
    merged_df['hedge_return'] = merged_df['return_backtest'] - merged_df['position_ratio'] * merged_df['return_index']
    
    # 转换为输出格式
    hedge_data = {
        "metadata": {
            "backtest_file": backtest_file,
            "position_file": position_file,
            "index_file": index_file,
            "calculation_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        },
        "data": []
    }
    
    for _, row in merged_df.iterrows():
        # 处理NaN值，确保所有字段都有有效值
        hedge_data["data"].append({
            "date": row['date'],
            "backtest_return": row['return_backtest'],
            "index_return": row['return_index'],
            "position_ratio": row['position_ratio'],
            "hedge_return": row['hedge_return'],
            "cash": row['cash'],
            "total_value": row['total_value'],
            "net_value": row['net_value']
        })
    
    # 如果指定了输出文件，则保存结果
    if output_file:
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(hedge_data, f, ensure_ascii=False, indent=2)
    
    return hedge_data