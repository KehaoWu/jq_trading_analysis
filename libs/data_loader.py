"""
数据加载模块

该模块提供了加载各种数据格式的功能，包括聚宽回测数据、持仓数据和指数数据。
"""

import json
import os
from typing import Dict, List


def load_backtest_data(file_path: str) -> List[Dict]:
    """
    加载聚宽回测数据
    
    Args:
        file_path: 回测数据文件路径(JSONL格式)
        
    Returns:
        List[Dict]: 解析后的回测数据列表
        
    Raises:
        FileNotFoundError: 文件不存在
        json.JSONDecodeError: JSON格式错误
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"回测数据文件不存在: {file_path}")
    
    data = []
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    item = json.loads(line)
                    # 只处理daily_data类型的数据
                    if item.get('type') == 'daily_data':
                        data.append(item)
                except json.JSONDecodeError as e:
                    print(f"警告: 跳过无效的JSON行: {e}")
                    continue
    
    return data


def load_position_data(file_path: str) -> Dict:
    """
    加载持仓数据
    
    Args:
        file_path: 持仓数据文件路径
        
    Returns:
        Dict: 解析后的持仓数据
        
    Raises:
        FileNotFoundError: 文件不存在
        json.JSONDecodeError: JSON格式错误
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"持仓数据文件不存在: {file_path}")
    
    with open(file_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    return data


def load_index_data(file_path: str) -> List[Dict]:
    """
    加载指数数据
    
    Args:
        file_path: 指数数据文件路径(JSONL格式)
        
    Returns:
        List[Dict]: 解析后的指数数据列表
        
    Raises:
        FileNotFoundError: 文件不存在
        json.JSONDecodeError: JSON格式错误
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"指数数据文件不存在: {file_path}")
    
    data = []
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line:
                data.append(json.loads(line))
    
    return data