"""
数据加载模块

该模块提供了加载各种数据格式的功能，包括聚宽回测数据、持仓数据和指数数据。
"""

import json
import os
import re
from typing import Dict, List
from datetime import datetime


def load_backtest_data(file_path: str) -> List[Dict]:
    """
    加载聚宽回测数据
    
    Args:
        file_path: 回测数据文件路径（支持JSONL和新的JSON结果文件）
        
    Returns:
        List[Dict]: 解析后的回测数据列表
        
    Raises:
        FileNotFoundError: 文件不存在
        json.JSONDecodeError: JSON格式错误
    """
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"回测数据文件不存在: {file_path}")
    
    # 根据文件扩展名与内容格式进行解析
    if file_path.endswith('.json'):
        # 新的JSON结果文件：包含顶层键 'results'
        with open(file_path, 'r', encoding='utf-8') as f:
            obj = json.load(f)
        
        items: List[Dict] = []
        # 兼容两种结构：dict包含results 或 list直接是记录
        records = []
        if isinstance(obj, dict) and 'results' in obj:
            records = obj.get('results', [])
        elif isinstance(obj, list):
            records = obj
        
        for rec in records:
            # 提取日期：time 格式为 'YYYY-MM-DD HH:MM:SS'
            time_str = rec.get('time', '')
            date_str = time_str.split(' ')[0] if time_str else ''
            date_key = date_str.replace('-', '') if date_str else ''
            
            # 新格式中 returns 是累积收益率（未乘以100%），需要转换为百分比
            returns_val = rec.get('returns', 0.0)  # 累积收益率（小数形式）
            cumulative_percent = returns_val * 100.0
            
            # 构造与旧JSONL一致的 daily_data 结构，供下游统一处理
            try:
                ts = int(datetime.strptime(date_str, "%Y-%m-%d").timestamp() * 1000)
            except Exception:
                # 如果日期解析失败，跳过该记录
                continue
            
            items.append({
                "type": "daily_data",
                "date": date_key,
                "data": {
                    "overallReturn": {
                        "count": 1,
                        "records": [
                            {
                                "timestamp": ts,
                                "date_string": f"{date_key} 16:00:00",
                                "value": cumulative_percent
                            }
                        ]
                    }
                }
            })
        
        return items
    else:
        # 默认按照JSONL逐行解析
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
    
    # 首先尝试正常解析为JSON
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data
    except json.JSONDecodeError:
        # 当文件内容损坏或不完整时，尝试进行容错解析：
        # 目标是尽可能恢复 balances 列表，忽略无法解析的条目。
        print(f"警告: 持仓数据JSON损坏，已启用容错解析: {file_path}")
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                lines = f.readlines()
        except Exception:
            # 无法读取文件，返回空结构
            return {"balances": []}

        content = ''.join(lines)
        result: Dict = {"balances": []}

        # 尝试提取 backtest_id（如果存在）
        m = re.search(r'"backtest_id"\s*:\s*"([^"]+)"', content)
        if m:
            result["backtest_id"] = m.group(1)

        # 寻找 balances 数组的起始位置
        start_idx = -1
        for i, line in enumerate(lines):
            if '"balances"' in line:
                start_idx = i
                break

        if start_idx == -1:
            # 未找到 balances，返回空
            return result

        # 找到 '[' 的位置
        array_start = -1
        for j in range(start_idx, len(lines)):
            if '[' in lines[j]:
                array_start = j + 1
                break
        if array_start == -1:
            return result

        # 逐条对象解析：用简单的括号计数恢复每个对象
        brace_level = 0
        current_chunk: list[str] = []

        for k in range(array_start, len(lines)):
            line = lines[k]
            # 当不在对象内且遇到数组结束 ']' 时退出
            if brace_level == 0 and ']' in line:
                break

            # 更新括号层级并收集行
            open_count = line.count('{')
            close_count = line.count('}')

            brace_level += open_count
            current_chunk.append(line)
            brace_level -= close_count

            # 完成一个对象
            if brace_level == 0 and any('{' in s for s in current_chunk):
                chunk_str = ''.join(current_chunk).strip()
                # 去掉可能的尾部逗号
                if chunk_str.endswith(','):
                    chunk_str = chunk_str[:-1]
                try:
                    obj = json.loads(chunk_str)
                    # 基本校验：必须包含 time 与 position_ratio 字段（允许缺失时也加入）
                    result["balances"].append(obj)
                except Exception:
                    # 跳过无法解析的对象
                    pass
                current_chunk = []

        return result


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