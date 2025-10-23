#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
金融指标计算模块
实现年化收益率、最大回撤、夏普比率、最长回撤修复期等指标计算

主要功能：
- 数据加载和日收益率提取
- 年化收益率计算
- 最大回撤计算
- 夏普比率计算
- 最长回撤修复期计算
- 综合指标计算
"""

import numpy as np
import pandas as pd
from typing import Dict, List, Tuple, Optional, Union
from datetime import datetime, timedelta
import json
import warnings


def load_backtest_data(file_path: str) -> List[Dict]:
    """
    加载回测数据文件
    
    Args:
        file_path: 回测数据文件路径
        
    Returns:
        回测数据列表
    """
    data = []
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            if line.strip():
                data.append(json.loads(line.strip()))
    return data


def extract_daily_returns(backtest_data: List[Dict]) -> pd.DataFrame:
    """
    从回测数据中提取日收益率
    
    重要说明：
    - overallReturn字段包含的是累积收益率，不是日收益率
    - 需要通过相邻日期的累积收益率差值计算日收益率
    - 日收益率 = (今日累积收益率 + 1) / (昨日累积收益率 + 1) - 1
    
    Args:
        backtest_data: 回测数据列表，每个元素包含daily_data类型的记录
        
    Returns:
        包含日期、策略日收益率、基准日收益率的DataFrame
    """
    daily_data = []
    
    for record in backtest_data:
        if record.get('type') == 'daily_data':
            date = record['date']
            
            # 提取策略累积收益率
            overall_return = record['data'].get('overallReturn', {})
            if overall_return.get('records'):
                strategy_cumulative_return = overall_return['records'][0]['value']
            else:
                strategy_cumulative_return = 0.0
            
            # 提取基准累积收益率
            benchmark = record['data'].get('benchmark', {})
            if benchmark.get('records'):
                benchmark_cumulative_return = benchmark['records'][0]['value']
            else:
                benchmark_cumulative_return = 0.0
            
            daily_data.append({
                'date': pd.to_datetime(date, format='%Y%m%d'),
                'strategy_cumulative_return': strategy_cumulative_return / 100.0,  # 转换为小数
                'benchmark_cumulative_return': benchmark_cumulative_return / 100.0
            })
    
    df = pd.DataFrame(daily_data)
    df = df.sort_values('date').reset_index(drop=True)
    
    # 计算日收益率：从累积收益率计算日收益率
    # 日收益率 = (今日累积收益率 + 1) / (昨日累积收益率 + 1) - 1
    df['strategy_return'] = 0.0
    df['benchmark_return'] = 0.0
    
    for i in range(1, len(df)):
        # 策略日收益率
        prev_cum_return = df.iloc[i-1]['strategy_cumulative_return']
        curr_cum_return = df.iloc[i]['strategy_cumulative_return']
        # 更严格的检查：避免除零、NaN值和无效值
        if (pd.notna(prev_cum_return) and pd.notna(curr_cum_return) and 
            np.isfinite(prev_cum_return) and np.isfinite(curr_cum_return) and 
            prev_cum_return > -0.999999):  # 避免接近-100%的情况
            daily_return = (1 + curr_cum_return) / (1 + prev_cum_return) - 1
            if pd.notna(daily_return) and np.isfinite(daily_return):
                df.iloc[i, df.columns.get_loc('strategy_return')] = daily_return
        
        # 基准日收益率
        prev_bench_return = df.iloc[i-1]['benchmark_cumulative_return']
        curr_bench_return = df.iloc[i]['benchmark_cumulative_return']
        # 更严格的检查：避免除零、NaN值和无效值
        if (pd.notna(prev_bench_return) and pd.notna(curr_bench_return) and 
            np.isfinite(prev_bench_return) and np.isfinite(curr_bench_return) and 
            prev_bench_return > -0.999999):  # 避免接近-100%的情况
            daily_return = (1 + curr_bench_return) / (1 + prev_bench_return) - 1
            if pd.notna(daily_return) and np.isfinite(daily_return):
                df.iloc[i, df.columns.get_loc('benchmark_return')] = daily_return
    
    # 第一天的日收益率就是累积收益率本身
    if len(df) > 0:
        df.iloc[0, df.columns.get_loc('strategy_return')] = df.iloc[0]['strategy_cumulative_return']
        df.iloc[0, df.columns.get_loc('benchmark_return')] = df.iloc[0]['benchmark_cumulative_return']
    
    # 只保留需要的列
    return df[['date', 'strategy_return', 'benchmark_return']]


def calculate_cumulative_returns(returns: pd.Series) -> pd.Series:
    """
    计算累积收益率
    
    Args:
        returns: 日收益率序列
        
    Returns:
        累积收益率序列
    """
    if len(returns) == 0:
        return pd.Series(dtype=float)
    
    # 过滤无效值
    valid_returns = returns.fillna(0)
    return (1 + valid_returns).cumprod() - 1


def calculate_annualized_return(returns: pd.Series, trading_days_per_year: int = 252) -> float:
    """
    计算年化收益率
    
    Args:
        returns: 日收益率序列
        trading_days_per_year: 每年交易日数，默认252天
        
    Returns:
        年化收益率
    """
    if len(returns) == 0:
        return 0.0
    
    # 限制收益率范围，避免数值溢出
    clipped_returns = returns.clip(-0.99, 10)
    
    # 使用累积收益率计算，避免直接相乘导致的溢出
    cumulative_return = (1 + clipped_returns).prod() - 1
    years = len(returns) / trading_days_per_year
    
    if years <= 0:
        return 0.0
    
    # 防止负数或零的幂运算
    if cumulative_return <= -1:
        return -1.0  # 完全亏损
    
    try:
        annualized_return = (1 + cumulative_return) ** (1 / years) - 1
        # 检查结果是否为有限数
        if not np.isfinite(annualized_return):
            return 0.0
        return annualized_return
    except (OverflowError, ValueError):
        return 0.0


def calculate_max_drawdown(cumulative_returns: pd.Series) -> Dict:
    """
    计算最大回撤及相关信息（修正版）
    
    最大回撤定义：任意峰值点到其后续最低点之间的最大跌幅
    不一定是全局最高点的回撤，而是所有可能回撤中的最大值
    
    使用高效的O(n)算法：
    1. 维护当前的峰值点
    2. 计算每个点相对于当前峰值的回撤
    3. 当遇到新的峰值时，更新峰值点
    
    Args:
        cumulative_returns: 累积收益率序列
        
    Returns:
        包含最大回撤信息的字典
    """
    if len(cumulative_returns) == 0:
        return {
            'max_drawdown': 0.0,
            'drawdown_start_date': None,
            'drawdown_end_date': None,
            'drawdown_start_index': None,
            'drawdown_end_index': None
        }
    
    # 转换为净值序列
    cumulative_nav = 1 + cumulative_returns
    
    # 初始化变量
    max_drawdown = 0.0
    max_drawdown_start_idx = None
    max_drawdown_end_idx = None
    max_drawdown_start_loc = None
    max_drawdown_end_loc = None
    
    # 当前峰值相关变量
    current_peak = cumulative_nav.iloc[0]
    current_peak_idx = cumulative_nav.index[0]
    current_peak_loc = 0
    
    # 遍历所有点
    for i in range(len(cumulative_nav)):
        current_value = cumulative_nav.iloc[i]
        current_idx = cumulative_nav.index[i]
        
        # 检查当前值是否有效
        if pd.isna(current_value) or not np.isfinite(current_value):
            continue
            
        # 如果当前值创新高，更新峰值
        if current_value > current_peak:
            current_peak = current_value
            current_peak_idx = current_idx
            current_peak_loc = i
        else:
            # 计算从当前峰值到当前点的回撤
            if current_peak > 0 and pd.notna(current_peak) and np.isfinite(current_peak):  # 避免除零和无效值
                drawdown = (current_value - current_peak) / current_peak
                
                # 检查回撤值是否有效
                if pd.notna(drawdown) and np.isfinite(drawdown):
                    # 如果这个回撤更大（更负），更新最大回撤
                    if drawdown < max_drawdown:
                        max_drawdown = drawdown
                        max_drawdown_start_idx = current_peak_idx
                        max_drawdown_end_idx = current_idx
                        max_drawdown_start_loc = current_peak_loc
                        max_drawdown_end_loc = i
    
    return {
        'max_drawdown': abs(max_drawdown),
        'drawdown_start_date': max_drawdown_start_idx,
        'drawdown_end_date': max_drawdown_end_idx,
        'drawdown_start_index': max_drawdown_start_loc,
        'drawdown_end_index': max_drawdown_end_loc
    }


def calculate_sharpe_ratio(returns: pd.Series, risk_free_rate: float = 0.03, 
                          trading_days_per_year: int = 252) -> float:
    """
    计算夏普比率
    
    Args:
        returns: 日收益率序列
        risk_free_rate: 无风险利率，默认3%
        trading_days_per_year: 每年交易日数，默认252天
        
    Returns:
        夏普比率
    """
    if len(returns) == 0 or returns.std() == 0:
        return 0.0
    
    excess_returns = returns - risk_free_rate / trading_days_per_year
    sharpe_ratio = excess_returns.mean() / returns.std() * np.sqrt(trading_days_per_year)
    return sharpe_ratio


def calculate_longest_drawdown_recovery(cumulative_returns: pd.Series) -> Dict:
    """
    计算最长回撤修复期
    根据需求：修复期是指从最大回撤的最低点开始，到净值重新回到回撤前的最高点为止的时间，单位为天
    
    Args:
        cumulative_returns: 累积收益率序列
        
    Returns:
        包含最长回撤修复期信息的字典
    """
    if len(cumulative_returns) == 0:
        return {
            'longest_recovery_days': 0,
            'recovery_start_date': None,
            'recovery_end_date': None,
            'recovery_start_index': None,
            'recovery_end_index': None
        }
    
    # 计算累积净值，避免数值溢出
    cumulative_nav = 1 + cumulative_returns
    
    # 计算历史最高点
    rolling_max = cumulative_nav.expanding().max()
    
    # 计算回撤
    drawdowns = (cumulative_nav - rolling_max) / rolling_max
    
    # 找到所有回撤期间（从开始下跌到完全恢复）
    longest_recovery = 0
    longest_start_idx = None
    longest_end_idx = None
    
    # 遍历每个可能的回撤起点
    for i in range(len(cumulative_nav)):
        # 如果当前点是一个新的历史高点，检查后续的回撤和恢复
        if i == 0 or cumulative_nav.iloc[i] >= rolling_max.iloc[i-1]:
            peak_value = cumulative_nav.iloc[i]
            peak_idx = i
            
            # 寻找从这个峰值开始的最大回撤点
            max_drawdown_idx = None
            max_drawdown_value = 0
            
            for j in range(i + 1, len(cumulative_nav)):
                current_drawdown = (cumulative_nav.iloc[j] - peak_value) / peak_value
                if current_drawdown < max_drawdown_value:
                    max_drawdown_value = current_drawdown
                    max_drawdown_idx = j
            
            # 如果找到了回撤，寻找恢复点
            if max_drawdown_idx is not None and max_drawdown_value < -0.001:  # 至少0.1%的回撤
                # 从回撤最低点开始寻找恢复到峰值的点
                for k in range(max_drawdown_idx + 1, len(cumulative_nav)):
                    if cumulative_nav.iloc[k] >= peak_value * 0.999:  # 允许0.1%的误差
                        # 找到恢复点，计算修复期
                        recovery_days = k - max_drawdown_idx
                        
                        if recovery_days > longest_recovery:
                            longest_recovery = recovery_days
                            longest_start_idx = max_drawdown_idx
                            longest_end_idx = k
                        break
    
    return {
        'longest_recovery_days': longest_recovery,
        'recovery_start_date': cumulative_returns.index[longest_start_idx] if longest_start_idx is not None else None,
        'recovery_end_date': cumulative_returns.index[longest_end_idx] if longest_end_idx is not None else None,
        'recovery_start_index': longest_start_idx,
        'recovery_end_index': longest_end_idx
    }


def calculate_all_metrics(returns_df: pd.DataFrame, 
                         start_date: Optional[str] = None,
                         end_date: Optional[str] = None) -> Dict:
    """
    计算所有指标
    
    Args:
        returns_df: 包含日期、策略收益率、基准收益率的DataFrame
        start_date: 开始日期，格式为'YYYY-MM-DD'
        end_date: 结束日期，格式为'YYYY-MM-DD'
        
    Returns:
        包含所有指标的字典
    """
    
    # 过滤时间范围
    if start_date or end_date:
        mask = pd.Series(True, index=returns_df.index)
        if start_date:
            mask &= returns_df['date'] >= pd.to_datetime(start_date)
        if end_date:
            mask &= returns_df['date'] <= pd.to_datetime(end_date)
        filtered_df = returns_df[mask].copy()
    else:
        filtered_df = returns_df.copy()
    
    if len(filtered_df) == 0:
        return {
            'period_start': start_date,
            'period_end': end_date,
            'trading_days': 0,
            'annualized_return': 0.0,
            'total_return': 0.0,
            'max_drawdown': 0.0,
            'sharpe_ratio': 0.0,
            'longest_recovery_days': 0,
            'drawdown_start_date': None,
            'drawdown_end_date': None,
            'recovery_start_date': None,
            'recovery_end_date': None,
            'benchmark_annualized_return': 0.0,
            'benchmark_total_return': 0.0,
            'excess_return': 0.0
        }
    
    # 设置日期为索引
    filtered_df = filtered_df.set_index('date')
    
    strategy_returns = filtered_df['strategy_return']
    benchmark_returns = filtered_df['benchmark_return']
    
    # 计算累积收益率
    strategy_cumulative = calculate_cumulative_returns(strategy_returns)
    benchmark_cumulative = calculate_cumulative_returns(benchmark_returns)
    
    # 计算各项指标
    annualized_return = calculate_annualized_return(strategy_returns)
    total_return = strategy_cumulative.iloc[-1] if len(strategy_cumulative) > 0 else 0.0
    
    max_drawdown_info = calculate_max_drawdown(strategy_cumulative)
    sharpe_ratio = calculate_sharpe_ratio(strategy_returns)
    recovery_info = calculate_longest_drawdown_recovery(strategy_cumulative)
    
    # 基准指标
    benchmark_annualized = calculate_annualized_return(benchmark_returns)
    benchmark_total = benchmark_cumulative.iloc[-1] if len(benchmark_cumulative) > 0 else 0.0
    
    return {
        'period_start': filtered_df.index[0].strftime('%Y-%m-%d') if len(filtered_df) > 0 else start_date,
        'period_end': filtered_df.index[-1].strftime('%Y-%m-%d') if len(filtered_df) > 0 else end_date,
        'trading_days': len(filtered_df),
        'annualized_return': annualized_return,
        'total_return': total_return,
        'max_drawdown': max_drawdown_info['max_drawdown'],
        'sharpe_ratio': sharpe_ratio,
        'longest_recovery_days': recovery_info['longest_recovery_days'],
        'drawdown_start_date': max_drawdown_info['drawdown_start_date'].strftime('%Y-%m-%d') if max_drawdown_info['drawdown_start_date'] is not None else None,
        'drawdown_end_date': max_drawdown_info['drawdown_end_date'].strftime('%Y-%m-%d') if max_drawdown_info['drawdown_end_date'] is not None else None,
        'recovery_start_date': recovery_info['recovery_start_date'].strftime('%Y-%m-%d') if recovery_info['recovery_start_date'] is not None else None,
        'recovery_end_date': recovery_info['recovery_end_date'].strftime('%Y-%m-%d') if recovery_info['recovery_end_date'] is not None else None,
        'benchmark_annualized_return': benchmark_annualized,
        'benchmark_total_return': benchmark_total,
        'excess_return': annualized_return - benchmark_annualized
    }


# 便捷函数
def calculate_metrics_from_file(file_path: str, 
                               start_date: Optional[str] = None,
                               end_date: Optional[str] = None,
                               trading_days_per_year: int = 252,
                               risk_free_rate: float = 0.03) -> Dict:
    """
    从文件直接计算指标的便捷函数
    
    Args:
        file_path: 回测数据文件路径
        start_date: 开始日期，格式为'YYYY-MM-DD'
        end_date: 结束日期，格式为'YYYY-MM-DD'
        trading_days_per_year: 每年交易日数，默认252天
        risk_free_rate: 无风险利率，默认3%
        
    Returns:
        包含所有指标的字典
    """
    backtest_data = load_backtest_data(file_path)
    returns_df = extract_daily_returns(backtest_data)
    return calculate_all_metrics(returns_df, start_date, end_date)


def print_metrics_report(file_path: str, 
                        start_date: Optional[str] = None,
                        end_date: Optional[str] = None,
                        trading_days_per_year: int = 252,
                        risk_free_rate: float = 0.03) -> None:
    """
    打印指标报告的便捷函数
    
    Args:
        file_path: 回测数据文件路径
        start_date: 开始日期，格式为'YYYY-MM-DD'
        end_date: 结束日期，格式为'YYYY-MM-DD'
        trading_days_per_year: 每年交易日数，默认252天
        risk_free_rate: 无风险利率，默认3%
    """
    metrics = calculate_metrics_from_file(file_path, start_date, end_date, trading_days_per_year, risk_free_rate)
    
    report = f"""
=== 策略表现分析报告 ===
分析期间: {metrics['period_start']} 至 {metrics['period_end']}
交易日数: {metrics['trading_days']} 天

=== 收益指标 ===
年化收益率: {metrics['annualized_return']:.2%}
总收益率: {metrics['total_return']:.2%}
基准年化收益率: {metrics['benchmark_annualized_return']:.2%}
基准总收益率: {metrics['benchmark_total_return']:.2%}
超额收益: {metrics['excess_return']:.2%}

=== 风险指标 ===
最大回撤: {metrics['max_drawdown']:.2%}
回撤开始日期: {metrics['drawdown_start_date'] or 'N/A'}
回撤结束日期: {metrics['drawdown_end_date'] or 'N/A'}

=== 风险调整收益指标 ===
夏普比率: {metrics['sharpe_ratio']:.4f}

=== 回撤修复指标 ===
最长回撤修复期: {metrics['longest_recovery_days']} 天
修复开始日期: {metrics['recovery_start_date'] or 'N/A'}
修复结束日期: {metrics['recovery_end_date'] or 'N/A'}
"""
    print(report)