"""
收益率计算模块

该模块提供了根据日收益率计算累积收益率的功能，以及将日收益率导出为CSV的功能。"""

import pandas as pd
from typing import List, Dict, Optional, Union, Tuple
import os
import numpy as np
from datetime import datetime

# 时间区间定义 (开始日期, 结束日期)
TIME_INTERVALS = {
    # 牛市期间
    "13-15年牛市": [
        ("2013-06-28", "2015-06-12"),  # 牛市期间
    ],
    
    # 熊市期间
    "15年熊市": [
        ("2015-06-12", "2015-09-18"),  # 熊市期间
    ],
    
    # 熔断区间
    "15-16年熔断": [
        ("2015-12-28", "2016-01-28"),  # 熔断区间
    ],
    
    # 大票风格区间
    "17-18年大票": [
        ("2017-01-01", "2018-01-25"),  # 大票风格区间
    ],
    
    # 抱团风格区间
    "20-21年抱团": [
        ("2020-07-10", "2021-02-18"),  # 抱团风格区间
    ],
}


def filter_data_by_time_intervals(
    dates: List[str], 
    daily_returns: List[float], 
    cumulative_returns: List[float],
    interval_name: str
) -> Tuple[List[str], List[float], List[float]]:
    """
    根据时间区间过滤数据
    
    Args:
        dates: 日期列表
        daily_returns: 日收益率列表
        cumulative_returns: 累积收益率列表
        interval_name: 时间区间名称
        
    Returns:
        Tuple[List[str], List[float], List[float]]: 过滤后的日期、日收益率、累积收益率
    """
    if interval_name not in TIME_INTERVALS:
        return [], [], []
    
    filtered_dates = []
    filtered_daily_returns = []
    filtered_cumulative_returns = []
    
    # 获取时间区间
    intervals = TIME_INTERVALS[interval_name]
    
    for i, date_str in enumerate(dates):
        # 统一日期格式处理
        if len(date_str) == 8 and date_str.isdigit():  # YYYYMMDD格式
            try:
                date_obj = datetime.strptime(date_str, '%Y%m%d')
            except:
                continue
        elif len(date_str) == 10 and '-' in date_str:  # YYYY-MM-DD格式
            try:
                date_obj = datetime.strptime(date_str, '%Y-%m-%d')
            except:
                continue
        else:
            continue
        
        # 检查是否在任何一个时间区间内
        for start_date_str, end_date_str in intervals:
            start_date = datetime.strptime(start_date_str, '%Y-%m-%d')
            end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
            
            if start_date <= date_obj <= end_date:
                filtered_dates.append(date_str)
                filtered_daily_returns.append(daily_returns[i])
                filtered_cumulative_returns.append(cumulative_returns[i])
                break
    
    return filtered_dates, filtered_daily_returns, filtered_cumulative_returns


def calculate_daily_returns(
    data: List[Dict],
    date_column: str = "date",
    cumulative_return_column: str = "cumulative_return"
) -> List[Dict]:
    """
    根据累积收益率计算日收益率
    
    Args:
        data: 包含日期和累积收益率的数据列表
        date_column: 日期列名
        cumulative_return_column: 累积收益率列名
        
    Returns:
        List[Dict]: 包含日收益率的数据列表
        
    Raises:
        ValueError: 输入参数无效
        
    Example:
        >>> data = [
        ...     {"date": "2023-01-01", "cumulative_return": 0.88},
        ...     {"date": "2023-01-02", "cumulative_return": 1.41},
        ...     {"date": "2023-01-03", "cumulative_return": 1.87}
        ... ]
        >>> result = calculate_daily_returns(data)
        >>> result[0]["daily_return"]
        0.0
        >>> round(result[1]["daily_return"], 2)
        28.19
    """
    # 验证参数
    if not data:
        raise ValueError("数据不能为空")
    
    # 转换为DataFrame
    df = pd.DataFrame(data)
    
    # 确保必要的列存在
    if date_column not in df.columns:
        raise ValueError(f"数据中缺少日期列: {date_column}")
    
    if cumulative_return_column not in df.columns:
        raise ValueError(f"数据中缺少累积收益率列: {cumulative_return_column}")
    
    # 按日期排序
    df = df.sort_values(date_column)
    
    # 计算日收益率
    daily_returns = []
    for i in range(len(df)):
        if i == 0:
            # 第一天的日收益率为0
            daily_return = 0.0
        else:
            # 计算日收益率 = [ (1 + 累积收益率Tₙ) / (1 + 累积收益率Tₙ₋₁) ] - 1
            # 注意：累积收益率已经是百分比形式，需要转换为小数进行计算
            current_cum_return = df.iloc[i][cumulative_return_column] / 100.0  # 转换为小数
            prev_cum_return = df.iloc[i-1][cumulative_return_column] / 100.0  # 转换为小数
            
            if prev_cum_return != -1.0:  # 避免除以0
                daily_return = ((1 + current_cum_return) / (1 + prev_cum_return) - 1) * 100  # 转换回百分比
            else:
                daily_return = 0.0
        
        daily_returns.append(daily_return)
    
    # 添加日收益率列
    df["daily_return"] = daily_returns
    
    # 转换回字典列表
    return df.to_dict('records')


def calculate_daily_returns_to_csv(
    data: List[Dict],
    output_file: str,
    date_column: str = "date",
    return_column: str = "hedge_return"
) -> None:
    """
    将日收益率数据导出为CSV文件
    
    Args:
        data: 包含日期和收益率的数据列表
        output_file: 输出CSV文件路径
        date_column: 日期列名
        return_column: 收益率列名
        
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
    
    # 转换为DataFrame进行验证
    df = pd.DataFrame(data)
    
    # 确保必要的列存在
    if date_column not in df.columns:
        raise ValueError(f"数据中缺少日期列: {date_column}")
    
    if return_column not in df.columns:
        raise ValueError(f"数据中缺少收益率列: {return_column}")
    
    # 按日期排序
    df = df.sort_values(date_column)
    
    # 导出为CSV
    df.to_csv(output_file, index=False, encoding='utf-8')


def calculate_cumulative_returns(
    daily_returns: List[float],
    initial_value: float = 100
) -> List[float]:
    """
    根据日收益率计算累积收益率
    
    Args:
        daily_returns: 日收益率列表（百分比形式，如1.5表示1.5%）
        initial_value: 初始值，默认为100
        
    Returns:
        List[float]: 累积收益率列表
        
    Example:
        >>> daily_returns = [1.0, -0.5, 2.0, 1.5]
        >>> calculate_cumulative_returns(daily_returns)
        [101.0, 100.495, 102.5049, 104.0425]
    """
    if not daily_returns:
        return []
    
    cumulative_values = [initial_value]
    
    for daily_return in daily_returns:
        # 将百分比转换为小数
        return_rate = daily_return / 100.0
        # 计算累积值
        new_value = cumulative_values[-1] * (1 + return_rate)
        cumulative_values.append(new_value)
    
    cumulative_values = [value - initial_value for value in cumulative_values]
    
    # 返回除初始值外的累积值
    return cumulative_values[1:]


def _parse_date_string(date_str: str) -> datetime:
    """
    解析日期字符串，支持多种格式
    
    Args:
        date_str: 日期字符串，支持YYYYMMDD或YYYY-MM-DD格式
        
    Returns:
        datetime: 解析后的日期对象
        
    Raises:
        ValueError: 如果日期格式不支持
    """
    from datetime import datetime
    
    # 尝试解析YYYYMMDD格式
    try:
        return datetime.strptime(date_str, '%Y%m%d')
    except ValueError:
        pass
    
    # 尝试解析YYYY-MM-DD格式
    try:
        return datetime.strptime(date_str, '%Y-%m-%d')
    except ValueError:
        pass
    
    # 如果都失败，抛出异常
    raise ValueError(f"不支持的日期格式: {date_str}，支持的格式: YYYYMMDD 或 YYYY-MM-DD")


def calculate_annualized_return(
    daily_returns: List[float],
    trading_days: int = 252,
    start_date: str = None,
    end_date: str = None
) -> float:
    """
    计算年化收益率
    
    使用复利基础的精确计算方法：
    1. 计算总收益率：期末资产 / 期初资产
    2. 计算日化收益率：总收益率 ^ (1/天数) - 1
    3. 计算年化收益率：(1 + 日化收益率) ^ 365 - 1
    
    Args:
        daily_returns: 日收益率列表（百分比形式，如1.5表示1.5%）
        trading_days: 一年中的交易日数量，默认为252
        start_date: 开始日期（YYYYMMDD或YYYY-MM-DD格式），用于计算实际年数
        end_date: 结束日期（YYYYMMDD或YYYY-MM-DD格式），用于计算实际年数
        
    Returns:
        float: 年化收益率（百分比形式）
        
    Example:
        >>> daily_returns = [0.1] * 252  # 每日0.1%收益
        >>> round(calculate_annualized_return(daily_returns), 2)
        28.58
    """
    if not daily_returns:
        return 0.0
    
    # 直接计算累积价值
    period_start_value = 100.0  # 期初资产价值（基准值）
    current_value = period_start_value
    
    for daily_return in daily_returns:
        # 将百分比转换为小数
        return_rate = daily_return / 100.0
        # 计算累积值
        current_value = current_value * (1 + return_rate)
    
    # 计算总收益率（期末资产 / 期初资产）
    period_end_value = current_value  # 期末资产价值
    total_return_ratio = period_end_value / period_start_value  # 总收益率比值
    
    # 计算实际天数
    actual_days = len(daily_returns)
    if start_date and end_date:
        # 使用实际日期计算天数
        try:
            start_dt = _parse_date_string(start_date)
            end_dt = _parse_date_string(end_date)
            actual_days = (end_dt - start_dt).days
            if actual_days <= 0:
                actual_days = len(daily_returns)
        except (ValueError, Exception):
            # 如果日期解析失败，使用交易日数量
            actual_days = len(daily_returns)
    
    if actual_days == 0:
        return 0.0
    
    # 处理负收益率或零收益率的情况
    if total_return_ratio <= 0:
        # 对于极端负收益率，使用绝对值计算后取负值
        abs_ratio = abs(total_return_ratio)
        if abs_ratio == 0:
            return -100.0  # 完全亏损
        daily_return_rate = abs_ratio ** (1 / actual_days) - 1
        annualized_return = (1 + daily_return_rate) ** 365 - 1
        return -annualized_return * 100  # 返回负值
    
    # 正常情况：计算日化收益率（复利基础）
    daily_return_rate = total_return_ratio ** (1 / actual_days) - 1
    
    # 计算年化收益率：(1 + 日化收益率) ^ 365 - 1
    annualized_return = (1 + daily_return_rate) ** 365 - 1
    
    # 处理复数结果
    if isinstance(annualized_return, complex):
        annualized_return = annualized_return.real
    
    # 转换为百分比
    return annualized_return * 100


def calculate_sharpe_ratio(
    daily_returns: List[float],
    risk_free_rate: float = 3.0,
    trading_days: int = 252,
    start_date: str = None,
    end_date: str = None
) -> float:
    """
    计算夏普比率
    
    Args:
        daily_returns: 日收益率列表（百分比形式，如1.5表示1.5%）
        risk_free_rate: 无风险利率（年化，百分比形式），默认为3.0%
        trading_days: 一年中的交易日数量，默认为252
        start_date: 开始日期（YYYYMMDD或YYYY-MM-DD格式），用于计算实际年数
        end_date: 结束日期（YYYYMMDD或YYYY-MM-DD格式），用于计算实际年数
        
    Returns:
        float: 夏普比率
        
    Example:
        >>> daily_returns = [0.1] * 252  # 每日0.1%收益
        >>> round(calculate_sharpe_ratio(daily_returns), 2)
        1.06
    """
    if not daily_returns:
        return 0.0
    
    # 计算年化收益率
    annualized_return = calculate_annualized_return(daily_returns, trading_days, start_date, end_date)
    
    # 计算年化波动率
    import numpy as np
    returns_decimal = [r / 100.0 for r in daily_returns]
    volatility = np.std(returns_decimal, ddof=1)
    annualized_volatility = volatility * (trading_days ** 0.5) * 100
    
    if annualized_volatility == 0:
        return 0.0
    
    # 计算夏普比率
    sharpe_ratio = (annualized_return - risk_free_rate) / annualized_volatility
    
    return sharpe_ratio


def calculate_max_drawdown(
    cumulative_values: List[float]
) -> float:
    """
    计算最大回撤
    
    Args:
        cumulative_values: 累积值列表
        
    Returns:
        float: 最大回撤（百分比形式）
        
    Example:
        >>> values = [100, 110, 105, 120, 115, 130, 125]
        >>> round(calculate_max_drawdown(values), 2)
        -4.17
    """
    if not cumulative_values:
        return 0.0
    
    import numpy as np
    
    # 转换为numpy数组
    values = np.array(cumulative_values)
    
    # 计算峰值
    peak = np.maximum.accumulate(values)
    
    # 计算回撤
    drawdown = (values - peak) / peak
    
    # 返回最大回撤（转换为百分比）
    return drawdown.min() * 100