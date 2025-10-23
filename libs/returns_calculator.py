"""
收益率计算模块

该模块提供了根据日收益率计算累积收益率的功能，以及将日收益率导出为CSV的功能。
"""

import pandas as pd
from typing import List, Dict, Optional, Union
import os


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


def calculate_annualized_return(
    daily_returns: List[float],
    trading_days: int = 252,
    start_date: str = None,
    end_date: str = None
) -> float:
    """
    计算年化收益率
    
    Args:
        daily_returns: 日收益率列表（百分比形式，如1.5表示1.5%）
        trading_days: 一年中的交易日数量，默认为252
        start_date: 开始日期（YYYYMMDD格式），用于计算实际年数
        end_date: 结束日期（YYYYMMDD格式），用于计算实际年数
        
    Returns:
        float: 年化收益率（百分比形式）
        
    Example:
        >>> daily_returns = [0.1] * 252  # 每日0.1%收益
        >>> round(calculate_annualized_return(daily_returns), 2)
        28.58
    """
    if not daily_returns:
        return 0.0
    
    # 计算累积收益率
    cumulative_values = calculate_cumulative_returns(daily_returns)
    if not cumulative_values:
        return 0.0
    
    # 获取最终累积收益率
    final_cumulative_return = (cumulative_values[-1] - 100.0) / 100.0  # 转换为小数
    
    # 计算年数
    if start_date and end_date:
        # 使用实际日期计算年数（聚宽标准）
        from datetime import datetime
        try:
            start_dt = datetime.strptime(start_date, '%Y%m%d')
            end_dt = datetime.strptime(end_date, '%Y%m%d')
            total_days = (end_dt - start_dt).days
            years = total_days / 365.25  # 考虑闰年
        except:
            # 如果日期解析失败，回退到交易日计算
            years = len(daily_returns) / trading_days
    else:
        # 使用交易日计算年数
        years = len(daily_returns) / trading_days
    
    if years == 0:
        return 0.0
    
    annualized_return = (1 + final_cumulative_return) ** (1 / years) - 1
    
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
        start_date: 开始日期（YYYYMMDD格式），用于计算实际年数
        end_date: 结束日期（YYYYMMDD格式），用于计算实际年数
        
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