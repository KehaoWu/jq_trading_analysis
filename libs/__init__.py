"""
对冲数据计算包

该包提供了根据聚宽回测数据和指数数据计算对冲数据的功能。
"""

from .data_loader import load_backtest_data, load_position_data, load_index_data
from .format_converter import (
    generate_hedge_backtest_format,
    generate_hedge_position_format,
    export_data_to_csv
)
from .returns_calculator import (
    calculate_daily_returns_to_csv,
    calculate_cumulative_returns,
    calculate_annualized_return,
    calculate_sharpe_ratio,
    calculate_max_drawdown
)
from .utils import parse_date_string
from .hedge_data_calc import calculate_hedge_data

__all__ = [
    'load_backtest_data',
    'load_position_data',
    'load_index_data',
    'generate_hedge_backtest_format',
    'generate_hedge_position_format',
    'export_data_to_csv',
    'calculate_daily_returns_to_csv',
    'calculate_cumulative_returns',
    'calculate_annualized_return',
    'calculate_sharpe_ratio',
    'calculate_max_drawdown',
    'parse_date_string',
    'calculate_hedge_data'
]