#!/usr/bin/env python3
"""
对冲分析可视化脚本

该脚本用于：
1. 自动识别指定文件夹中的回测数据文件和仓位比例数据文件
2. 计算与指定指数的对冲收益
3. 绘制累积收益曲线，包括：
   - 所有回测数据的累积收益率
   - 每个回测数据与指定指数的对冲累积收益
   - 所有指数的累积收益

使用方法:
    python hedge_analysis_visualization.py --input_dir /path/to/backtest/data --index zz500
"""

import argparse
import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path
import traceback
from typing import Dict, List, Tuple, Optional
import csv

import numpy as np
import pandas as pd

# 添加项目根目录到Python路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from libs.hedge_data_calc import calculate_hedge_data
from libs.data_loader import load_backtest_data, load_index_data
from libs.returns_calculator import (
    calculate_daily_returns, 
    calculate_cumulative_returns,
    calculate_annualized_return,
    calculate_sharpe_ratio,
    calculate_max_drawdown
)


class BacktestFileIdentifier:
    """回测文件识别器"""
    
    def __init__(self, input_dir: str):
        """
        初始化文件识别器
        
        Args:
            input_dir: 输入目录路径
        """
        self.input_dir = Path(input_dir)
        if not self.input_dir.exists():
            raise FileNotFoundError(f"输入目录不存在: {input_dir}")
    
    def identify_files(self) -> List[Dict[str, str]]:
        """
        识别目录中的回测数据文件和对应的持仓比例文件
        
        Returns:
            List[Dict]: 包含文件信息的列表，每个字典包含：
                - backtest_file: 回测数据文件路径
                - position_file: 持仓比例文件路径（可能为None）
                - backtest_id: 回测ID
                - backtest_name: 回测名称
        """
        files_info = []
        
        # 查找所有.jsonl文件（回测数据文件）
        backtest_files = list(self.input_dir.glob("*.jsonl"))
        
        for backtest_file in backtest_files:
            # 从文件名中提取回测ID
            backtest_id = self._extract_backtest_id(backtest_file.name)
            backtest_name = self._extract_backtest_name(backtest_file.name)
            
            # 查找对应的持仓比例文件
            position_file = self._find_position_file(backtest_id)
            
            files_info.append({
                'backtest_file': str(backtest_file),
                'position_file': str(position_file) if position_file else None,
                'backtest_id': backtest_id,
                'backtest_name': backtest_name
            })
        
        return files_info
    
    def _extract_backtest_id(self, filename: str) -> str:
        """
        从文件名中提取回测ID
        
        Args:
            filename: 文件名
            
        Returns:
            str: 回测ID
        """
        # 匹配32位十六进制ID
        match = re.search(r'([a-f0-9]{32})', filename)
        return match.group(1) if match else ""
    
    def _extract_backtest_name(self, filename: str) -> str:
        """
        从文件名中提取回测名称
        
        Args:
            filename: 文件名
            
        Returns:
            str: 回测名称
        """
        # 提取文件名中第一个下划线前的部分作为策略名称
        parts = filename.split('_')
        if len(parts) > 0:
            return parts[0]
        return filename.replace('.jsonl', '')
    
    def _find_position_file(self, backtest_id: str) -> Optional[Path]:
        """
        查找对应的持仓比例文件
        
        Args:
            backtest_id: 回测ID
            
        Returns:
            Optional[Path]: 持仓比例文件路径，如果不存在则返回None
        """
        position_file = self.input_dir / f"position_ratio_{backtest_id}.json"
        print(position_file)
        return position_file if position_file.exists() else None


class IndexDataManager:
    """指数数据管理器"""
    
    def __init__(self, index_data_dir: str):
        """
        初始化指数数据管理器
        
        Args:
            index_data_dir: 指数数据目录路径
        """
        self.index_data_dir = Path(index_data_dir)
        if not self.index_data_dir.exists():
            raise FileNotFoundError(f"指数数据目录不存在: {index_data_dir}")
    
    def get_available_indices(self) -> List[str]:
        """
        获取可用的指数列表
        
        Returns:
            List[str]: 可用指数名称列表
        """
        indices = []
        for file in self.index_data_dir.glob("*.jsonl"):
            # 从文件名中提取指数名称
            index_name = file.name.split('_')[0]
            if index_name not in indices:
                indices.append(index_name)
        return indices
    
    def get_index_file(self, index_name: str) -> Optional[str]:
        """
        获取指定指数的数据文件路径
        
        Args:
            index_name: 指数名称
            
        Returns:
            Optional[str]: 指数数据文件路径，如果不存在则返回None
        """
        # 查找匹配的指数文件（选择最新的文件）
        pattern = f"{index_name}_*.jsonl"
        matching_files = list(self.index_data_dir.glob(pattern))
        
        if not matching_files:
            return None
        
        # 选择最新的文件（按文件名排序）
        latest_file = sorted(matching_files, key=lambda x: x.name)[-1]
        return str(latest_file)
    
    def load_all_indices_data(self) -> Dict[str, List[Dict]]:
        """
        加载所有指数数据
        
        Returns:
            Dict[str, List[Dict]]: 指数名称到数据的映射
        """
        all_indices_data = {}
        
        for index_name in self.get_available_indices():
            index_file = self.get_index_file(index_name)
            if index_file:
                try:
                    index_data = load_index_data(index_file)
                    all_indices_data[index_name] = index_data
                except Exception as e:
                    print(f"警告: 加载指数数据失败 {index_name}: {e}")
        
        return all_indices_data


def prepare_backtest_data_for_visualization(backtest_data: List[Dict]) -> Dict[str, List]:
    """
    准备回测数据用于可视化
    
    Args:
        backtest_data: 回测数据
        
    Returns:
        Dict: 包含日期、日收益率和累积收益率的字典
    """
    dates = []
    cumulative_returns = []
    
    for item in backtest_data:
        dates.append(item.get('date', ''))
        overall_return = item.get('data', {}).get('overallReturn', {}).get('records', [{}])[0].get('value', 0)
        # overallReturn 已经是百分比形式的累积收益率，直接使用
        cumulative_returns.append(overall_return)
    
    # 使用returns_calculator.py中的标准方法计算日收益率
    # 构造数据格式以符合calculate_daily_returns函数的要求
    cumulative_data = []
    for i, date in enumerate(dates):
        cumulative_data.append({
            'date': date,
            'cumulative_return': cumulative_returns[i]
        })
    
    # 使用标准库方法计算日收益率
    daily_returns_data = calculate_daily_returns(cumulative_data)
    daily_returns = [item['daily_return'] for item in daily_returns_data]
    
    return {
        'dates': dates,
        'daily_returns': daily_returns,
        'cumulative_returns': cumulative_returns
    }


def prepare_index_data_for_visualization(index_data: List[Dict]) -> Dict[str, List]:
    """
    准备指数数据用于可视化
    
    Args:
        index_data: 指数数据
        
    Returns:
        Dict: 包含日期、日收益率和累积收益率的字典
    """
    dates = []
    daily_returns = []
    
    for item in index_data:
        dates.append(item.get('date', ''))
        daily_returns.append(item.get('pctChg', 0))
    
    # 计算累积收益率
    # 过滤无效数据
    filtered_returns = []
    for ret in daily_returns:
        if ret is None or np.isnan(ret) or np.isinf(ret):
            filtered_returns.append(0.0)
        else:
            filtered_returns.append(ret)
    
    # 直接计算累积收益率（百分比形式）
    cumulative_returns = []
    cumulative_value = 100.0  # 初始值
    
    for daily_return in filtered_returns:
        # 将百分比转换为小数进行计算
        return_rate = daily_return / 100.0
        cumulative_value = cumulative_value * (1 + return_rate)
        # 计算相对于初始值的收益率百分比
        cumulative_return_percent = (cumulative_value - 100.0)
        cumulative_returns.append(cumulative_return_percent)
    
    return {
        'dates': dates,
        'daily_returns': filtered_returns,
        'cumulative_returns': cumulative_returns
    }


class StatisticsCalculator:
    """统计指标计算器"""
    
    def __init__(self):
        """初始化统计指标计算器"""
        pass
    
    def _format_date(self, date_str: str) -> str:
        """
        统一日期格式为YYYY-MM-DD
        
        Args:
            date_str: 输入日期字符串（可能是YYYYMMDD或YYYY-MM-DD格式）
            
        Returns:
            str: 格式化后的日期字符串（YYYY-MM-DD）
        """
        if not date_str:
            return ''
        
        # 如果已经是YYYY-MM-DD格式，直接返回
        if '-' in date_str and len(date_str) == 10:
            return date_str
        
        # 如果是YYYYMMDD格式，转换为YYYY-MM-DD
        if len(date_str) == 8 and date_str.isdigit():
            try:
                from datetime import datetime
                dt = datetime.strptime(date_str, '%Y%m%d')
                return dt.strftime('%Y-%m-%d')
            except:
                return date_str
        
        return date_str
    
    def calculate_max_drawdown_with_period(self, cumulative_values: List[float]) -> Tuple[float, int, int]:
        """
        计算最大回撤及其区间起始点
        
        Args:
            cumulative_values: 累积值列表
            
        Returns:
            Tuple[float, int, int]: (最大回撤百分比, 回撤开始索引, 回撤结束索引)
        """
        if not cumulative_values:
            return 0.0, -1, -1
        
        import numpy as np
        
        # 转换为numpy数组
        values = np.array(cumulative_values)
        
        # 计算峰值
        peak = np.maximum.accumulate(values)
        
        # 计算回撤
        drawdown = (values - peak) / peak
        
        # 找到最大回撤的索引
        max_dd_idx = np.argmin(drawdown)
        max_drawdown = drawdown[max_dd_idx] * 100
        
        # 找到回撤开始的索引（最大回撤点之前的最后一个峰值）
        start_idx = 0
        for i in range(max_dd_idx, -1, -1):
            if values[i] == peak[max_dd_idx]:
                start_idx = i
                break
        
        return max_drawdown, start_idx, max_dd_idx
    
    def calculate_statistics(self, daily_returns: List[float], cumulative_returns: List[float], 
                           name: str, data_type: str, dates: List[str] = None) -> Dict[str, float]:
        """
        计算统计指标
        
        Args:
            daily_returns: 日收益率列表（百分比形式）
            cumulative_returns: 累积收益率列表（百分比形式）
            name: 数据名称
            data_type: 数据类型（backtest, hedge, index）
            dates: 日期列表（可选）
            
        Returns:
            Dict: 包含统计指标的字典
        """
        if not daily_returns or not cumulative_returns:
            return {
                'name': name,
                'type': data_type,
                'total_return': 0.0,
                'annualized_return': 0.0,
                'max_drawdown': 0.0,
                'max_drawdown_start_date': '',
                'max_drawdown_end_date': '',
                'sharpe_ratio': 0.0,
                'trading_days': 0
            }
        
        # 计算区间收益率（总收益率）
        total_return = cumulative_returns[-1] if cumulative_returns else 0.0
        
        # 获取开始和结束日期
        start_date = dates[0] if dates else None
        end_date = dates[-1] if dates else None
        
        # 计算年化收益率
        annualized_return = calculate_annualized_return(daily_returns, start_date=start_date, end_date=end_date)
        
        # 计算最大回撤（需要累积值，不是累积收益率）
        # 将累积收益率转换为累积值
        cumulative_values = [100 + ret for ret in cumulative_returns]
        max_drawdown, start_idx, end_idx = self.calculate_max_drawdown_with_period(cumulative_values)
        
        # 获取最大回撤区间的日期
        max_drawdown_start_date = ''
        max_drawdown_end_date = ''
        if dates and start_idx >= 0 and end_idx >= 0:
            start_date_raw = dates[start_idx] if start_idx < len(dates) else ''
            end_date_raw = dates[end_idx] if end_idx < len(dates) else ''
            
            # 统一日期格式为YYYY-MM-DD
            max_drawdown_start_date = self._format_date(start_date_raw)
            max_drawdown_end_date = self._format_date(end_date_raw)
        
        # 计算夏普比率
        sharpe_ratio = calculate_sharpe_ratio(daily_returns, start_date=start_date, end_date=end_date)
        
        return {
            'name': name,
            'type': data_type,
            'total_return': round(total_return, 2),
            'annualized_return': round(annualized_return, 2),
            'max_drawdown': round(max_drawdown, 2),
            'max_drawdown_start_date': max_drawdown_start_date,
            'max_drawdown_end_date': max_drawdown_end_date,
            'sharpe_ratio': round(sharpe_ratio, 4),
            'trading_days': len(daily_returns)
        }


class DebugDataExporter:
    """调试数据导出器"""
    
    def __init__(self, debug_dir: Path):
        """
        初始化调试数据导出器
        
        Args:
            debug_dir: 调试数据输出目录
        """
        self.debug_dir = debug_dir
        self.debug_dir.mkdir(parents=True, exist_ok=True)
        
    def export_hedge_data(self, backtest_name: str, hedge_data: Dict, index_name: str):
        """
        导出对冲数据到CSV文件
        
        Args:
            backtest_name: 回测名称
            hedge_data: 对冲数据
            index_name: 指数名称
        """
        # 创建文件名
        filename = f"{backtest_name}_{index_name}_hedge_debug.csv"
        filepath = self.debug_dir / filename
        
        # 准备数据
        export_data = []
        for item in hedge_data['data']:
            export_data.append({
                'date': item['date'],
                'backtest_return': item.get('return_backtest', 0),
                'index_return': item.get('return_index', 0),
                'hedge_return': item.get('hedge_return', 0),
                'position_ratio': item.get('position_ratio', 0),
                'cash': item.get('cash', 0),
                'total_value': item.get('total_value', 0),
                'net_value': item.get('net_value', 1)
            })
        
        # 写入CSV文件
        if export_data:
            with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = ['date', 'backtest_return', 'index_return', 'hedge_return', 
                             'position_ratio', 'cash', 'total_value', 'net_value']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(export_data)
            
            print(f"调试数据已导出: {filepath}")
    
    def export_cumulative_returns(self, backtest_name: str, dates: List[str], 
                                 daily_returns: List[float], cumulative_returns: List[float], 
                                 data_type: str):
        """
        导出累积收益率数据到CSV文件
        
        Args:
            backtest_name: 回测名称
            dates: 日期列表
            daily_returns: 日收益率列表
            cumulative_returns: 累积收益率列表
            data_type: 数据类型（如：hedge, backtest, index）
        """
        # 创建文件名
        filename = f"{backtest_name}_{data_type}_cumulative_returns.csv"
        filepath = self.debug_dir / filename
        
        # 准备数据
        export_data = []
        for i, date in enumerate(dates):
            export_data.append({
                'date': date,
                'daily_return': daily_returns[i] if i < len(daily_returns) else 0,
                'cumulative_return': cumulative_returns[i] if i < len(cumulative_returns) else 0
            })
        
        # 写入CSV文件
        if export_data:
            with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = ['date', 'daily_return', 'cumulative_return']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(export_data)
            
            print(f"累积收益率数据已导出: {filepath}")
    
    def export_index_data(self, index_name: str, dates: List[str], 
                         daily_returns: List[float], cumulative_returns: List[float]):
        """
        导出指数数据到CSV文件
        
        Args:
            index_name: 指数名称
            dates: 日期列表
            daily_returns: 日收益率列表
            cumulative_returns: 累积收益率列表
        """
        # 创建文件名
        filename = f"{index_name}_index_data.csv"
        filepath = self.debug_dir / filename
        
        # 准备数据
        export_data = []
        for i, date in enumerate(dates):
            export_data.append({
                'date': date,
                'daily_return': daily_returns[i] if i < len(daily_returns) else 0,
                'cumulative_return': cumulative_returns[i] if i < len(cumulative_returns) else 0
            })
        
        # 写入CSV文件
        if export_data:
            with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
                fieldnames = ['date', 'daily_return', 'cumulative_return']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(export_data)
            
            print(f"指数数据已导出: {filepath}")
    
    def export_statistics_summary(self, statistics_list: List[Dict]):
        """
        导出统计指标汇总到CSV文件
        
        Args:
            statistics_list: 统计指标列表
        """
        if not statistics_list:
            return
        
        # 创建文件名
        filename = "statistics_summary.csv"
        filepath = self.debug_dir / filename
        
        # 写入CSV文件
        with open(filepath, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['name', 'type', 'total_return', 'annualized_return', 
                         'max_drawdown', 'max_drawdown_start_date', 'max_drawdown_end_date',
                         'sharpe_ratio', 'trading_days']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(statistics_list)
        
        print(f"统计指标汇总已导出: {filepath}")


class EChartsVisualizer:
    """ECharts可视化器"""
    
    def __init__(self):
        """初始化可视化器"""
        self.chart_data = {
            'backtest_series': [],
            'hedge_series': [],
            'index_series': []
        }
    
    def add_backtest_series(self, name: str, dates: List[str], returns: List[float]):
        """
        添加回测数据系列
        
        Args:
            name: 系列名称
            dates: 日期列表（YYYYMMDD格式）
            returns: 累积收益率列表（百分比形式，如10.0表示10%的收益率）
        """
        # 过滤掉无效数据并转换日期格式
        valid_data = []
        for date, ret in zip(dates, returns):
            if ret is not None and not np.isnan(ret):
                # 将YYYYMMDD格式转换为YYYY-MM-DD格式
                if len(date) == 8 and date.isdigit():
                    formatted_date = f"{date[:4]}-{date[4:6]}-{date[6:8]}"
                else:
                    formatted_date = date  # 如果已经是正确格式，保持不变
                valid_data.append([formatted_date, ret])
        
        self.chart_data['backtest_series'].append({
            'name': f"回测-{name}",
            'type': 'line',
            'data': valid_data,
            'smooth': True,
            'symbol': 'none',  # 移除数据点
            'lineStyle': {'width': 1}
        })
    
    def add_hedge_series(self, name: str, dates: List[str], returns: List[float]):
        """
        添加对冲数据系列
        
        Args:
            name: 系列名称
            dates: 日期列表
            returns: 累积收益率列表（百分比形式，如10.0表示10%的收益率）
        """
        # 过滤掉无效数据
        valid_data = []
        for date, ret in zip(dates, returns):
            if ret is not None and not np.isnan(ret):
                valid_data.append([date, ret])
        
        self.chart_data['hedge_series'].append({
            'name': f"对冲-{name}",
            'type': 'line',
            'data': valid_data,
            'smooth': True,
            'symbol': 'none',  # 移除数据点
            'lineStyle': {'width': 1, 'type': 'dashed'}
        })
    
    def add_index_series(self, name: str, dates: List[str], returns: List[float]):
        """
        添加指数数据系列
        
        Args:
            name: 系列名称
            dates: 日期列表
            returns: 累积收益率列表（百分比形式，如10.0表示10%的收益率）
        """
        # 过滤掉无效数据
        valid_data = []
        for date, ret in zip(dates, returns):
            if ret is not None and not np.isnan(ret):
                valid_data.append([date, ret])
        
        self.chart_data['index_series'].append({
            'name': f"指数-{name}",
            'type': 'line',
            'data': valid_data,
            'smooth': True,
            'symbol': 'none',  # 移除数据点
            'lineStyle': {'width': 1}
        })
    
    def generate_html(self, output_file: str, title: str = "对冲分析可视化"):
        """
        生成HTML文件
        
        Args:
            output_file: 输出文件路径
            title: 图表标题
        """
        # 合并所有系列
        all_series = (self.chart_data['backtest_series'] + 
                     self.chart_data['hedge_series'] + 
                     self.chart_data['index_series'])
        
        html_template = f"""
<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>{title}</title>
    <script src="https://cdn.jsdelivr.net/npm/echarts@5.0.0/dist/echarts.min.js"></script>
    <style>
        body {{
            font-family: Arial, sans-serif;
            margin: 20px;
        }}
        #chart {{
            width: 100%;
            height: 600px;
        }}
        .info {{
            margin-bottom: 20px;
            padding: 10px;
            background-color: #f5f5f5;
            border-radius: 5px;
        }}
    </style>
</head>
<body>
    <div class="info">
        <h1>{title}</h1>
        <p>生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        <p>包含 {len(self.chart_data['backtest_series'])} 个回测策略, {len(self.chart_data['hedge_series'])} 个对冲策略, {len(self.chart_data['index_series'])} 个指数</p>
    </div>
    <div id="chart"></div>
    
    <script>
        var chartDom = document.getElementById('chart');
        var myChart = echarts.init(chartDom);
        
        var option = {{
            title: {{
                text: '{title}',
                left: 'center'
            }},
            tooltip: {{
                trigger: 'axis',
                formatter: function(params) {{
                    var result = params[0].axisValue + '<br/>';
                    params.forEach(function(item) {{
                        result += item.marker + item.seriesName + ': ' + item.value[1].toFixed(2) + '%<br/>';
                    }});
                    return result;
                }}
            }},
            legend: {{
                top: '30px',
                type: 'scroll'
            }},
            grid: {{
                left: '3%',
                right: '4%',
                bottom: '15%',
                top: '80px',
                containLabel: true
            }},
            dataZoom: [
                {{
                    type: 'slider',
                    show: true,
                    xAxisIndex: [0],
                    start: 0,
                    end: 100,
                    bottom: '5%',
                    height: '8%'
                }},
                {{
                    type: 'inside',
                    xAxisIndex: [0],
                    start: 0,
                    end: 100
                }}
            ],
            xAxis: {{
                type: 'time',
                boundaryGap: false
            }},
            yAxis: {{
                type: 'value',
                name: '收益率 (%)',
                axisLabel: {{
                    formatter: '{{value}}%'
                }}
            }},
            series: {json.dumps(all_series, ensure_ascii=False)}
        }};
        
        myChart.setOption(option);
        
        // 响应式调整
        window.addEventListener('resize', function() {{
            myChart.resize();
        }});
    </script>
</body>
</html>
"""
        
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(html_template)
        
        print(f"可视化文件已生成: {output_file}")


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='对冲分析可视化脚本')
    parser.add_argument('--input_dir', required=True, help='回测数据目录路径')
    parser.add_argument('--index', required=True, help='指定的对冲指数名称（如：zz500）')
    parser.add_argument('--output', default='hedge_analysis_visualization.html', help='输出HTML文件路径')
    parser.add_argument('--index_data_dir', default='index_data', help='指数数据目录路径')
    parser.add_argument('--debug', action='store_true', help='启用调试模式，输出中间数据到CSV文件')
    
    args = parser.parse_args()
    
    # 转换为绝对路径
    project_root = Path(__file__).parent.parent
    input_dir = Path(args.input_dir)
    if not input_dir.is_absolute():
        input_dir = project_root / input_dir
    
    index_data_dir = Path(args.index_data_dir)
    if not index_data_dir.is_absolute():
        index_data_dir = project_root / index_data_dir
    
    # 根据输入文件夹名称创建对应的输出子文件夹
    input_folder_name = input_dir.name
    output_dir = project_root / 'output' / input_folder_name
    
    output_file = Path(args.output)
    if not output_file.is_absolute():
        output_file = output_dir / output_file
    else:
        # 如果用户提供了绝对路径，仍然使用对应的子文件夹结构
        output_file = output_dir / output_file.name
    
    # 确保输出目录存在
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    # 初始化调试数据导出器（如果启用调试模式）
    debug_exporter = None
    if args.debug:
        debug_dir = output_dir / 'debug_data'
        debug_exporter = DebugDataExporter(debug_dir)
        print(f"调试模式已启用，调试数据将输出到: {debug_dir}")
    
    # 初始化统计计算器和结果列表
    stats_calculator = StatisticsCalculator()
    all_statistics = []
    
    try:
        # 1. 识别回测文件
        print("正在识别回测文件...")
        identifier = BacktestFileIdentifier(str(input_dir))
        files_info = identifier.identify_files()
        print(f"找到 {len(files_info)} 个回测文件")
        
        # 2. 初始化指数数据管理器
        print("正在初始化指数数据管理器...")
        index_manager = IndexDataManager(str(index_data_dir))
        available_indices = index_manager.get_available_indices()
        print(f"可用指数: {', '.join(available_indices)}")
        
        # 验证指定的指数是否存在
        if args.index not in available_indices:
            print(f"错误: 指定的指数 '{args.index}' 不存在")
            print(f"可用指数: {', '.join(available_indices)}")
            return
        
        # 3. 初始化可视化器
        visualizer = EChartsVisualizer()
        
        # 4. 处理每个回测文件
        print("正在处理回测数据...")
        for file_info in files_info:
            print(f"处理: {file_info['backtest_name']}, {file_info['position_file']}")
            
            # 加载回测数据
            backtest_data = load_backtest_data(file_info['backtest_file'])
            backtest_viz_data = prepare_backtest_data_for_visualization(backtest_data)
            
            # 调试输出：导出回测数据
            if debug_exporter:
                debug_exporter.export_cumulative_returns(
                    file_info['backtest_name'],
                    backtest_viz_data['dates'],
                    backtest_viz_data['daily_returns'],
                    backtest_viz_data['cumulative_returns'],
                    "backtest"
                )
            
            # 计算回测数据统计指标
            backtest_stats = stats_calculator.calculate_statistics(
                backtest_viz_data['daily_returns'],
                backtest_viz_data['cumulative_returns'],
                file_info['backtest_name'],
                'backtest',
                backtest_viz_data['dates']
            )
            all_statistics.append(backtest_stats)
            
            # 添加回测数据到可视化
            visualizer.add_backtest_series(
                file_info['backtest_name'],
                backtest_viz_data['dates'],
                backtest_viz_data['cumulative_returns']
            )
            
            # 计算对冲数据
            index_file = index_manager.get_index_file(args.index)
            if index_file:
                try:
                    hedge_data = calculate_hedge_data(
                        backtest_file=file_info['backtest_file'],
                        position_file=file_info['position_file'],
                        index_file=index_file
                    )
                    
                    # 计算对冲累积收益率
                    # hedge_return已经是百分比形式，不需要再乘以100
                    hedge_returns = [item['hedge_return'] for item in hedge_data['data']]
                    hedge_dates = [item['date'] for item in hedge_data['data']]
                    
                    # 直接计算累积收益率（百分比形式）
                    hedge_cumulative = []
                    cumulative_value = 100.0  # 初始值
                    
                    for daily_return in hedge_returns:
                        # 将百分比转换为小数进行计算
                        return_rate = daily_return / 100.0
                        cumulative_value = cumulative_value * (1 + return_rate)
                        # 计算相对于初始值的收益率百分比
                        cumulative_return_percent = (cumulative_value - 100.0)
                        hedge_cumulative.append(cumulative_return_percent)

                    # 调试输出：导出对冲数据
                    if debug_exporter:
                        debug_exporter.export_hedge_data(
                            file_info['backtest_name'], 
                            hedge_data, 
                            args.index
                        )
                        debug_exporter.export_cumulative_returns(
                            file_info['backtest_name'],
                            hedge_dates,
                            hedge_returns,
                            hedge_cumulative,
                            f"hedge_{args.index}"
                        )
                    
                    # 计算对冲数据统计指标
                    hedge_stats = stats_calculator.calculate_statistics(
                        hedge_returns,
                        hedge_cumulative,
                        f"{file_info['backtest_name']}-{args.index}",
                        'hedge',
                        hedge_dates
                    )
                    all_statistics.append(hedge_stats)
                    
                    # 添加对冲数据到可视化
                    visualizer.add_hedge_series(
                        f"{file_info['backtest_name']}-{args.index}",
                        hedge_dates,
                        hedge_cumulative
                    )
                    
                except Exception as e:
                    traceback.print_exc()
                    print(f"警告: 计算对冲数据失败 {file_info['backtest_name']}: {e}")
        
        # 5. 加载所有指数数据
        print("正在加载指数数据...")
        all_indices_data = index_manager.load_all_indices_data()
        
        for index_name, index_data in all_indices_data.items():
            index_viz_data = prepare_index_data_for_visualization(index_data)
            
            # 调试输出：导出指数数据
            if debug_exporter:
                debug_exporter.export_index_data(
                    index_name,
                    index_viz_data['dates'],
                    index_viz_data['daily_returns'],
                    index_viz_data['cumulative_returns']
                )
            
            # 计算指数数据统计指标
            index_stats = stats_calculator.calculate_statistics(
                index_viz_data['daily_returns'],
                index_viz_data['cumulative_returns'],
                index_name,
                'index',
                index_viz_data['dates']
            )
            all_statistics.append(index_stats)
            
            visualizer.add_index_series(
                index_name,
                index_viz_data['dates'],
                index_viz_data['cumulative_returns']
            )
        
        # 6. 导出统计指标汇总
        print("正在导出统计指标...")
        # 创建统计结果导出器（无论是否启用调试模式都导出统计结果）
        if not debug_exporter:
            debug_dir = output_dir / 'debug_data'
            stats_exporter = DebugDataExporter(debug_dir)
        else:
            stats_exporter = debug_exporter
        
        stats_exporter.export_statistics_summary(all_statistics)
        
        # 7. 生成可视化文件
        print("正在生成可视化文件...")
        visualizer.generate_html(
            str(output_file),
            f"对冲分析可视化 - {args.index}"
        )
        
        print("分析完成!")
        
    except Exception as e:
        print(f"错误: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main())