#!/usr/bin/env python3
"""
回测结果可视化脚本

该脚本用于可视化回测结果，包括：
- 累积收益率曲线
- 日收益率直方图
- 最大回撤曲线

使用方法:
    python backtest_vis.py <回测数据文件> [选项]

示例:
    python backtest_vis.py output/merged_backtest.jsonl
    python backtest_vis.py output/merged_backtest.jsonl -o backtest_visualization.html
"""

import argparse
import os
import sys
import json
import pandas as pd
import numpy as np
from datetime import datetime
from pyecharts import options as opts
from pyecharts.charts import Line, Bar, Grid, Page
from pyecharts.globals import ThemeType

# 添加项目根目录到Python路径
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)


def load_backtest_data(file_path):
    """
    加载回测数据
    
    Args:
        file_path: 回测数据文件路径
        
    Returns:
        DataFrame: 包含日期和累积收益率的数据框
    """
    data = []
    
    with open(file_path, 'r', encoding='utf-8') as f:
        for line in f:
            record = json.loads(line.strip())
            if record['type'] == 'daily_data':
                date = record['date']
                # 格式化日期为YYYY-MM-DD
                formatted_date = f"{date[:4]}-{date[4:6]}-{date[6:8]}"
                
                # 获取累积收益率
                overall_return = record['data']['overallReturn']['records'][0]['value']
                
                data.append({
                    'date': formatted_date,
                    'cumulative_return': overall_return
                })
    
    # 转换为DataFrame并按日期排序
    df = pd.DataFrame(data)
    df['date'] = pd.to_datetime(df['date'])
    df = df.sort_values('date')
    
    # 计算日收益率
    df['daily_return'] = df['cumulative_return'].pct_change() * 100
    df['daily_return'] = df['daily_return'].fillna(0)  # 第一天的收益率设为0
    
    return df


def plot_cumulative_returns(df, output_file=None, mark_drawdowns=True, top_n=10):
    """
    绘制累积收益率曲线
    
    Args:
        df: 包含回测数据的DataFrame
        output_file: 输出文件路径
        mark_drawdowns: 是否标记回撤区间
        top_n: 标记前N大回撤区间
        
    Returns:
        Line: 累积收益率曲线图
    """
    # 准备数据
    dates = [date.strftime('%Y-%m-%d') for date in df['date']]
    cumulative_returns = df['cumulative_return'].tolist()
    
    # 创建累积收益率曲线图
    line_chart = (
        Line(init_opts=opts.InitOpts(theme=ThemeType.LIGHT, width="1200px", height="600px"))
        .add_xaxis(xaxis_data=dates)
        .add_yaxis(
            series_name="累积收益率",
            y_axis=cumulative_returns,
            label_opts=opts.LabelOpts(is_show=False),
            linestyle_opts=opts.LineStyleOpts(width=2),
            itemstyle_opts=opts.ItemStyleOpts(color="#1890ff"),
        )
    )
    
    # 添加回撤区间标记
    if mark_drawdowns:
        # 确保已计算回撤
        if 'drawdown' not in df.columns:
            df = calculate_max_drawdown(df)
        
        # 获取前N大回撤区间
        drawdown_periods = identify_top_drawdown_periods(df, top_n)
        
        # 为每个回撤区间添加标记
        for i, period in enumerate(drawdown_periods):
            start_date = period['start_date'].strftime('%Y-%m-%d')
            end_date = period['end_date'].strftime('%Y-%m-%d')
            min_drawdown = period['min_drawdown']
            duration = period['duration']
            
            # 添加标记点
            line_chart.add_yaxis(
                series_name=f"回撤区间{i+1}",
                y_axis=[None] * len(dates),  # 不显示数据点
                markpoint_opts=opts.MarkPointOpts(
                    data=[
                        opts.MarkPointItem(
                            type_="min",
                            name=f"回撤区间{i+1}: {start_date}至{end_date}, 深度{min_drawdown:.2f}%, 持续{duration}天",
                            symbol="pin",
                            symbol_size=50,
                            x=period['start_idx'],
                            y=df.iloc[period['start_idx']]['cumulative_return'],
                            itemstyle_opts=opts.ItemStyleOpts(color="#ff4d4f"),
                        ),
                        opts.MarkPointItem(
                            type_="min",
                            name=f"回撤区间{i+1}结束",
                            symbol="pin",
                            symbol_size=50,
                            x=period['end_idx'],
                            y=df.iloc[period['end_idx']]['cumulative_return'],
                            itemstyle_opts=opts.ItemStyleOpts(color="#ff4d4f"),
                        ),
                    ]
                ),
            )
    
    # 设置全局选项
    line_chart.set_global_opts(
        title_opts=opts.TitleOpts(title="策略累积收益率曲线（含回撤区间标记）"),
        tooltip_opts=opts.TooltipOpts(
            trigger="axis",
            axis_pointer_type="cross",
            formatter="{b}<br/>累积收益率: {c}%"
        ),
        datazoom_opts=[
            opts.DataZoomOpts(
                is_show=True,
                type_="slider",
                xaxis_index=[0],
                range_start=0,
                range_end=100,
            ),
            opts.DataZoomOpts(
                is_show=True,
                type_="inside",
                xaxis_index=[0],
                range_start=0,
                range_end=100,
            ),
        ],
        xaxis_opts=opts.AxisOpts(
            type_="category",
            boundary_gap=False,
            axislabel_opts=opts.LabelOpts(rotate=45),
        ),
        yaxis_opts=opts.AxisOpts(
            name="收益率 (%)",
            type_="value",
            splitline_opts=opts.SplitLineOpts(is_show=True),
        ),
        legend_opts=opts.LegendOpts(
            is_show=True,
            pos_left="right",
            orient="vertical",
        ),
    )
    
    return line_chart


def plot_daily_returns_histogram(df, output_file=None):
    """
    绘制日收益率直方图
    
    Args:
        df: 包含回测数据的DataFrame
        output_file: 输出文件路径
        
    Returns:
        Bar: 日收益率直方图
    """
    # 准备数据
    daily_returns = df['daily_return'].tolist()
    
    # 计算直方图数据
    # 创建区间边界
    min_return = min(daily_returns)
    max_return = max(daily_returns)
    
    # 创建20个区间
    bins = np.linspace(min_return, max_return, 21)
    hist, bin_edges = np.histogram(daily_returns, bins=bins)
    
    # 准备标签
    bin_labels = [f"{bin_edges[i]:.2f}%~{bin_edges[i+1]:.2f}%" for i in range(len(bin_edges)-1)]
    
    # 创建直方图
    bar_chart = (
        Bar(init_opts=opts.InitOpts(theme=ThemeType.LIGHT, width="1200px", height="600px"))
        .add_xaxis(bin_labels)
        .add_yaxis(
            "频次",
            hist.tolist(),
            label_opts=opts.LabelOpts(is_show=False),
            itemstyle_opts=opts.ItemStyleOpts(color="#52c41a"),
        )
        .set_global_opts(
            title_opts=opts.TitleOpts(title="日收益率分布"),
            tooltip_opts=opts.TooltipOpts(
                trigger="axis",
                axis_pointer_type="shadow",
                formatter="{b}<br/>频次: {c}"
            ),
            xaxis_opts=opts.AxisOpts(
                name="收益率区间",
                axislabel_opts=opts.LabelOpts(rotate=45),
            ),
            yaxis_opts=opts.AxisOpts(
                name="频次",
                splitline_opts=opts.SplitLineOpts(is_show=True),
            ),
        )
    )
    
    return bar_chart


def calculate_max_drawdown(df):
    """
    计算最大回撤
    
    Args:
        df: 包含回测数据的DataFrame
        
    Returns:
        DataFrame: 添加了最大回撤列的DataFrame
    """
    # 计算累积最高点
    df['cumulative_max'] = df['cumulative_return'].expanding().max()
    
    # 计算回撤
    df['drawdown'] = (df['cumulative_return'] - df['cumulative_max']) / df['cumulative_max'] * 100
    
    return df


def identify_top_drawdown_periods(df, top_n=10):
    """
    识别前N大最大回撤区间
    
    Args:
        df: 包含回测数据的DataFrame，必须包含drawdown列
        top_n: 要识别的回撤区间数量
        
    Returns:
        list: 包含前N大回撤区间的列表，每个元素是一个字典，包含开始日期、结束日期、回撤深度等信息
    """
    # 确保已计算回撤
    if 'drawdown' not in df.columns:
        df = calculate_max_drawdown(df)
    
    # 识别回撤区间
    drawdown_periods = []
    in_drawdown = False
    start_idx = None
    min_drawdown = 0
    
    for i, (date, drawdown) in enumerate(zip(df['date'], df['drawdown'])):
        if drawdown < 0 and not in_drawdown:
            # 开始回撤
            in_drawdown = True
            start_idx = i
            min_drawdown = drawdown
        elif drawdown < 0 and in_drawdown:
            # 回撤持续，更新最小值
            min_drawdown = min(min_drawdown, drawdown)
        elif drawdown >= 0 and in_drawdown:
            # 回撤结束
            end_idx = i - 1
            
            # 计算回撤持续时间
            duration = (df.iloc[end_idx]['date'] - df.iloc[start_idx]['date']).days + 1
            
            # 添加到回撤区间列表
            drawdown_periods.append({
                'start_date': df.iloc[start_idx]['date'],
                'end_date': df.iloc[end_idx]['date'],
                'start_idx': start_idx,
                'end_idx': end_idx,
                'min_drawdown': min_drawdown,
                'duration': duration
            })
            
            # 重置状态
            in_drawdown = False
            start_idx = None
            min_drawdown = 0
    
    # 处理最后一个回撤区间（如果回测结束时仍在回撤中）
    if in_drawdown:
        end_idx = len(df) - 1
        duration = (df.iloc[end_idx]['date'] - df.iloc[start_idx]['date']).days + 1
        
        drawdown_periods.append({
            'start_date': df.iloc[start_idx]['date'],
            'end_date': df.iloc[end_idx]['date'],
            'start_idx': start_idx,
            'end_idx': end_idx,
            'min_drawdown': min_drawdown,
            'duration': duration
        })
    
    # 按最小回撤值排序，取前N个
    drawdown_periods.sort(key=lambda x: x['min_drawdown'])
    return drawdown_periods[:top_n]


def plot_max_drawdown(df, output_file=None):
    """
    绘制最大回撤曲线
    
    Args:
        df: 包含回测数据的DataFrame
        output_file: 输出文件路径
        
    Returns:
        Line: 最大回撤曲线图
    """
    # 计算最大回撤
    df = calculate_max_drawdown(df)
    
    # 准备数据
    dates = [date.strftime('%Y-%m-%d') for date in df['date']]
    drawdowns = df['drawdown'].tolist()
    
    # 创建回撤曲线图
    line_chart = (
        Line(init_opts=opts.InitOpts(theme=ThemeType.LIGHT, width="1200px", height="600px"))
        .add_xaxis(xaxis_data=dates)
        .add_yaxis(
            series_name="回撤",
            y_axis=drawdowns,
            label_opts=opts.LabelOpts(is_show=False),
            linestyle_opts=opts.LineStyleOpts(width=2),
            itemstyle_opts=opts.ItemStyleOpts(color="#ff4d4f"),
            areastyle_opts=opts.AreaStyleOpts(opacity=0.5),
        )
        .set_global_opts(
            title_opts=opts.TitleOpts(title="策略回撤曲线"),
            tooltip_opts=opts.TooltipOpts(
                trigger="axis",
                axis_pointer_type="cross",
                formatter="{b}<br/>回撤: {c:.2f}%"
            ),
            datazoom_opts=[
                opts.DataZoomOpts(
                    is_show=True,
                    type_="slider",
                    xaxis_index=[0],
                    range_start=0,
                    range_end=100,
                ),
                opts.DataZoomOpts(
                    is_show=True,
                    type_="inside",
                    xaxis_index=[0],
                    range_start=0,
                    range_end=100,
                ),
            ],
            xaxis_opts=opts.AxisOpts(
                type_="category",
                boundary_gap=False,
                axislabel_opts=opts.LabelOpts(rotate=45),
            ),
            yaxis_opts=opts.AxisOpts(
                name="回撤 (%)",
                type_="value",
                splitline_opts=opts.SplitLineOpts(is_show=True),
            ),
        )
    )
    
    return line_chart


def analyze_performance(df):
    """
    分析回测表现
    
    Args:
        df: 包含回测数据的DataFrame
    """
    # 计算统计指标
    daily_returns = df['daily_return'].values
    cumulative_returns = df['cumulative_return'].values
    
    # 计算最大回撤
    df = calculate_max_drawdown(df)
    max_drawdown = df['drawdown'].min()
    
    # 年化收益率
    trading_days = len(df)
    years = trading_days / 252  # 假设一年有252个交易日
    total_return = cumulative_returns[-1] / 100
    annual_return = (1 + total_return) ** (1/years) - 1
    
    # 年化波动率
    # 过滤掉无穷大的值
    valid_returns = daily_returns[np.isfinite(daily_returns)]
    if len(valid_returns) > 0:
        annual_volatility = np.std(valid_returns) * np.sqrt(252)
    else:
        annual_volatility = 0
    
    # 夏普比率 (假设无风险利率为3%)
    risk_free_rate = 0.03
    if annual_volatility > 0:
        sharpe_ratio = (annual_return - risk_free_rate) / annual_volatility
    else:
        sharpe_ratio = 0
    
    # 胜率
    win_rate = np.sum(daily_returns > 0) / len(daily_returns) * 100
    
    # 平均日收益率
    valid_returns_for_avg = daily_returns[np.isfinite(daily_returns)]
    if len(valid_returns_for_avg) > 0:
        avg_daily_return = np.mean(valid_returns_for_avg)
    else:
        avg_daily_return = 0
    
    # 打印统计结果
    print("\n回测表现分析:")
    print("=" * 50)
    print(f"交易天数: {trading_days} ({years:.2f}年)")
    print(f"总收益率: {total_return:.2%}")
    print(f"年化收益率: {annual_return:.2%}")
    print(f"年化波动率: {annual_volatility:.2%}")
    print(f"夏普比率 (无风险利率={risk_free_rate:.0%}): {sharpe_ratio:.2f}")
    print(f"最大回撤: {max_drawdown:.2f}%")
    print(f"胜率: {win_rate:.2f}%")
    print(f"平均日收益率: {avg_daily_return:.4f}%")


def generate_visualization(df, output_file=None, include_analysis=True, mark_drawdowns=True, top_n=10):
    """
    生成可视化图表
    
    Args:
        df: 包含回测数据的DataFrame
        output_file: 输出文件路径
        include_analysis: 是否包含分析结果
        mark_drawdowns: 是否标记最大回撤区间
        top_n: 标记前N个最大回撤区间
    """
    # 创建图表
    cumulative_chart = plot_cumulative_returns(df, mark_drawdowns=mark_drawdowns, top_n=top_n)
    daily_returns_chart = plot_daily_returns_histogram(df)
    max_drawdown_chart = plot_max_drawdown(df)
    
    # 创建页面
    page = Page(page_title="回测结果可视化")
    page.add(cumulative_chart)
    page.add(daily_returns_chart)
    page.add(max_drawdown_chart)
    
    # 保存图表
    if output_file:
        if output_file.endswith('.html'):
            page.render(output_file)
            print(f"图表已保存到: {output_file}")
        else:
            # 如果不是HTML文件，则生成HTML文件
            html_file = output_file.replace('.png', '.html')
            page.render(html_file)
            print(f"图表已保存到: {html_file}")
    else:
        # 默认文件名
        output_file = "backtest_visualization.html"
        page.render(output_file)
        print(f"图表已保存到: {output_file}")
    
    # 分析表现
    if include_analysis:
        analyze_performance(df)


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='回测结果可视化脚本')
    parser.add_argument('file', help='回测数据文件路径')
    parser.add_argument('-o', '--output', help='输出图表文件路径（默认为backtest_visualization.html）')
    parser.add_argument('--no-analysis', action='store_true', help='不包含分析结果')
    parser.add_argument('--no-mark-drawdowns', action='store_true', help='不标记最大回撤区间')
    parser.add_argument('--top-n', type=int, default=10, help='标记前N个最大回撤区间（默认为10）')
    
    args = parser.parse_args()
    
    # 检查输入文件是否存在
    if not os.path.exists(args.file):
        print(f"错误: 文件不存在: {args.file}")
        return 1
    
    # 加载数据
    try:
        print("正在加载回测数据...")
        df = load_backtest_data(args.file)
        print(f"已加载 {len(df)} 条数据记录")
    except Exception as e:
        print(f"加载数据时出错: {e}")
        return 1
    
    # 生成可视化
    try:
        print("正在生成可视化图表...")
        include_analysis = not args.no_analysis
        mark_drawdowns = not args.no_mark_drawdowns
        generate_visualization(df, args.output, include_analysis, mark_drawdowns, args.top_n)
    except Exception as e:
        print(f"生成可视化时出错: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main())