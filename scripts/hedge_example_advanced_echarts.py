"""
对冲数据计算示例脚本
使用聚宽回测数据和指数数据计算对冲后的收益率
使用echarts进行图表生成
"""

import argparse
import os
import sys
import json
import pandas as pd
from datetime import datetime
from pyecharts import options as opts
from pyecharts.charts import Line, Grid
from pyecharts.globals import ThemeType

# 添加项目根目录到Python路径
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from libs.hedge_data_calc import calculate_hedge_data


def plot_hedge_results(hedge_data_file, output_file=None):
    """
    使用echarts绘制对冲结果图表
    
    Args:
        hedge_data_file: 对冲数据文件路径
        output_file: 输出图表文件路径，如果为None则生成HTML文件
    """
    # 读取对冲数据
    with open(hedge_data_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # 提取数据
    records = data['data']
    df = pd.DataFrame(records)
    df['date'] = pd.to_datetime(df['date'])
    
    # 准备数据
    dates = [date.strftime('%Y-%m-%d') for date in df['date']]
    hedge_net_value = [value / 1e8 for value in df['hedge_net_value']]  # 转换为亿元
    backtest_returns = df['backtest_return'].tolist()
    index_returns = df['index_return'].tolist()
    hedge_returns = df['hedge_return'].tolist()
    
    # 创建净值曲线图
    net_value_chart = (
        Line(init_opts=opts.InitOpts(theme=ThemeType.LIGHT, width="1200px", height="400px"))
        .add_xaxis(xaxis_data=dates)
        .add_yaxis(
            series_name="对冲组合净值",
            y_axis=hedge_net_value,
            label_opts=opts.LabelOpts(is_show=False),
            linestyle_opts=opts.LineStyleOpts(width=2),
            itemstyle_opts=opts.ItemStyleOpts(color="#1890ff"),
        )
        .set_global_opts(
            title_opts=opts.TitleOpts(title="对冲组合累积净值曲线"),
            tooltip_opts=opts.TooltipOpts(trigger="axis"),
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
                name="净值 (亿元)",
                type_="value",
                splitline_opts=opts.SplitLineOpts(is_show=True),
            ),
        )
    )
    
    # 创建收益率对比图
    returns_chart = (
        Line(init_opts=opts.InitOpts(theme=ThemeType.LIGHT, width="1200px", height="400px"))
        .add_xaxis(xaxis_data=dates)
        .add_yaxis(
            series_name="回测策略收益率",
            y_axis=backtest_returns,
            label_opts=opts.LabelOpts(is_show=False),
            linestyle_opts=opts.LineStyleOpts(width=1.5),
            itemstyle_opts=opts.ItemStyleOpts(color="#ff4d4f"),
        )
        .add_yaxis(
            series_name="指数收益率",
            y_axis=index_returns,
            label_opts=opts.LabelOpts(is_show=False),
            linestyle_opts=opts.LineStyleOpts(width=1.5),
            itemstyle_opts=opts.ItemStyleOpts(color="#52c41a"),
        )
        .add_yaxis(
            series_name="对冲收益率",
            y_axis=hedge_returns,
            label_opts=opts.LabelOpts(is_show=False),
            linestyle_opts=opts.LineStyleOpts(width=1.5),
            itemstyle_opts=opts.ItemStyleOpts(color="#1890ff"),
        )
        .set_global_opts(
            title_opts=opts.TitleOpts(title="日收益率对比"),
            tooltip_opts=opts.TooltipOpts(trigger="axis"),
            legend_opts=opts.LegendOpts(pos_left="center"),
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
        )
    )
    
    # 组合图表
    grid_chart = Grid(init_opts=opts.InitOpts(width="1200px", height="900px"))
    grid_chart.add(
        net_value_chart,
        grid_opts=opts.GridOpts(pos_bottom="55%", height="40%"),
    )
    grid_chart.add(
        returns_chart,
        grid_opts=opts.GridOpts(pos_top="55%", height="40%"),
    )
    
    # 保存图表
    if output_file:
        if output_file.endswith('.html'):
            grid_chart.render(output_file)
            print(f"图表已保存到: {output_file}")
        else:
            # 如果不是HTML文件，则生成HTML文件
            html_file = output_file.replace('.png', '.html')
            grid_chart.render(html_file)
            print(f"图表已保存到: {html_file}")
    else:
        # 默认文件名
        output_file = "hedge_results.html"
        grid_chart.render(output_file)
        print(f"图表已保存到: {output_file}")


def analyze_hedge_performance(hedge_data_file):
    """
    分析对冲组合表现
    
    Args:
        hedge_data_file: 对冲数据文件路径
    """
    # 读取对冲数据
    with open(hedge_data_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # 提取数据
    records = data['data']
    df = pd.DataFrame(records)
    df['date'] = pd.to_datetime(df['date'])
    
    # 计算统计指标
    hedge_returns = df['hedge_return'].values
    backtest_returns = df['backtest_return'].values
    index_returns = df['index_return'].values
    
    # 年化收益率
    trading_days = len(df)
    years = trading_days / 252  # 假设一年有252个交易日
    
    hedge_annual_return = (1 + df['hedge_net_value'].iloc[-1] / 1e8) ** (1/years) - 1
    backtest_annual_return = (1 + df['backtest_return'].mean() / 100) ** 252 - 1
    index_annual_return = (1 + df['index_return'].mean() / 100) ** 252 - 1
    
    # 年化波动率
    hedge_volatility = df['hedge_return'].std() * (252 ** 0.5)
    backtest_volatility = df['backtest_return'].std() * (252 ** 0.5)
    index_volatility = df['index_return'].std() * (252 ** 0.5)
    
    # 夏普比率 (假设无风险利率为3%)
    risk_free_rate = 0.03
    hedge_sharpe = (hedge_annual_return - risk_free_rate) / hedge_volatility
    backtest_sharpe = (backtest_annual_return - risk_free_rate) / backtest_volatility
    index_sharpe = (index_annual_return - risk_free_rate) / index_volatility
    
    # 最大回撤
    def calculate_max_drawdown(values):
        peak = values.expanding().max()
        drawdown = (values - peak) / peak
        return drawdown.min()
    
    hedge_max_drawdown = calculate_max_drawdown(df['hedge_net_value'])
    
    # 打印统计结果
    print("\n对冲组合表现分析:")
    print("=" * 50)
    print(f"交易天数: {trading_days} ({years:.2f}年)")
    print(f"\n年化收益率:")
    print(f"  对冲组合: {hedge_annual_return:.2%}")
    print(f"  回测策略: {backtest_annual_return:.2%}")
    print(f"  指数: {index_annual_return:.2%}")
    
    print(f"\n年化波动率:")
    print(f"  对冲组合: {hedge_volatility:.2%}")
    print(f"  回测策略: {backtest_volatility:.2%}")
    print(f"  指数: {index_volatility:.2%}")
    
    print(f"\n夏普比率 (无风险利率={risk_free_rate:.0%}):")
    print(f"  对冲组合: {hedge_sharpe:.2f}")
    print(f"  回测策略: {backtest_sharpe:.2f}")
    print(f"  指数: {index_sharpe:.2f}")
    
    print(f"\n最大回撤:")
    print(f"  对冲组合: {hedge_max_drawdown:.2%}")


def main():
    parser = argparse.ArgumentParser(description='对冲数据计算示例')
    parser.add_argument('-b', '--backtest', required=True, help='回测数据文件路径')
    parser.add_argument('-i', '--index', required=True, help='指数数据文件路径')
    parser.add_argument('-p', '--position', help='持仓数据文件路径（可选）')
    parser.add_argument('-o', '--output', required=True, help='输出对冲数据文件路径')
    parser.add_argument('--hedge-ratio', type=float, default=1.0, help='对冲比例（默认1.0）')
    parser.add_argument('--plot', help='生成图表文件路径（可选）')
    parser.add_argument('--analyze', action='store_true', help='分析对冲组合表现')
    
    args = parser.parse_args()
    
    # 检查输入文件是否存在
    if not os.path.exists(args.backtest):
        print(f"错误: 回测数据文件不存在: {args.backtest}")
        return 1
    
    if not os.path.exists(args.index):
        print(f"错误: 指数数据文件不存在: {args.index}")
        return 1
    
    if args.position and not os.path.exists(args.position):
        print(f"错误: 持仓数据文件不存在: {args.position}")
        return 1
    
    # 计算对冲数据
    try:
        print("正在计算对冲数据...")
        calculate_hedge_data(
            backtest_file=args.backtest,
            position_file=args.position,
            index_file=args.index,
            output_file=args.output,
            hedge_ratio=args.hedge_ratio
        )
        print(f"对冲数据已保存到: {args.output}")
    except Exception as e:
        print(f"计算对冲数据时出错: {e}")
        return 1
    
    # 生成图表
    if args.plot:
        try:
            print("正在生成图表...")
            plot_hedge_results(args.output, args.plot)
        except Exception as e:
            print(f"生成图表时出错: {e}")
    
    # 分析表现
    if args.analyze:
        try:
            print("正在分析对冲组合表现...")
            analyze_hedge_performance(args.output)
        except Exception as e:
            print(f"分析表现时出错: {e}")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())