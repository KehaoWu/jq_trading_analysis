#!/usr/bin/env python3
"""
根据聚宽回测结果计算日收益率并保存为CSV文件

使用方法:
    python main.py calculate_daily_returns <回测数据文件路径> [输出CSV文件路径]

示例:
    python main.py calculate_daily_returns backtest_data/模拟盘-T1卖出_028edf215c65d9b365fa51bb27b9788b_20200102_20250721_daily.jsonl
    
    python main.py calculate_daily_returns backtest_data/模拟盘-T1卖出_028edf215c65d9b365fa51bb27b9788b_20200102_20250721_daily.jsonl output/daily_returns.csv
"""

import os
import sys
import argparse

# 添加项目根目录到Python路径
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from libs.data_loader import load_backtest_data
from libs.returns_calculator import calculate_daily_returns, calculate_daily_returns_to_csv
from libs.utils import parse_date_string


def main():
    """计算日收益率的主函数"""
    # 解析命令行参数
    parser = argparse.ArgumentParser(description='根据聚宽回测结果计算日收益率并保存为CSV文件')
    parser.add_argument('backtest_file', help='聚宽回测数据文件路径(JSONL格式)')
    parser.add_argument('output_file', nargs='?', help='输出CSV文件路径(可选，默认为回测文件同目录下的daily_returns.csv)')
    
    args = parser.parse_args()
    
    # 检查回测文件是否存在
    if not os.path.exists(args.backtest_file):
        print(f"错误: 回测数据文件不存在: {args.backtest_file}")
        sys.exit(1)
    
    # 如果未指定输出文件，则使用默认路径
    if not args.output_file:
        backtest_dir = os.path.dirname(args.backtest_file)
        backtest_name = os.path.splitext(os.path.basename(args.backtest_file))[0]
        args.output_file = os.path.join(backtest_dir, f"{backtest_name}_daily_returns.csv")
    
    # 确保输出目录存在
    output_dir = os.path.dirname(args.output_file)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)
    
    try:
        # 加载回测数据
        print(f"正在加载回测数据文件: {args.backtest_file}")
        backtest_data = load_backtest_data(args.backtest_file)
        
        if not backtest_data:
            print("错误: 回测数据文件为空或格式不正确")
            sys.exit(1)
        
        # 转换回测数据为累积收益率数据
        cumulative_returns_data = []
        
        # 按日期排序
        backtest_data.sort(key=lambda x: x.get('date', ''))
        
        # 提取累积收益率
        for item in backtest_data:
            date = parse_date_string(item.get('date', ''))
            overall_return = item.get('data', {}).get('overallReturn', {}).get('records', [{}])[0].get('value', 0)
            cumulative_returns_data.append({
                'date': date,
                'cumulative_return': overall_return
            })
        
        # 计算日收益率
        print("正在计算日收益率...")
        daily_returns_data = calculate_daily_returns(cumulative_returns_data)
        
        # 导出为CSV
        calculate_daily_returns_to_csv(daily_returns_data, args.output_file, 'date', 'daily_return')
        
        # 打印结果信息
        print(f"成功计算日收益率并保存到: {args.output_file}")
        print(f"数据行数: {len(daily_returns_data)}")
        
        # 计算统计信息
        import pandas as pd
        df = pd.DataFrame(daily_returns_data)
        print("\n策略日收益率统计信息:")
        print(df['daily_return'].describe())
        
        print("\n处理完成!")
        
    except Exception as e:
        print(f"处理失败: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()