#!/usr/bin/env python3
"""
对冲数据计算示例脚本

该脚本演示如何使用hedge_data_calc模块计算对冲数据。
"""

import argparse
import os
import sys

# 添加libs目录到Python路径
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'libs'))

from libs.hedge_data_calc import calculate_hedge_data
from libs.format_converter import generate_hedge_backtest_format, generate_hedge_position_format


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='对冲数据计算示例')
    parser.add_argument('--backtest', '-b', required=True, help='回测数据文件路径')
    parser.add_argument('--position', '-p', help='持仓数据文件路径(可选)')
    parser.add_argument('--index', '-i', required=True, help='指数数据文件路径')
    parser.add_argument('--ratio', '-r', type=float, default=1.0, help='对冲比例(默认: 1.0)')
    parser.add_argument('--output', '-o', help='输出文件路径(可选)')
    parser.add_argument('--format', '-f', choices=['json', 'backtest', 'position'], 
                       default='json', help='输出格式(默认: json)')
    
    args = parser.parse_args()
    
    # 验证输入文件
    if not os.path.exists(args.backtest):
        print(f"错误: 回测数据文件不存在: {args.backtest}")
        sys.exit(1)
    
    if not os.path.exists(args.index):
        print(f"错误: 指数数据文件不存在: {args.index}")
        sys.exit(1)
    
    if args.position and not os.path.exists(args.position):
        print(f"错误: 持仓数据文件不存在: {args.position}")
        sys.exit(1)
    
    # 计算对冲数据
    try:
        print("正在计算对冲数据...")
        hedge_data = calculate_hedge_data(
            backtest_file=args.backtest,
            position_file=args.position,
            index_file=args.index,
            hedge_ratio=args.ratio,
            output_file=args.output if args.format == 'json' else None
        )
        
        print(f"成功计算对冲数据，共 {len(hedge_data['data'])} 条记录")
        
        # 根据指定格式输出数据
        if args.format == 'backtest':
            output_file = args.output or f"{args.backtest}.hedge.backtest.jsonl"
            generate_hedge_backtest_format(hedge_data, output_file)
            print(f"聚宽回测格式数据已保存到: {output_file}")
        elif args.format == 'position':
            output_file = args.output or f"{args.backtest}.hedge.position.json"
            generate_hedge_position_format(hedge_data, output_file)
            print(f"聚宽持仓格式数据已保存到: {output_file}")
        else:
            if not args.output:
                print("未指定输出文件，仅显示前5条记录:")
                for i, item in enumerate(hedge_data['data'][:5]):
                    print(f"{i+1}. 日期: {item['date']}, 对冲收益率: {item['hedge_return']:.4f}%")
            else:
                print(f"JSON格式数据已保存到: {args.output}")
        
        # 显示统计信息
        returns = [item['hedge_return'] for item in hedge_data['data']]
        avg_return = sum(returns) / len(returns)
        max_return = max(returns)
        min_return = min(returns)
        
        print(f"\n统计信息:")
        print(f"平均对冲收益率: {avg_return:.4f}%")
        print(f"最大对冲收益率: {max_return:.4f}%")
        print(f"最小对冲收益率: {min_return:.4f}%")
        
    except Exception as e:
        print(f"计算对冲数据时出错: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()