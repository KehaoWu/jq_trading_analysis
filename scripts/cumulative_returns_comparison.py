#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
累积收益曲线对比脚本包装器

该脚本是bt_scripts/cumulative_returns_comparison.py的包装器，
用于通过main.py统一调用。
"""

import os
import sys
from pathlib import Path

# 添加项目根目录到系统路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# 导入bt_scripts中的实际实现
from bt_scripts.cumulative_returns_comparison import CumulativeReturnsComparator


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='累积收益曲线对比分析')
    parser.add_argument('--csv_file', 
                       default='/home/wukehao/Projects/jq_trading_analysis/backtest_data/naive_top200/BT-T2/cumulative.csv',
                       help='CSV格式的累积收益数据文件路径')
    parser.add_argument('--json_file',
                       default='/home/wukehao/Projects/jq_trading_analysis/backtest_data/naive_top200/朴素的Top200-0930_daily_return_de88fe2ab6a36006bfa2e87a5422808a.json',
                       help='JSON格式的回测数据文件路径')
    parser.add_argument('--csv_name',
                       default='BT-T2',
                       help='CSV数据的显示名称')
    parser.add_argument('--json_name',
                       default='朴素Top200-0930',
                       help='JSON数据的显示名称')
    parser.add_argument('--output_file',
                       default='cumulative_returns_comparison.html',
                       help='输出HTML文件名')
    parser.add_argument('--output_dir',
                       default='/home/wukehao/Projects/jq_trading_analysis/bt_scripts/output',
                       help='输出目录')
    
    args = parser.parse_args()
    
    # 检查文件是否存在
    if not os.path.exists(args.csv_file):
        print(f"错误: CSV文件不存在: {args.csv_file}")
        sys.exit(1)
        
    if not os.path.exists(args.json_file):
        print(f"错误: JSON文件不存在: {args.json_file}")
        sys.exit(1)
    
    # 创建对比器
    comparator = CumulativeReturnsComparator(args.output_dir)
    
    # 执行对比
    html_path = comparator.compare_returns(
        csv_file=args.csv_file,
        json_file=args.json_file,
        csv_name=args.csv_name,
        json_name=args.json_name,
        output_file=args.output_file
    )
    
    print(f"\n可以在浏览器中打开以下文件查看对比结果:")
    print(f"file://{html_path}")


if __name__ == "__main__":
    main()