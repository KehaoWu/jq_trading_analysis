#!/usr/bin/env python3
"""
聚宽交易分析项目主入口

该脚本提供了对整个项目功能的统一入口，包括：
- calculate_daily_returns: 计算日收益率
- combine_two_backtests: 合并两个不同时间段的回测数据
- backtest_hedge_plot: 回测对冲数据计算和可视化
- backtest_vis: 回测结果可视化
- hedge_analysis_visualization: 对冲分析可视化（自动识别文件并生成完整分析）
- cleanup: 清理项目中的临时文件和测试脚本

使用方法:
    python main.py <功能名称> [参数...]

示例:
    python main.py calculate_daily_returns backtest_data/模拟盘-T1卖出_028edf215c65d9b365fa51bb27b9788b_20200102_20250721_daily.jsonl
    
    python main.py combine_two_backtests
    
    python main.py backtest_hedge_plot
    
    python main.py backtest_vis output/merged_backtest.jsonl
    
    python main.py hedge_analysis_visualization --input_dir backtest_data/ex_tm1_top30 --index zz500
"""

import os
import sys
import argparse
import importlib

# 添加项目根目录到Python路径
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)


def run_script(script_name, args):
    """运行指定的脚本"""
    try:
        # 导入脚本模块
        module = importlib.import_module(f"scripts.{script_name}")
        
        # 获取脚本的主函数
        if hasattr(module, 'main'):
            func = getattr(module, 'main')
        elif hasattr(module, script_name):
            func = getattr(module, script_name)
        else:
            print(f"错误: 脚本 {script_name} 中没有找到主函数")
            sys.exit(1)
        
        # 设置sys.argv以传递参数
        sys.argv = [f"main.py {script_name}"] + args
        
        # 运行脚本
        func()
        
    except ImportError as e:
        print(f"错误: 无法导入脚本 {script_name}: {str(e)}")
        sys.exit(1)
    except Exception as e:
        print(f"错误: 运行脚本 {script_name} 时出错: {str(e)}")
        sys.exit(1)


def main():
    """主函数"""
    # 如果没有提供参数或参数是--help/-h，显示帮助信息
    if len(sys.argv) == 1 or sys.argv[1] in ('--help', '-h'):
        print("聚宽交易分析项目主入口\n")
        print("使用方法: python main.py <功能名称> [参数...]\n")
        print("可用命令:")
        print("  calculate_daily_returns - 计算日收益率")
        print("  combine_two_backtests - 合并两个不同时间段的回测数据")
        print("  backtest_hedge_plot - 回测对冲数据计算和可视化")
        print("  backtest_vis - 回测结果可视化")
        print("  cleanup - 清理项目中的临时文件和测试脚本")
        print("\n使用 'python main.py <功能名称> --help' 查看具体功能的详细帮助信息")
        return
    
    # 获取命令和参数
    command = sys.argv[1]
    args = sys.argv[2:] if len(sys.argv) > 2 else []
    
    # 运行指定的脚本
    run_script(command, args)


if __name__ == "__main__":
    main()
