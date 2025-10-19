#!/usr/bin/env python3
"""
清理脚本 - 删除不必要的临时文件和测试脚本
"""

import os
import glob
import argparse
from pathlib import Path

def clean_temp_files():
    """清理临时文件"""
    print("清理临时文件...")
    
    # 定义要删除的临时文件模式
    temp_patterns = [
        "**/__pycache__",
        "**/*.pyc",
        "**/*.pyo",
        "**/*.pyd",
        "**/.pytest_cache",
        "**/.coverage",
        "**/*.log",
        "**/*.tmp"
    ]
    
    deleted_count = 0
    for pattern in temp_patterns:
        for file_path in glob.glob(pattern, recursive=True):
            try:
                if os.path.isfile(file_path):
                    os.remove(file_path)
                    print(f"  删除文件: {file_path}")
                    deleted_count += 1
                elif os.path.isdir(file_path):
                    import shutil
                    shutil.rmtree(file_path)
                    print(f"  删除目录: {file_path}")
                    deleted_count += 1
            except Exception as e:
                print(f"  删除失败 {file_path}: {e}")
    
    print(f"临时文件清理完成，共删除 {deleted_count} 个文件/目录")

def clean_test_scripts():
    """清理测试脚本"""
    print("\n清理测试脚本...")
    
    # 定义要删除的测试脚本
    test_scripts = [
        "test_hedge.py",
        "test_temp.py",
        "temp_test.py"
    ]
    
    deleted_count = 0
    for script in test_scripts:
        if os.path.exists(script):
            try:
                os.remove(script)
                print(f"  删除测试脚本: {script}")
                deleted_count += 1
            except Exception as e:
                print(f"  删除失败 {script}: {e}")
    
    print(f"测试脚本清理完成，共删除 {deleted_count} 个文件")

def clean_old_hedge_data(days=7):
    """清理旧的对冲数据文件"""
    print(f"\n清理 {days} 天前的对冲数据文件...")
    
    import time
    
    hedge_data_dir = Path("hedge_data")
    if not hedge_data_dir.exists():
        print("  hedge_data 目录不存在")
        return
    
    current_time = time.time()
    cutoff_time = current_time - (days * 24 * 60 * 60)
    
    deleted_count = 0
    for file_path in hedge_data_dir.glob("*.json"):
        if file_path.stat().st_mtime < cutoff_time:
            try:
                os.remove(file_path)
                print(f"  删除旧数据文件: {file_path}")
                deleted_count += 1
            except Exception as e:
                print(f"  删除失败 {file_path}: {e}")
    
    print(f"旧对冲数据清理完成，共删除 {deleted_count} 个文件")

def main():
    parser = argparse.ArgumentParser(description="清理项目中的临时文件和测试脚本")
    parser.add_argument("--temp", action="store_true", help="清理临时文件")
    parser.add_argument("--test", action="store_true", help="清理测试脚本")
    parser.add_argument("--hedge", type=int, metavar="DAYS", help="清理指定天数前的对冲数据文件")
    parser.add_argument("--all", action="store_true", help="执行所有清理操作")
    
    args = parser.parse_args()
    
    if not any([args.temp, args.test, args.hedge, args.all]):
        parser.print_help()
        return
    
    if args.all or args.temp:
        clean_temp_files()
    
    if args.all or args.test:
        clean_test_scripts()
    
    if args.hedge:
        clean_old_hedge_data(args.hedge)
    
    print("\n清理完成！")

if __name__ == "__main__":
    main()