"""聚宽回测数据下载器

根据PRD要求，实现以下功能：
1. 使用 get_backtest 方法获取指定回测的信息
2. 使用 gt.get_balances() 获取持仓比例数据
3. 使用 get_results() 获取回测结果
4. 使用 get_positions() 获取持仓详情
5. 使用 get_orders() 获取订单数据
6. 将数据保存为JSON格式，文件命名规范：
   - {backtest_name}_position_ratio_{backtest_id}.json (余额数据)
   - {backtest_name}_position_details_{backtest_id}.json (持仓详情)
   - {backtest_name}_daily_return_{backtest_id}.json (回测结果)
   - {backtest_name}_orders_{backtest_id}.json (订单数据)

注意：此代码需要在聚宽环境中运行
"""

import pandas as pd
import json
import os
import re
import tarfile
import traceback
import gc  # 添加垃圾回收模块
import psutil  # 添加系统资源监控模块
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any
from tqdm import tqdm


def get_memory_usage() -> Dict[str, float]:
    """
    获取当前内存使用情况
    
    Returns:
    dict: 包含内存使用信息的字典
    """
    try:
        process = psutil.Process()
        memory_info = process.memory_info()
        memory_percent = process.memory_percent()
        
        return {
            "rss_mb": memory_info.rss / 1024 / 1024,  # 物理内存使用量(MB)
            "vms_mb": memory_info.vms / 1024 / 1024,  # 虚拟内存使用量(MB)
            "percent": memory_percent,                 # 内存使用百分比
            "available_mb": psutil.virtual_memory().available / 1024 / 1024  # 可用内存(MB)
        }
    except ImportError:
        # 如果psutil不可用，返回空字典
        return {}


def force_garbage_collection(verbose: bool = False) -> None:
    """
    强制执行垃圾回收并可选择性地显示回收信息
    
    Parameters:
    verbose (bool): 是否显示垃圾回收详细信息
    """
    if verbose:
        before_memory = get_memory_usage()
        
    # 执行三代垃圾回收
    collected = gc.collect()
    
    if verbose and before_memory:
        after_memory = get_memory_usage()
        memory_freed = before_memory.get("rss_mb", 0) - after_memory.get("rss_mb", 0)
        print(f"垃圾回收完成: 回收了 {collected} 个对象，释放了 {memory_freed:.2f} MB 内存")
        print(f"当前内存使用: {after_memory.get('rss_mb', 0):.2f} MB ({after_memory.get('percent', 0):.1f}%)")


def save_backtest_balances(backtest_id: str, output_dir: str = "data") -> Optional[Dict]:
    """
    获取回测的账户余额数据，保存为JSON文件，并计算仓位比例
    
    Parameters:
    backtest_id (str): 聚宽回测ID
    output_dir (str): 输出目录，默认为"data"
    
    Returns:
    dict: 处理后的数据字典，如果失败返回None
    """
    try:
        # 确保输出目录存在
        os.makedirs(output_dir, exist_ok=True)
        
        # 获取回测对象
        gt = get_backtest(backtest_id)
        
        # 获取回测名称
        params = gt.get_params()
        backtest_name = params.get('name', 'unknown').replace(' ', '_')
        
        # 获取余额数据
        results = gt.get_balances()
        
        if not results:
            print(f"未获取到回测 {backtest_id} 的余额数据")
            return None
        
        # 准备保存的数据结构
        output_data = {
            "backtest_id": backtest_id,
            "backtest_name": backtest_name,
            "data_type": "balances",
            "download_time": datetime.now().isoformat(),
            "balances": [],
            "position_analysis": {}
        }
        
        # 处理每个时间点的余额数据并计算仓位比例
        for i, balance in enumerate(results):
            balance_dict = {
                "time": balance['time'],
                "aval_cash": balance['aval_cash'],
                "total_value": balance['total_value'],
                "cash": balance['cash'],
                "net_value": balance['net_value']
            }
            
            # 计算仓位比例：1 - (可用现金/总资产)
            if balance['total_value'] > 0:
                position_ratio = 1 - (balance['aval_cash'] / balance['total_value'])
                balance_dict["position_ratio"] = round(position_ratio, 4)
            else:
                balance_dict["position_ratio"] = 0
            
            output_data["balances"].append(balance_dict)
            
            # 记录首尾数据用于分析
            if i == 0:
                output_data["position_analysis"]["first_position"] = balance_dict["position_ratio"]
            if i == len(results) - 1:
                output_data["position_analysis"]["last_position"] = balance_dict["position_ratio"]
                output_data["position_analysis"]["final_net_value"] = balance['net_value']
        
        # 计算平均仓位比例
        if output_data["balances"]:
            avg_position = sum(b["position_ratio"] for b in output_data["balances"]) / len(output_data["balances"])
            output_data["position_analysis"]["average_position"] = round(avg_position, 4)
        
        # 保存为JSON文件 - 使用新的命名格式
        filename = os.path.join(output_dir, f"{backtest_name}_position_ratio_{backtest_id}.json")
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=2, ensure_ascii=False, default=str)
        
        print(f"回测余额数据已保存到: {filename}")
        print(f"初始仓位比例: {output_data['position_analysis']['first_position']:.2%}")
        print(f"最终仓位比例: {output_data['position_analysis']['last_position']:.2%}")
        print(f"平均仓位比例: {output_data['position_analysis']['average_position']:.2%}")
        print(f"最终净值: {output_data['position_analysis']['final_net_value']:,.2f}")
        
        # 内存优化：及时释放大变量
        del results  # 释放原始余额数据
        del params   # 释放参数数据
        force_garbage_collection(verbose=True)  # 强制垃圾回收并显示详细信息
        
        return output_data
        
    except Exception as e:
        traceback.print_exc()
        print(f"处理回测余额数据时出错 (ID: {backtest_id}): {e}")
        return None


def save_backtest_positions(backtest_id: str, output_dir: str = "data", use_quarterly: bool = True) -> Optional[Dict]:
    """
    获取回测的持仓详情数据，保存为JSON文件
    支持分季度下载以防止内存溢出，每季度数据下载后立即保存到临时文件
    
    Parameters:
    backtest_id (str): 聚宽回测ID
    output_dir (str): 输出目录，默认为"data"
    use_quarterly (bool): 是否使用分季度下载，默认为True
    
    Returns:
    dict: 处理后的数据字典，如果失败返回None
    """
    try:
        # 确保输出目录存在
        os.makedirs(output_dir, exist_ok=True)
        
        # 获取回测对象
        gt = get_backtest(backtest_id)
        
        # 获取回测名称
        params = gt.get_params()
        backtest_name = params.get('name', 'unknown').replace(' ', '_')
        
        if not use_quarterly:
            # 传统方式：一次性获取所有数据
            positions = gt.get_positions()
            
            if not positions:
                print(f"未获取到回测 {backtest_id} 的持仓数据")
                return None
            
            all_positions = positions
            
        else:
            # 分季度下载方式，使用 JSONL 格式进行流式写入
            # 首先获取回测结果以确定日期范围
            results = gt.get_results()
            if not results:
                print(f"无法获取回测 {backtest_id} 的结果数据，无法确定日期范围")
                return None
            
            start_date = str(results[0]['time']).split(' ')[0]  # 提取日期部分
            end_date = str(results[-1]['time']).split(' ')[0]
            
            print(f"回测日期范围: {start_date} 至 {end_date}")
            
            # 生成季度日期范围
            date_ranges = generate_quarterly_date_ranges(start_date, end_date)
            print(f"将分 {len(date_ranges)} 个季度下载持仓数据")
            
            # 准备最终文件路径（使用 JSONL 格式）
            filename = os.path.join(output_dir, f"{backtest_name}_position_details_{backtest_id}.jsonl")
            
            total_records = 0
            
            # 清空文件（如果存在）
            with open(filename, 'w', encoding='utf-8') as f:
                pass  # 创建空文件
            
            # 使用进度条显示下载进度
            for i, (quarter_start, quarter_end) in enumerate(tqdm(date_ranges, desc="下载持仓数据")):
                try:
                    print(f"\n下载第 {i+1}/{len(date_ranges)} 个季度: {quarter_start} 至 {quarter_end}")
                    
                    # 获取该季度的持仓数据
                    quarter_positions = gt.get_positions(start_date=quarter_start, end_date=quarter_end)
                    
                    if quarter_positions:
                        total_records += len(quarter_positions)
                        
                        # 使用 JSONL 格式追加写入，每条记录一行
                        with open(filename, 'a', encoding='utf-8') as f:
                            for pos in quarter_positions:
                                # 为每条记录添加元数据
                                f.write(json.dumps(pos, ensure_ascii=False, default=str) + '\n')
                        
                        print(f"获取到 {len(quarter_positions)} 条持仓记录，已追加到 JSONL 文件")
                        
                        # 释放内存
                        del quarter_positions
                        force_garbage_collection()  # 每季度处理完后强制垃圾回收
                    else:
                        print(f"该季度无持仓数据")
                        
                except Exception as e:
                    print(f"获取季度 {quarter_start} 至 {quarter_end} 的持仓数据时出错: {e}")
                    continue
            
            if total_records == 0:
                print(f"未获取到回测 {backtest_id} 的任何持仓数据")
                return None
        
            del params         # 释放参数数据
            force_garbage_collection(verbose=True)  # 强制垃圾回收并显示详细信息
                    
    except Exception as e:
        traceback.print_exc()
        print(f"处理回测持仓数据时出错 (ID: {backtest_id}): {e}")
        return None


def save_backtest_results(backtest_id: str, output_dir: str = "data") -> Optional[Dict]:
    """
    获取回测结果数据，保存为JSON文件
    
    Parameters:
    backtest_id (str): 聚宽回测ID
    output_dir (str): 输出目录，默认为"data"
    
    Returns:
    dict: 处理后的数据字典，如果失败返回None
    """
    try:
        # 确保输出目录存在
        os.makedirs(output_dir, exist_ok=True)
        
        # 获取回测对象
        gt = get_backtest(backtest_id)
        
        # 获取回测名称
        params = gt.get_params()
        backtest_name = params.get('name', 'unknown').replace(' ', '_')
        
        # 获取回测结果
        results = gt.get_results()
        
        if not results:
            print(f"未获取到回测 {backtest_id} 的结果数据")
            return None
        
        # 准备保存的数据结构
        output_data = {
            "backtest_id": backtest_id,
            "backtest_name": backtest_name,
            "data_type": "results",
            "download_time": datetime.now().isoformat(),
            "results": results,
            "total_records": len(results)
        }
        
        # 保存为JSON文件
        filename = os.path.join(output_dir, f"{backtest_name}_daily_return_{backtest_id}.json")
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=2, ensure_ascii=False, default=str)
        
        print(f"回测结果数据已保存到: {filename}")
        print(f"结果记录数: {len(results)}")
        
        # 内存优化：及时释放大变量
        del results  # 释放原始结果数据
        del params   # 释放参数数据
        force_garbage_collection(verbose=True)  # 强制垃圾回收并显示详细信息
        
        return output_data
        
    except Exception as e:
        traceback.print_exc()
        print(f"处理回测结果数据时出错 (ID: {backtest_id}): {e}")
        return None


def save_backtest_orders(backtest_id: str, output_dir: str = "data", use_quarterly: bool = True) -> Optional[Dict]:
    """
    获取回测的订单数据，保存为JSON文件
    支持分季度下载以防止内存溢出，每季度数据下载后立即保存到临时文件
    
    Parameters:
    backtest_id (str): 聚宽回测ID
    output_dir (str): 输出目录，默认为"data"
    use_quarterly (bool): 是否使用分季度下载，默认为True
    
    Returns:
    dict: 处理后的数据字典，如果失败返回None
    """
    try:
        # 确保输出目录存在
        os.makedirs(output_dir, exist_ok=True)
        
        # 获取回测对象
        gt = get_backtest(backtest_id)
        
        # 获取回测名称
        params = gt.get_params()
        backtest_name = params.get('name', 'unknown').replace(' ', '_')
        
        if not use_quarterly:
            # 传统方式：一次性获取所有数据
            orders = gt.get_orders()
            
            if not orders:
                print(f"未获取到回测 {backtest_id} 的订单数据")
                return None
            
            all_orders = orders
            
        else:
            # 分季度下载方式，使用 JSONL 格式进行流式写入
            # 首先获取回测结果以确定日期范围
            results = gt.get_results()
            if not results:
                print(f"无法获取回测 {backtest_id} 的结果数据，无法确定日期范围")
                return None
            
            start_date = str(results[0]['time']).split(' ')[0]  # 提取日期部分
            end_date = str(results[-1]['time']).split(' ')[0]
            
            print(f"回测日期范围: {start_date} 至 {end_date}")
            
            # 生成季度日期范围
            date_ranges = generate_quarterly_date_ranges(start_date, end_date)
            print(f"将分 {len(date_ranges)} 个季度下载订单数据")
            
            # 准备最终文件路径（使用 JSONL 格式）
            filename = os.path.join(output_dir, f"{backtest_name}_orders_{backtest_id}.jsonl")
            
            total_orders = 0
            
            # 清空文件（如果存在）
            with open(filename, 'w', encoding='utf-8') as f:
                pass  # 创建空文件
            
            # 使用进度条显示下载进度
            for i, (quarter_start, quarter_end) in enumerate(tqdm(date_ranges, desc="下载订单数据")):
                try:
                    print(f"\n下载第 {i+1}/{len(date_ranges)} 个季度: {quarter_start} 至 {quarter_end}")
                    
                    # 获取该季度的订单数据
                    quarter_orders = gt.get_orders(start_date=quarter_start, end_date=quarter_end)
                    
                    if quarter_orders:
                        total_orders += len(quarter_orders)
                        
                        # 使用 JSONL 格式追加写入，每条记录一行
                        with open(filename, 'a', encoding='utf-8') as f:
                            for order in quarter_orders:
                                # 为每条记录添加元数据
                                f.write(json.dumps(order, ensure_ascii=False, default=str) + '\n')
                        
                        print(f"获取到 {len(quarter_orders)} 条订单记录，已追加到 JSONL 文件")
                        
                        # 释放内存
                        del quarter_orders
                        force_garbage_collection()  # 每季度处理完后强制垃圾回收
                    else:
                        print(f"该季度无订单数据")
                        
                except Exception as e:
                    print(f"获取季度 {quarter_start} 至 {quarter_end} 的订单数据时出错: {e}")
                    continue
            
            if total_orders == 0:
                print(f"未获取到回测 {backtest_id} 的任何订单数据")
                return None
        
        # 根据下载方式区分保存逻辑
        
    except Exception as e:
        traceback.print_exc()
        print(f"处理回测订单数据时出错 (ID: {backtest_id}): {e}")
        return None


def _merge_temp_files(temp_files: List[str]) -> List[Dict]:
    """
    合并多个临时JSON文件的数据
    
    Parameters:
    temp_files (list): 临时文件路径列表
    
    Returns:
    list: 合并后的数据列表
    """
    merged_data = []
    
    for temp_file in temp_files:
        try:
            with open(temp_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if isinstance(data, list):
                    merged_data.extend(data)
                else:
                    merged_data.append(data)
        except Exception as e:
            print(f"读取临时文件 {temp_file} 时出错: {e}")
            continue
        finally:
            # 每个文件处理完后释放内存
            if 'data' in locals():
                del data
            force_garbage_collection()
    
    return merged_data


def _cleanup_temp_files(temp_files: List[str], temp_dir: str) -> None:
    """
    清理临时文件和临时目录
    
    Parameters:
    temp_files (list): 临时文件路径列表
    temp_dir (str): 临时目录路径
    """
    # 删除临时文件
    for temp_file in temp_files:
        try:
            if os.path.exists(temp_file):
                os.remove(temp_file)
        except Exception as e:
            print(f"删除临时文件 {temp_file} 时出错: {e}")
    
    # 删除临时目录
    try:
        if os.path.exists(temp_dir):
            os.rmdir(temp_dir)
    except Exception as e:
        print(f"删除临时目录 {temp_dir} 时出错: {e}")


def generate_quarterly_date_ranges(start_date: str, end_date: str) -> List[tuple]:
    """
    生成季度日期范围列表，用于分批下载数据
    
    Parameters:
    start_date (str): 开始日期，格式为 'YYYY-MM-DD'
    end_date (str): 结束日期，格式为 'YYYY-MM-DD'
    
    Returns:
    list: 包含(start_date, end_date)元组的列表
    """
    try:
        start = datetime.strptime(start_date, '%Y-%m-%d')
        end = datetime.strptime(end_date, '%Y-%m-%d')
        
        date_ranges = []
        current_start = start
        
        while current_start < end:
            # 计算当前季度的结束日期（3个月后）
            if current_start.month <= 3:
                quarter_end = datetime(current_start.year, 3, 31)
            elif current_start.month <= 6:
                quarter_end = datetime(current_start.year, 6, 30)
            elif current_start.month <= 9:
                quarter_end = datetime(current_start.year, 9, 30)
            else:
                quarter_end = datetime(current_start.year, 12, 31)
            
            # 确保不超过总的结束日期
            actual_end = min(quarter_end, end)
            
            date_ranges.append((
                current_start.strftime('%Y-%m-%d'),
                actual_end.strftime('%Y-%m-%d')
            ))
            
            # 移动到下一个季度的开始
            if quarter_end.month == 12:
                current_start = datetime(quarter_end.year + 1, 1, 1)
            else:
                next_month = quarter_end.month + 1
                current_start = datetime(quarter_end.year, next_month, 1)
        
        return date_ranges
        
    except Exception as e:
        print(f"生成季度日期范围时出错: {e}")
        return [(start_date, end_date)]  # 如果出错，返回原始范围


def _sanitize_filename(name: str) -> str:
    """
    将名称转换为适合文件名的安全字符串（允许中文）：
    - 替换空格为下划线
    - 保留字母、数字、下划线、连字符、点号
    - 保留中文字符（CJK Unified + Ext-A）
    """
    try:
        name = str(name).strip().replace(" ", "_")
        sanitized = re.sub(r"[^A-Za-z0-9_\-.\u4e00-\u9fff\u3400-\u4dbf]", "", name)
        return sanitized if sanitized else "unknown"
    except Exception:
        return "unknown"


def package_backtest_data(backtest_id: str, output_dir: str = "data", archive_dir: str = "data", compression: str = "gz") -> Optional[str]:
    """
    将指定回测ID的所有已下载数据打包为tar归档文件。

    - 会扫描 `output_dir` 下所有包含该 `backtest_id` 的文件（json/jsonl）并打包。
    - 归档文件名包含回测名称、日期范围和回测ID，便于识别。

    Parameters:
    backtest_id (str): 聚宽回测ID
    output_dir (str): 已下载数据所在目录，默认"data"
    archive_dir (str): 归档文件输出目录，默认"data"
    compression (str): 压缩格式，可选"gz"、"bz2"、"xz"，或传入空字符串使用不压缩的tar

    Returns:
    str: 归档文件的路径；如果没有找到可打包文件或出错，返回None
    """
    try:
        os.makedirs(output_dir, exist_ok=True)
        os.makedirs(archive_dir, exist_ok=True)

        # 获取回测名称与日期范围，用于构建有意义的归档文件名
        backtest_name = "unknown"
        start_date = None
        end_date = None
        try:
            gt = get_backtest(backtest_id)
            params = gt.get_params()
            backtest_name = _sanitize_filename(params.get("name", "unknown"))
            results = gt.get_results()
            if results:
                start_date = str(results[0]["time"]).split(" ")[0]
                end_date = str(results[-1]["time"]).split(" ")[0]
        except Exception:
            pass  # 非聚宽环境或获取失败时，回退到仅使用ID

        # 搜集需要打包的文件：凡是文件名包含 backtest_id 的都归档
        candidates: List[str] = []
        for fname in os.listdir(output_dir):
            # 只考虑普通文件
            fpath = os.path.join(output_dir, fname)
            if os.path.isfile(fpath) and backtest_id in fname:
                candidates.append(fpath)

        if not candidates:
            print(f"未在目录 {output_dir} 找到与回测ID {backtest_id} 相关的文件，打包取消。")
            return None

        # 构造有意义的归档文件名
        start_part = _sanitize_filename(start_date) if start_date else "unknown_start"
        end_part = _sanitize_filename(end_date) if end_date else "unknown_end"
        id_part = _sanitize_filename(backtest_id)
        name_part = _sanitize_filename(backtest_name)

        mode = "w" if not compression else f"w:{compression}"
        suffix = "tar" if not compression else f"tar.{compression}"
        archive_name = f"{name_part}_{start_part}_{end_part}_{id_part}.{suffix}"
        archive_path = os.path.join(archive_dir, archive_name)

        # 创建tar归档
        with tarfile.open(archive_path, mode) as tar:
            for fpath in candidates:
                # 归档内使用相对文件名，去掉目录前缀
                arcname = os.path.basename(fpath)
                tar.add(fpath, arcname=arcname)

        print(f"已生成回测数据归档文件: {archive_path}")
        print(f"包含 {len(candidates)} 个文件：")
        for f in candidates:
            print(f" - {os.path.basename(f)}")

        # 尝试释放可能的引用
        try:
            del candidates
            if 'results' in locals():
                del results
            if 'params' in locals():
                del params
            force_garbage_collection()
        except Exception:
            pass

        return archive_path
    except Exception as e:
        traceback.print_exc()
        print(f"打包回测数据时出错 (ID: {backtest_id}): {e}")
        return None


def cleanup_backtest_files(backtest_id: str, output_dir: str = "data", exclude_paths: Optional[List[str]] = None) -> int:
    """
    删除 `output_dir` 下文件名包含 `backtest_id` 的原始文件，以节省磁盘空间。

    Parameters:
    backtest_id (str): 聚宽回测ID
    output_dir (str): 原始文件所在目录
    exclude_paths (list[str] | None): 需要排除的文件路径（例如刚生成的归档文件）

    Returns:
    int: 实际删除的文件数量
    """
    deleted = 0
    try:
        os.makedirs(output_dir, exist_ok=True)
        exclude_set = set(exclude_paths or [])
        for fname in os.listdir(output_dir):
            fpath = os.path.join(output_dir, fname)
            if os.path.isfile(fpath) and backtest_id in fname:
                if fpath in exclude_set:
                    continue
                try:
                    os.remove(fpath)
                    deleted += 1
                except Exception as e:
                    print(f"删除文件 {fpath} 时出错: {e}")
        print(f"已删除 {deleted} 个原始文件，释放磁盘空间。")
    except Exception as e:
        traceback.print_exc()
        print(f"清理回测原始文件时出错 (ID: {backtest_id}): {e}")
    finally:
        force_garbage_collection()
    return deleted


def extract_backtest_ids(file_paths_text: str) -> List[str]:
    """
    从文件路径字符串中提取所有backtestID
    
    Parameters:
    file_paths_text (str): 包含文件路径的文本
    
    Returns:
    list: 包含所有backtestID的列表
    """
    # 定义匹配backtestID的正则表达式模式
    # backtestID是32个十六进制字符（小写字母和数字）
    pattern = r'[a-f0-9]{32}'
    
    # 使用findall方法提取所有匹配的backtestID
    backtest_ids = re.findall(pattern, file_paths_text)
    
    return list(set(backtest_ids))  # 去重


def download_all_backtest_data(backtest_id: str, output_dir: str = "data", use_quarterly: bool = True) -> Dict[str, Any]:
    """
    下载指定回测ID的所有数据，并在完成后自动打包与清理原始文件。

    - 下载内容：余额、持仓（可分季度）、回测结果、订单（可分季度）。
    - 完成后：将 `output_dir` 下该回测ID相关文件打包为 `tar.gz` 并删除原始文件，只保留归档。

    Parameters:
    backtest_id (str): 聚宽回测ID
    output_dir (str): 输出目录，默认为"data"
    use_quarterly (bool): 是否对持仓和订单数据使用分季度下载，默认为True
    
    Returns:
    dict: 包含下载模式、归档路径和删除文件数量等信息的字典
    """
    print(f"\n开始下载回测数据: {backtest_id}")
    print(f"下载模式: {'分季度下载' if use_quarterly else '一次性下载'}")
    print("=" * 60)
    
    results = {
        "backtest_id": backtest_id,
        "download_time": datetime.now().isoformat(),
        "download_mode": "quarterly" if use_quarterly else "full"
    }
    
    # 下载余额数据（不需要分季度，数据量相对较小）
    print("\n1. 下载余额数据...")
    save_backtest_balances(backtest_id, output_dir)
    
    # 下载持仓数据（支持分季度）
    print("\n2. 下载持仓数据...")
    save_backtest_positions(backtest_id, output_dir, use_quarterly)
    
    # 下载回测结果（不需要分季度，数据量相对较小）
    print("\n3. 下载回测结果...")
    save_backtest_results(backtest_id, output_dir)
    
    # 下载订单数据（支持分季度）
    print("\n4. 下载订单数据...")
    save_backtest_orders(backtest_id, output_dir, use_quarterly)
    
    # 下载完成后执行打包
    print("\n5. 打包回测数据并清理原始文件...")
    archive_path = package_backtest_data(backtest_id, output_dir=output_dir, archive_dir=output_dir, compression="gz")
    deleted_count = 0
    if archive_path:
        deleted_count = cleanup_backtest_files(backtest_id, output_dir=output_dir, exclude_paths=[archive_path])
    else:
        print("打包失败或没有可打包的文件，跳过清理以避免误删。")

    results.update({
        "archive_path": archive_path,
        "deleted_files": deleted_count
    })

    print(f"\n回测数据下载与打包完成: {backtest_id}")
    print(f"下载模式: {'分季度下载' if use_quarterly else '一次性下载'}")
    if archive_path:
        print(f"归档文件: {archive_path}")
        print(f"删除原始文件数量: {deleted_count}")
    print("=" * 60)
    
    return results


def batch_download_backtest_data(backtest_ids: List[str], output_dir: str = "data", use_quarterly: bool = True) -> Dict[str, Any]:
    """
    批量下载多个回测的数据
    
    Parameters:
    backtest_ids (list): 聚宽回测ID列表
    output_dir (str): 输出目录，默认为"data"
    use_quarterly (bool): 是否对持仓和订单数据使用分季度下载，默认为True
    
    Returns:
    dict: 包含所有下载结果的汇总字典
    """
    print(f"开始批量下载 {len(backtest_ids)} 个回测的数据")
    print("=" * 80)
    
    batch_results = {
        "total_backtests": len(backtest_ids),
        "download_time": datetime.now().isoformat(),
        "output_dir": output_dir,
        "individual_results": {}
    }
    
    for i, backtest_id in enumerate(backtest_ids, 1):
        print(f"\n处理第 {i}/{len(backtest_ids)} 个回测: {backtest_id}")
        
        try:
            result = download_all_backtest_data(backtest_id, output_dir, use_quarterly)
            batch_results["individual_results"][backtest_id] = result
        except Exception as e:
            print(f"处理回测 {backtest_id} 时发生错误: {e}")
            batch_results["individual_results"][backtest_id] = {"error": str(e)}
    
    return batch_results


# 主函数示例
def main():
    """
    主函数示例，展示如何使用该模块
    """
    # 示例1: 下载单个回测数据
    # backtest_id = "1bd0701b75e2b680a5bd1dbfd34a8b07"
    # download_all_backtest_data(backtest_id)
    
    # 示例2: 从文件路径文本中提取ID并批量下载
#     file_paths = """
#     https://www.joinquant.com/algorithm/backtest/detail?backtestId=dce5115d8f239eed25a9543e6045c097
#     https://www.joinquant.com/algorithm/backtest/detail?backtestId=078a414996c727937b2366f5e3c45734
#     https://www.joinquant.com/algorithm/backtest/detail?backtestId=8af6c5d6ce60e7669d62c13e46c6aeaf
#     https://www.joinquant.com/algorithm/backtest/detail?backtestId=32f1b6eb216fa1c79bcc86ea630c4a90
#     https://www.joinquant.com/algorithm/backtest/detail?backtestId=0032754579989431f54e85a675f69a9d
#     https://www.joinquant.com/algorithm/backtest/detail?backtestId=35b3f0db1412989f93e5b301890a4464
#     https://www.joinquant.com/algorithm/backtest/detail?backtestId=8bcdee25453320e092959c5ebe1e41f6
#     https://www.joinquant.com/algorithm/backtest/detail?backtestId=3a527e02b8b7b9afe78946879e349118
#     https://www.joinquant.com/algorithm/backtest/detail?backtestId=cd3ed22d1c54224675f40ce012bdad3c
#     https://www.joinquant.com/algorithm/backtest/detail?backtestId=09da205c977b56b8ba5564208f465e7f
#     """

    file_paths = '''https://www.joinquant.com/algorithm/backtest/detail?backtestId=e61488015d7d0c85e6f72ed47fcb4a41
    https://www.joinquant.com/algorithm/backtest/detail?backtestId=c6c9efa601515577b338ee097bfcf514
    https://www.joinquant.com/algorithm/backtest/detail?backtestId=944f81905c487036206dce3862943272
    https://www.joinquant.com/algorithm/backtest/detail?backtestId=b1a4e331b19b97eca706f3c01da7d44d
    '''

    #file_paths = '''Top30-T2_position_ratio_09da205c977b56b8ba5564208f465e7f'''
    file_paths = file_paths.split("\n")[1]
    backtest_ids = extract_backtest_ids(file_paths)
    print(f"提取到的backtestID列表: {backtest_ids}")
    
    # 批量下载
    batch_download_backtest_data(backtest_ids)


if __name__ == "__main__":
    main()