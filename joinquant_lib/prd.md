1、使用聚宽的 get_backtest 方法，获取指定回测的有关信息
2、使用 gt.get_balances() 获取持仓比例数据（见参考代码）
3、使用 get_results() 获取回测结果。并且根据 get_results 获得开始和结束日期
4、下载下来的保存为json数据结构，其中 get_balances获取的数据格式与以下 sample 代码保持一致
5、get_positions获取的数据，保存为 {backtest_name}_position_details_{backtest_id}.json
6、get_results获取的数据，保存为 {backtest_name}_daily_return_{backtest_id}.json
7、get_orders获取的数据，保存为 {backtest_name}_orders_{backtest_id}.json
8、get_balance获取的数据，保存为 {backtest_name}_position_ratio_{backtest_id}.json
8、这几个方法的调用示例参考 sample.ipynb
9、注意，这个代码是在聚宽环境才能运行，所以你无需真正运行该代码进行测试
10、其中：get_positions\get_orders 因为数据量大，可能会导致内存溢出，所以每三个月获取一次数据，并添加进度条
11、backtest_name使用gt.get_params()获取

```
gt.get_params()

"""输出结果
{'algorithm_id': None,
 'end_date': '2025-07-21 23:59:59',
 'extras': None,
 'frequency': 'day',
 'initial_cash': '100000000',
 'initial_positions': None,
 'initial_value': None,
 'name': '朴素的Top200 T2',
 'package_version': '1.0',
 'python_version': '3',
 'start_date': '2009-01-01 00:00:00',
 'subportfolios': [{'account_type': 'stock',
   'set_subportfolios': True,
   'starting_cash': 100000000.0,
   'subAccountId': 0}]}
"""
```

```sample代码
import pandas as pd
import json
import os
import traceback

def save_backtest_balances(backtest_id):
    """
    获取回测的账户余额数据，保存为JSON文件，并计算仓位比例
    
    Parameters:
    backtest_id (str): 聚宽回测ID
    """
    try:
        # 获取回测对象
        gt = get_backtest(backtest_id)
        
        # 获取余额数据
        results = gt.get_balances()
        
        if not results:
            print("未获取到回测余额数据")
            return None
        
        # 准备保存的数据结构
        output_data = {
            "backtest_id": backtest_id,
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
        
        # 保存为JSON文件
        filename = f"data/position_ratio_{backtest_id}.json"
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(output_data, f, indent=2, ensure_ascii=False, default=str)
        
        print(f"回测数据已保存到: {filename}")
        print(f"初始仓位比例: {output_data['position_analysis']['first_position']:.2%}")
        print(f"最终仓位比例: {output_data['position_analysis']['last_position']:.2%}")
        print(f"平均仓位比例: {output_data['position_analysis']['average_position']:.2%}")
        print(f"最终净值: {output_data['position_analysis']['final_net_value']:,.2f}")
        
        return output_data
        
    except Exception as e:
        traceback.print_exc()
        print(f"处理回测数据时出错: {e}")
        return None

```

```
import re

def extract_backtest_ids(file_paths_text):
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
    
    return backtest_ids

# 您提供的字符串
file_paths = """排除T-1的top200信号-non3800-T10_4752c730c7f1ed942f7c47852a834dc2_20090105_20250721_daily.jsonl
排除T-1的top200信号-non3800-T15_0095dc82d81735dcfeec16487553d22f_20090105_20250721_daily.jsonl
排除T-1的top200信号-non3800-T20_eeb6de829662977acb45afa7aeff0242_20090105_20250721_daily.jsonl
排除T-1的top200信号-non3800-T2_2b61dc71dd7015b70324585b7490882d_20090105_20250721_daily.jsonl
排除T-1的top200信号-non3800-T3_708a418047f3618634fee2ba77bca7e2_20090105_20250721_daily.jsonl
排除T-1的top200信号-non3800-T5_400dabc4e0753c206a75fff37657e05e_20090105_20250721_daily.jsonl"""
# 提取backtestID
backtest_ids = extract_backtest_ids(file_paths)

print("提取到的backtestID列表:")
for i, backtest_id in enumerate(backtest_ids, 1):
    save_backtest_balances(backtest_id)
```