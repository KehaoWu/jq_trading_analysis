#!/usr/bin/env python3
"""
持仓金额可视化脚本

该脚本用于：
1. 读取交易记录（orders.jsonl格式）
2. 还原每天的持仓金额、现金、资产总额
3. 计算每天的买入金额、卖出金额、持仓比例
4. 导出每日交易和资产汇总到CSV文件
5. 绘制持仓比例、资产分布、资产总额曲线到HTML文件

使用方法:
    python position_value_visualization.py --input data/orders_samples.jsonl --output output/position_value.html --initial_cash 10000000000
"""

import argparse
import csv
import json
import sys
from datetime import datetime
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Tuple

import numpy as np


class PositionTracker:
    """持仓跟踪器"""
    
    def __init__(self, initial_cash: float = 10000000.0):
        """
        初始化持仓跟踪器
        
        Args:
            initial_cash: 初始资金（默认1000万）
        """
        # 持仓数据: {security: {'quantity': int, 'cost': float}}
        self.positions = defaultdict(lambda: {'quantity': 0, 'cost': 0.0})
        # 每日持仓金额: {date: float}
        self.daily_position_value = {}
        # 每日持仓明细: {date: {security: {'quantity': int, 'price': float, 'value': float}}}
        self.daily_position_detail = defaultdict(dict)
        # 累计手续费
        self.total_commission = 0.0
        # 现金
        self.cash = initial_cash
        self.initial_cash = initial_cash
        # 每日现金: {date: float}
        self.daily_cash = {}
        # 每日资产总额: {date: float}
        self.daily_total_assets = {}
        # 每日持仓比例: {date: float}
        self.daily_position_ratio = {}
        # 每日买入金额: {date: float}
        self.daily_buy_amount = {}
        # 每日卖出金额: {date: float}
        self.daily_sell_amount = {}
        # 当前日期（用于累计当日买卖金额）
        self.current_date = None
        self.current_buy_amount = 0.0
        self.current_sell_amount = 0.0
        
    def process_order(self, order: Dict, date: str):
        """
        处理一笔交易订单
        
        Args:
            order: 订单信息字典
            date: 交易日期
        """
        # 如果是新的一天，保存上一天的累计买卖金额
        if self.current_date != date:
            if self.current_date is not None:
                self.daily_buy_amount[self.current_date] = self.current_buy_amount
                self.daily_sell_amount[self.current_date] = self.current_sell_amount
            # 重置当日累计
            self.current_date = date
            self.current_buy_amount = 0.0
            self.current_sell_amount = 0.0
        
        security = order['security']
        action = order['action']
        filled = order['filled']
        price = order['price']
        commission = order.get('commission', 0.0)
        
        # 累计手续费
        self.total_commission += commission
        
        if action == 'open':
            # 开仓（买入）：增加持仓，减少现金
            current_qty = self.positions[security]['quantity']
            current_cost = self.positions[security]['cost']
            
            # 计算买入金额（含手续费）
            buy_amount = price * filled + commission
            self.cash -= buy_amount
            
            # 累计当日买入金额
            self.current_buy_amount += buy_amount
            
            # 计算新的平均成本
            total_cost = current_cost * current_qty + price * filled + commission
            new_qty = current_qty + filled
            
            self.positions[security]['quantity'] = new_qty
            self.positions[security]['cost'] = total_cost / new_qty if new_qty > 0 else 0.0
            
        elif action == 'close':
            # 平仓（卖出）：减少持仓，增加现金
            current_qty = self.positions[security]['quantity']
            new_qty = max(0, current_qty - filled)
            
            # 计算卖出金额（扣除手续费）
            sell_amount = price * filled - commission
            self.cash += sell_amount
            
            # 累计当日卖出金额（记录实际到账金额）
            self.current_sell_amount += sell_amount
            
            self.positions[security]['quantity'] = new_qty
            # 成本保持不变，只减少数量
            
    def calculate_daily_value(self, date: str, prices: Dict[str, float]):
        """
        计算指定日期的持仓总金额、资产总额和持仓比例
        
        Args:
            date: 日期（YYYY-MM-DD格式）
            prices: 当日各证券的价格 {security: price}
        
        Returns:
            Tuple[float, float, float]: (持仓总金额, 资产总额, 持仓比例)
        """
        # 保存当天的买卖金额（如果是当前正在处理的日期）
        if self.current_date == date:
            self.daily_buy_amount[date] = self.current_buy_amount
            self.daily_sell_amount[date] = self.current_sell_amount
        
        total_value = 0.0
        position_detail = {}
        
        for security, pos in self.positions.items():
            quantity = pos['quantity']
            if quantity > 0:
                # 使用当日价格（如果有），否则使用成本价
                current_price = prices.get(security, pos['cost'])
                value = quantity * current_price
                total_value += value
                
                position_detail[security] = {
                    'quantity': quantity,
                    'price': current_price,
                    'value': value
                }
        
        # 计算资产总额（现金 + 持仓金额）
        total_assets = self.cash + total_value
        
        # 计算持仓比例
        position_ratio = (total_value / total_assets * 100.0) if total_assets > 0 else 0.0
        
        # 记录数据
        self.daily_position_value[date] = total_value
        self.daily_position_detail[date] = position_detail
        self.daily_cash[date] = self.cash
        self.daily_total_assets[date] = total_assets
        self.daily_position_ratio[date] = position_ratio
        
        return total_value, total_assets, position_ratio
    
    def get_current_positions(self) -> Dict:
        """
        获取当前持仓
        
        Returns:
            Dict: 持仓信息
        """
        return {k: v for k, v in self.positions.items() if v['quantity'] > 0}


class OrdersDataLoader:
    """交易记录数据加载器"""
    
    def __init__(self, orders_file: str):
        """
        初始化数据加载器
        
        Args:
            orders_file: 交易记录文件路径
        """
        self.orders_file = Path(orders_file)
        if not self.orders_file.exists():
            raise FileNotFoundError(f"交易记录文件不存在: {orders_file}")
    
    def load_orders(self) -> List[Dict]:
        """
        加载交易记录
        
        Returns:
            List[Dict]: 交易记录列表，按时间排序
        """
        orders = []
        
        print(f"正在加载交易记录: {self.orders_file}")
        
        with open(self.orders_file, 'r', encoding='utf-8') as f:
            for line_num, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                    
                try:
                    order = json.loads(line)
                    # 只处理已成交的订单
                    if order.get('status') == 'done':
                        orders.append(order)
                except json.JSONDecodeError as e:
                    print(f"警告: 解析第 {line_num} 行失败: {e}")
                    continue
        
        # 按时间排序
        orders.sort(key=lambda x: x['time'])
        
        print(f"加载完成，共 {len(orders)} 笔成交订单")
        return orders
    
    def group_orders_by_date(self, orders: List[Dict]) -> Dict[str, List[Dict]]:
        """
        按日期分组订单
        
        Args:
            orders: 订单列表
            
        Returns:
            Dict[str, List[Dict]]: 日期到订单列表的映射
        """
        orders_by_date = defaultdict(list)
        
        for order in orders:
            # 提取日期部分
            time_str = order['time']
            date = time_str.split()[0]  # 'YYYY-MM-DD HH:MM:SS' -> 'YYYY-MM-DD'
            orders_by_date[date].append(order)
        
        return orders_by_date


class PositionValueCalculator:
    """持仓金额计算器"""
    
    def __init__(self, initial_cash: float = 10000000.0):
        """
        初始化计算器
        
        Args:
            initial_cash: 初始资金（默认1000万）
        """
        self.tracker = PositionTracker(initial_cash)
    
    def calculate(self, orders: List[Dict]) -> Tuple[List[str], List[float], List[float], List[float], List[float], Dict]:
        """
        计算每日持仓金额、资产总额和持仓比例
        
        Args:
            orders: 订单列表（已按时间排序）
            
        Returns:
            Tuple: (日期列表, 持仓金额列表, 现金列表, 资产总额列表, 持仓比例列表, 统计信息)
        """
        loader = OrdersDataLoader.__new__(OrdersDataLoader)
        orders_by_date = defaultdict(list)
        
        for order in orders:
            time_str = order['time']
            date = time_str.split()[0]
            orders_by_date[date].append(order)
        
        # 获取所有日期并排序
        all_dates = sorted(orders_by_date.keys())
        
        dates = []
        position_values = []
        cash_values = []
        total_assets = []
        position_ratios = []
        
        print("正在计算每日持仓金额、资产总额和持仓比例...")
        
        for date in all_dates:
            # 处理当日所有订单
            daily_orders = orders_by_date[date]
            daily_prices = {}
            
            for order in daily_orders:
                self.tracker.process_order(order, date)
                # 记录当日价格
                daily_prices[order['security']] = order['price']
            
            # 计算当日持仓金额、资产总额和持仓比例
            pos_value, total_asset, pos_ratio = self.tracker.calculate_daily_value(date, daily_prices)
            
            dates.append(date)
            position_values.append(pos_value)
            cash_values.append(self.tracker.cash)
            total_assets.append(total_asset)
            position_ratios.append(pos_ratio)
        
        # 统计信息
        stats = {
            'initial_cash': self.tracker.initial_cash,
            'final_cash': self.tracker.cash,
            'total_commission': self.tracker.total_commission,
            'max_position_value': max(position_values) if position_values else 0,
            'min_position_value': min(position_values) if position_values else 0,
            'max_total_assets': max(total_assets) if total_assets else 0,
            'min_total_assets': min(total_assets) if total_assets else 0,
            'final_total_assets': total_assets[-1] if total_assets else 0,
            'max_position_ratio': max(position_ratios) if position_ratios else 0,
            'min_position_ratio': min(position_ratios) if position_ratios else 0,
            'final_position_ratio': position_ratios[-1] if position_ratios else 0,
            'final_position_count': len(self.tracker.get_current_positions()),
            'trading_days': len(dates)
        }
        
        print(f"计算完成，共 {len(dates)} 个交易日")
        print(f"初始资金: {stats['initial_cash']:,.2f}")
        print(f"最终资金: {stats['final_cash']:,.2f}")
        print(f"最大持仓金额: {stats['max_position_value']:,.2f}")
        print(f"最小持仓金额: {stats['min_position_value']:,.2f}")
        print(f"最终资产总额: {stats['final_total_assets']:,.2f}")
        print(f"最终持仓比例: {stats['final_position_ratio']:.2f}%")
        print(f"累计手续费: {stats['total_commission']:,.2f}")
        print(f"最终持仓数量: {stats['final_position_count']}")
        
        # 获取每日买入和卖出金额
        buy_amounts = [self.tracker.daily_buy_amount.get(date, 0.0) for date in dates]
        sell_amounts = [self.tracker.daily_sell_amount.get(date, 0.0) for date in dates]
        
        return dates, position_values, cash_values, total_assets, position_ratios, buy_amounts, sell_amounts, stats


class CSVExporter:
    """CSV数据导出器"""
    
    def __init__(self):
        """初始化导出器"""
        pass
    
    def export_daily_summary(self, dates: List[str], buy_amounts: List[float], 
                           sell_amounts: List[float], cash_values: List[float], 
                           total_assets: List[float], position_values: List[float],
                           position_ratios: List[float], output_file: str):
        """
        导出每日交易和资产汇总到CSV文件
        
        Args:
            dates: 日期列表
            buy_amounts: 买入金额列表
            sell_amounts: 卖出金额列表
            cash_values: 现金列表
            total_assets: 资产总额列表
            position_values: 持仓金额列表
            position_ratios: 持仓比例列表
            output_file: 输出文件路径
        """
        # 准备数据
        rows = []
        for i, date in enumerate(dates):
            rows.append({
                'date': date,
                'buy_amount': buy_amounts[i] if i < len(buy_amounts) else 0.0,
                'sell_amount': sell_amounts[i] if i < len(sell_amounts) else 0.0,
                'cash': cash_values[i] if i < len(cash_values) else 0.0,
                'position_value': position_values[i] if i < len(position_values) else 0.0,
                'total_assets': total_assets[i] if i < len(total_assets) else 0.0,
                'position_ratio': position_ratios[i] if i < len(position_ratios) else 0.0
            })
        
        # 写入CSV文件
        with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
            fieldnames = ['date', 'buy_amount', 'sell_amount', 'cash', 
                         'position_value', 'total_assets', 'position_ratio']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            # 写入表头
            writer.writeheader()
            
            # 写入数据
            writer.writerows(rows)
        
        print(f"每日交易和资产汇总已导出: {output_file}")


class HTMLVisualizer:
    """HTML可视化器"""
    
    def __init__(self):
        """初始化可视化器"""
        pass
    
    def generate_html(self, dates: List[str], position_values: List[float], 
                     cash_values: List[float], total_assets: List[float],
                     position_ratios: List[float], output_file: str, stats: Dict, 
                     title: str = "持仓分析可视化"):
        """
        生成HTML可视化文件
        
        Args:
            dates: 日期列表
            position_values: 持仓金额列表
            cash_values: 现金列表
            total_assets: 资产总额列表
            position_ratios: 持仓比例列表
            output_file: 输出文件路径
            stats: 统计信息
            title: 图表标题
        """
        # 准备数据
        position_data = [[date, value] for date, value in zip(dates, position_values)]
        cash_data = [[date, value] for date, value in zip(dates, cash_values)]
        assets_data = [[date, value] for date, value in zip(dates, total_assets)]
        ratio_data = [[date, ratio] for date, ratio in zip(dates, position_ratios)]
        
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
            background-color: #f5f5f5;
        }}
        .container {{
            max-width: 1400px;
            margin: 0 auto;
            background-color: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        h1 {{
            color: #333;
            text-align: center;
            margin-bottom: 20px;
        }}
        h2 {{
            color: #555;
            margin-top: 40px;
            margin-bottom: 15px;
            font-size: 18px;
        }}
        .stats {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 15px;
            margin-bottom: 30px;
        }}
        .stat-card {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 15px;
            border-radius: 8px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.1);
        }}
        .stat-card h3 {{
            margin: 0 0 10px 0;
            font-size: 14px;
            opacity: 0.9;
        }}
        .stat-card .value {{
            font-size: 24px;
            font-weight: bold;
            margin: 0;
        }}
        .chart {{
            width: 100%;
            height: 500px;
            margin-top: 20px;
            margin-bottom: 40px;
        }}
        .footer {{
            text-align: center;
            margin-top: 20px;
            color: #666;
            font-size: 12px;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>{title}</h1>
        
        <div class="stats">
            <div class="stat-card">
                <h3>交易日数</h3>
                <p class="value">{stats['trading_days']}</p>
            </div>
            <div class="stat-card">
                <h3>初始资金</h3>
                <p class="value">¥{stats['initial_cash']:,.0f}</p>
            </div>
            <div class="stat-card">
                <h3>最终资产</h3>
                <p class="value">¥{stats['final_total_assets']:,.0f}</p>
            </div>
            <div class="stat-card">
                <h3>最终现金</h3>
                <p class="value">¥{stats['final_cash']:,.0f}</p>
            </div>
            <div class="stat-card">
                <h3>最终持仓比例</h3>
                <p class="value">{stats['final_position_ratio']:.2f}%</p>
            </div>
            <div class="stat-card">
                <h3>累计手续费</h3>
                <p class="value">¥{stats['total_commission']:,.2f}</p>
            </div>
            <div class="stat-card">
                <h3>最终持仓数</h3>
                <p class="value">{stats['final_position_count']}</p>
            </div>
        </div>
        
        <h2>持仓比例变化</h2>
        <div id="ratioChart" class="chart"></div>
        
        <h2>资产分布（持仓 vs 现金）</h2>
        <div id="assetsChart" class="chart"></div>
        
        <h2>资产总额变化</h2>
        <div id="totalAssetsChart" class="chart"></div>
        
        <div class="footer">
            生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        </div>
    </div>
    
    <script>
        // 持仓比例图表
        var ratioChartDom = document.getElementById('ratioChart');
        var ratioChart = echarts.init(ratioChartDom);
        
        var ratioOption = {{
            tooltip: {{
                trigger: 'axis',
                formatter: function(params) {{
                    var date = params[0].axisValue;
                    var ratio = params[0].value[1];
                    return date + '<br/>' + 
                           '持仓比例: ' + ratio.toFixed(2) + '%';
                }}
            }},
            grid: {{
                left: '3%',
                right: '4%',
                bottom: '15%',
                top: '40px',
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
                name: '持仓比例 (%)',
                min: 0,
                max: 100,
                axisLabel: {{
                    formatter: '{{value}}%'
                }}
            }},
            series: [
                {{
                    name: '持仓比例',
                    type: 'line',
                    smooth: true,
                    symbol: 'none',
                    lineStyle: {{
                        width: 3,
                        color: '#f093fb'
                    }},
                    areaStyle: {{
                        color: {{
                            type: 'linear',
                            x: 0,
                            y: 0,
                            x2: 0,
                            y2: 1,
                            colorStops: [
                                {{
                                    offset: 0,
                                    color: 'rgba(240, 147, 251, 0.4)'
                                }},
                                {{
                                    offset: 1,
                                    color: 'rgba(240, 147, 251, 0.05)'
                                }}
                            ]
                        }}
                    }},
                    data: {json.dumps(ratio_data)}
                }}
            ]
        }};
        
        ratioChart.setOption(ratioOption);
        
        // 资产分布图表（堆叠面积图）
        var assetsChartDom = document.getElementById('assetsChart');
        var assetsChart = echarts.init(assetsChartDom);
        
        var assetsOption = {{
            tooltip: {{
                trigger: 'axis',
                formatter: function(params) {{
                    var date = params[0].axisValue;
                    var result = date + '<br/>';
                    params.forEach(function(item) {{
                        var value = item.value[1];
                        result += item.marker + item.seriesName + ': ¥' + 
                                 value.toFixed(2).replace(/\\B(?=(\\d{{3}})+(?!\\d))/g, ',') + '<br/>';
                    }});
                    return result;
                }}
            }},
            legend: {{
                data: ['持仓金额', '现金'],
                top: 10
            }},
            grid: {{
                left: '3%',
                right: '4%',
                bottom: '15%',
                top: '60px',
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
                name: '金额 (元)',
                axisLabel: {{
                    formatter: function(value) {{
                        if (value >= 1000000) {{
                            return (value / 1000000).toFixed(1) + 'M';
                        }} else if (value >= 1000) {{
                            return (value / 1000).toFixed(1) + 'K';
                        }}
                        return value.toFixed(0);
                    }}
                }}
            }},
            series: [
                {{
                    name: '持仓金额',
                    type: 'line',
                    stack: 'total',
                    smooth: true,
                    symbol: 'none',
                    lineStyle: {{
                        width: 0
                    }},
                    areaStyle: {{
                        color: 'rgba(102, 126, 234, 0.6)'
                    }},
                    data: {json.dumps(position_data)}
                }},
                {{
                    name: '现金',
                    type: 'line',
                    stack: 'total',
                    smooth: true,
                    symbol: 'none',
                    lineStyle: {{
                        width: 0
                    }},
                    areaStyle: {{
                        color: 'rgba(52, 211, 153, 0.6)'
                    }},
                    data: {json.dumps(cash_data)}
                }}
            ]
        }};
        
        assetsChart.setOption(assetsOption);
        
        // 资产总额图表
        var totalAssetsChartDom = document.getElementById('totalAssetsChart');
        var totalAssetsChart = echarts.init(totalAssetsChartDom);
        
        var totalAssetsOption = {{
            tooltip: {{
                trigger: 'axis',
                formatter: function(params) {{
                    var date = params[0].axisValue;
                    var value = params[0].value[1];
                    return date + '<br/>' + 
                           '资产总额: ¥' + value.toFixed(2).replace(/\\B(?=(\\d{{3}})+(?!\\d))/g, ',');
                }}
            }},
            grid: {{
                left: '3%',
                right: '4%',
                bottom: '15%',
                top: '40px',
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
                name: '资产总额 (元)',
                axisLabel: {{
                    formatter: function(value) {{
                        if (value >= 1000000) {{
                            return (value / 1000000).toFixed(1) + 'M';
                        }} else if (value >= 1000) {{
                            return (value / 1000).toFixed(1) + 'K';
                        }}
                        return value.toFixed(0);
                    }}
                }}
            }},
            series: [
                {{
                    name: '资产总额',
                    type: 'line',
                    smooth: true,
                    symbol: 'none',
                    lineStyle: {{
                        width: 2,
                        color: '#4ade80'
                    }},
                    areaStyle: {{
                        color: {{
                            type: 'linear',
                            x: 0,
                            y: 0,
                            x2: 0,
                            y2: 1,
                            colorStops: [
                                {{
                                    offset: 0,
                                    color: 'rgba(74, 222, 128, 0.3)'
                                }},
                                {{
                                    offset: 1,
                                    color: 'rgba(74, 222, 128, 0.05)'
                                }}
                            ]
                        }}
                    }},
                    data: {json.dumps(assets_data)}
                }}
            ]
        }};
        
        totalAssetsChart.setOption(totalAssetsOption);
        
        // 响应式调整
        window.addEventListener('resize', function() {{
            ratioChart.resize();
            assetsChart.resize();
            totalAssetsChart.resize();
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
    parser = argparse.ArgumentParser(description='持仓金额可视化脚本')
    parser.add_argument('--input', required=True, help='交易记录文件路径（.jsonl格式）')
    parser.add_argument('--output', default=None, help='输出HTML文件路径（默认: output/position_value.html）')
    parser.add_argument('--initial_cash', type=float, default=1000000000.0, help='初始资金（默认: 1000000000.0）')
    
    args = parser.parse_args()
    
    # 转换为绝对路径
    project_root = Path(__file__).parent.parent
    input_file = Path(args.input)
    if not input_file.is_absolute():
        input_file = project_root / input_file
    
    # 确定输出文件路径
    if args.output:
        output_file = Path(args.output)
        if not output_file.is_absolute():
            output_file = project_root / output_file
    else:
        output_dir = project_root / 'output'
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / 'position_value.html'
    
    # 确保输出目录存在
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        # 1. 加载交易记录
        loader = OrdersDataLoader(str(input_file))
        orders = loader.load_orders()
        
        if not orders:
            print("错误: 没有找到有效的交易记录")
            return 1
        
        # 2. 计算每日持仓金额、资产总额和持仓比例
        calculator = PositionValueCalculator(args.initial_cash)
        dates, position_values, cash_values, total_assets, position_ratios, buy_amounts, sell_amounts, stats = calculator.calculate(orders)
        
        if not dates:
            print("错误: 无法计算持仓金额")
            return 1
        
        # 3. 导出CSV文件
        csv_file = output_file.with_suffix('.csv')
        exporter = CSVExporter()
        exporter.export_daily_summary(dates, buy_amounts, sell_amounts, cash_values, 
                                     total_assets, position_values, position_ratios, str(csv_file))
        
        # 4. 生成可视化
        visualizer = HTMLVisualizer()
        visualizer.generate_html(dates, position_values, cash_values, total_assets, 
                                position_ratios, str(output_file), stats)
        
        print("处理完成!")
        
    except Exception as e:
        print(f"错误: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main())

