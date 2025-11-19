#!/usr/bin/env python3
"""
持仓比例可视化脚本

该脚本用于：
1. 自动识别指定文件夹中的持仓比例数据文件
2. 绘制多个回测的持仓比例曲线

使用方法:
    python position_ratio_visualization.py --input_dir /path/to/backtest/data
"""

import argparse
import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np

# 添加项目根目录到Python路径
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


class PositionRatioFileIdentifier:
    """持仓比例文件识别器"""
    
    def __init__(self, input_dir: str):
        """
        初始化文件识别器
        
        Args:
            input_dir: 输入目录路径
        """
        self.input_dir = Path(input_dir)
        if not self.input_dir.exists():
            raise FileNotFoundError(f"输入目录不存在: {input_dir}")
    
    def identify_files(self) -> List[Dict[str, str]]:
        """
        识别目录中的持仓比例数据文件
        
        Returns:
            List[Dict]: 包含文件信息的列表，每个字典包含：
                - position_file: 持仓比例文件路径
                - backtest_id: 回测ID
                - backtest_name: 回测名称
        """
        files_info = []
        
        # 查找所有 position_ratio_*.json 文件
        position_files = list(self.input_dir.glob("*position_ratio_*.json"))
        
        for position_file in sorted(position_files, key=lambda p: p.name):
            # 从文件名中提取回测ID
            backtest_id = self._extract_backtest_id(position_file.name)
            backtest_name = self._extract_backtest_name(position_file.name)
            
            if not backtest_id:
                print(f"警告: 无法从文件名中提取回测ID: {position_file.name}")
                continue
            
            files_info.append({
                'position_file': str(position_file),
                'backtest_id': backtest_id,
                'backtest_name': backtest_name
            })
        
        return files_info
    
    def _extract_backtest_id(self, filename: str) -> str:
        """
        从文件名中提取回测ID
        
        Args:
            filename: 文件名
            
        Returns:
            str: 回测ID
        """
        # 匹配32位十六进制ID
        match = re.search(r'([a-f0-9]{32})', filename)
        return match.group(1) if match else ""
    
    def _extract_backtest_name(self, filename: str) -> str:
        """
        从文件名中提取回测名称
        
        Args:
            filename: 文件名
            
        Returns:
            str: 回测名称
        """
        # 提取 position_ratio 之前的部分作为回测名称
        # 文件名格式：<backtest_name>_position_ratio_<id>.json
        if '_position_ratio_' in filename:
            parts = filename.split('_position_ratio_')
            if parts:
                # 移除可能的前缀和后缀
                name = parts[0]
                # 如果名称以 _1000M 等结尾，保留这部分
                return name
        
        # 如果无法提取，返回不含扩展名的文件名
        return filename.replace('.json', '')


class PositionRatioDataLoader:
    """持仓比例数据加载器"""
    
    @staticmethod
    def load_position_ratio_data(file_path: str) -> Dict:
        """
        加载持仓比例数据文件
        
        Args:
            file_path: 文件路径
            
        Returns:
            Dict: 持仓比例数据
        """
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data
    
    @staticmethod
    def extract_time_series(position_data: Dict) -> Dict[str, List]:
        """
        从持仓比例数据中提取时间序列
        
        Args:
            position_data: 持仓比例数据
            
        Returns:
            Dict: 包含日期和持仓比例的字典
        """
        dates = []
        position_ratios = []
        
        for item in position_data.get('balances', []):
            # 提取时间并格式化为 YYYY-MM-DD
            time_str = item.get('time', '')
            if time_str:
                # 时间格式：2009-01-05 16:00:00
                date = time_str.split(' ')[0]
                dates.append(date)
            else:
                dates.append('')
            
            # 提取持仓比例（转换为百分比）
            ratio = item.get('position_ratio', 0.0)
            position_ratios.append(ratio * 100)  # 转换为百分比
        
        return {
            'dates': dates,
            'position_ratios': position_ratios
        }


class EChartsVisualizer:
    """ECharts可视化器"""
    
    def __init__(self):
        """初始化可视化器"""
        self.chart_data = {
            'series': []
        }
    
    def add_position_ratio_series(self, name: str, dates: List[str], ratios: List[float]):
        """
        添加持仓比例数据系列
        
        Args:
            name: 系列名称
            dates: 日期列表（YYYY-MM-DD格式）
            ratios: 持仓比例列表（百分比形式，如10.0表示10%）
        """
        # 过滤掉无效数据
        valid_data = []
        for date, ratio in zip(dates, ratios):
            if ratio is not None and not np.isnan(ratio):
                valid_data.append([date, ratio])
        
        self.chart_data['series'].append({
            'name': name,
            'type': 'line',
            'data': valid_data,
            'smooth': True,
            'symbol': 'none',  # 移除数据点
            'lineStyle': {'width': 1.5}
        })
    
    def generate_html(self, output_file: str, title: str = "持仓比例可视化"):
        """
        生成HTML文件
        
        Args:
            output_file: 输出文件路径
            title: 图表标题
        """
        all_series = self.chart_data['series']
        
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
        }}
        #chart {{
            width: 100%;
            height: 600px;
        }}
        .info {{
            margin-bottom: 20px;
            padding: 10px;
            background-color: #f5f5f5;
            border-radius: 5px;
        }}
    </style>
</head>
<body>
    <div class="info">
        <h1>{title}</h1>
        <p>生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}</p>
        <p>包含 {len(all_series)} 个回测策略的持仓比例曲线</p>
    </div>
    <div id="chart"></div>
    
    <script>
        var chartDom = document.getElementById('chart');
        var myChart = echarts.init(chartDom);
        
        var option = {{
            title: {{
                text: '{title}',
                left: 'center'
            }},
            tooltip: {{
                trigger: 'axis',
                formatter: function(params) {{
                    var result = params[0].axisValue + '<br/>';
                    params.forEach(function(item) {{
                        result += item.marker + item.seriesName + ': ' + item.value[1].toFixed(2) + '%<br/>';
                    }});
                    return result;
                }}
            }},
            legend: {{
                top: '30px',
                type: 'scroll'
            }},
            grid: {{
                left: '3%',
                right: '4%',
                bottom: '15%',
                top: '80px',
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
                axisLabel: {{
                    formatter: '{{value}}%'
                }},
                min: 0,
                max: 100
            }},
            series: {json.dumps(all_series, ensure_ascii=False)}
        }};
        
        myChart.setOption(option);
        
        // 响应式调整
        window.addEventListener('resize', function() {{
            myChart.resize();
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
    parser = argparse.ArgumentParser(description='持仓比例可视化脚本')
    parser.add_argument('--input_dir', required=True, help='回测数据目录路径')
    parser.add_argument('--output', default=None, help='输出HTML文件路径（默认在输出目录下生成 <输入目录名>_position_ratio_visualization.html）')
    
    args = parser.parse_args()
    
    # 转换为绝对路径
    project_root = Path(__file__).parent.parent
    input_dir = Path(args.input_dir)
    if not input_dir.is_absolute():
        input_dir = project_root / input_dir
    
    # 根据输入文件夹名称创建对应的输出子文件夹
    input_folder_name = input_dir.name
    output_dir = project_root / 'output' / input_folder_name
    
    # 计算默认输出文件名：<输入目录名>_position_ratio_visualization.html
    default_output_name = f"{input_folder_name}_position_ratio_visualization.html"
    
    if args.output and str(args.output).strip():
        # 用户指定了输出文件名，则遵循其命名，但仍落在对应输出子目录
        provided = Path(args.output)
        if not provided.is_absolute():
            output_file = output_dir / provided
        else:
            # 如果用户提供了绝对路径，仍然使用对应的子文件夹结构
            output_file = output_dir / provided.name
    else:
        # 未指定时使用默认带前缀的文件名
        output_file = output_dir / default_output_name
    
    # 确保输出目录存在
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        # 1. 识别持仓比例文件
        print("正在识别持仓比例文件...")
        identifier = PositionRatioFileIdentifier(str(input_dir))
        files_info = identifier.identify_files()
        print(f"找到 {len(files_info)} 个持仓比例文件")
        
        if not files_info:
            print("错误: 未找到任何持仓比例文件")
            return 1
        
        # 2. 初始化可视化器
        visualizer = EChartsVisualizer()
        
        # 3. 加载数据加载器
        data_loader = PositionRatioDataLoader()
        
        # 4. 处理每个持仓比例文件
        print("正在加载持仓比例数据...")
        for file_info in files_info:
            print(f"处理: {file_info['backtest_name']}")
            
            try:
                # 加载持仓比例数据
                position_data = data_loader.load_position_ratio_data(file_info['position_file'])
                
                # 提取时间序列
                time_series = data_loader.extract_time_series(position_data)
                
                # 添加到可视化器
                visualizer.add_position_ratio_series(
                    file_info['backtest_name'],
                    time_series['dates'],
                    time_series['position_ratios']
                )
                
                print(f"  - 加载了 {len(time_series['dates'])} 个数据点")
                
            except Exception as e:
                print(f"警告: 处理文件失败 {file_info['backtest_name']}: {e}")
                import traceback
                traceback.print_exc()
        
        # 5. 生成可视化文件
        print("正在生成可视化文件...")
        visualizer.generate_html(
            str(output_file),
            "持仓比例可视化"
        )
        
        print("分析完成!")
        
    except Exception as e:
        print(f"错误: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main())

