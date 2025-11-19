# 持仓比例可视化脚本

该脚本用于绘制多个回测策略的持仓比例曲线，使用 ECharts 进行可视化。

## 功能特性

1. 自动识别指定目录中的所有持仓比例数据文件（`*position_ratio_*.json`）
2. 提取每个回测策略的持仓比例时间序列数据
3. 使用 ECharts 生成交互式的可视化图表
4. 支持多条曲线同时展示和对比
5. 支持缩放、滚动查看不同时间段的数据

## 使用方法

### 基本用法

```bash
python3 scripts/position_ratio_visualization.py --input_dir <回测数据目录>
```

或者通过 main.py 调用：

```bash
python3 main.py position_ratio_visualization --input_dir <回测数据目录>
```

例如：

```bash
python3 main.py position_ratio_visualization --input_dir backtest_data/naive_top30_3800
```

这将生成一个 HTML 文件：`output/naive_top30_3800/naive_top30_3800_position_ratio_visualization.html`

### 自定义输出文件名

```bash
python3 main.py position_ratio_visualization --input_dir backtest_data/naive_top30_3800 --output my_custom_name.html
```

这将生成一个 HTML 文件：`output/naive_top30_3800/my_custom_name.html`

## 输入文件格式

脚本会自动识别以下格式的文件：

- 文件名模式：`*position_ratio_*.json`
- 文件格式：JSON

示例文件名：
- `S5-5-HP10-TS10-200901-202507_1000M_position_ratio_5151ac4ea131974382b40266285eb814.json`
- `Top30-T10_position_ratio_abc123def456.json`

## 数据格式

持仓比例数据文件应包含以下字段：

```json
{
  "backtest_id": "回测ID",
  "backtest_name": "回测名称",
  "data_type": "balances",
  "download_time": "下载时间",
  "balances": [
    {
      "time": "2009-01-05 16:00:00",
      "aval_cash": 1000000000.0,
      "total_value": 1000000000.0,
      "cash": 1000000000.0,
      "net_value": 1000000000.0,
      "position_ratio": 0.0
    },
    ...
  ]
}
```

## 输出文件

生成的 HTML 文件包含：

1. **标题信息**：生成时间、回测策略数量
2. **交互式图表**：
   - 所有策略的持仓比例曲线
   - 鼠标悬停显示详细数值
   - 图例可点击切换显示/隐藏特定曲线
   - 时间轴缩放和滚动功能

## 输出目录结构

脚本遵循以下输出目录结构：

```
project_root/
├── backtest_data/
│   └── <input_folder_name>/
│       ├── *position_ratio_*.json  # 输入文件
│       └── ...
└── output/
    └── <input_folder_name>/
        ├── <input_folder_name>_position_ratio_visualization.html  # 输出文件
        └── ...
```

## 参数说明

| 参数 | 必需 | 默认值 | 说明 |
|------|------|--------|------|
| `--input_dir` | 是 | - | 回测数据目录路径（相对或绝对路径） |
| `--output` | 否 | `<输入目录名>_position_ratio_visualization.html` | 输出 HTML 文件名 |

## 示例

### 示例 1：处理策略5数据

```bash
python3 main.py position_ratio_visualization --input_dir backtest_data/策略5-166-1113-limitratio
```

输出：`output/策略5-166-1113-limitratio/策略5-166-1113-limitratio_position_ratio_visualization.html`

### 示例 2：处理 naive_top30 数据

```bash
python3 main.py position_ratio_visualization --input_dir backtest_data/naive_top30_3800
```

输出：`output/naive_top30_3800/naive_top30_3800_position_ratio_visualization.html`

## 图表功能

生成的图表支持以下交互功能：

1. **缩放**：使用鼠标滚轮或拖动时间轴滑块进行缩放
2. **平移**：拖动图表区域或移动时间轴滑块进行平移
3. **图例控制**：点击图例可显示/隐藏对应的曲线
4. **数据提示**：鼠标悬停在曲线上可查看具体日期和持仓比例值
5. **响应式布局**：窗口大小改变时自动调整图表大小

## 与 hedge_analysis_visualization.py 的一致性

该脚本在以下方面与 `hedge_analysis_visualization.py` 保持一致：

1. **参数设计**：使用相同的 `--input_dir` 和 `--output` 参数
2. **输出目录结构**：遵循相同的输出目录逻辑
3. **文件命名规则**：使用 `<输入目录名>_<功能名>_visualization.html` 的命名规则
4. **代码结构**：采用类似的类组织和函数命名风格

## 注意事项

1. 确保输入目录中包含至少一个持仓比例数据文件
2. 输出目录会自动创建（如果不存在）
3. 如果输出文件已存在，会被覆盖
4. 图表需要在浏览器中打开查看
5. 需要互联网连接以加载 ECharts 库（从 CDN）

## 故障排除

### 找不到持仓比例文件

如果脚本报告找到 0 个文件，请检查：
- 输入目录路径是否正确
- 目录中是否包含 `*position_ratio_*.json` 文件
- 文件名格式是否正确

### 数据加载失败

如果某个文件加载失败，脚本会：
- 显示警告信息
- 跳过该文件
- 继续处理其他文件

### HTML 文件无法打开

- 使用现代浏览器（Chrome、Firefox、Safari、Edge）打开
- 确保有互联网连接（用于加载 ECharts 库）

