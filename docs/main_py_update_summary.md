# Main.py 更新汇总

## 更新时间
2025-11-19

## 更新内容

### 1. 新增脚本入口

在 `main.py` 中新增以下脚本的调用入口：

#### ✅ hedge_analysis_visualization
- **描述**：对冲分析可视化（自动识别文件并生成完整分析）
- **调用示例**：
  ```bash
  python main.py hedge_analysis_visualization --input_dir backtest_data/ex_tm1_top30 --index zz500
  ```

#### ✅ position_ratio_visualization (新创建)
- **描述**：持仓比例可视化（绘制多个回测的持仓比例曲线）
- **调用示例**：
  ```bash
  python main.py position_ratio_visualization --input_dir backtest_data/naive_top30_3800
  ```

#### ✅ back_test_downloader
- **描述**：从聚宽下载回测数据
- **调用示例**：
  ```bash
  python main.py back_test_downloader
  ```

### 2. Scripts 文件夹脚本清单

以下是 `scripts/` 文件夹中所有 Python 脚本及其在 `main.py` 中的状态：

| 脚本名称 | 是否在 main.py 中 | 说明 |
|---------|------------------|------|
| `back_test_downloader.py` | ✅ | 从聚宽下载回测数据 |
| `backtest_hedge_plot.py` | ✅ | 回测对冲数据计算和可视化 |
| `backtest_vis.py` | ✅ | 回测结果可视化 |
| `calculate_daily_returns.py` | ✅ | 计算日收益率 |
| `cleanup.py` | ✅ | 清理项目中的临时文件和测试脚本 |
| `combine_two_backtests.py` | ✅ | 合并两个不同时间段的回测数据 |
| `cumulative_returns_comparison.py` | ✅ | 累积收益曲线对比分析 |
| `hedge_analysis_visualization.py` | ✅ | 对冲分析可视化 |
| `position_ratio_visualization.py` | ✅ | 持仓比例可视化 |
| `metric_calc_example.py` | ❌ | 示例脚本，不需要添加到 main.py |

### 3. main.py 帮助信息

运行 `python main.py --help` 或 `python main.py` 会显示以下帮助信息：

```
聚宽交易分析项目主入口

使用方法: python main.py <功能名称> [参数...]

可用命令:
  calculate_daily_returns - 计算日收益率
  combine_two_backtests - 合并两个不同时间段的回测数据
  backtest_hedge_plot - 回测对冲数据计算和可视化
  backtest_vis - 回测结果可视化
  hedge_analysis_visualization - 对冲分析可视化（自动识别文件并生成完整分析）
  position_ratio_visualization - 持仓比例可视化（绘制多个回测的持仓比例曲线）
  cumulative_returns_comparison - 累积收益曲线对比分析
  back_test_downloader - 从聚宽下载回测数据
  cleanup - 清理项目中的临时文件和测试脚本

使用 'python main.py <功能名称> --help' 查看具体功能的详细帮助信息
```

### 4. 测试结果

所有脚本都已测试通过 `main.py` 调用：

#### ✅ position_ratio_visualization 测试
```bash
$ python main.py position_ratio_visualization --input_dir backtest_data/naive_top30_3800
正在识别持仓比例文件...
找到 6 个持仓比例文件
正在加载持仓比例数据...
处理: Top30-T10
  - 加载了 4019 个数据点
处理: Top30-T15
  - 加载了 4019 个数据点
...
可视化文件已生成: output/naive_top30_3800/naive_top30_3800_position_ratio_visualization.html
分析完成!
```

#### ✅ hedge_analysis_visualization 测试
```bash
$ python main.py hedge_analysis_visualization --help
usage: main.py hedge_analysis_visualization [-h] --input_dir INPUT_DIR --index INDEX ...
```

### 5. 文档更新

文档字符串已更新，包含所有可用功能的完整列表和使用示例。

### 6. 移除的命令

- `daily_orders_positions_bar_chart` - 该命令在 scripts 文件夹中没有对应的脚本，已从帮助信息中移除

## 使用建议

1. **统一入口**：建议使用 `python main.py <功能名称>` 的方式调用所有脚本，而不是直接调用 `scripts/` 目录下的脚本
2. **查看帮助**：对于任何命令，都可以使用 `python main.py <功能名称> --help` 查看详细帮助信息
3. **参数传递**：所有原始脚本的参数都可以直接传递给 main.py

## 兼容性

- 原有的直接调用方式（如 `python scripts/position_ratio_visualization.py`）仍然有效
- 通过 main.py 调用和直接调用的功能完全一致

