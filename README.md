# 聚宽交易分析项目

这是一个用于分析聚宽交易数据的项目，提供了多种数据处理和分析功能。

## 项目结构

- `docs/` - 存放说明文档
- `prds/` - 存放需求文档
- `backtest_data/` - 存放回测数据
- `hedge_data/` - 存放对冲后的数据
- `index_data/` - 存放指数涨跌幅数据
- `index_constituents_data/` - 存放指数历史成分股数据
- `libs/` - 存放公共函数和方法
- `scripts/` - 存放具体需求的分析脚本
- `main.py` - 项目主入口文件

## 使用方法

本项目使用 `main.py` 作为统一入口，所有功能都可以通过 `python main.py <功能名称> [参数...]` 的方式调用。

### 可用命令

1. **calculate_daily_returns** - 计算日收益率
   ```
   python main.py calculate_daily_returns <回测数据文件> [输出文件]
   ```

2. **hedge_example** - 对冲数据计算示例
   ```
   python main.py hedge_example -b <回测数据文件> -i <指数数据文件> [-p <持仓数据文件>] [-r <对冲比例>] [-o <输出文件>] [-f <输出格式>]
   ```

3. **hedge_example_advanced** - 高级对冲数据计算示例
   ```
   python main.py hedge_example_advanced -b <回测数据文件> -i <指数数据文件> [-p <持仓数据文件>] [-r <对冲比例>] [-o <输出文件>] [-f <输出格式>]
   ```

4. **hedge_example_advanced_echarts** - 使用ECharts的高级对冲数据计算示例
   ```
   python main.py hedge_example_advanced_echarts -b <回测数据文件> -i <指数数据文件> [-p <持仓数据文件>] [-r <对冲比例>] [-o <输出文件>] [-f <输出格式>]
   ```

5. **cleanup** - 清理项目中的临时文件和测试脚本
   ```
   python main.py cleanup [--temp] [--test] [--hedge <天数>] [--all]
   ```

## 环境要求

本项目使用Python虚拟环境，请确保在运行前激活虚拟环境：

```bash
source .venv/bin/activate
```

## 代码规范

本项目遵循PEP8代码规范。

## 绘图

本项目使用ECharts 5.0进行绘图。

## 开发注意事项

- 每次开发完都清删除不必要的临时文件和测试脚本
- 所有脚本的最终运行起点为main.py