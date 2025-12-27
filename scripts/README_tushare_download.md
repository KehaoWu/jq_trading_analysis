# Tushare 指数数据下载工具使用说明

## 功能说明

这个脚本用于从 Tushare 下载指数数据，并保存到 `index_data` 目录，数据格式与现有的 jsonl 文件完全一致。

## 前置要求

1. 安装 tushare 和 pandas：
```bash
pip install tushare pandas
```

2. 获取 Tushare Token：
   - 访问 https://tushare.pro/register
   - 注册账号并获取 token
   - 设置环境变量：
   ```bash
   export TUSHARE_TOKEN='你的token'
   ```

## 使用方法

### 方式1: 下载所有预定义指数（推荐）

```bash
cd /home/wukehao/Projects/jq_trading_analysis/scripts
python download_index_data_tushare.py
```

这将下载以下指数：
- 中证1000 (zz1000)
- 中证500 (zz500)
- 沪深300 (hs300)
- 创业板指 (cyb)

### 方式2: 下载单个指数

修改 `main()` 函数，注释掉 `download_all_predefined()`，使用：

```python
# 下载中证1000
downloader.download_predefined_index('zz1000', start_date="20090105")

# 下载沪深300
downloader.download_predefined_index('hs300', start_date="20090105")
```

### 方式3: 下载自定义指数

```python
downloader.download_index(
    ts_code='000001.SH',    # Tushare指数代码
    index_name='上证指数',   # 中文名称
    file_prefix='sh',       # 文件名前缀
    start_date="20090105",  # 开始日期
    end_date="20251219"     # 结束日期（可选，默认今天）
)
```

## 预定义指数列表

| 指数键名 | Tushare代码 | 指数名称 | 文件前缀 |
|---------|------------|---------|---------|
| zz1000  | 000852.SH  | 中证1000 | zz1000  |
| zz500   | 000905.SH  | 中证500  | zz500   |
| hs300   | 000300.SH  | 沪深300  | hs300   |
| cyb     | 399006.SZ  | 创业板指 | cyb     |

## 输出格式

数据保存在 `index_data/` 目录，文件名格式：`{prefix}_{start_date}_{end_date}.jsonl`

例如：`zz1000_20090105_20251219.jsonl`

每行是一个JSON对象，包含以下字段：

```json
{
  "date": "2009-01-05",
  "code": "sh.000852",
  "open": 1856.371,
  "high": 1920.098,
  "low": 1853.959,
  "close": 1920.098,
  "preclose": 1830.823,
  "volume": 1400926348,
  "amount": 8660465219.0,
  "adjustflag": "3",
  "turn": 0.0,
  "tradestatus": "1",
  "pctChg": 4.876224,
  "isST": "0",
  "index_name": "中证1000"
}
```

## 日志输出

脚本会生成两份日志：
1. 控制台输出：实时显示下载进度和摘要
2. 日志文件：`download_index_data_tushare.log`

## 常见问题

### Q1: 提示"未设置TUSHARE_TOKEN环境变量"

**A**: 设置环境变量：
```bash
export TUSHARE_TOKEN='你的token'
```

或者在脚本中直接传入：
```python
downloader = IndexDataDownloader(data_dir="index_data", token="你的token")
```

### Q2: API调用频率限制

**A**: Tushare 有API调用频率限制，如果遇到限制，可以：
- 升级账号权限
- 减少下载的数据量
- 分批下载

### Q3: 数据格式不一致

**A**: 脚本已经处理了数据格式转换：
- 日期格式：YYYYMMDD → YYYY-MM-DD
- 代码格式：000852.SH → sh.000852, 399006.SZ → sz.399006
- 所有字段类型和字段名都与现有格式一致

## 代码示例

### 示例1: 下载最近一年的数据

```python
from datetime import datetime, timedelta

today = datetime.now().strftime("%Y%m%d")
one_year_ago = (datetime.now() - timedelta(days=365)).strftime("%Y%m%d")

downloader = IndexDataDownloader(token=token)
downloader.download_predefined_index('zz1000', start_date=one_year_ago, end_date=today)
```

### 示例2: 批量下载多个指数

```python
downloader = IndexDataDownloader(token=token)

indices = ['zz1000', 'zz500', 'hs300', 'cyb']
for index_key in indices:
    downloader.download_predefined_index(index_key, start_date="20200101")
```

## 技术细节

- 数据来源：Tushare Pro API
- 数据接口：`pro.index_daily()`
- 成交量单位：手
- 成交额单位：千元
- 日期排序：从早到晚（升序）

## 联系与支持

如有问题，请查看：
- Tushare官方文档：https://tushare.pro/document/2
- 项目文档：`/docs/` 目录
