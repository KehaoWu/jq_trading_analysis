# 指数数据格式说明

指数数据采用JSONL格式，每行包含一个交易日的指数数据。

## 数据格式

每行数据是一个JSON对象，包含以下字段：

```json
{
  "date": "2009-01-05",        // 交易日期，格式为YYYY-MM-DD
  "code": "sh.000300",         // 指数代码
  "open": 1848.326,            // 开盘价
  "high": 1882.959,            // 最高价
  "low": 1837.839,             // 最低价
  "close": 1882.959,           // 收盘价
  "preclose": 1817.722,        // 昨收价
  "volume": 48187020,          // 成交量
  "amount": 39217076.79,       // 成交额
  "adjustflag": "3",           // 复权标志
  "turn": 0.0,                 // 换手率
  "tradestatus": "1",          // 交易状态
  "pctChg": 3.5889,            // 涨跌幅百分比（对冲计算中使用的字段）
  "isST": "0",                 // 是否ST股
  "index_name": "沪深300"       // 指数名称
}
```

## 字段说明

| 字段名 | 类型 | 说明 |
|--------|------|------|
| date | string | 交易日期，格式为YYYY-MM-DD |
| code | string | 指数代码，如"sh.000300"表示沪深300 |
| open | float | 开盘价 |
| high | float | 最高价 |
| low | float | 最低价 |
| close | float | 收盘价 |
| preclose | float | 昨收价 |
| volume | integer | 成交量 |
| amount | float | 成交额 |
| adjustflag | string | 复权标志 |
| turn | float | 换手率 |
| tradestatus | string | 交易状态 |
| pctChg | float | 涨跌幅百分比，对冲计算中使用的字段 |
| isST | string | 是否ST股 |
| index_name | string | 指数名称 |

## 注意事项

1. 对冲计算主要使用 `date` 和 `pctChg` 字段
2. `pctChg` 字段表示指数的涨跌幅百分比，是对冲收益率计算的关键输入
3. 日期格式统一为YYYY-MM-DD，便于与其他数据格式匹配
4. 文件扩展名为 `.jsonl`，表示每行是一个JSON对象