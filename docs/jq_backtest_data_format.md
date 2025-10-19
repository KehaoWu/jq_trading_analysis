以下数据是来自聚宽的回测数据

```
{
    "type": "daily_data",
    "date": "20090318", ## 年月日
    "data": {
        "benchmark": { ## 指数，注意请不要使用此处的数据作为benchmark
            "count": 1,
            "records": [
                {
                    "timestamp": 1237363200000,
                    "date_string": "20090318 16:00:00",
                    "value": 28.33 ## 累积收益率：28.33%
                }
            ]
        },
        "gains": {
            "count": 2,
            "records": [
                {
                    "timestamp": 1237363200000,
                    "date_string": "20090318 16:00:00",
                    "value": 0,
                    "sub_field": "earn"
                },
                {
                    "timestamp": 1237363200000,
                    "date_string": "20090318 16:00:00",
                    "value": 0,
                    "sub_field": "lose"
                }
            ]
        },
        "orders": {
            "count": 2,
            "records": [
                {
                    "timestamp": 1237363200000,
                    "date_string": "20090318 16:00:00",
                    "value": 0,
                    "sub_field": "buy"
                },
                {
                    "timestamp": 1237363200000,
                    "date_string": "20090318 16:00:00",
                    "value": 0,
                    "sub_field": "sell"
                }
            ]
        },
        "overallReturn": {
            "count": 1,
            "records": [
                {
                    "timestamp": 1237363200000,
                    "date_string": "20090318 16:00:00",
                    "value": 0.1 ## 累积收益率：0.1%
                }
            ]
        }
    }
}
```