#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
指数数据下载器 - Tushare版本
从Tushare下载指数数据并保存到index_data目录
"""

import tushare as ts
import pandas as pd
import os
import json
from datetime import datetime
import logging

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('download_index_data_tushare.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class IndexDataDownloader:
    """指数数据下载器 - Tushare版本"""
    
    def __init__(self, data_dir="index_data", token=None):
        """
        初始化下载器
        
        Args:
            data_dir: 数据保存目录，默认为 index_data
            token: Tushare API token
        """
        self.data_dir = data_dir
        self.ensure_data_dir()
        
        # 初始化Tushare
        if token:
            ts.set_token(token)
        self.pro = ts.pro_api()
        
        # 预定义的常用指数
        self.predefined_indices = {
            'zz1000': {
                'ts_code': '000852.SH',
                'name': '中证1000'
            },
            'zz500': {
                'ts_code': '000905.SH',
                'name': '中证500'
            },
            'hs300': {
                'ts_code': '000300.SH',
                'name': '沪深300'
            },
            'cyb': {
                'ts_code': '399006.SZ',
                'name': '创业板指'
            }
        }
    
    def ensure_data_dir(self):
        """确保数据目录存在"""
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)
            logger.info(f"创建数据目录: {self.data_dir}")
    
    def format_index_data(self, df, ts_code, index_name):
        """
        格式化指数数据以匹配现有的jsonl格式
        
        Args:
            df: 原始数据DataFrame (Tushare格式)
            ts_code: Tushare指数代码
            index_name: 指数中文名称
            
        Returns:
            DataFrame: 格式化后的数据
        """
        formatted_data = []
        
        for _, row in df.iterrows():
            # 转换日期格式: YYYYMMDD -> YYYY-MM-DD
            trade_date = pd.to_datetime(row['trade_date'], format='%Y%m%d').strftime('%Y-%m-%d')
            
            # 转换代码格式: 000852.SH -> sh.000852, 399006.SZ -> sz.399006
            if ts_code.endswith('.SH'):
                code = f"sh.{ts_code.split('.')[0]}"
            elif ts_code.endswith('.SZ'):
                code = f"sz.{ts_code.split('.')[0]}"
            else:
                code = ts_code
            
            # 构建数据记录，确保与现有格式完全一致
            record = {
                "date": trade_date,
                "code": code,
                "open": float(row['open']) if pd.notna(row['open']) else 0.0,
                "high": float(row['high']) if pd.notna(row['high']) else 0.0,
                "low": float(row['low']) if pd.notna(row['low']) else 0.0,
                "close": float(row['close']) if pd.notna(row['close']) else 0.0,
                "preclose": float(row['pre_close']) if pd.notna(row['pre_close']) else 0.0,
                "volume": int(row['vol']) if pd.notna(row['vol']) else 0,  # 成交量，单位：手
                "amount": float(row['amount']) if pd.notna(row['amount']) else 0.0,  # 成交额，单位：千元
                "adjustflag": "3",
                "turn": 0.0,
                "tradestatus": "1",
                "pctChg": float(row['pct_chg']) if pd.notna(row['pct_chg']) else 0.0,
                "isST": "0",
                "index_name": index_name
            }
            
            formatted_data.append(record)
        
        return pd.DataFrame(formatted_data)
    
    def save_to_jsonl(self, df, file_prefix, start_date, end_date):
        """
        保存数据为JSONL格式
        
        Args:
            df: 数据DataFrame
            file_prefix: 文件名前缀 (如: 'zz1000', 'hs300')
            start_date: 开始日期 (格式: YYYYMMDD)
            end_date: 结束日期 (格式: YYYYMMDD)
        """
        if df is None or df.empty:
            logger.warning("没有数据需要保存")
            return
        
        # 生成文件名: {prefix}_{start}_{end}.jsonl
        filename = f"{file_prefix}_{start_date}_{end_date}.jsonl"
        filepath = os.path.join(self.data_dir, filename)
        
        try:
            with open(filepath, 'w', encoding='utf-8') as f:
                for _, row in df.iterrows():
                    json.dump(row.to_dict(), f, ensure_ascii=False)
                    f.write('\n')
            
            logger.info(f"数据已保存到: {filepath}")
            logger.info(f"保存记录数: {len(df)}")
            
        except Exception as e:
            logger.error(f"保存数据失败: {str(e)}")
    
    def download_index(self, ts_code, index_name, file_prefix, start_date="20090105", end_date=None):
        """
        下载指定指数数据
        
        Args:
            ts_code: Tushare指数代码 (如: '000852.SH', '399006.SZ')
            index_name: 指数中文名称 (如: '中证1000', '创业板指')
            file_prefix: 文件名前缀 (如: 'zz1000', 'cyb')
            start_date: 开始日期 (格式: YYYYMMDD)
            end_date: 结束日期 (格式: YYYYMMDD)，默认为今天
            
        Returns:
            DataFrame: 格式化后的数据
        """
        if end_date is None:
            end_date = datetime.now().strftime("%Y%m%d")
        
        logger.info("=" * 60)
        logger.info(f"开始下载 {index_name} 数据...")
        logger.info(f"Tushare代码: {ts_code}")
        logger.info(f"日期范围: {start_date} 到 {end_date}")
        
        try:
            # 调用Tushare API获取指数日线数据
            df = self.pro.index_daily(
                ts_code=ts_code,
                start_date=start_date,
                end_date=end_date
            )
            
            if df is None or df.empty:
                logger.warning(f"未获取到 {index_name} 数据")
                return None
            
            # 按日期排序（从早到晚）
            df = df.sort_values('trade_date')
            
            # 转换为目标格式
            df_formatted = self.format_index_data(df, ts_code, index_name)
            
            if df_formatted is not None and not df_formatted.empty:
                # 保存为JSONL格式
                self.save_to_jsonl(df_formatted, file_prefix, start_date, end_date)
                
                # 生成数据摘要
                self.print_summary(df_formatted, index_name, start_date, end_date)
                
                logger.info(f"✓ 成功下载 {index_name} 数据，共 {len(df_formatted)} 条记录")
                return df_formatted
            else:
                logger.error(f"✗ 格式化 {index_name} 数据失败")
                return None
                
        except Exception as e:
            logger.error(f"✗ 下载 {index_name} 数据失败: {str(e)}")
            return None
    
    def download_predefined_index(self, index_key, start_date="20090105", end_date=None):
        """
        下载预定义的指数数据
        
        Args:
            index_key: 预定义指数键名 ('zz1000', 'zz500', 'hs300', 'cyb')
            start_date: 开始日期 (格式: YYYYMMDD)
            end_date: 结束日期 (格式: YYYYMMDD)
            
        Returns:
            DataFrame: 格式化后的数据
        """
        if index_key not in self.predefined_indices:
            logger.error(f"不支持的预定义指数: {index_key}")
            logger.info(f"可用的预定义指数: {list(self.predefined_indices.keys())}")
            return None
        
        index_info = self.predefined_indices[index_key]
        return self.download_index(
            ts_code=index_info['ts_code'],
            index_name=index_info['name'],
            file_prefix=index_key,
            start_date=start_date,
            end_date=end_date
        )
    
    def print_summary(self, df, index_name, start_date, end_date):
        """
        打印数据摘要
        
        Args:
            df: 数据DataFrame
            index_name: 指数名称
            start_date: 开始日期
            end_date: 结束日期
        """
        if df is None or df.empty:
            return
        
        start_close = df.iloc[0]['close']
        end_close = df.iloc[-1]['close']
        total_return = (end_close / start_close - 1) * 100
        
        logger.info("-" * 60)
        logger.info(f"【{index_name} 数据摘要】")
        logger.info(f"  数据源      : Tushare")
        logger.info(f"  请求范围    : {start_date} 到 {end_date}")
        logger.info(f"  实际范围    : {df['date'].min()} 到 {df['date'].max()}")
        logger.info(f"  记录数量    : {len(df):,} 条")
        logger.info(f"  起始点位    : {start_close:,.2f}")
        logger.info(f"  结束点位    : {end_close:,.2f}")
        logger.info(f"  期间涨跌幅  : {total_return:+.2f}%")
        logger.info(f"  最高点位    : {df['high'].max():,.2f}")
        logger.info(f"  最低点位    : {df['low'].min():,.2f}")
        logger.info(f"  平均成交量  : {df['volume'].mean():,.0f} 手")
        logger.info(f"  平均成交额  : {df['amount'].mean():,.2f} 千元")
        logger.info("-" * 60)
    
    def download_all_predefined(self, start_date="20090105", end_date=None):
        """
        下载所有预定义的指数数据
        
        Args:
            start_date: 开始日期 (格式: YYYYMMDD)
            end_date: 结束日期 (格式: YYYYMMDD)
        """
        logger.info("=" * 60)
        logger.info("开始批量下载所有预定义指数数据...")
        logger.info("=" * 60)
        
        results = {}
        for index_key in self.predefined_indices.keys():
            df = self.download_predefined_index(index_key, start_date, end_date)
            results[index_key] = df is not None
        
        # 打印总结
        logger.info("=" * 60)
        logger.info("下载任务完成！")
        logger.info("-" * 60)
        for index_key, success in results.items():
            status = "✓ 成功" if success else "✗ 失败"
            logger.info(f"  {self.predefined_indices[index_key]['name']:8s} : {status}")
        logger.info("=" * 60)


def main():
    """主函数"""
    # 从环境变量获取Tushare token
    token = os.getenv('TUSHARE_TOKEN')
    if not token:
        logger.error("=" * 60)
        logger.error("错误: 未设置TUSHARE_TOKEN环境变量")
        logger.error("请先设置环境变量:")
        logger.error("  export TUSHARE_TOKEN='你的token'")
        logger.error("或者在代码中直接传入token参数")
        logger.error("=" * 60)
        return
    
    # 创建下载器实例
    downloader = IndexDataDownloader(data_dir="index_data", token=token)
    
    # 获取当前日期
    today = datetime.now().strftime("%Y%m%d")
    
    # 选择下载方式
    # 方式1: 下载所有预定义指数
    downloader.download_all_predefined(start_date="20090105", end_date=today)
    
    # 方式2: 下载单个预定义指数
    # downloader.download_predefined_index('zz1000', start_date="20090105", end_date=today)
    # downloader.download_predefined_index('hs300', start_date="20090105", end_date=today)
    
    # 方式3: 下载自定义指数
    # downloader.download_index(
    #     ts_code='000001.SH',    # 上证指数
    #     index_name='上证指数',
    #     file_prefix='sh',
    #     start_date="20090105",
    #     end_date=today
    # )


if __name__ == "__main__":
    main()
