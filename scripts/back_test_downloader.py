#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
JoinQuant回测数据下载器
基于提供的curl命令开发，用于下载指定backtestId的回测结果数据
支持自动迭代offset获取完整数据，保存benchmark、gains、orders、overallReturn字段
支持按日期转换数据，从config.yaml读取认证信息
"""

import requests
import json
import argparse
import os
import yaml
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple
from collections import defaultdict
import re
from bs4 import BeautifulSoup


class BacktestDataDownloader:
    """JoinQuant回测数据下载器"""
    
    def __init__(self, config_path: str = "config.yaml", source_note: str = ""):
        self.base_url = "https://www.joinquant.com/algorithm/backtest/result"
        self.detail_url = "https://www.joinquant.com/algorithm/backtest/detail"
        self.session = requests.Session()
        
        # 数据来源备注
        self.source_note = source_note
        
        # 从curl命令中提取的headers
        self.headers = {
            'Accept': 'application/json, text/javascript, */*; q=0.01',
            'Accept-Language': 'en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7',
            'Connection': 'keep-alive',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Origin': 'https://www.joinquant.com',
            'Referer': 'https://www.joinquant.com/algorithm/backtest/detail',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36',
            'X-Requested-With': 'XMLHttpRequest',
            'sec-ch-ua': '"Not;A=Brand";v="99", "Google Chrome";v="139", "Chromium";v="139"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"macOS"'
        }
        
        # 用于获取回测详情页面的headers（基于提供的curl命令）
        self.detail_headers = {
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7',
            'Cache-Control': 'max-age=0',
            'Connection': 'keep-alive',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36',
            'sec-ch-ua': '"Google Chrome";v="141", "Not?A_Brand";v="8", "Chromium";v="141"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"'
        }
        
        # 从curl命令中提取的cookies字符串
        self.detail_cookies_str = 'default_position_cols=amount%2Cprice%2Cposition%2Cgain%2Cvalue; uid=wKgyrWiqZuQDPHbPTPlqAg==; getStrategy=1; token=96a5b8fb803888724eb1600df20c347c1c8cd880; from=edit; PHPSESSID=dvviqlgmic069993bb7etlu3g0; finishExtInfo5d079f830afd624cebfd9bb258c6ed9d=1; _xsrf=2|33571f5a|b7277403fb3c38fe25d7a4d2a376fcaf|1760796129; newBacktest=11abd239c0b50c21fa767dd6a546efc6'
        
        # 目标字段配置 - 保存所有四个字段
        self.target_fields = ['benchmark', 'gains', 'orders', 'overallReturn']
        
        # 加载配置
        self.config = self.load_config(config_path)
        self.cookies = {}
        self.token = ""
        self.data_dir = ""
        
        # 设置认证信息
        self.setup_auth()
    
    def load_config(self, config_path: str) -> Dict[str, Any]:
        """加载配置文件"""
        try:
            # 尝试从脚本目录的上级目录加载配置
            script_dir = Path(__file__).parent
            config_file = script_dir.parent / config_path
            
            if not config_file.exists():
                # 如果上级目录没有，尝试当前目录
                config_file = Path(config_path)
            
            if not config_file.exists():
                raise FileNotFoundError(f"配置文件未找到: {config_path}")
            
            with open(config_file, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
            
            print(f"成功加载配置文件: {config_file}")
            return config
            
        except Exception as e:
            print(f"加载配置文件失败: {e}")
            print("请确保config.yaml文件存在并包含joinquant配置")
            raise
    
    def setup_auth(self):
        """设置认证信息"""
        try:
            joinquant_config = self.config.get('joinquant', {})
            cookies_str = joinquant_config.get('cookies', '')
            self.token = joinquant_config.get('token', '')
            self.data_dir = joinquant_config.get('data_dir', 'other_scripts/backtest_data')
            
            if not cookies_str or not self.token:
                print("警告: config.yaml中的cookies或token为空")
                print("请在config.yaml中设置joinquant.cookies和joinquant.token")
            
            # 解析cookies字符串
            if cookies_str:
                for cookie in cookies_str.split('; '):
                    if '=' in cookie:
                        key, value = cookie.split('=', 1)
                        self.cookies[key] = value
            
            self.session.cookies.update(self.cookies)
            print(f"认证信息设置完成，数据目录: {self.data_dir}")
            
        except Exception as e:
            print(f"设置认证信息失败: {e}")
            raise
    
    def set_auth_info(self, cookies_str: str = None, token: str = None):
        """
        手动设置认证信息（覆盖配置文件）
        
        Args:
            cookies_str: cookie字符串，格式如 "uid=xxx; token=xxx; ..."
            token: POST请求中的token
        """
        if cookies_str:
            self.cookies = {}
            for cookie in cookies_str.split('; '):
                if '=' in cookie:
                    key, value = cookie.split('=', 1)
                    self.cookies[key] = value
            self.session.cookies.update(self.cookies)
        
        if token:
            self.token = token
        
        print("手动认证信息设置完成")
    
    def get_backtest_name(self, backtest_id: str) -> str:
        """
        获取回测名称
        
        Args:
            backtest_id: 回测ID
            
        Returns:
            回测名称，如果获取失败返回空字符串
        """
        try:
            # 构建详情页面URL
            detail_url = f"{self.detail_url}?backtestId={backtest_id}"
            
            # 解析并设置cookies
            detail_cookies = {}
            for cookie in self.detail_cookies_str.split('; '):
                if '=' in cookie:
                    key, value = cookie.split('=', 1)
                    detail_cookies[key] = value
            
            # 发送GET请求获取详情页面
            response = self.session.get(
                detail_url,
                headers=self.detail_headers,
                cookies=detail_cookies,
                timeout=30
            )

            
            response.raise_for_status()
            
            # 设置正确的编码
            if response.encoding is None or response.encoding == 'ISO-8859-1':
                response.encoding = 'utf-8'
            
            with open('backtest_detail.html', 'w', encoding='utf-8') as f:
                f.write(response.text)

            # 使用BeautifulSoup解析HTML
            soup = BeautifulSoup(response.text, 'html.parser')
            print(soup)
            # 尝试多种方式查找回测名称
            backtest_name = ""
            
            # 方法1: 优先查找id为title-box的span元素
            title_box = soup.find('span', id='title-box')
            print("标题", title_box)
            if title_box and title_box.text:
                backtest_name = title_box.text.strip()
                print(f"从title-box元素获取回测名称: {backtest_name}")
            
            # 方法2: 查找页面标题中的回测名称
            if not backtest_name:
                title_tag = soup.find('title')
                if title_tag and title_tag.text:
                    title_text = title_tag.text.strip()
                    # 提取标题中的回测名称（通常在"回测详情"之前）
                    if '回测详情' in title_text:
                        backtest_name = title_text.replace('回测详情', '').strip(' -')
            
            # 方法3: 查找包含回测名称的特定元素
            if not backtest_name:
                # 查找class包含backtest或name的元素
                name_elements = soup.find_all(['h1', 'h2', 'h3', 'div', 'span'], 
                                            class_=re.compile(r'(backtest|name|title)', re.I))
                for element in name_elements:
                    if element.text and element.text.strip():
                        text = element.text.strip()
                        if len(text) > 0 and len(text) < 200:  # 合理的名称长度
                            backtest_name = text
                            break
            
            # 方法4: 查找页面中的JavaScript变量
            if not backtest_name:
                script_tags = soup.find_all('script')
                for script in script_tags:
                    if script.string:
                        # 查找可能包含回测名称的JavaScript变量
                        name_match = re.search(r'["\']?name["\']?\s*:\s*["\']([^"\']+)["\']', script.string)
                        if name_match:
                            backtest_name = name_match.group(1)
                            break
                        
                        title_match = re.search(r'["\']?title["\']?\s*:\s*["\']([^"\']+)["\']', script.string)
                        if title_match:
                            backtest_name = title_match.group(1)
                            break
            
            # 清理回测名称
            if backtest_name:
                # 保留中文、英文、数字、下划线、连字符，将其他字符替换为下划线
                # 使用更宽泛的中文字符范围
                cleaned_name = ""
                for char in backtest_name:
                    if char.isalnum() or char in '-_' or '\u4e00' <= char <= '\u9fff':
                        cleaned_name += char
                    else:
                        cleaned_name += '_'
                
                # 移除多余的下划线
                backtest_name = re.sub(r'_+', '_', cleaned_name).strip('_')
                print(f"成功获取回测名称: {backtest_name}")
            else:
                print(f"未能获取到回测名称，使用默认名称")
                backtest_name = f"backtest_{backtest_id[:8]}"
            
            return backtest_name
            
        except Exception as e:
            print(f"获取回测名称失败: {e}")
            # 返回默认名称
            return f"backtest_{backtest_id[:8]}"
    
    def download_single_batch(self, backtest_id: str, offset: int = 0, user_record_offset: int = 0) -> Optional[Dict[Any, Any]]:
        """
        下载单批回测数据
        
        Args:
            backtest_id: 回测ID
            offset: 偏移量
            user_record_offset: 用户记录偏移量
            
        Returns:
            返回的JSON数据，如果失败返回None
        """
        # 构建URL参数
        params = {
            'backtestId': backtest_id,
            'offset': offset,
            'userRecordOffset': user_record_offset,
            'ajax': 1
        }
        
        # 构建POST数据
        post_data = {
            'undefined': '',
            'ajax': 1,
            'token': self.token
        }
        
        try:
            # 更新Referer
            self.headers['Referer'] = f'https://www.joinquant.com/algorithm/backtest/detail?backtestId={backtest_id}'
            
            # 发送请求
            response = self.session.post(
                self.base_url,
                params=params,
                data=post_data,
                headers=self.headers,
                timeout=30
            )
            
            response.raise_for_status()
            
            # 解析JSON响应
            data = response.json()
            
            print(f"Offset {offset}: 请求成功，状态码: {response.status_code}")
            
            return data
            
        except requests.exceptions.RequestException as e:
            print(f"Offset {offset}: 请求失败: {e}")
            return None
        except json.JSONDecodeError as e:
            print(f"Offset {offset}: JSON解析失败: {e}")
            print(f"响应内容: {response.text[:500]}...")
            return None
    
    def extract_target_data(self, data: Dict[Any, Any]) -> Dict[str, Any]:
        """
        从响应数据中提取目标字段
        
        Args:
            data: API响应数据
            
        Returns:
            提取的目标字段数据，如果没有实际数据则返回空字典
        """
        extracted = {}
        total_data_points = 0
        
        try:
            # 导航到数据 - 实际路径是 data.result
            result_data = data.get('data', {}).get('result', {})
            
            # 提取目标字段
            for field in self.target_fields:
                if field in result_data:
                    extracted[field] = result_data[field]
                    
                    # 计算数据点数量
                    if isinstance(result_data[field], dict):
                        if 'time' in result_data[field]:
                            # 简单的时间序列数据
                            time_count = len(result_data[field].get('time', []))
                            total_data_points += time_count
                            print(f"提取字段 {field}: {time_count} 条记录")
                        else:
                            # 嵌套结构（如gains, orders）
                            nested_count = 0
                            for sub_field, sub_data in result_data[field].items():
                                if isinstance(sub_data, dict) and 'time' in sub_data:
                                    sub_time_count = len(sub_data.get('time', []))
                                    nested_count += sub_time_count
                            total_data_points += nested_count
                            print(f"提取字段 {field}: 嵌套结构，{nested_count} 条记录")
                    elif isinstance(result_data[field], list):
                        list_count = len(result_data[field])
                        total_data_points += list_count
                        print(f"提取字段 {field}: 列表，{list_count} 条记录")
                    else:
                        print(f"提取字段 {field}: {type(result_data[field])}")
                else:
                    print(f"警告: 字段 {field} 不存在于响应数据中")
            
            # 如果没有实际数据，返回空字典
            if total_data_points == 0:
                print("没有实际数据，返回空字典")
                return {}
            
            # 同时提取offset和count信息
            if 'offset' in result_data:
                extracted['offset'] = result_data['offset']
            if 'count' in result_data:
                extracted['count'] = result_data['count']
            
        except Exception as e:
            print(f"提取数据时出错: {e}")
            import traceback
            traceback.print_exc()
        
        return extracted
    
    def has_more_data(self, data: Dict[Any, Any]) -> bool:
        """
        检查是否还有更多数据
        
        Args:
            data: API响应数据
            
        Returns:
            是否还有更多数据
        """
        try:
            # 检查API响应状态
            if data.get('status') != '0' or data.get('code') != '00000':
                print(f"API响应状态异常: status={data.get('status')}, code={data.get('code')}")
                return False
            
            # 检查响应数据结构 - 实际路径是 data.result
            result_data = data.get('data', {}).get('result', {})
            
            # 如果没有数据，说明没有更多数据
            if not result_data:
                print("没有找到数据结构")
                return False
            
            # 首先检查count字段，如果存在且为0，直接返回False
            count = result_data.get('count', 0)
            if count == 0:
                print(f"数据数量为0，停止下载")
                return False
            elif count > 0:
                print(f"找到 {count} 条数据记录")
            
            # 检查是否有任何目标字段包含实际数据
            total_data_points = 0
            has_data = False
            
            for field in self.target_fields:
                if field in result_data:
                    field_data = result_data[field]
                    field_data_count = 0
                    
                    if isinstance(field_data, dict):
                        # 处理简单的time/value结构
                        if 'time' in field_data and 'value' in field_data:
                            time_array = field_data.get('time', [])
                            value_array = field_data.get('value', [])
                            field_data_count = len(time_array) if time_array else 0
                            if field_data_count > 0:
                                print(f"字段 {field} 包含 {field_data_count} 条时间数据")
                                has_data = True
                        else:
                            # 处理嵌套结构（如gains, orders）
                            for sub_field_name, sub_field_data in field_data.items():
                                if isinstance(sub_field_data, dict) and 'time' in sub_field_data:
                                    sub_time_array = sub_field_data.get('time', [])
                                    sub_count = len(sub_time_array) if sub_time_array else 0
                                    field_data_count += sub_count
                                    if sub_count > 0:
                                        print(f"字段 {field}.{sub_field_name} 包含 {sub_count} 条时间数据")
                                        has_data = True
                    elif isinstance(field_data, list):
                        field_data_count = len(field_data)
                        if field_data_count > 0:
                            print(f"字段 {field} 包含 {field_data_count} 条列表数据")
                            has_data = True
                    
                    total_data_points += field_data_count
            
            # 如果所有字段的数据点总数为0，说明没有更多数据
            if total_data_points == 0:
                print("所有目标字段的数据点总数为0，停止下载")
                return False
            
            if not has_data:
                print("所有目标字段都没有有效数据")
                return False
            
            print(f"总共找到 {total_data_points} 个数据点")
            return True
            
        except Exception as e:
            print(f"检查数据状态时出错: {e}")
            import traceback
            traceback.print_exc()
            return False
    
    def download_all_data(self, backtest_id: str, start_offset: int = 0, user_record_offset: int = 0) -> List[Dict[str, Any]]:
        """
        下载所有回测数据（自动迭代offset）
        
        Args:
            backtest_id: 回测ID
            start_offset: 起始偏移量
            user_record_offset: 用户记录偏移量
            
        Returns:
            所有提取的数据列表
        """
        all_data = []
        current_offset = start_offset
        
        print(f"开始下载回测数据，ID: {backtest_id}")
        print(f"目标字段: {', '.join(self.target_fields)}")
        
        while True:
            print(f"\n--- 下载 Offset {current_offset} ---")
            
            # 下载当前批次数据
            batch_data = self.download_single_batch(backtest_id, current_offset, user_record_offset)
            
            if batch_data is None:
                print(f"Offset {current_offset}: 下载失败，停止迭代")
                break
            
            # 检查是否还有更多数据
            if not self.has_more_data(batch_data):
                print(f"Offset {current_offset}: 没有更多数据，停止迭代")
                break
            
            # 提取目标字段数据
            extracted_data = self.extract_target_data(batch_data)
            
            if extracted_data:
                # 检查提取的数据是否实际包含有效数据点
                total_extracted_points = 0
                for field in self.target_fields:
                    if field in extracted_data:
                        field_data = extracted_data[field]
                        if isinstance(field_data, dict):
                            if 'time' in field_data and 'value' in field_data:
                                # 简单结构
                                time_array = field_data.get('time', [])
                                total_extracted_points += len(time_array) if time_array else 0
                            else:
                                # 嵌套结构
                                for sub_field_name, sub_field_data in field_data.items():
                                    if isinstance(sub_field_data, dict) and 'time' in sub_field_data:
                                        sub_time_array = sub_field_data.get('time', [])
                                        total_extracted_points += len(sub_time_array) if sub_time_array else 0
                
                if total_extracted_points == 0:
                    print(f"Offset {current_offset}: 提取的数据中没有有效数据点，停止迭代")
                    break
                
                # 添加offset信息
                extracted_data['offset'] = current_offset
                extracted_data['backtest_id'] = backtest_id
                all_data.append(extracted_data)
                print(f"Offset {current_offset}: 成功提取数据，包含 {total_extracted_points} 个数据点")
            else:
                print(f"Offset {current_offset}: 没有提取到有效数据，停止迭代")
                break
            
            # 增加offset继续下一批
            current_offset += 1000  # 根据API的分页大小调整
        
        print(f"\n=== 下载完成 ===")
        print(f"总共下载了 {len(all_data)} 个批次的数据")
        
        return all_data
    
    def extract_time_range(self, all_data: List[Dict[str, Any]]) -> Tuple[str, str]:
        """
        从下载的数据中提取最早和最晚的时间
        
        Args:
            all_data: 所有下载的数据批次
            
        Returns:
            (earliest_date, latest_date) 格式为 YYYYMMDD
        """
        all_timestamps = []
        
        try:
            for data_batch in all_data:
                # 遍历所有字段，不仅仅是目标字段
                for field_name, field_data in data_batch.items():
                    if isinstance(field_data, dict):
                        # 处理简单的time/value结构
                        if 'time' in field_data:
                            time_array = field_data.get('time', [])
                            all_timestamps.extend(time_array)
                        else:
                            # 处理嵌套结构（如gains, orders）
                            for sub_field_name, sub_field_data in field_data.items():
                                if isinstance(sub_field_data, dict) and 'time' in sub_field_data:
                                    sub_time_array = sub_field_data.get('time', [])
                                    all_timestamps.extend(sub_time_array)
            
            if not all_timestamps:
                # 如果没有找到时间戳，返回当前日期
                current_date = datetime.now().strftime("%Y%m%d")
                return current_date, current_date
            
            # 转换时间戳为日期字符串
            earliest_timestamp = min(all_timestamps)
            latest_timestamp = max(all_timestamps)
            
            # 将毫秒时间戳转换为日期字符串
            earliest_date = datetime.fromtimestamp(earliest_timestamp / 1000).strftime("%Y%m%d")
            latest_date = datetime.fromtimestamp(latest_timestamp / 1000).strftime("%Y%m%d")
            
            print(f"数据时间范围: {earliest_date} 到 {latest_date}")
            return earliest_date, latest_date
            
        except Exception as e:
            print(f"提取时间范围时出错: {e}")
            # 出错时返回当前日期
            current_date = datetime.now().strftime("%Y%m%d")
            return current_date, current_date
    
    def save_to_jsonl(self, all_data: List[Dict[str, Any]], backtest_id: str) -> str:
        """
        将数据保存为JSONL格式（新的命名规则，包含回测名称）
        
        Args:
            all_data: 所有提取的数据
            backtest_id: 回测ID
            
        Returns:
            保存的文件路径
        """
        # 确保数据目录存在
        script_dir = Path(__file__).parent
        if self.data_dir.startswith('/'):
            # 绝对路径
            output_dir = Path(self.data_dir)
        else:
            # 相对路径，相对于脚本目录的上级目录
            output_dir = script_dir.parent / self.data_dir
        
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # 获取回测名称
        backtest_name = self.get_backtest_name(backtest_id)
        
        # 提取数据时间范围
        earliest_date, latest_date = self.extract_time_range(all_data)
        
        # 构建时间范围字符串
        if earliest_date == latest_date:
            time_range = earliest_date
        else:
            time_range = f"{earliest_date}_{latest_date}"
        
        # 构建文件名（包含回测名称）
        if backtest_name:
            filename = f"{backtest_name}_{backtest_id}_{time_range}.jsonl"
        else:
            filename = f"{backtest_id}_{time_range}.jsonl"
        
        # 确保文件名安全
        filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
        
        filepath = output_dir / filename
        
        # 保存数据
        with open(filepath, 'w', encoding='utf-8') as f:
            for data_batch in all_data:
                # 添加元数据
                metadata = {
                    'backtest_id': backtest_id,
                    'backtest_name': backtest_name,
                    'download_time': datetime.now().isoformat(),
                    'source_note': self.source_note,
                    'data_fields': self.target_fields
                }
                data_batch['metadata'] = metadata
                
                # 写入JSONL格式
                f.write(json.dumps(data_batch, ensure_ascii=False) + '\n')
        
        print(f"数据已保存到: {filepath}")
        return str(filepath)
    
    def save_daily_data_directly(self, all_data: List[Dict[str, Any]], backtest_id: str) -> str:
        """
        直接将数据转换为日线格式并保存，不保存原始文件
        
        Args:
            all_data: 所有下载的数据批次
            backtest_id: 回测ID
            
        Returns:
            保存的日线文件路径
        """
        # 确保数据目录存在
        script_dir = Path(__file__).parent
        if self.data_dir.startswith('/'):
            # 绝对路径
            output_dir = Path(self.data_dir)
        else:
            # 相对路径，相对于脚本目录的上级目录
            output_dir = script_dir.parent / self.data_dir
        
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # 提取数据时间范围
        earliest_date, latest_date = self.extract_time_range(all_data)
        
        # 构建时间范围字符串
        if earliest_date == latest_date:
            time_range = earliest_date
        else:
            time_range = f"{earliest_date}_{latest_date}"
        
        # 获取回测名称
        backtest_name = self.get_backtest_name(backtest_id)
        
        # 构建日线文件名（包含回测名称）
        if backtest_name:
            filename = f"{backtest_name}_{backtest_id}_{time_range}_daily.jsonl"
        else:
            filename = f"{backtest_id}_{time_range}_daily.jsonl"
        
        # 确保文件名安全
        filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
        
        filepath = output_dir / filename
        
        # 创建转换器并直接转换数据
        converter = JSONLDateConverter()
        
        # 按日期分组数据
        date_groups = converter.group_data_by_date(all_data, self.target_fields)
        
        # 保存转换后的数据
        with open(filepath, 'w', encoding='utf-8') as f:
            # 直接写入每日数据，不保存元数据
            for date_key in sorted(date_groups.keys()):
                daily_data = {
                    'type': 'daily_data',
                    'date': date_key,
                    'data': {}
                }
                
                # 为每个字段收集当日数据
                for field in self.target_fields:
                    if field in date_groups[date_key] and date_groups[date_key][field]:
                        daily_data['data'][field] = {
                            'count': len(date_groups[date_key][field]),
                            'records': date_groups[date_key][field]
                        }
                
                # 添加元数据
                daily_data['metadata'] = {
                    'backtest_id': backtest_id,
                    'backtest_name': backtest_name,
                    'download_time': datetime.now().isoformat(),
                    'source_note': self.source_note,
                    'data_fields': self.target_fields
                }
                
                # 只写入有数据的日期
                if daily_data['data']:
                    f.write(json.dumps(daily_data, ensure_ascii=False) + '\n')
        
        print(f"日线数据已保存到: {filepath}")
        return str(filepath)


class JSONLDateConverter:
    """JSONL数据转换器 - 将数据按日期分行"""
    
    def __init__(self):
        self.target_fields = ['benchmark', 'gains', 'orders', 'overallReturn']
        
    def timestamp_to_date_string(self, timestamp: int) -> str:
        """将时间戳转换为YYYYMMDD hh:mm:ss格式"""
        dt = datetime.fromtimestamp(timestamp / 1000)
        return dt.strftime("%Y%m%d %H:%M:%S")
    
    def timestamp_to_date_key(self, timestamp: int) -> str:
        """将时间戳转换为日期键（YYYYMMDD）"""
        dt = datetime.fromtimestamp(timestamp / 1000)
        return dt.strftime("%Y%m%d")
    
    def load_jsonl_file(self, file_path: str) -> List[Dict[str, Any]]:
        """加载JSONL文件"""
        data_batches = []
        with open(file_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line:
                    data_batches.append(json.loads(line))
        return data_batches
    
    def extract_field_data(self, field_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """提取字段数据，返回时间戳和值的列表"""
        if not isinstance(field_data, dict):
            return []
        
        result = []
        
        # 检查是否是简单的time/value结构（如benchmark, overallReturn）
        if 'time' in field_data and 'value' in field_data:
            times = field_data.get('time', [])
            values = field_data.get('value', [])
            
            if len(times) != len(values):
                print(f"警告: 时间戳数量({len(times)})与值数量({len(values)})不匹配")
                min_len = min(len(times), len(values))
                times = times[:min_len]
                values = values[:min_len]
            
            for timestamp, value in zip(times, values):
                result.append({
                    'timestamp': timestamp,
                    'date_string': self.timestamp_to_date_string(timestamp),
                    'value': value
                })
        
        # 检查是否是嵌套结构（如gains, orders）
        else:
            for sub_field_name, sub_field_data in field_data.items():
                if isinstance(sub_field_data, dict) and 'time' in sub_field_data and 'value' in sub_field_data:
                    times = sub_field_data.get('time', [])
                    values = sub_field_data.get('value', [])
                    
                    if len(times) != len(values):
                        print(f"警告: {sub_field_name}字段时间戳数量({len(times)})与值数量({len(values)})不匹配")
                        min_len = min(len(times), len(values))
                        times = times[:min_len]
                        values = values[:min_len]
                    
                    for timestamp, value in zip(times, values):
                        result.append({
                            'timestamp': timestamp,
                            'date_string': self.timestamp_to_date_string(timestamp),
                            'value': value,
                            'sub_field': sub_field_name  # 标记子字段类型（如earn/lose, buy/sell）
                        })
        
        return result
    
    def group_data_by_date(self, data_batches: List[Dict[str, Any]], target_fields: List[str] = None) -> Dict[str, Dict[str, List[Dict[str, Any]]]]:
        """将所有字段的数据按日期分组"""
        if not data_batches:
            raise ValueError("没有数据批次")
        
        # 如果没有提供target_fields，从第一个批次中推断
        if target_fields is None:
            target_fields = []
            if data_batches:
                for key in data_batches[0].keys():
                    if key not in ['offset', 'count']:
                        target_fields.append(key)
        
        # 按日期分组，每个日期包含所有字段的数据
        date_groups = defaultdict(lambda: {field: [] for field in target_fields})
        
        for batch in data_batches:
            for field in target_fields:
                if field in batch:
                    field_data_list = self.extract_field_data(batch[field])
                    
                    for data_point in field_data_list:
                        date_key = self.timestamp_to_date_key(data_point['timestamp'])
                        date_groups[date_key][field].append(data_point)
        
        return dict(date_groups)
    
    def convert_to_daily_jsonl(self, input_file: str, output_file: str = None) -> str:
        """
        转换为按日期分行的JSONL格式
        
        Args:
            input_file: 输入文件路径
            output_file: 输出文件路径，如果为None则自动生成
            
        Returns:
            输出文件路径
        """
        # 加载数据
        data_batches = self.load_jsonl_file(input_file)
        
        # 确定输出文件路径
        if output_file is None:
            input_path = Path(input_file)
            output_file = input_path.parent / f"{input_path.stem}_daily.jsonl"
        
        # 按日期分组数据
        date_groups = self.group_data_by_date(data_batches)
        
        # 写入转换后的数据
        with open(output_file, 'w', encoding='utf-8') as f:
            # 写入元数据行
            if data_batches:
                backtest_id = data_batches[0].get('backtest_id', '')
                metadata_line = {
                    'type': 'metadata',
                    'original_backtest_id': backtest_id,
                    'conversion_time': datetime.now().strftime("%Y%m%d %H:%M:%S"),
                    'total_dates': len(date_groups),
                    'date_range': {
                        'start': min(date_groups.keys()) if date_groups else '',
                        'end': max(date_groups.keys()) if date_groups else ''
                    },
                    'description': '每行包含一个日期的回测数据，日期格式为YYYYMMDD hh:mm:ss，同时保留原始时间戳'
                }
                f.write(json.dumps(metadata_line, ensure_ascii=False) + '\n')
            
            # 按日期排序并写入每日数据
            for date_key in sorted(date_groups.keys()):
                daily_data = date_groups[date_key]
                
                # 构建每日数据行
                daily_line = {
                    'type': 'daily_data',
                    'date': date_key,
                    'data': {}
                }
                
                # 添加每个字段的数据
                for field in self.target_fields:
                    field_data = daily_data[field]
                    if field_data:  # 只添加有数据的字段
                        daily_line['data'][field] = {
                            'count': len(field_data),
                            'records': field_data
                        }
                
                # 只写入有数据的日期
                if daily_line['data']:
                    f.write(json.dumps(daily_line, ensure_ascii=False) + '\n')
        
        return str(output_file)
    
    def get_conversion_summary(self, data_batches: List[Dict[str, Any]]) -> Dict[str, Any]:
        """获取转换摘要信息"""
        if not data_batches:
            return {}
        
        date_groups = self.group_data_by_date(data_batches)
        
        # 计算每个字段的统计信息
        field_stats = {}
        total_points = 0
        
        for field in self.target_fields:
            field_points = 0
            field_dates = 0
            
            for date_data in date_groups.values():
                if date_data[field]:
                    field_points += len(date_data[field])
                    field_dates += 1
            
            field_stats[field] = {
                'total_points': field_points,
                'dates_with_data': field_dates
            }
            total_points += field_points
        
        dates_with_data = len([d for d in date_groups.keys() if any(date_groups[d][f] for f in self.target_fields)])
        
        return {
            'total_data_points': total_points,
            'total_dates': dates_with_data,
            'date_range': {
                'start': min(date_groups.keys()) if date_groups else '',
                'end': max(date_groups.keys()) if date_groups else ''
            },
            'field_statistics': field_stats,
            'data_batches_processed': len(data_batches)
        }


def main():
    """主函数"""
    parser = argparse.ArgumentParser(description='JoinQuant回测数据下载器 - 支持配置文件和按日期转换')
    parser.add_argument('backtest_id', help='回测ID')
    parser.add_argument('--config', default='config.yaml', help='配置文件路径 (默认: config.yaml)')
    parser.add_argument('--cookies', help='Cookie字符串 (覆盖配置文件)')
    parser.add_argument('--token', help='POST请求token (覆盖配置文件)')
    parser.add_argument('--start-offset', type=int, default=0, help='起始偏移量 (默认: 0)')
    parser.add_argument('--user-record-offset', type=int, default=0, help='用户记录偏移量 (默认: 0)')
    parser.add_argument('--source-note', default='', help='数据来源备注，将包含在文件名中')
    
    # 数据转换选项
    parser.add_argument('--convert-to-daily', action='store_true', help='将数据转换为按日期分行的格式')
    parser.add_argument('--daily-output', help='按日期转换后的输出文件路径（默认为原文件名_daily.jsonl）')
    parser.add_argument('--show-summary', action='store_true', help='显示数据转换摘要')
    
    args = parser.parse_args()
    
    try:
        # 创建下载器
        downloader = BacktestDataDownloader(args.config, args.source_note)
        
        # 如果提供了命令行参数，覆盖配置文件
        if args.cookies or args.token:
            downloader.set_auth_info(args.cookies, args.token)
        
        # 下载所有数据
        all_data = downloader.download_all_data(
            args.backtest_id, 
            args.start_offset, 
            args.user_record_offset
        )
        
        if all_data:
            print(f"\n=== 下载完成 ===")
            print(f"总共下载了 {len(all_data)} 个数据批次")
            
            # 显示数据统计
            print(f"\n=== 数据统计 ===")
            total_records = 0
            for i, batch in enumerate(all_data):
                print(f"批次 {i+1} (Offset {batch.get('offset', 'N/A')}):")
                for field in downloader.target_fields:
                    if field in batch:
                        field_data = batch[field]
                        if isinstance(field_data, dict) and 'time' in field_data:
                            record_count = len(field_data['time'])
                            total_records += record_count
                            print(f"  {field}: {record_count} 条记录")
                        else:
                            print(f"  {field}: 有数据")
                    else:
                        print(f"  {field}: 无数据")
            
            print(f"\n总记录数: {total_records}")
            
            # 直接转换为日线数据并保存
            print(f"\n=== 开始转换为日线数据 ===")
            
            # 显示转换摘要
            if args.show_summary:
                converter = JSONLDateConverter()
                summary = converter.get_conversion_summary(all_data)
                print(f"\n转换摘要:")
                print(f"  处理的数据批次: {summary['data_batches_processed']}")
                print(f"  总数据点数: {summary['total_data_points']}")
                print(f"  总日期数: {summary['total_dates']}")
                print(f"  日期范围: {summary['date_range']['start']} - {summary['date_range']['end']}")
                
                print(f"\n字段统计:")
                for field, stats in summary['field_statistics'].items():
                    print(f"  {field}: {stats['total_points']} 个数据点, {stats['dates_with_data']} 个日期")
            
            # 直接保存为日线格式
            print(f"正在转换数据为按日期分行格式...")
            daily_output_file = downloader.save_daily_data_directly(all_data, args.backtest_id)
            print(f"转换完成! 最终数据文件保存在: {daily_output_file}")
                
        else:
            print("下载失败或没有获取到数据！")
            return 1
    
    except Exception as e:
        print(f"程序执行出错: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())