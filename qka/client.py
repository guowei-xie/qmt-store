import requests
from typing import Any, Dict, List, Optional
import logging

# 配置日志
logger = logging.getLogger(__name__)


class QMTDataClient:
    """QMT数据服务客户端，用于调用远程数据服务API"""
    
    def __init__(self, base_url: str = "http://localhost:8000", token: str = None):
        """初始化数据服务客户端
        Args:
            base_url: API服务器地址，默认为本地8000端口
            token: 访问令牌，必须与服务器的token一致
        """
        self.base_url = base_url.rstrip('/')
        self.session = requests.Session()
        if not token:
            raise ValueError("必须提供访问令牌(token)")
        self.token = token
        self.headers = {"X-Token": self.token}

    def api(self, method_name: str, **params) -> Any:
        """通用调用接口方法
        Args:
            method_name: 要调用的接口名称
            **params: 接口参数，作为关键字参数传入
        Returns:
            接口返回的数据
        """
        try:
            response = self.session.post(
                f"{self.base_url}/api/{method_name}",
                json=params or {},
                headers=self.headers
            )
            response.raise_for_status()
            result = response.json()
            
            if not result.get('success'):
                raise Exception(f"API调用失败: {result.get('detail')}")
            
            return result.get('data')
        except requests.exceptions.RequestException as e:
            logger.error(f"调用 {method_name} 失败: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"调用 {method_name} 失败: {str(e)}")
            raise

    def get_stock_list_in_sector(self, sector_name: str) -> List[str]:
        """获取板块成分股
        Args:
            sector_name: 板块名称(如: '沪深A股')
        Returns:
            list: 板块成分股代码列表
        """
        return self.api('get_stock_list_in_sector', sector_name=sector_name)

    def get_stock_list_in_main_board(self) -> List[str]:
        """获取沪深A股主板成分股
        Returns:
            list: 沪深A股主板成分股代码列表
        """
        return self.api('get_stock_list_in_main_board')

    # 便捷方法：数据下载和获取
    def download_stock_history_data(self, stock_list: List[str], start_time: str, 
                                    end_time: str = '', period: str = '1d', 
                                    process_bar: bool = True) -> bool:
        """下载股票历史K线数据
        Args:
            stock_list: 股票代码列表
            start_time: 开始时间
            end_time: 结束时间，默认为空
            period: 周期，'1d'为日线(默认)，'1m'为1分钟线
            process_bar: 进度条显示，默认显示
        Returns:
            bool: 是否成功
        """
        return self.api('download_stock_history_data', 
                       stock_list=stock_list, 
                       start_time=start_time, 
                       end_time=end_time, 
                       period=period, 
                       process_bar=process_bar)

    def get_daily_bars(self, stock_list: List[str], period: str = '1d', 
                       start_time: str = '', end_time: str = '', 
                       count: int = -1) -> Dict:
        """获取行情数据
        Args:
            stock_list: 股票列表
            period: 周期，默认为'1d'
            start_time: 开始时间，默认为空
            end_time: 结束时间，默认为空
            count: 数量，默认为-1
        Returns:
            dict: 行情数据
        """
        return self.api('get_daily_bars', 
                       stock_list=stock_list, 
                       period=period, 
                       start_time=start_time, 
                       end_time=end_time, 
                       count=count)
