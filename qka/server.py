from fastapi import FastAPI, HTTPException, Header, Depends
from pydantic import BaseModel
import inspect
from typing import Any, Optional, List
import uvicorn
import uuid
import hashlib
import pandas as pd
from qka import data


class QMTDataServer:
    """QMT数据服务服务器，提供数据获取和处理功能的API接口"""
    
    def __init__(self, host: str = "0.0.0.0", port: int = 8000, token: str = None):
        """初始化数据服务器
        Args:
            host: 服务器地址，默认0.0.0.0
            port: 服务器端口，默认8000
            token: 可选的自定义token
        """
        self.host = host
        self.port = port
        self.app = FastAPI()
        self.token = token if token else self.generate_token()  # 使用自定义token或生成固定token
        print(f"\n授权Token: {self.token}\n")  # 打印token供客户端使用

    def generate_token(self) -> str:
        """生成基于机器码的固定token"""
        # 获取机器码（例如MAC地址）
        mac = uuid.getnode()
        # 将机器码转换为字符串
        mac_str = str(mac)
        # 使用SHA256哈希生成固定长度的token
        token = hashlib.sha256(mac_str.encode()).hexdigest()
        return token

    async def verify_token(self, x_token: str = Header(...)):
        """验证token的依赖函数"""
        if x_token != self.token:
            raise HTTPException(status_code=401, detail="无效的Token")
        return x_token

    def convert_to_dict(self, obj):
        """将结果转换为可序列化的字典"""
        # 如果是基本类型，直接返回
        if isinstance(obj, (int, float, str, bool, type(None))):
            return obj
        # 如果已经是字典类型，递归转换值
        elif isinstance(obj, dict):
            return {k: self.convert_to_dict(v) for k, v in obj.items()}
        # 如果是列表或元组，递归转换每个元素
        elif isinstance(obj, (list, tuple)):
            return [self.convert_to_dict(item) for item in obj]
        # 如果是pandas Series，转换为列表
        elif isinstance(obj, pd.Series):
            return obj.tolist()
        # 如果是pandas DataFrame，转换为字典列表
        elif isinstance(obj, pd.DataFrame):
            return obj.to_dict('records')
        # 如果是自定义对象，获取所有公开属性
        elif hasattr(obj, '__dir__'):
            attrs = obj.__dir__()
            # 过滤掉内部属性和方法
            public_attrs = {attr: self.convert_to_dict(getattr(obj, attr)) 
                           for attr in attrs 
                           if not attr.startswith('_') and not callable(getattr(obj, attr))}
            return public_attrs
        # 其他类型直接返回字符串
        return str(obj)

    def convert_function_to_endpoint(self, func_name: str, func):
        """将 data 模块中的函数转换为 FastAPI 端点"""
        sig = inspect.signature(func)
        param_names = list(sig.parameters.keys())
        
        # 创建动态的请求模型
        class_fields = {
            '__annotations__': {}  # 添加类型注解字典
        }
        
        for param_name, param in sig.parameters.items():
            if param_name == 'self':
                continue
            # 获取参数类型注解，如果没有则使用 Any
            param_type = param.annotation if param.annotation != inspect.Parameter.empty else Any
            class_fields['__annotations__'][param_name] = param_type
            # 如果有默认值，使用默认值，否则为 None
            if param.default != inspect.Parameter.empty:
                class_fields[param_name] = param.default
            else:
                class_fields[param_name] = None

        RequestModel = type(f'{func_name}Request', (BaseModel,), class_fields)

        async def endpoint(request: RequestModel, token: str = Depends(self.verify_token)):
            try:
                params = request.dict(exclude_unset=True)
                result = func(**params)
                converted_result = self.convert_to_dict(result)
                return {'success': True, 'data': converted_result}
            except Exception as e:
                raise HTTPException(status_code=500, detail=str(e))

        self.app.post(f'/api/{func_name}')(endpoint)

    def setup_routes(self):
        """设置所有路由"""
        # 获取 data 模块中的所有函数
        data_functions = inspect.getmembers(
            data, 
            predicate=lambda x: inspect.isfunction(x) and x.__module__ == data.__name__
        )
        
        # 排除私有函数和特殊函数
        excluded_functions = set()
        
        for func_name, func in data_functions:
            if not func_name.startswith('_') and func_name not in excluded_functions:
                self.convert_function_to_endpoint(func_name, func)

    def start(self):
        """启动服务器"""
        self.setup_routes()
        uvicorn.run(self.app, host=self.host, port=self.port)


def qmt_data_server(host: str = "0.0.0.0", port: int = 8000, token: str = None):
    """快速创建并启动数据服务器的便捷函数
    Args:
        host: 服务器地址，默认0.0.0.0
        port: 服务器端口，默认8000
        token: 可选的自定义token
    """
    server = QMTDataServer(host, port, token)
    server.start()
