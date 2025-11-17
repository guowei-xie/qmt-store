"""
启动QMT数据服务器
"""

import configparser
import os
import sys

# 添加项目根目录到Python路径
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from qka.server import QMTDataServer

if __name__ == "__main__":
    server = QMTDataServer(host='0.0.0.0', port=8000)
    server.start()