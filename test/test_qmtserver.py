"""
测试QMTServer
"""

import os
import sys
import configparser

# 添加项目根目录到Python路径
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, project_root)

from qka.server import QMTDataServer

# 读取配置文件（使用项目根目录的config.ini）
config_path = os.path.join(project_root, 'config.ini')
config = configparser.ConfigParser()
with open(config_path, 'r', encoding='utf-8') as f:
    config.read_file(f)

if __name__ == "__main__":
    server = QMTDataServer(host='0.0.0.0', port=8000)
    server.start()
   