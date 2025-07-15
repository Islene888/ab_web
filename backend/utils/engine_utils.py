import os
import urllib.parse
from sqlalchemy import create_engine

def get_db_connection():
    # 远程数据仓库（主业务）
    password = urllib.parse.quote_plus(os.environ.get('DB_PASSWORD', 'flowgpt@2024.com'))
    DATABASE_URL = f"mysql+pymysql://bigdata:{password}@3.135.224.186:9030/flow_ab_test?charset=utf8mb4"
    return create_engine(DATABASE_URL)

def get_local_cache_engine():
    # Google Cloud MySQL 配置信息
    password = urllib.parse.quote_plus(os.environ.get('LOCAL_DB_PASSWORD', '201549'))  # 推荐用环境变量存密码
    DATABASE_URL = f"mysql+pymysql://islene:{password}@34.67.45.82:3306/flowabtesting?charset=utf8mb4"
    return create_engine(DATABASE_URL)
