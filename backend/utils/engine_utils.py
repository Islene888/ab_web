import os
import urllib.parse
from sqlalchemy import create_engine

def get_db_connection():
    # 远程数据仓库（主业务）
    password = urllib.parse.quote_plus(os.environ.get('DB_PASSWORD', 'flowgpt@2024.com'))
    DATABASE_URL = f"mysql+pymysql://bigdata:{password}@3.135.224.186:9030/flow_ab_test?charset=utf8mb4"
    return create_engine(DATABASE_URL)

def get_local_cache_engine():
    # password = urllib.parse.quote_plus(os.environ.get('LOCAL_DB_PASSWORD', 'Root2024!'))
    # LOCAL_DB_URL = f"mysql+pymysql://root:{password}@127.0.0.1:3306/ab_test?charset=utf8mb4"
    # return create_engine(LOCAL_DB_URL)
    password = urllib.parse.quote_plus(os.environ.get('DB_PASSWORD', 'flowgpt@2024.com'))
    DATABASE_URL = f"mysql+pymysql://bigdata:{password}@3.135.224.186:9030/flow_ab_test?charset=utf8mb4"
    return create_engine(DATABASE_URL)
