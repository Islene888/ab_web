from sqlalchemy import create_engine, text
import urllib.parse

def get_experiment_details_by_tag(tag):
    engine = None   # 先声明，防止 finally 里未定义
    try:
        # 连接数据库
        password = urllib.parse.quote_plus("flowgpt@2024.com")
        DATABASE_URL = f"mysql+pymysql://bigdata:{password}@3.135.224.186:9030/flow_ab_test?charset=utf8mb4"
        engine = create_engine(DATABASE_URL)

        query = text("""
            SELECT experiment_name, phase_start_time, phase_end_time, 
                   number_of_variations, control_group_key
            FROM tbl_experiment_data 
            WHERE tags = :tag
        """)

        with engine.connect() as connection:
            result = connection.execute(query, {'tag': tag})
            experiment = result.mappings().fetchone()

            if experiment:
                experiment_data = {
                    "experiment_name": experiment['experiment_name'],
                    "phase_start_time": experiment['phase_start_time'],
                    "phase_end_time": experiment['phase_end_time'],
                    "number_of_variations": experiment['number_of_variations'],
                    "control_group_key": experiment['control_group_key']
                }
                return experiment_data
            else:
                print(f"没有找到符合标签 '{tag}' 的实验。")
                return None

    except Exception as e:
        print(f"查询失败: {e}")
        return None

    finally:
        if engine is not None:
            engine.dispose()
