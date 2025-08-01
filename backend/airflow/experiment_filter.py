import os
import requests
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

def get_last_phase_start_time(exp):
    phases = exp.get('phases', [])
    if phases:
        last_phase = phases[-1]
        s = last_phase.get('dateStarted')
        if s:
            try:
                return datetime.strptime(s, '%Y-%m-%dT%H:%M:%S.%fZ').replace(microsecond=0)
            except Exception:
                return None
    return None

def get_last_phase_end_time(exp):
    phases = exp.get('phases', [])
    if phases:
        last_phase = phases[-1]
        s = last_phase.get('dateEnded')
        if s:
            try:
                return datetime.strptime(s, '%Y-%m-%dT%H:%M:%S.%fZ').replace(microsecond=0)
            except Exception:
                return None
    return None

def get_valid_experiments(status_filter="running", phase_within_months=3):
    """
    返回所有status为running且last phase开始时间在N个月内的实验参数（列表）
    """
    GROWTHBOOK_API_URL = "https://api.growthbook.io/api/v1/experiments"
    GROWTHBOOK_API_KEY = os.getenv("GROWTHBOOK_API_KEY")

    headers = {
        "Authorization": f"Bearer {GROWTHBOOK_API_KEY}",
    }

    all_experiments = []
    limit = 100
    offset = 0

    while True:
        params = {'limit': limit, 'offset': offset}
        response = requests.get(GROWTHBOOK_API_URL, headers=headers, params=params)
        if response.status_code != 200:
            print(f"请求失败，状态码: {response.status_code}, 错误信息: {response.text}")
            break
        data = response.json()
        experiments = data.get('experiments', [])
        if not experiments:
            break
        all_experiments.extend(experiments)
        if len(experiments) < limit:
            break
        offset += limit

    # 计算N个月前的时间点
    months_ago = datetime.now() - timedelta(days=phase_within_months * 30)

    exp_info = []
    for exp in all_experiments:
        status = exp.get("status", "")
        if status != status_filter:
            continue

        phase_start_time = get_last_phase_start_time(exp)
        if phase_start_time is None or phase_start_time < months_ago:
            continue

        experiment_name = exp.get("name")
        tags = exp.get("tags", [])
        tags_str = ', '.join(tags).replace(',', '_').replace(' ', '')
        variations = exp.get("variations", [])
        num_variations = len(variations)
        control_group_key = variations[0].get('key') if variations else None
        phases = exp.get("phases", [])
        phase_end_time = get_last_phase_end_time(exp) or datetime.now().replace(microsecond=0)
        exp_info.append({
            "experiment_name": experiment_name,
            "tags": tags_str,
            "phase_start_time": phase_start_time.strftime('%Y-%m-%d %H:%M:%S') if phase_start_time else None,
            "phase_end_time": phase_end_time.strftime('%Y-%m-%d %H:%M:%S') if phase_end_time else None,
            "number_of_variations": num_variations,
            "control_group_key": control_group_key,
            "status": status,
            "phases": phases
        })

    return exp_info

if __name__ == "__main__":
    result = get_valid_experiments()
    print("\n--- 符合条件的实验参数 ---")
    for exp in result:
        print(exp)
