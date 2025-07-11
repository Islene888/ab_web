from flask import Flask, jsonify, Blueprint
from datetime import datetime
import os
import requests
from dotenv import load_dotenv

load_dotenv()

bp = Blueprint("growthbook_experiments", __name__)



def get_last_phase_start_time(exp):
    phases = exp.get('phases', [])
    if phases:
        last_phase = phases[-1]
        s = last_phase.get('dateStarted')
        if s:
            try:
                return datetime.strptime(s, '%Y-%m-%dT%H:%M:%S.%fZ').replace(microsecond=0).isoformat(sep=' ')
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
                return datetime.strptime(s, '%Y-%m-%dT%H:%M:%S.%fZ').replace(microsecond=0).isoformat(sep=' ')
            except Exception:
                return None
    return None

def fetch_growthbook_experiments():
    GROWTHBOOK_API_URL = "https://api.growthbook.io/api/v1/experiments"
    GROWTHBOOK_API_KEY = os.getenv("GROWTHBOOK_API_KEY")

    headers = {
        "Authorization": f"Bearer {GROWTHBOOK_API_KEY}",
    }

    all_experiments = []
    limit = 100
    offset = 0

    while True:
        params = {
            'limit': limit,
            'offset': offset,
        }
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
            # 最后一页了
            break
        offset += limit

    exp_info = []
    for exp in all_experiments:
        date_created_str = exp.get('dateCreated')
        if date_created_str:
            try:
                date_created = datetime.strptime(date_created_str, '%Y-%m-%dT%H:%M:%S.%fZ')
                if date_created.year < 2025:
                    continue
            except Exception:
                pass

        experiment_name = exp.get("name")
        tags = exp.get("tags", [])
        tags_str = ', '.join(tags).replace(',', '_').replace(' ', '')

        variations = exp.get("variations", [])
        num_variations = len(variations)
        control_group_key = variations[0].get('key') if variations else None

        phases = exp.get("phases", [])
        phase_start_time = None
        phase_end_time = None
        if phases:
            phase_start_time = get_last_phase_start_time(exp)
            phase_end_time = get_last_phase_end_time(exp) or datetime.now().replace(microsecond=0).isoformat(sep=' ')

        exp_info.append({
            "experiment_name": experiment_name,
            "tags": tags_str,
            "phase_start_time": phase_start_time,
            "phase_end_time": phase_end_time,
            "number_of_variations": num_variations,
            "control_group_key": control_group_key,
            "phases": phases
        })

    return exp_info

@bp.route("/api/experiments")
def api_experiments():
    result = fetch_growthbook_experiments()
    return jsonify(result)


