# backend/main.py
from flask import Flask, request
from backend.service.service import register_indicator_routes
from backend.service.config import INDICATOR_CONFIG
from backend.service.all import all_bp
from backend.service.all_in_one import bp as all_in_one_bp
from backend.growthbook_fetch.experiment_data import bp as growthbook_bp
from backend.service.cohort import bp_cohort

app = Flask(__name__)
app.register_blueprint(bp_cohort)

# ✅ 健康检查路由（给本地和 K8s 探活用）
@app.get("/healthz")
def healthz():
    return {"status": "ok"}

@app.before_request
def log_request_info():
    print(f"[全局请求] {request.method} {request.path} args={dict(request.args)}", flush=True)
    if request.method == "POST":
        try:
            print(f"POST body: {request.get_json(silent=True)}", flush=True)
        except Exception as e:
            print(f"POST body 解析失败: {e}", flush=True)

app.register_blueprint(growthbook_bp)
app.register_blueprint(all_bp)
app.register_blueprint(all_in_one_bp)
register_indicator_routes(app, INDICATOR_CONFIG)

print('路由注册完成', flush=True)
for rule in app.url_map.iter_rules():
    print(f"Route: {rule}", flush=True)

# 本地 python 直接跑时用 5001；线上 gunicorn 会忽略这里
if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=True)
