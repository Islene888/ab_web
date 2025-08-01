from flask import Flask, request
from backend.service.service import register_indicator_routes
from backend.service.config import INDICATOR_CONFIG
from backend.service.all import all_bp
from backend.service.all_in_one import bp as all_in_one_bp
from backend.growthbook_fetch.experiment_data import bp as growthbook_bp
from backend.service.cohort import bp_cohort

app = Flask(__name__)
app.register_blueprint(bp_cohort)


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

print('路由注册完成')
for rule in app.url_map.iter_rules():
    print(f"Route: {rule}")

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=True)
