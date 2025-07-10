from flask import Flask
from backend.service.service import register_indicator_routes
from backend.service.config import INDICATOR_CONFIG
from backend.service.all import all_bp
from backend.service.all_in_one import bp as all_in_one_bp

app = Flask(__name__)
app.register_blueprint(all_bp)
app.register_blueprint(all_in_one_bp)
register_indicator_routes(app, INDICATOR_CONFIG)

if __name__ == "__main__":
    print(app.url_map)
    app.run(host="0.0.0.0", port=5050, debug=True)
