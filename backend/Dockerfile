#FROM python:3.10-slim
#
#WORKDIR /app
#
#COPY backend/requirements.txt .
#RUN pip install --no-cache-dir -r requirements.txt
#
#
#COPY ./backend ./backend
#
#
#ENV PYTHONPATH "${PYTHONPATH}:/app"
#
#
#CMD ["python", "-m", "flask", "run", "--host=0.0.0.0", "--port=5000"]

FROM python:3.10-slim

WORKDIR /app

# 先装依赖
COPY backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 拷贝代码
COPY ./backend ./backend

# 让 Python 能 import backend
ENV PYTHONPATH=/app
ENV PORT=8080

# 可选：给可观测工具看个端口（非必须）
EXPOSE 8080

# 用 gunicorn 跑 Flask 应用（你的 app 在 backend/main.py 里叫 app）
CMD ["gunicorn", "-w", "2", "-k", "gthread", "-t", "90", \
     "-b", "0.0.0.0:8080", "backend.main:app", "--threads", "8", "--log-level", "info"]
