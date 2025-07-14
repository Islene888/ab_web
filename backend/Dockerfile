FROM python:3.10-slim

# 将工作目录设置为 /app
WORKDIR /app

# 先只复制依赖文件，这样可以利用 Docker 的缓存机制
# 当 requirements.txt 不变时，下面这步不会重复执行
COPY backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 复制整个 backend 文件夹到工作目录 /app 下
# 这会创建出 /app/backend 文件夹
COPY ./backend ./backend

# 关键：设置 PYTHONPATH 环境变量
# 告诉 Python 应该在 /app 目录寻找包
ENV PYTHONPATH "${PYTHONPATH}:/app"

# 以模块方式运行 backend 包里的 main 模块
CMD ["python", "-m", "backend.main"]