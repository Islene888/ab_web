FROM python:3.10-slim

WORKDIR /app

COPY backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt


COPY ./backend ./backend


ENV PYTHONPATH "${PYTHONPATH}:/app"


CMD ["python", "-m", "backend.main"]