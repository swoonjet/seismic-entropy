FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY backend/ backend/
COPY frontend/ frontend/

WORKDIR /app/backend
EXPOSE 8420

CMD ["uvicorn", "server:app", "--host", "0.0.0.0", "--port", "8420"]
