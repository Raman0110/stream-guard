FROM python:3.11-slim
WORKDIR /app
COPY monitoring/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY common ./common
COPY ml ./ml
COPY monitoring ./monitoring
ENTRYPOINT ["python", "-m", "monitoring.main"]
