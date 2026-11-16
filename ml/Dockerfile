FROM python:3.11-slim
WORKDIR /app
COPY ml/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY common ./common
COPY producer ./producer
COPY ml ./ml
# models/ and reports/ are mounted by docker-compose, so outputs land on the host
ENTRYPOINT ["python", "-m", "ml.train"]
