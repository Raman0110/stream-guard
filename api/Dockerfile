FROM python:3.11-slim
WORKDIR /app
COPY api/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY common ./common
COPY ml ./ml
COPY api ./api
COPY scripts ./scripts
COPY alembic.ini .
COPY migrations ./migrations
EXPOSE 8000
CMD ["uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]
