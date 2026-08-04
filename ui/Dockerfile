# syntax=docker/dockerfile:1
FROM python:3.12-slim
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY dfs_ui.py .

EXPOSE 8501

# DFS_NODES is injected by docker-compose so the UI can reach nodes by service name.
ENTRYPOINT ["streamlit", "run", "dfs_ui.py", \
    "--server.port=8501", "--server.address=0.0.0.0", "--server.headless=true"]
