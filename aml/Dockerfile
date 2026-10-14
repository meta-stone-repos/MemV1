FROM python:3.12-slim
WORKDIR /adapter
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY *.py ./
CMD ["python", "-m", "uvicorn", "server:app", "--host", "0.0.0.0", "--port", "18083", "--workers", "1", "--no-access-log"]
