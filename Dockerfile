FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY speeding ./speeding
COPY dashboards ./dashboards
COPY demo ./demo
ENTRYPOINT ["python", "speeding/speeding_incidents.py"]
