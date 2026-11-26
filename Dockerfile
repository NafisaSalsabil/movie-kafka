FROM python:3.11-slim-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless && rm -rf /var/lib/apt/lists/*
WORKDIR /app
RUN pip install --no-cache-dir pyspark==3.5.8 pyarrow==19.0.1 kafka-python==2.2.15
ENTRYPOINT []
CMD ["python3", "starter.py"]
