FROM docker.arvancloud.ir/python:3.11

WORKDIR /app

RUN rm -rf /var/lib/apt/lists/*
RUN rm -rf /etc/apt/sources.list.d
COPY sources.list /etc/apt/sources.list

# Install netcat for network checking
RUN apt-get update && apt-get install -y netcat-traditional && apt-get clean

COPY requirements.txt .
RUN pip install --no-cache-dir \
    --index-url https://mirror-pypi.runflare.com/simple \
    -r requirements.txt

COPY producer.py .

CMD ["python", "producer.py"]