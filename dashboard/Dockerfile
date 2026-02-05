FROM docker.arvancloud.ir/python:3.11

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt .

# Install Python dependencies
RUN pip install -i https://mirror-pypi.runflare.com/simple --no-cache-dir -r requirements.txt

# Copy application files
COPY dashboard_app.py .
COPY dashboard_web.py .

# Create directory for models
RUN mkdir -p /app/models

# Expose ports
EXPOSE 8000 8501

# Default command (can be overridden)
CMD ["python", "dashboard_app.py"]