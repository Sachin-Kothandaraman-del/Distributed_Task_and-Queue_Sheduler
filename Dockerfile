FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

# Install dependencies first for better layer caching.
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Project code.
COPY pyproject.toml README.md ./
COPY dtq ./dtq
COPY examples ./examples
RUN pip install --no-cache-dir -e .

# Default: the control plane. docker-compose overrides the command per service.
EXPOSE 8000 9100
CMD ["python", "-m", "dtq", "api"]
