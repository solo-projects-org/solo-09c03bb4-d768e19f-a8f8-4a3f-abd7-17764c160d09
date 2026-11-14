FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    DATA_ROOT=/data \
    TABLE_NAME=water_level

WORKDIR /srv

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

# The table lives in a shared volume so both processes see the same log.
RUN mkdir -p /data
VOLUME ["/data"]

EXPOSE 8000

# Single worker per container: the two-process requirement is provided by two
# containers (api-1 / api-2) in docker-compose sharing one volume.
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
