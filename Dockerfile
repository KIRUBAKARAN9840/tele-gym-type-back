########################
# Stage 1 – builder
########################
FROM python:3.12-slim AS builder
WORKDIR /app
RUN apt-get update && \
    apt-get install -y --no-install-recommends build-essential tzdata && \
    apt-get upgrade -y --no-install-recommends && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
# Install deps system-wide so they end up in /usr/local
RUN pip install --no-cache-dir -r requirements.txt


FROM python:3.12-slim

WORKDIR /app

# tzdata + security updates (fixes libexpat CVE, etc.)
RUN apt-get update && \
    apt-get install -y --no-install-recommends tzdata && \
    ln -snf /usr/share/zoneinfo/Asia/Kolkata /etc/localtime && \
    echo "Asia/Kolkata" > /etc/timezone && \
    apt-get upgrade -y --no-install-recommends && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

ENV TZ=Asia/Kolkata

# Prometheus multiprocess mode: each uvicorn worker writes metrics to this dir,
# /metrics endpoint aggregates them so counters are correct across all workers
ENV PROMETHEUS_MULTIPROC_DIR=/tmp/prometheus_multiproc

# bring in the packages installed by the builder stage
COPY --from=builder /usr/local /usr/local

COPY . /app

EXPOSE 8000

# Clean stale multiprocess files before starting (important after restarts)
CMD rm -rf /tmp/prometheus_multiproc && mkdir -p /tmp/prometheus_multiproc && exec uvicorn main:app --host 0.0.0.0 --port 8000 --workers 2 --ws-ping-interval 25 --ws-ping-timeout 10


# FROM python:3.12-slim AS builder

# WORKDIR /app

# RUN apt-get update && apt-get install -y --no-install-recommends \
#     build-essential tzdata && \
#     rm -rf /var/lib/apt/lists/*

# COPY requirements.txt .
# RUN pip install --user --no-cache-dir -r requirements.txt

# FROM python:3.12-slim

# WORKDIR /app


# RUN apt-get update && apt-get install -y --no-install-recommends tzdata && \
#     ln -snf /usr/share/zoneinfo/Asia/Kolkata /etc/localtime && \
#     echo "Asia/Kolkata" > /etc/timezone && \
#     rm -rf /var/lib/apt/lists/*

# ENV TZ=Asia/Kolkata

# COPY --from=builder /root/.local /root/.local
# ENV PATH=/root/.local/bin:$PATH

# COPY . /app

# EXPOSE 8000

# # CMD ["gunicorn", "main:app", "-k", "uvicorn.workers.UvicornWorker", "--bind", "0.0.0.0:8000", "--workers", "2"]

# CMD ["uvicorn", "main:app","--host", "0.0.0.0", "--port", "8000","--workers", "2","--ws-ping-interval", "25","--ws-ping-timeout",  "10"]  
