# ─────────────────────────────── builder stage ────────────────────────────────
FROM python:3.12-slim AS builder
WORKDIR /build

# copy dependency list first to leverage Docker layer-cache
COPY requirements.txt .

# compile / install deps into the image user-site (/root/.local)
RUN apt-get update && \
    apt-get install -y --no-install-recommends gcc build-essential tzdata && \
    pip install --user --no-cache-dir -r requirements.txt && \
    # throw away build tooling to keep final image slim
    apt-get purge -y --auto-remove gcc build-essential && \
    rm -rf /var/lib/apt/lists/*

# ─────────────────────────────── runtime stage ────────────────────────────────
FROM python:3.12-slim
WORKDIR /app

# set zoneinfo so datetime.now() matches IST
RUN apt-get update && apt-get install -y --no-install-recommends tzdata && \
    ln -snf /usr/share/zoneinfo/Asia/Kolkata /etc/localtime && \
    echo "Asia/Kolkata" > /etc/timezone && \
    rm -rf /var/lib/apt/lists/*

ENV TZ=Asia/Kolkata
# pull python packages from builder layer
COPY --from=builder /root/.local /root/.local
ENV PATH=/root/.local/bin:$PATH

# copy application source
COPY . /app

# open port only if you later add a health endpoint; harmless otherwise
EXPOSE 8000

# default command – change to whatever entry script you use
CMD ["python", "ecs_reminder.py"]
