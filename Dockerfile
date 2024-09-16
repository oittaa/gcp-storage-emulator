FROM python:3.12.5-slim

# Allow statements and log messages to immediately appear in the Knative logs
ENV PYTHONUNBUFFERED True
ENV APP_HOME /app
ENV PORT 8080
ENV HOST 0.0.0.0
ENV STORAGE_BASE /
ENV STORAGE_DIR storage

# Python app installation
WORKDIR $APP_HOME
COPY README.md pyproject.toml ./
COPY src src/

RUN PYTHONDONTWRITEBYTECODE=1 pip install --no-cache-dir -t /deps -r requirements.lock
ENV PYTHONPATH="/deps"

ENTRYPOINT ["gcp-storage-emulator"]
CMD ["start"]
