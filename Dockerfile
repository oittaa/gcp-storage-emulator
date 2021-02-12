FROM python:3.9.1-slim

# Allow statements and log messages to immediately appear in the Knative logs
ENV PYTHONUNBUFFERED True
ENV APP_HOME /app
ENV PORT 8080

# Python app installation
WORKDIR $APP_HOME
COPY . ./
RUN pip install .

ENTRYPOINT $APP_HOME/bin/gcp-storage-emulator start --host=0.0.0.0 --port=$PORT
