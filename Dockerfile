FROM python:3.9.2-slim

# Allow statements and log messages to immediately appear in the Knative logs
ENV PYTHONUNBUFFERED True
ENV APP_HOME /app
ENV PORT 8080
ENV HOST 0.0.0.0

# Python app installation
WORKDIR $APP_HOME
COPY . ./
RUN ./install.sh

ENTRYPOINT ["gcp-storage-emulator"]
CMD ["start"]
