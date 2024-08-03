#!/bin/bash

# Define image details
IMAGE_NAME="gcp-storage-emulator"
IMAGE_TAG="latest"
DOCKERFILE_PATH="."
DOCKERHUB_USERNAME="jamesmtc"

# Build the image for linux, x86, and arm64
docker buildx build --platform linux/amd64,linux/arm64 --tag $DOCKERHUB_USERNAME/$IMAGE_NAME:$IMAGE_TAG --push $DOCKERFILE_PATH
