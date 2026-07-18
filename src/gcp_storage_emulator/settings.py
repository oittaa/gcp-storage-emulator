import os

API_ENDPOINT = "/storage/v1"
UPLOAD_API_ENDPOINT = "/upload/storage/v1"
BATCH_API_ENDPOINT = "/batch/storage/v1"
DOWNLOAD_API_ENDPOINT = "/download/storage/v1"

# Disk storage is rooted at STORAGE_BASE / STORAGE_DIR
STORAGE_BASE = os.path.abspath(os.environ.get("STORAGE_BASE", "./"))
STORAGE_DIR = os.environ.get("STORAGE_DIR", ".cloudstorage")
