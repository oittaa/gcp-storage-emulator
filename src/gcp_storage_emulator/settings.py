import os

API_ENDPOINT = "/storage/v1"
UPLOAD_API_ENDPOINT = "/upload/storage/v1"
BATCH_API_ENDPOINT = "/batch/storage/v1"
DOWNLOAD_API_ENDPOINT = "/download/storage/v1"

# Disk storage is rooted at STORAGE_BASE / STORAGE_DIR
STORAGE_BASE = os.path.abspath(os.environ.get("STORAGE_BASE", "./"))
STORAGE_DIR = os.environ.get("STORAGE_DIR", ".cloudstorage")

# Default GCP-style project number on bucket resources (issue #118).
# Override with env PROJECT_NUMBER or --project-number / create_server(...).
DEFAULT_PROJECT_NUMBER = os.environ.get("PROJECT_NUMBER", "1234")
