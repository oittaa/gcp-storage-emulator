import os
from setuptools import setup, find_packages

NAME = "gcp-storage-emulator"
PACKAGES = find_packages()

DESCRIPTION = "A stub emulator for the Google Cloud Storage API"
URL = "https://github.com/oittaa/gcp-storage-emulator"
LONG_DESCRIPTION = open(os.path.join(os.path.dirname(__file__), "README.md")).read()

AUTHOR = "Eero Vuojolahti"
AUTHOR_EMAIL = "contact@oittaa.com"
GITHUB_REF = os.environ.get("GITHUB_REF")
PREFIX = "refs/tags/"

if GITHUB_REF and GITHUB_REF.startswith(PREFIX):
    prefix_len = len(PREFIX)
    VERSION = GITHUB_REF[prefix_len:]
else:
    VERSION = "0.0.0.dev0"

setup(
    name=NAME,
    version=VERSION,
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    long_description_content_type="text/markdown",
    url=URL,
    author=AUTHOR,
    author_email=AUTHOR_EMAIL,
    packages=PACKAGES,
    zip_safe=False,
    keywords=[
        "Google Cloud Storage",
        "Google App Engine",
        "Google Cloud Platform",
        "GCS",
        "GAE",
        "GCP",
    ],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
    ],
    entry_points={
        "console_scripts": [
            "gcp-storage-emulator=gcp_storage_emulator.__main__:main",
        ],
    },
    install_requires=[
        "fs==2.4.15",
        "google-crc32c==1.3.0",
    ],
    python_requires=">=3.7",
)
