#!/bin/sh
set -e
export DEBIAN_FRONTEND=noninteractive 
ARCH=$(uname -m)
echo "ARCH: $ARCH"
if [ "$ARCH" != "x86_64" ]
then
    apt-get update
    apt-get install -y gcc build-essential python3-dev
fi
pip install .
if [ "$ARCH" != "x86_64" ]
then
    apt-get remove --autoremove --purge -y gcc build-essential '*-dev'
    rm -rf /var/lib/apt/lists/*
fi
