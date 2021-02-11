# Local Emulator for Google Cloud Storage

Google doesn't (yet) ship an emulator for the Cloud Storage API like they do for
Cloud Datastore.

This is a stub emulator so you can run your tests and do local development without
having to connect to the production Storage APIs.


**THIS IS A WORK IN PROGRESS AND ONLY SUPPORTS A LIMITED SUBSET OF THE API**

---

## Installation

`pip install .`


## CLI Usage


### Starting the emulator
Start the emulator with:

```bash
$ gcp-storage-emulator start --port=9090
```

By default, data is stored under `$PWD/.cloudstorage`. You can configure the folder using the env variables `STORAGE_BASE` (default `./`) and `STORAGE_DIR` (default `.cloudstorage`).

If you wish to run the emulator in a testing environment or if you don't want to persist any data, you can use the `--no-store-on-disk` parameter. For tests, you might want to consider starting up the server from your code (see the [Python APIs](#python-apis))

If you're using the Google client library (e.g. `google-cloud-storage` for Python) then you can set the `STORAGE_EMULATOR_HOST` environment variable to tell the library to connect to your emulator endpoint rather than the standard `https://storage.googleapis.com`, e.g.:

```bash
$ export STORAGE_EMULATOR_HOST=http://localhost:9090
```


### Wiping data

You can wipe the data by running

```bash
$ gcp-storage-emulator wipe
```

## Python APIs

To start a server from your code you can do

```python
from gcp_storage_emulator.server import create_server

server = create_server("localhost", 9023, in_memory=False)

server.start()
# ........
server.stop()
```

You can wipe the data (e.g. for text execution) by calling `server.wipe()`

This can also be achieved (e.g. during tests) by hitting the `/wipe` endpoint


## Docker

TODO
