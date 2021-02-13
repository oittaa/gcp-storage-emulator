#!/usr/bin/env python

# This script runs the `gcp-storage-emulator` runner using the system environment Python binary.
# The `.py` extension allows Windows users to run the script from the `cmd` when the `.py` files
# are associated with the Python runtime.

from gcp_storage_emulator.__main__ import main

if __name__ == "__main__":
    main()
