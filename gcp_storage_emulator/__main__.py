#!/usr/bin/env python

import argparse
import logging
import sys

from gcp_storage_emulator.handlers.buckets import create_bucket
from gcp_storage_emulator.server import create_server
from gcp_storage_emulator.storage import Storage

# One after gcloud-task-emulator one
DEFAULT_PORT = 9023
DEFAULT_HOST = "localhost"


def get_server(host, port, memory=False, default_bucket=None):
    server = create_server(host, port, memory, default_bucket)
    return server


def wipe():
    print("Wiping...")
    server = create_server(None, None, False)
    server.wipe()
    print("Done.")
    return 0


def prepare_args_parser():
    parser = argparse.ArgumentParser(description="Google Cloud Storage Emulator")
    subparsers = parser.add_subparsers(title="subcommands", dest="subcommand")

    start = subparsers.add_parser("start", help="start the emulator")
    start.add_argument(
        "--port", type=int, help="the port to run the server on", default=DEFAULT_PORT
    )
    start.add_argument(
        "-H", "--host", help="the host to run the server on", default=DEFAULT_HOST
    )
    start.add_argument(
        "--default-bucket", help="The default bucket. If provided, bucket will be created automatically"
    )
    start.add_argument("-q", "--quiet", action="store_true", default=False, help="only outputs critical level logging")
    start.add_argument("-M", "--no-store-on-disk", action="store_true", default=False, help="use in-memory storage")

    subparsers.add_parser("wipe", help="Wipe the local data")

    create_bucket = subparsers.add_parser("create_bucket", help="create bucket")
    create_bucket.add_argument(
        "-n", "--name",
        help="Name of the new bucket"
    )

    return parser, subparsers


def main(args=sys.argv[1:], test_mode=False):
    parser, subparsers = prepare_args_parser()
    args = parser.parse_args(args)
    if args.subcommand not in subparsers.choices.keys():
        parser.print_usage()
        sys.exit(1)

    if args.subcommand == "wipe":
        answer = input("This operation will IRREVERSIBLY DELETE all your data. Do you wish to proceed? [y/N] ").lower()
        if answer in ("y", "ye", "yes"):
            sys.exit(wipe())
        else:
            print("wipe command cancelled")
            sys.exit(1)

    if args.subcommand == "create_bucket":
        storage = Storage()
        create_bucket(args.name, storage)
        sys.exit(1)

    root = logging.getLogger("")
    stream_handler = logging.StreamHandler()
    root.addHandler(stream_handler)
    if args.quiet:
        root.setLevel(logging.CRITICAL)
    else:
        root.setLevel(logging.DEBUG)
    server = get_server(args.host, args.port, args.no_store_on_disk, args.default_bucket)
    if test_mode:
        return server
    sys.exit(server.run())


if __name__ == "__main__":
    main()
