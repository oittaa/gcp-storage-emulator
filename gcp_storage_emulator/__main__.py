#!/usr/bin/env python

import argparse
import logging
import os
import sys

from gcp_storage_emulator.handlers.buckets import create_bucket
from gcp_storage_emulator.server import create_server
from gcp_storage_emulator.storage import Storage

# One after gcloud-task-emulator one
DEFAULT_PORT = int(os.environ.get("PORT", 9023))
DEFAULT_HOST = os.environ.get("HOST", "localhost")


def get_server(host, port, memory=False, default_bucket=None, data_dir=None):
    server = create_server(host, port, memory, default_bucket, data_dir=data_dir)
    return server


def wipe(data_dir=None, keep_buckets=False):
    keep_str = " while keeping the buckets" if keep_buckets else ""
    print(f"Wiping...{keep_str}")
    server = create_server(None, None, False, data_dir=data_dir)
    server.wipe(keep_buckets=keep_buckets)
    print("Done.")
    return 0


def prepare_args_parser():
    parser = argparse.ArgumentParser(description="Google Cloud Storage Emulator")
    parser.add_argument(
        "-d", "--data-dir", default=None, help="directory to use as the storage root"
    )
    subparsers = parser.add_subparsers(title="subcommands", dest="subcommand")

    start = subparsers.add_parser("start", help="start the emulator")
    start.add_argument(
        "--port", type=int, help="the port to run the server on", default=DEFAULT_PORT
    )
    start.add_argument(
        "-H", "--host", help="the host to run the server on", default=DEFAULT_HOST
    )
    start.add_argument(
        "--default-bucket",
        help="The default bucket. If provided, bucket will be created automatically",
    )
    start.add_argument(
        "-q",
        "--quiet",
        action="store_true",
        default=False,
        help="only outputs critical level logging",
    )
    start.add_argument(
        "-M",
        "--no-store-on-disk",
        "--in-memory",
        action="store_true",
        default=False,
        help="use in-memory storage",
    )

    wipe = subparsers.add_parser("wipe", help="Wipe the local data")
    wipe.add_argument(
        "--keep-buckets",
        action="store_true",
        default=False,
        help="If provided the data will be wiped but the existing buckets are kept",
    )

    create_bucket = subparsers.add_parser("create_bucket", help="create bucket")
    # -n, --name deprecated
    create_bucket.add_argument(
        "-n", "--name", action="store_true", help=argparse.SUPPRESS
    )
    create_bucket.add_argument("name", help="Name of the new bucket")

    return parser, subparsers


def main(args=sys.argv[1:], test_mode=False):
    parser, subparsers = prepare_args_parser()
    args = parser.parse_args(args)
    if args.subcommand not in subparsers.choices.keys():
        parser.print_usage()
        sys.exit(1)

    if args.subcommand == "wipe":
        answer = input(
            "This operation will IRREVERSIBLY DELETE all your data. Do you wish to proceed? [y/N] "
        )
        if answer.lower() in ("y", "ye", "yes"):
            sys.exit(wipe(data_dir=args.data_dir, keep_buckets=args.keep_buckets))
        else:
            print("wipe command cancelled")
            sys.exit(1)

    if args.subcommand == "create_bucket":
        storage = Storage(data_dir=args.data_dir)
        create_bucket(args.name, storage)
        sys.exit(0)

    root = logging.getLogger("")
    stream_handler = logging.StreamHandler()
    root.addHandler(stream_handler)
    if args.quiet:
        root.setLevel(logging.CRITICAL)
    else:
        root.setLevel(logging.DEBUG)
    server = get_server(
        args.host, args.port, args.no_store_on_disk, args.default_bucket, args.data_dir
    )
    if test_mode:
        return server
    sys.exit(server.run())


if __name__ == "__main__":
    main()
