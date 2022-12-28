#! /usr/bin/env python3
"""coalesce_parquets.py

gist of how to coalesce small row groups into larger row groups.
Solves the problem described in https://issues.apache.org/jira/browse/PARQUET-1115

Taken from:
https://gist.github.com/NickCrews/7a47ef4083160011e8e533531d73428c
and modified for its intended purpose
"""
from __future__ import annotations

import os
import sys
import pprint
import argparse
import textwrap
import time
from datetime import datetime
import logging
import re

from pathlib import Path
from typing import Callable, Iterable, TypeVar

import pyarrow as pa
import pyarrow.parquet as pq

program_name = os.path.basename(__file__)
VERSION = 0.1
logger = logging.getLogger(__name__)


def stream_to_parquet(path: Path, tables: Iterable[pa.Table]) -> None:
    try:
        first = next(tables)
    except StopIteration:
        return
    schema = first.schema
    with pq.ParquetWriter(path, schema) as writer:
        writer.write_table(first)
        for table in tables:
            table = table.cast(schema)  # enforce schema
            writer.write_table(table)


def stream_from_parquet(path: Path) -> Iterable[pa.Table]:
    reader = pq.ParquetFile(path)
    for batch in reader.iter_batches():
        yield pa.Table.from_batches([batch])


T = TypeVar("T")


def coalesce(
    items: Iterable[T], max_size: int, sizer: Callable[[T], int] = len
) -> Iterable[list[T]]:
    """Coalesce items into chunks. Tries to maximize chunk size and not exceed max_size.

    If an item is larger than max_size, we will always exceed max_size, so make a
    best effort and place it in its own chunk.

    You can supply a custom sizer function to determine the size of an item.
    Default is len.

    >>> list(coalesce([1, 2, 11, 4, 4, 1, 2], 10, lambda x: x))
    [[1, 2], [11], [4, 4, 1], [2]]
    """
    batch = []
    current_size = 0
    for item in items:
        this_size = sizer(item)
        if current_size + this_size > max_size:
            yield batch
            batch = []
            current_size = 0
        batch.append(item)
        current_size += this_size
    if batch:
        yield batch


def stream_from_files(in_files: list) -> Iterable[pa.Table]:
    # directory = Path(directory)
    # for path in directory.glob("*.parquet"):
    #     yield from stream_from_parquet(path)
    for parquet_file in in_files:
        yield from stream_from_parquet(Path(parquet_file))


def coalesce_parquets(in_files: list, outpath, max_size: int = 2**20) -> None:
    tables = stream_from_files(in_files)
    # Instead of coalescing using number of rows as your metric, you could
    # use pa.Table.nbytes or something.
    # table_groups = coalesce(tables, max_size, sizer=lambda t: t.nbytes)
    table_groups = coalesce(tables, max_size)
    coalesced_tables = (pa.concat_tables(group) for group in table_groups)
    stream_to_parquet(outpath, coalesced_tables)


###############################################################################
class ArgumentParser(argparse.ArgumentParser):

    def error(self, message):
        print('\n\033[1;33mError: {}\x1b[0m\n'.format(message))
        self.print_help(sys.stderr)
        # self.exit(2, '%s: error: %s\n' % (self.prog, message))
        self.exit(2)


###############################################################################
class CustomConsoleFormatter(logging.Formatter):
    """
        Log facility format
    """

    def format(self, record):
        # info = '\033[0;32m'
        info = ''
        warning = '\033[0;33m'
        error = '\033[1;33m'
        debug = '\033[1;34m'
        reset = "\x1b[0m"

        formatter = "%(levelname)s - %(message)s"
        if record.levelno == logging.INFO:
            log_fmt = info + formatter + reset
            self._style._fmt = log_fmt
        elif record.levelno == logging.WARNING:
            log_fmt = warning + formatter + reset
            self._style._fmt = log_fmt
        elif record.levelno == logging.ERROR:
            log_fmt = error + formatter + reset
            self._style._fmt = log_fmt
        elif record.levelno == logging.DEBUG:
            # formatter = '%(asctime)s %(levelname)s [%(filename)s.py:%(lineno)s/%(funcName)s] %(message)s'
            formatter = '%(levelname)s [%(filename)s:%(lineno)s/%(funcName)s] %(message)s'
            log_fmt = debug + formatter + reset
            self._style._fmt = log_fmt
        else:
            self._style._fmt = formatter

        return super().format(record)


###############################################################################
# Subroutines
# ------------------------------------------------------------------------------
def get_logger(args):
    logger = logging.getLogger(__name__)

    # Create handlers
    console_handler = logging.StreamHandler()
    #    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    formatter = CustomConsoleFormatter()
    console_handler.setFormatter(formatter)

    logger.setLevel(logging.INFO)

    if args.debug:
        logger.setLevel(logging.DEBUG)

    # add handlers to the logger
    logger.addHandler(console_handler)

    return logger


# ------------------------------------------------------------------------------
def parser_add_arguments():
    """
        Parse command line parameters
    """
    parser = ArgumentParser(
        prog=program_name,
        description=textwrap.dedent('''\
                        Coalesce nfdump parquet files in a hive partitioned directory
                        (e.g. "date=2022-11-12/hour=22/"). All parquet files in each
                        "hour" directory will be combined into one parquet file.
                        The directory for the current date is ignored, only 
                        directories for yesterday and earlier will be coalesced.
                        
                        The original parquet files WILL.BE.DELETED!
                        
                        '''),
        formatter_class=argparse.RawTextHelpFormatter, )

    parser.add_argument("basedir",
                        help=textwrap.dedent('''\
                        Base directory containing (hive partitioned) parquet files
                        '''),
                        action="store",
                        )

    parser.add_argument("--debug",
                        help="show debug output",
                        action="store_true")

    parser.add_argument("-V", "--version",
                        help="print version and exit",
                        action="version",
                        version='%(prog)s (version {})'.format(VERSION))

    return parser


# ------------------------------------------------------------------------------
def list_dirs(directory, pattern, recursive=False):
    dirlist = []
    if not os.path.isdir(directory):
        return dirlist

    if not directory.endswith("/"):
        directory = directory + '/'
    with os.scandir(directory) as it:
        for entry in it:
            if not entry.name.startswith('.'):
                if entry.is_dir():
                    if entry.name.startswith(pattern):
                        dirlist.append(entry.name)

    return dirlist


# ------------------------------------------------------------------------------
def list_files(directory, pattern):
    filelist = []
    if not os.path.isdir(directory):
        return filelist

    if not directory.endswith("/"):
        directory = directory + '/'
    with os.scandir(directory) as it:
        for entry in it:
            if not entry.name.startswith('.'):
                if entry.is_file():
                    if re.match(pattern, entry.name):
                        filelist.append('{0}{1}'.format(directory, entry.name))

    return filelist


###############################################################################
def main():
    pp = pprint.PrettyPrinter(indent=4)

    parser = parser_add_arguments()
    args = parser.parse_args()
    logger = get_logger(args)

    directory = args.basedir
    if not directory.endswith("/"):
        directory = directory + '/'

    datelist = sorted(list_dirs(directory, 'date='))
    now = datetime.now()
    datenow = now.strftime('%Y-%m-%d')
    logger.info(f'Current date is {datenow}')
    # Only coalesce parquet files before today:
    # drop the current date from the list of directories
    datelist = [date for date in datelist if not date.endswith(datenow)]
    logger.debug(datelist)

    date_hour_dict = {}
    for datedir in datelist:
        hourlist = sorted(list_dirs(directory+datedir, 'hour='))
        date_hour_dict[datedir] = hourlist

    # Now go through everything and coalesce each directory.
    # The list of original parquet files is deleted after coalescing
    for datedir, hours in date_hour_dict.items():
        for hourdir in hours:
            coalesce_dir = f'{directory}{datedir}/{hourdir}/'
            files = sorted(list_files(coalesce_dir, '\d{12}.*\.parquet'))
            nr_of_files = len(files)
            logger.info(f'Combining {nr_of_files} parquet files in {datedir}/{hourdir}')
            if files:
                datestr = datedir.split('=')[-1]
                hourstr = hourdir.split('=')[-1]
                coalesce_parquets(files, coalesce_dir+f'{datestr}-{hourstr}:00.parquet')
                for parquet_file in files:
                    os.remove(parquet_file)


###############################################################################
if __name__ == '__main__':
    # Run the main process
    main()
