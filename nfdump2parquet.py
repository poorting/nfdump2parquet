#! /usr/bin/env python3
import argparse
import logging
import os
import pprint
import re
import shutil
import subprocess
import sys
import tempfile
import textwrap
import time
from logging.handlers import QueueHandler
from multiprocessing import Process, Manager
from multiprocessing.pool import Pool

import pyarrow as pa
import pyarrow.csv
import pyarrow.parquet as pq

program_name = os.path.basename(__file__)
VERSION = 0.2

pattern = "nfcapd\.\d{12}"


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
def parser_add_arguments():
    """
        Parse command line parameters
    """
    parser = ArgumentParser(
        prog=program_name,
        description=textwrap.dedent('''\
                        Convert nfcapd file(s) (produced by the nfdump toolset) to parquet format
                        '''),
        formatter_class=argparse.RawTextHelpFormatter, )

    parser.add_argument("source",
                        help=textwrap.dedent('''\
                        Source nfcapd file or directory containing nfcapd files
                        '''),
                        action="store",
                        )

    parser.add_argument("parquetdir",
                        help=textwrap.dedent('''\
                        Directory where to store resulting parquet files
                        '''),
                        action="store",
                        )

    parser.add_argument("-f",
                        metavar='flowsrc',
                        help=textwrap.dedent('''\
                        Additional flowsrc name stored in the flowsrc column
                        '''),
                        action="store",
                        default=''
                        )

    parser.add_argument("-p",
                        metavar='processes',
                        help=textwrap.dedent('''\
                        Number of processes to use for conversion (default 2)
                        '''),
                        action="store",
                        type=int,
                        default=2
                        )

    parser.add_argument("-r", "--recursive",
                        help="recursively searches for nfcapd files if source specifies a directory.",
                        action="store_true")

    parser.add_argument("--debug",
                        help="show debug output",
                        action="store_true")

    parser.add_argument("-V", "--version",
                        help="print version and exit",
                        action="version",
                        version='%(prog)s (version {})'.format(VERSION))

    return parser


# ------------------------------------------------------------------------------
def list_files(directory, recursive=False):
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
                elif recursive:
                    filelist.extend(list_files(directory + entry.name, recursive))

    return filelist


# ------------------------------------------------------------------------------
def convert_process(args_dict):
    convert(args_dict['src_file'],
            args_dict['dst_dir'],
            args_dict['flowsrc'],
            args_dict['queue'],
            args_dict['loglevel'],
            )


# ------------------------------------------------------------------------------
def convert(src_file: str, dst_dir: str, flowsrc = '', queue=None, loglevel=logging.INFO):

    # Max size of chunk to read at a time
    block_size = 2 * 1024 * 1024

    # The default fields (order) present in the nfcapd files
    # Can be overridden by providing an nfdump_fields=[] to the constructor
    nf_fields = ['ts', 'te', 'td', 'sa', 'da', 'sp', 'dp', 'pr', 'flg',
                 'fwd', 'stos', 'ipkt', 'ibyt', 'opkt', 'obyt', 'in',
                 'out', 'sas', 'das', 'smk', 'dmk', 'dtos', 'dir',
                 'nh', 'nhb', 'svln', 'dvln', 'ismc', 'odmc', 'idmc',
                 'osmc', 'mpls1', 'mpls2', 'mpls3', 'mpls4', 'mpls5',
                 'mpls6', 'mpls7', 'mpls8', 'mpls9', 'mpls10', 'cl',
                 'sl', 'al', 'ra', 'eng', 'exid', 'tr']

    # The default fields that should be carried over to the parquet file
    # Can be overridden by providing a parquet_fields=[] to the constructor
    # exid == exporter id
    parquet_fields = ['ts', 'te', 'td', 'sa', 'da', 'sp', 'dp', 'pr', 'flg',
                      'ipkt', 'ibyt', 'opkt', 'obyt', 'dir', 'ra', 'exid']

    drop_columns = [a for a in nf_fields if a not in parquet_fields]

    logger = logging.getLogger(program_name)
    logger.setLevel(loglevel)

    if not os.path.isfile(src_file):
        raise FileNotFoundError(src_file)

    logger.info(f'converting {src_file}')
    start = time.time()

    # Create a temp file for the intermediate CSV
    tmp_file, tmp_filename = tempfile.mkstemp()
    os.close(tmp_file)

    # Create a temporary directory for writing parquet files in
    # first, before ultimately copying to the destination
    # This avoids errors querying parquet files while they
    # are being written as much as possible
    tmp_dirname = tempfile.mkdtemp()

    try:
        with open(tmp_filename, 'a', encoding='utf-8') as f:
            subprocess.run(['nfdump', '-r', src_file, '-o', 'csv', '-q'], stdout=f)
    except Exception as e:
        logger.error(f'Error reading {src_file} : {e}')
        return

    duration = time.time() - start

    sf = os.path.basename(src_file)
    basename = os.path.basename(src_file)[-12:]
    hivedir=f'{dst_dir}/date={basename[:4]}-{basename[4:6]}-{basename[6:8]}/hour={basename[8:10]}'
    os.makedirs(hivedir, exist_ok=True)

    logger.debug(f"{sf} to CSV in {duration:.2f}s")
    start = time.time()
    pqwriter = None

    try:
        with pyarrow.csv.open_csv(input_file=tmp_filename,
                                  read_options=pyarrow.csv.ReadOptions(
                                      block_size=block_size,
                                      column_names=nf_fields)
                                  ) as reader:
            chunk_nr = 0
            for next_chunk in reader:
                chunk_nr += 1
                if next_chunk is None:
                    break
                table = pa.Table.from_batches([next_chunk])
                try:
                    table = table.drop(drop_columns)
                except KeyError as ke:
                    logger.error(ke)

                table = table.append_column('flowsrc', [[flowsrc] * table.column('te').length()])

                basename_template = f'{basename}-chunk-{chunk_nr}' + '-part-{i}.parquet'

                if not pqwriter:
                    pqwriter = pq.ParquetWriter(f'{hivedir}/{basename}.parquet', table.schema)

                pqwriter.write_table(table)

    except pyarrow.lib.ArrowInvalid as e:
        logger.error(e)

    if pqwriter:
        pqwriter.close()

    duration = time.time() - start
    logger.debug(f"{sf} CSV to Parquet in {duration:.2f}s")

    # Now copy the results to its final destination
    logger.debug(f"{sf} Copying results from temp directory to {dst_dir}")
    shutil.copytree(tmp_dirname, dst_dir, dirs_exist_ok=True)

    logger.debug(f"{sf} Removing temporary files and directories")
    # Remove temporary file
    os.remove(tmp_filename)

    # Remove temporary directory
    shutil.rmtree(tmp_dirname, ignore_errors=True)

    return sf


# ------------------------------------------------------------------------------
# executed in a process that performs logging
def logger_process(queue, loglevel):
    # create a logger
    logger = logging.getLogger(program_name)
    # configure a stream handler
    handler = logging.StreamHandler()
    formatter = CustomConsoleFormatter()
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # log all messages, debug and up
    logger.setLevel(loglevel)
    # run forever
    while True:
        try:
            # consume a log message, block until one arrives
            message = queue.get()
            # check for shutdown
            if message is None:
                break
            # log the message
            logger.handle(message)
        except Exception:
            import sys, traceback
            print('Whoops! Problem:', file=sys.stderr)
            traceback.print_exc(file=sys.stderr)


###############################################################################
def main():

    pp = pprint.PrettyPrinter(indent=4)
    parser = parser_add_arguments()
    args = parser.parse_args()

    loglevel = logging.INFO
    if args.debug:
        loglevel = logging.DEBUG
    # # create the shared queue
    queue = Manager().Queue(-1)

    # start the logger process
    logger_p = Process(target=logger_process, args=(queue, loglevel,))
    logger_p.start()

    logger = logging.getLogger(program_name)
    logger.addHandler(QueueHandler(queue))
    logger.setLevel(loglevel)

    filelist = []
    filename = args.source

    if os.path.isdir(filename):
        filelist = list_files(filename, args.recursive)
    else:
        filelist.append(filename)

    filelist = sorted(filelist)

    pool = Pool(args.p,)

    kwargs_arr = []
    for filename in filelist:
        keywords = {
            'src_file': filename,
            'dst_dir': args.parquetdir,
            'flowsrc': args.f,
            'queue': queue,
            'loglevel': loglevel,
        }
        kwargs_arr.append(keywords)

    logger.debug("Firing off all conversions")
    pool.map(convert_process, kwargs_arr)

    logger.debug("All conversions finished, calling close & join")
    pool.close()
    pool.join()

    logger.debug("Shutting down the Queue and logging process")
    # shutdown the queue correctly
    queue.put(None)
    logger_p.join()


###############################################################################
if __name__ == '__main__':
    # Run the main process
    main()
