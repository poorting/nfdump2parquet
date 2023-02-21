#! /usr/bin/env python3
import os
import sys
import time
import pprint
import logging
import argparse
import configparser
import textwrap
import signal
import shutil
import tempfile
import subprocess

import pyarrow as pa
import pyarrow.csv
import pyarrow.parquet as pq

from watchdog.observers import Observer
from watchdog.events import RegexMatchingEventHandler
from watchdog.events import FileModifiedEvent
from multiprocessing.pool import Pool

# from nfdump2parquet import convert

program_name = os.path.basename(__file__)
VERSION = 0.1
logger = logging.getLogger(program_name)

sig_received = False


###############################################################################
# class Handler(PatternMatchingEventHandler):
class Handler(RegexMatchingEventHandler):

    def __init__(self, pool, ch_table='nfsen.flows', flowsrc=''):
        super().__init__(regexes=['.*/nfcapd.\d{12}'],
                         ignore_directories=True)
        # super().__init__(regexes=['.*'],
        #                  ignore_directories=True)
        self.flowsrc = flowsrc
        self.ch_table = ch_table
        self.pool = pool

    def completed_callback(self, result):
        logger.info(f"Completed: {result['src']} in {result['toCSV']+result['toParquet']+result['toCH']:.2f} seconds")
        logger.info(f"\t to CSV: {result['toCSV']:.2f}s, to Parquet: {result['toCSV']:.2f}s, CH ingest: {result['toCH']:.2f}s")

    def error_callback(self, error):
        logger.error(f"Error: {error}")

    def __convert(self, source_file):
        logger.info(f"Converting {source_file}")
        self.pool.apply_async(convert, args=(source_file, self.ch_table, self.flowsrc),
                              callback=self.completed_callback,
                              error_callback=self.error_callback)

    def on_moved(self, event):
        logger.debug(f'Received moved event - {event.dest_path}')
        self.__convert(event.dest_path)

    def on_created(self, event):
        logger.debug(f'Received created event - {event.src_path}')
        self.__convert(event.src_path)

    # For some reasons the watcher fails after a length of time with
    # TypeError: expected str, bytes or os.PathLike object, not NoneType
    # In Handler.dispatch(event) (watchdog/events.py:476 in dispatch)
    # Which is this line:
    #  paths.append(os.fsdecode(event.dest_path))
    # Overriding the dispatch method to catch this exception and logging it...
    # So that at least the exception doesn't stop the watchdog
    def dispatch(self, event):
        pp = pprint.PrettyPrinter(indent=4)
        try:
            if not isinstance(event, FileModifiedEvent):
                logger.debug(event)
            super().dispatch(event)
        except TypeError as te:
            logger.error('TypeError on dispatch event')
            logger.error(te)
            logger.error(event)


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
def get_logger(logfile=None, debug=False):
    logger = logging.getLogger(program_name)

    # Create handlers
    console_handler = logging.StreamHandler()
    console_formatter = CustomConsoleFormatter()
    console_handler.setFormatter(console_formatter)

    if logfile:
        file_handler = logging.FileHandler(filename=logfile)
        file_formatter = logging.Formatter('%(asctime)s  %(levelname)-5s %(filename)-10s %(lineno)d %(funcName)-20s %(message)s')
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)
    else:
        logger.addHandler(console_handler)

    logger.setLevel(logging.INFO)

    if debug:
        logger.setLevel(logging.DEBUG)

    return logger


# Subroutines
# ------------------------------------------------------------------------------
def parser_add_arguments():
    """
        Parse command line parameters
    """
    parser = ArgumentParser(
        prog=program_name,
        description=textwrap.dedent('''\
                        Watches a directory (and its subdirectories) for nfcapd files and converts to parquet

                        Only files named 'nfcapd.YYYYMMDDHHMM' are picked up, thereby effectively ignoring 
                        files currently being generated by the nfdump tools.
                        '''),
        formatter_class=argparse.RawTextHelpFormatter, )

    parser.add_argument("-b",
                        metavar="basedir",
                        help=textwrap.dedent('''\
                        Base directory to watch for nfdump files
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

    parser.add_argument("-c",
                        metavar='config file',
                        help=textwrap.dedent('''\
                        load config from this file. 
                        If a config file is specified then all
                        other command line options are ignored
                        '''),
                        action="store",
                        default=''
                        )

    parser.add_argument("-l",
                        metavar='log file',
                        help=textwrap.dedent('''\
                        Log to the specified file instead
                        of logging to console.
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
def init_worker():
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGTERM, signal.SIG_IGN)


# ------------------------------------------------------------------------------
def convert(src_file: str, ch_table='nfsen.flows', flowsrc='', loglevel=logging.INFO):

    # Max size of chunk to read at a time
    block_size = 2 * 1024 * 1024

    # The default fields (order) present in the nfcapd files
    nf_fields = ['ts', 'te', 'td', 'sa', 'da', 'sp', 'dp', 'pr', 'flg',
                 'fwd', 'stos', 'ipkt', 'ibyt', 'opkt', 'obyt', 'in',
                 'out', 'sas', 'das', 'smk', 'dmk', 'dtos', 'dir',
                 'nh', 'nhb', 'svln', 'dvln', 'ismc', 'odmc', 'idmc',
                 'osmc', 'mpls1', 'mpls2', 'mpls3', 'mpls4', 'mpls5',
                 'mpls6', 'mpls7', 'mpls8', 'mpls9', 'mpls10', 'cl',
                 'sl', 'al', 'ra', 'eng', 'exid', 'tr']

    # The default fields that should be carried over to the parquet file
    # exid == exporter id
    parquet_fields = ['ts', 'te', 'sa', 'da', 'sp', 'dp', 'pr', 'flg',
                      'ipkt', 'ibyt', 'ra']

    info_return = {
        'src': src_file,
    }

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

    try:
        with open(tmp_filename, 'a', encoding='utf-8') as f:
            subprocess.run(['nfdump', '-r', src_file, '-o', 'csv', '-q'], stdout=f)
    except Exception as e:
        logger.error(f'Error reading {src_file} : {e}')
        return

    duration = time.time() - start
    info_return['toCSV'] = duration
    logger.debug(f"{src_file} to CSV in {duration:.2f}s")

    # Create a temp file for the parquet file
    tmp_file, tmp_parquetfile = tempfile.mkstemp()
    os.close(tmp_file)

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

                if not pqwriter:
                    pqwriter = pq.ParquetWriter(tmp_parquetfile, table.schema)

                pqwriter.write_table(table)

    except pyarrow.lib.ArrowInvalid as e:
        logger.error(e)

    if pqwriter:
        pqwriter.close()

    duration = time.time() - start
    info_return['toParquet'] = duration
    logger.debug(f"{src_file} CSV to Parquet in {duration:.2f}s")

    logger.debug(f"{src_file} Removing temporary file")
    # Remove temporary file
    os.remove(tmp_filename)

    # Import the parquet file into clickhouse
    start = time.time()
    try:
        with open(tmp_parquetfile, 'rb') as f:
            subprocess.run(['clickhouse-client', '--query', f"INSERT INTO {ch_table} FORMAT Parquet"], stdin=f)
    except Exception as e:
        print(f'Error : {e}')

    duration = time.time() - start
    info_return['toCH'] = duration
    logger.debug(f"Parquet ingest in CH in {duration:.2f}s")

    # Remove the temporary parquet file
    os.remove(tmp_parquetfile)

    return info_return


###############################################################################
def main():

    def signal_handler(signum, frame):
        global sig_received
        sig_received = True
        signame = signal.Signals(signum).name
        logger.info(f'Signal {signame} received. Exiting gracefully.')

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    pp = pprint.PrettyPrinter(indent=4)

    logger = logging.getLogger(program_name)
    logfile = None

    parser = parser_add_arguments()
    args = parser.parse_args()

    if not args.c:
        if not args.b:
            parser.error("No basedir provided. Provide either a basedir and parquetdir or a configuration file")
            exit(1)

    watches = list()

    if args.l:
        logfile = args.l

    flowsrc=''
    if args.f:
        flowsrc = args.f

    if args.b and not os.path.isdir(args.b):
        logger.error(f"Directory to watch ({args.b}) not found or not a directory")
        exit(2)

    if args.b:
        watches.append({'watchdir':args.b,
                        'flowsrc': flowsrc})

    # See if we have a config file
    if args.c and os.path.isfile(args.c):
        config = configparser.ConfigParser()
        config.read(args.c)
        try:
            logfile = config['DEFAULT']['logfile']
        except KeyError:
            None

        for section in config.sections():
            try:
                watchdir = config[section]['watchdir']
                ch_table = config[section]['ch_table']

                if os.path.isdir(watchdir):
                    watches.append({'watchdir': watchdir,
                                    'ch_table': ch_table,
                                    'flowsrc': section})
                else:
                    logger.error(f'watchdir in section [{section}] of {args.c} does not exist or is not a directory')

            except KeyError:
                logger.error(f'watchdir missing in section [{section}] of {args.c}')

    logger = get_logger(logfile=logfile, debug=args.debug)

    if len(watches) == 0:
        logger.error("No directories to watch, exiting.")
        exit(1)

    pool = Pool(len(watches), init_worker)
    observer = Observer()

    for watch in watches:
        event_handler = Handler(pool, ch_table= watch['ch_table'], flowsrc=watch['flowsrc'])
        observer.schedule(event_handler, watch['watchdir'], recursive=True)
        logger.info(f"Starting watch on {watch['watchdir']}, with flowsr='{watch['flowsrc']}'")

    observer.start()
    try:
        while not sig_received:
            time.sleep(1)
    finally:
        observer.stop()
        observer.join()
        pool.close()
        pool.join()


###############################################################################
if __name__ == '__main__':
    # Run the main process
    main()