#! /usr/bin/env python3
import os
import sys
import shutil
import time
import logging
import pprint
import argparse
import textwrap
import tempfile
import subprocess
import re

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.csv

program_name = os.path.basename(__file__)
VERSION = 0.1
logger = logging.getLogger(__name__)

pattern = "nfcapd\.\d{12}"


###############################################################################
class Nfdump2Parquet:

    # Max size of chunk to read at a time, just short of 2GB (the max)
    block_size = 2047 * 1024 * 1024

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

    mem_table = None

    # ------------------------------------------------------------------------------
    def __init__(self, source_file: str, destination_dir: str, hives: bool = True, parquet_fields: list[str] = None,
                 nfdump_fields: list[str] = None, flowsrc = ''):
        """Initialises Nfdump2Parquet instance.

        Provide nfdump_fields parameter **only** if defaults don't work
        Defaults for parquet_fields: ts, te, td, sa, da, sp, dp, pr, flg, ipkt, ibyt, opkt, obyt, dir, ra, exid

        :param source_file: name of the nfcapd file to convert
        :param destination_dir: directory for storing resulting parquet file
        :param parquet_fields: the fields from ncapd file to translate to parquet
        :param nfdump_fields: the fields (and order) in the nfcapd file
        """
        if not os.path.isfile(source_file):
            raise FileNotFoundError(source_file)
        self.src_file = source_file
        self.basename = os.path.basename(self.src_file)[-12:]
        self.dst_dir = destination_dir
        self.hives = hives
        self.flowsrc = flowsrc

        if parquet_fields:
            self.parquet_fields = parquet_fields
        if nfdump_fields:
            self.nf_fields = nfdump_fields

        self.drop_columns = [a for a in self.nf_fields if a not in self.parquet_fields]

    # ------------------------------------------------------------------------------
    @staticmethod
    def __trunc_datetime(datetime_column: pyarrow.ChunkedArray):

        trunc_date = []
        trunc_hour = []
        trunc_datetime = []

        for entry in datetime_column:
            trunc_date.append(entry.as_py().strftime('%Y-%m-%d'))
            trunc_hour.append(entry.as_py().strftime('%H'))
            trunc_datetime.append(entry.as_py().strftime('%Y-%m-%d %H:%M:00'))

        return {
            'date': trunc_date,
            'hour': trunc_hour,
            'datetime': trunc_datetime,
        }

    # ------------------------------------------------------------------------------
    def convert(self):
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
                subprocess.run(['nfdump', '-r', self.src_file, '-o', 'csv', '-q'], stdout=f)
        except Exception as e:
            logger.error(f'Error reading {self.src_file} : {e}')
            return

        duration = time.time() - start
        sf = os.path.basename(self.src_file)
        logger.debug(f"{sf} to CSV in {duration:.2f}s")
        start = time.time()
        try:
            with pyarrow.csv.open_csv(input_file=tmp_filename,
                                      read_options=pyarrow.csv.ReadOptions(
                                          block_size=self.block_size,
                                          column_names=self.nf_fields)
                                      ) as reader:
                chunk_nr = 0
                for next_chunk in reader:
                    chunk_nr += 1
                    if next_chunk is None:
                        break
                    table = pa.Table.from_batches([next_chunk])
                    try:
                        table = table.drop(self.drop_columns)
                    except KeyError as ke:
                        logger.error(ke)

                    trunc_ts = self.__trunc_datetime(table.column('te'))
                    table = table.append_column('date', [trunc_ts['date']])
                    table = table.append_column('hour', [trunc_ts['hour']])
                    table = table.append_column('flowsrc', [[self.flowsrc] * table.column('te').length()])

                    basename_template = f'{self.basename}-chunk-{chunk_nr}' + '-part-{i}.parquet'

                    if self.hives:
                        ds.write_dataset(data=table,
                                         # base_dir=self.dst_dir,
                                         base_dir=tmp_dirname,
                                         basename_template=basename_template,
                                         format='parquet',
                                         partitioning=['date', 'hour'],
                                         partitioning_flavor='hive',
                                         max_partitions=4096,
                                         max_open_files=4096,
                                         existing_data_behavior='overwrite_or_ignore',
                                         )
                    else:
                        ds.write_dataset(data=table,
                                         # base_dir=self.dst_dir,
                                         base_dir=tmp_dirname,
                                         basename_template=basename_template,
                                         format='parquet',
                                         max_partitions=4096,
                                         max_open_files=4096,
                                         existing_data_behavior='overwrite_or_ignore',
                                         )
        except pyarrow.lib.ArrowInvalid as e:
            logger.error(e)

        duration = time.time() - start
        logger.debug(f"CSV to Parquet in {duration:.2f}s")

        # Now copy the results to its final destination
        logger.debug(f"Copying results from temp directory to {self.dst_dir}")
        shutil.copytree(tmp_dirname, self.dst_dir, dirs_exist_ok=True)

        logger.debug(f"Removing temporary files and directories")
        # Remove temporary file
        os.remove(tmp_filename)
        # Remove temporary directory
        shutil.rmtree(tmp_dirname, ignore_errors=True)


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

    parser.add_argument("-n", "--nohives",
                        help="Disables hive partitioning (date=YYYYin output parquet directory",
                        action="store_true")

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


###############################################################################
def main():

    pp = pprint.PrettyPrinter(indent=4)
    parser = parser_add_arguments()
    args = parser.parse_args()

    logger = get_logger(args)

    filelist = []
    filename = args.source

    if os.path.isdir(filename):
        filelist = list_files(filename, args.recursive)
    else:
        filelist.append(filename)

    filelist = sorted(filelist)
    # pp.pprint(filelist)

    for filename in filelist:
        logger.info(f'converting {filename}')
        try:
            nr2pqt = Nfdump2Parquet(filename, args.parquetdir, hives=not args.nohives, flowsrc=args.f)
            nr2pqt.convert()
        except FileNotFoundError as fnf:
            logger.error(f'File not found: {fnf}')
            exit(2)


###############################################################################
if __name__ == '__main__':
    # Run the main process
    main()
