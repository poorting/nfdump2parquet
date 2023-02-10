#! /usr/bin/env python3
import os
import sys
import time
import pprint
import logging
import argparse
import textwrap
import signal

from watchdog.observers import Observer
from watchdog.events import RegexMatchingEventHandler
from watchdog.events import FileModifiedEvent
from multiprocessing.pool import Pool

from nfdump2parquet import convert

program_name = os.path.basename(__file__)
VERSION = 0.1
logger = logging.getLogger(program_name)

sig_received = False


###############################################################################
# class Handler(PatternMatchingEventHandler):
class Handler(RegexMatchingEventHandler):

    def __init__(self, dest_dir, pool, flowsrc=''):
        super().__init__(regexes=['.*/nfcapd.\d{12}'],
                         ignore_directories=True)
        # super().__init__(regexes=['.*'],
        #                  ignore_directories=True)
        self.dest_dir = dest_dir
        self.flowsrc = flowsrc
        self.pool = pool

    def completed_callback(self, result):
        logger.info(f"Completed: {result['src']} in {result['toCSV']+result['toParquet']:.2f} seconds")

    def error_callback(self, error):
        logger.error(f"Error: {error}")

    # def __proc_convert(self, source_file, dest_dir, hives, flowsrc):
    #     # convert(src_file=source_file, dst_dir=dest_dir, flowsrc=flowsrc, loglevel=logging.DEBUG)
    #     self.pool.apply_async()

    def __convert(self, source_file):
        logger.info(f"Converting {source_file}")
        self.pool.apply_async(convert, args=(source_file, self.dest_dir, self.flowsrc),
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
    #  paths.append(os.fsdecode(event.dest_path))
    # overriding method to catch this exception and logging it...
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
def get_logger(args):
    # logging.basicConfig(level=logging.DEBUG,
    #                     format='%(asctime)s %(name)-20s %(levelname)-8s %(message)s',
    #                     datefmt='%m-%d %H:%M',
    #                     filename='watch.log',
    #                     filemode='w')
    logger = logging.getLogger(program_name)

    # Create handlers
    console_handler = logging.StreamHandler()
    console_formatter = CustomConsoleFormatter()
    console_handler.setFormatter(console_formatter)
    file_handler = logging.FileHandler(filename='watch.log')
    file_formatter = logging.Formatter('%(asctime)s  %(levelname)-5s %(filename)-10s %(lineno)d %(funcName)-20s %(message)s')
    file_handler.setFormatter(file_formatter)

    logger.setLevel(logging.INFO)

    if args.debug:
        logger.setLevel(logging.DEBUG)

    # add handlers to the logger
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

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

    parser.add_argument("basedir",
                        help=textwrap.dedent('''\
                        Base directory to watch for nfdump files
                        '''),
                        action="store",
                        )

    parser.add_argument("parquetdir",
                        help=textwrap.dedent('''\
                        Base directory where to store parquet files
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

    parser = parser_add_arguments()
    args = parser.parse_args()
    logger = get_logger(args)

    pool = Pool(2, init_worker)
    event_handler = Handler(args.parquetdir, pool, flowsrc=args.f)
    observer = Observer()
    observer.schedule(event_handler, args.basedir, recursive=True)
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
