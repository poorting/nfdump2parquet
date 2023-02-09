#! /usr/bin/env python3
import os
import sys
import threading
import time
import pprint
import logging
import argparse
import textwrap

from watchdog.observers import Observer
from watchdog.events import RegexMatchingEventHandler
from multiprocessing import Process
from multiprocessing.pool import Pool

from nfdump2parquet import convert

program_name = os.path.basename(__file__)
VERSION = 0.1
logger = logging.getLogger('nfdump2parquet')


###############################################################################
# class Handler(PatternMatchingEventHandler):
class Handler(RegexMatchingEventHandler):

    def __init__(self, dest_dir, flowsrc=''):
        super().__init__(regexes=['.*/nfcapd.\d{12}'],
                         ignore_directories=True)
        # super().__init__(regexes=['.*'],
        #                  ignore_directories=True)
        self.dest_dir = dest_dir
        self.flowsrc = flowsrc
        self.pool = Pool(1)


    def completed_callback(self, result):
        logger.info(f"Completed: {result['src']}")


    def error_callback(self, error):
        logger.error(f"Error: {error}")

    # def __proc_convert(self, source_file, dest_dir, hives, flowsrc):
    #     # convert(src_file=source_file, dst_dir=dest_dir, flowsrc=flowsrc, loglevel=logging.DEBUG)
    #     self.pool.apply_async()

    def __convert(self, source_file):
        self.pool.apply_async(convert, args=(source_file, self.dest_dir, self.flowsrc),
                              callback=self.completed_callback,
                              error_callback=self.error_callback)
        # p = Process(target=Handler.__proc_convert, args=(source_file, self.dest_dir, self.flowsrc,))
        # p.start()

        # nf2pqt = Nfdump2Parquet(source_file, self.dest_dir, hives=self.hives, flowsrc=self.flowsrc)
        # nf2pqt.convert()

    def on_moved(self, event):
        logger.debug(f'Received moved event - {event.dest_path}')
        self.__convert(event.dest_path)

    def on_created(self, event):
        logger.debug(f'Received created event - {event.src_path}')
        self.__convert(event.src_path)


###############################################################################
class Watcher:
    dir_to_watch = ''

    def __init__(self, watchdir, dest_dir, recursive=True, flowsrc=''):
        self.dir_to_watch = watchdir
        self.dest_dir = dest_dir
        self.recursive = recursive
        self.flowsrc = flowsrc
        self.observer = Observer(watchdir, recursive)
        logger.info(f'Watching directory {watchdir}, recursive={recursive}')

    def run(self):
        print(f"Watcher.run {threading.current_thread().name}")
        event_handler = Handler(self.dest_dir, flowsrc=self.flowsrc)
        self.observer.schedule(event_handler, self.dir_to_watch, recursive=self.recursive)
        self.observer.start()
        try:
            while True:
                time.sleep(1)
        except Exception as e:
            self.observer.stop()
            print(f"Error {e}")

        self.observer.join()


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
    logger = logging.getLogger('nfdump2parquet')

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


###############################################################################
def main():
    pp = pprint.PrettyPrinter(indent=4)

    parser = parser_add_arguments()
    args = parser.parse_args()
    logger = get_logger(args)

    print(f"Program start {threading.current_thread().name}")
    w = Watcher(args.basedir, args.parquetdir, flowsrc=args.f)
    w.run()


###############################################################################
if __name__ == '__main__':
    # Run the main process
    main()
