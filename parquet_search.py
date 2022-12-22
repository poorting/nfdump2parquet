#! /usr/bin/env python3
import os
import sys
import time
import pprint
import argparse
import textwrap

import duckdb
import pyarrow as pa
import pyarrow.dataset as ds
import pandas as pd

program_name = os.path.basename(__file__)


###############################################################################
class ArgumentParser(argparse.ArgumentParser):

    def error(self, message):
        print('\n\033[1;33mError: {}\x1b[0m\n'.format(message))
        self.print_help(sys.stderr)
        # self.exit(2, '%s: error: %s\n' % (self.prog, message))
        self.exit(2)


###############################################################################
# Subroutines
# ------------------------------------------------------------------------------
def parser_add_arguments():
    """
        Parse command line parameters
    """
    parser = ArgumentParser(
        prog=program_name,
        description=textwrap.dedent('''\
                        Query parquet using duckdb
                        '''),
        formatter_class=argparse.RawTextHelpFormatter, )

    parser.add_argument("basedir",
                        help=textwrap.dedent('''\
                        Base directory of the partitioned parquet dataset
                        '''),
                        action="store",
                        )

    return parser


###############################################################################
def main():

    pp = pprint.PrettyPrinter(indent=4)

    parser = parser_add_arguments()
    args = parser.parse_args()

    basedir = args.basedir
    if not basedir.endswith('/'):
        basedir = basedir + '/'
    nfdump = ds.dataset(basedir, partitioning=ds.partitioning(flavor='hive'))

    # Get database connection
    con = duckdb.connect()

    print('Total number of flows')
    start = time.time()
    # Run query that selects part of the data
    query = con.execute("""
        SELECT count(1)
        FROM nfdump
        """)
    results = query.fetchdf()
    duration = time.time() - start
    columns, rows = os.get_terminal_size(0)
    with pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.width', columns):  # more options can be specified also
        pp.pprint(results)
    print(f"Executing took {duration} seconds")

    print('\n10 biggest flows (ibyt)')
    start = time.time()
    # Run query that selects part of the data
    query = con.execute("""
        SELECT date, hour, ts, td, sa, da, ibyt+obyt as tot_byt, ipkt, ibyt, opkt, obyt
        FROM nfdump
        ORDER BY tot_byt DESC
        LIMIT 10
        """)
    results = query.fetchdf()
    duration = time.time() - start
    columns, rows = os.get_terminal_size(0)
    with pd.option_context('display.max_rows', None, 'display.max_columns', None, 'display.width', columns):  # more options can be specified also
        pp.pprint(results)
    print(f"Executing took {duration} seconds")


###############################################################################
if __name__ == '__main__':
    # Run the main process
    main()
