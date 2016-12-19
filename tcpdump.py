import itertools
import logging
import os
import pickle
import sys
import traceback
from glob import glob
from optparse import OptionParser

import datetime

from lograph.parse import SeriesSet
from lograph.parser.tcpdump import TcpDumpLogParser
from lograph.render import plot_series, UnitOption

logging.basicConfig(level='INFO')

logger = logging.getLogger(__name__)

parser = OptionParser()
parser.add_option("-d", "--dpi", dest="dpi", type="int", help="DPI to save", metavar="DPI", default=100)

def format_title(s):
    return s.replace('_', ' ') if s else None

def format_week(d):
    return "%s-%s" % (d.strftime("%Y%m%d"), (d + datetime.timedelta(days=6)).strftime("%Y%m%d"))

def map_to_key(s):
    if isinstance(s.key, datetime.datetime):
        return s.key.date() - datetime.timedelta(days=s.key.isoweekday()-1)

def main():
    (options, args) = parser.parse_args()

    if args:
        dirs = itertools.chain(*(glob(arg) for arg in args))
    else:
        dirs = ('./log/tcpdump',)

    dpi = options.dpi

    for d in dirs:
        basename = os.path.basename(os.path.abspath(d))

        series_list = list(data.filter(lambda s: True))
        if series_list:
            try:
                plot_series('Connection reset count', series_list).savefig("%s.png" % basename, bbox_inches="tight")
            except ValueError as e:
                sys.stderr.writelines([traceback.format_exc(), '\n'])
                logger.error("Can't wrote figure for '%s': %s", title, e)


if __name__ == '__main__':
    main()

