import itertools
import logging
import os
import sys
import traceback
from glob import glob
from optparse import OptionParser

from lograph.parse import SeriesSet
from lograph.parser.tcpdump import TcpDumpLogParser
from lograph.render import plot_series

logging.basicConfig(level='INFO')

logger = logging.getLogger(__name__)

parser = OptionParser()


def format_title(s):
    return s.replace('_', ' ') if s else None


def main():
    (options, args) = parser.parse_args()

    if args:
        dirs = itertools.chain(*(glob(arg) for arg in args))
    else:
        dirs = ('./log/tcpdump',)

    for d in dirs:
        data = SeriesSet()
        data.load(source_path=d, parsers=[TcpDumpLogParser()])
        basename = os.path.basename(os.path.abspath(d))

        series_list = list(data.filter(lambda s: True))
        if series_list:
            title = basename
            try:
                plot_series(format_title(title), series_list).savefig("%s.png" % title, bbox_inches="tight")
            except ValueError as e:
                sys.stderr.writelines([traceback.format_exc(), '\n'])
                logger.error("Can't wrote figure for '%s': %s", title, e)


if __name__ == '__main__':
    main()

