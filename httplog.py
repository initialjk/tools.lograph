import itertools
import logging
import os
import sys
import traceback
from glob import glob
from optparse import OptionParser

from lograph.parse import SeriesSet
from lograph.parser.http import HttpAccessLogParser
from lograph.render import plot_series

logging.basicConfig(level='INFO')

logger = logging.getLogger(__name__)

parser = OptionParser()
parser.add_option("-s", "--size", dest="size", metavar="SIZE", type="int", nargs=2,
                  help="Size of figure")


def format_title(s):
    return s.replace('_', ' ') if s else None

def main():
    (options, args) = parser.parse_args()

    if args:
        dirs = itertools.chain(*(glob(arg) for arg in args))
    else:
        dirs = ('./log/http',)

    for d in dirs:
        data = SeriesSet()
        data.load(source_path=d, parsers=[HttpAccessLogParser()])
        basename = os.path.basename(os.path.abspath(d))

        series_list = list(data.filter(lambda s: True))
        if series_list:
            title = "%s_interval" % basename
            try:
                plot_series(format_title(title), series_list).savefig("%s.png" % title, bbox_inches="tight", legend=None)
            except ValueError as e:
                sys.stderr.writelines([traceback.format_exc(), '\n'])
                logger.error("Can't wrote figure for '%s': %s", title, e)


if __name__ == '__main__':
    main()

