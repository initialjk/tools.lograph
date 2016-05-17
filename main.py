import logging
from glob import glob
from optparse import OptionParser

import itertools

from lograph.parse import SeriesSet
from lograph.parser.dntest import DownloadTestLogParser
from lograph.parser.erftest import ErfTestLogParser
from lograph.parser.pingtest import PingTestLogParser
from lograph.render import plot_series
import sys
import os

logging.basicConfig(level='INFO')

parser = OptionParser()
parser.add_option("-s", "--size", dest="size", metavar="SIZE", type="int", nargs=2,
                  help="PIP repository to distribute. Default value is 'sknow'."
                       "You can use 'void' for repository to avoid actual distribution.")


def format_title(s):
    return s.replace('_', ' ') if s else None


def main():

    (options, args) = parser.parse_args()

    if args:
        dirs = itertools.chain(*(glob(arg) for arg in args))
    else:
        dirs = ('./log/',)

    for d in dirs:
        data = SeriesSet()
        data.load(source_path=d, parsers=[ErfTestLogParser(), PingTestLogParser(), DownloadTestLogParser()])
        basename = os.path.basename(os.path.abspath(d))

        series_list = list(data.filter(lambda s: 'pingtest' in s.dimension and 'rtt' in s.dimension))
        if series_list:
            title = "%s_pingtest" % basename
            scales = {'ms': (0, 500)}
            plot_series(format_title(title), series_list, scales=scales).savefig("%s.png" % title, bbox_inches="tight")

        series_list = list(data.filter(lambda s: 'erftest' in s.dimension))
        if series_list:
            title = "%s_erftest" % basename
            plot_series(format_title(title), series_list).savefig("%s.png" % title, bbox_inches="tight")


        series_list = list(data.filter(lambda s: ('dntest' in s.dimension) and ('1200K' in s.dimension)))
        if series_list:
            title = "%s_dntest" % basename
            scales = {'seconds': (0, 200)}
            plot_series(format_title(title), series_list, scales=scales).savefig("%s.png" % title, bbox_inches="tight")


if __name__ == '__main__':
    main()

