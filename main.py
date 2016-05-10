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



def main():
    data = SeriesSet()

    (options, args) = parser.parse_args()

    if args:
        dirs = itertools.chain(*(glob(arg) for arg in args))
    else:
        dirs = ('./log/',)

    for d in dirs:
        data.load(source_path=d, parsers=[ErfTestLogParser(), PingTestLogParser(), DownloadTestLogParser()])
        basename = os.path.basename(os.path.abspath(d))

        series_list = list(data.filter(lambda s: 'pingtest' in s.dimension))
        if series_list:
            title = "%s_pingtest" % basename
            for s in (s for s in series_list if not s.is_continuous):
                s.priority = -1
            plot_series(title.replace('_', ' '), series_list).savefig("%s.png" % title, bbox_inches="tight")

        series_list = list(data.filter(lambda s: 'erftest' in s.dimension))
        if series_list:
            title = "%s_erftest" % basename
            plot_series(title.replace('_', ' '), series_list).savefig("%s.png" % title, bbox_inches="tight")

        series_list = list(data.filter(lambda s: ('dntest' in s.dimension) and ('1266541' in s.dimension)))
        if series_list:
            title = "%s_dntest" % basename
            for s in (s for s in series_list if not s.is_continuous):
                s.priority = -1
            plot_series(title.replace('_', ' '), series_list).savefig("%s.png" % title, bbox_inches="tight")


if __name__ == '__main__':
    main()

