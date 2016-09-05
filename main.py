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

            offset = 0
            for ss in itertools.chain(*(s.subordinates_series for s in series_list if s.subordinates_series)):
                if 'loss' in ss.dimension:
                    offset += 10
                    for sample in ss.samples:
                        sample.value = offset

            try:
                plot_series(format_title(title), series_list, scales=scales).savefig("%s.png" % title, bbox_inches="tight")
            except ValueError as e:
                print("Can't wrote figure for '%s'" % title, e)

        series_list = list(data.filter(lambda s: contains_all(s.dimension,'erftest', 'speed')))
        if series_list:
            title = "%s_erftest" % basename
            count = align_subordinates_index(series_list, 'error', base_offset=20)

            try:
                plot_series(format_title(title), series_list, scales={'index': (0,21+count)}).savefig("%s.png" % title, bbox_inches="tight")
            except ValueError as e:
                print("Can't wrote figure for '%s'" % title, e)

        series_list = list(data.filter(lambda s: ('dntest' in s.dimension) and ('1200K' in s.dimension)))
        if series_list:
            title = "%s_dntest" % basename
            count = align_subordinates_index(series_list, 'error', base_offset=20)

                if 'error' in ss.dimension:
                    offset += 10
                    ss.unit = 'seconds'
            try:
                plot_series(format_title(title), series_list, scales={'index': (0,21+count)}).savefig("%s.png" % title, bbox_inches="tight")
            except ValueError as e:
                print("Can't wrote figure for '%s'" % title, e)

        series_list = list(data.filter(lambda s: ('dntest' in s.dimension) and ('0K' in s.dimension)))
        if series_list:
            title = "%s_dntest_small" % basename
            count = align_subordinates_index(series_list, 'error', base_offset=20)

            try:
                plot_series(format_title(title), series_list, scales={'index': (0,21+count)}).savefig("%s.png" % title, bbox_inches="tight")
            except ValueError as e:
                print("Can't wrote figure for '%s'" % title, e)


def align_subordinates_index(series_list, key, base_offset=0):
    offset = 0
    for offset, ss in enumerate(ss for s in series_list for ss in s.subordinates_series if key in ss.dimension):
        ss.unit = 'index'
        for sample in ss.samples:
            sample.value = base_offset + offset
    return offset


if __name__ == '__main__':
    main()

