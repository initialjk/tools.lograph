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
        pickle_file = os.path.join(os.path.dirname(os.path.abspath(d)), basename + '.pickle')
        if os.path.isfile(pickle_file):
            with open(pickle_file, 'rb') as f:
                data = pickle.load(f)
        else:
            data = SeriesSet()
            data.load(source_path=d, parsers=[TcpDumpLogParser()])

            with open(pickle_file, 'wb') as f:
                pickle.dump(data, f)


        for k, v in data.split_by(map_to_key).iteritems():
            series_list = list(v.filter(lambda s: True))
            if series_list:
                try:
                    series_list = sorted(series_list, key=lambda s: reduce((lambda r,v: r+v.value), s.samples, 0), reverse=True)
                    series_list = list(itertools.islice(series_list, 20))
                    for s in series_list:
                        s.consolidate()
                        s.is_continuous = False
                        s.graph_hint = 'stacked'
                        for ss in s.samples:
                            if ss.value > 301:
                                ss.value = 301
                        #  s.samples = s.samples[:100]

                    fig_width_px = (60 * 24 * 7) / 0.775  # Extend to figure margin
                    fit_height_px = 800 / 0.8  # Extend to figure margin
                    plot = plot_series('Connection reset count', series_list,
                                       figsize=(float(fig_width_px)/dpi, float(fit_height_px)/dpi), dpi=dpi,
                                       unit_options=dict(count=UnitOption(True, (0, 300), (k, k + datetime.timedelta(days=7)))),
                                       draw_options=dict(width=1.0/(24*60) ,edgecolor='none', align='center'))
                    plot.savefig("%s_%s.png" % (basename, format_week(k)), bbox_inches="tight", dpi=dpi)
                except ValueError as e:
                    sys.stderr.writelines([traceback.format_exc(), '\n'])
                    logger.error("Can't wrote figure for '%s': %s", basename, e)


if __name__ == '__main__':
    main()

