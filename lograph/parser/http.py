import datetime
import logging
import os
import re
import sys
import traceback
import numpy

from lograph.parse import LogParser, Series

logger = logging.getLogger(__name__)

RE_PATTERN_HTTP = re.compile(r'^(?P<host>\S+)\s+(?P<client>\S+)\s+\((?P<origin>[^, ]+)?(?P<hops>(,\s?[^, ]+))\)\s+'
                             r'(?P<user>\S+)\s+(?P<username>\S+)\s+'
                             r'\[(?P<datetime>.*)\]\s+"(?P<method>[A-Z]+)\s(?P<path>\S+)\s(?P<version>\S+)"\s+'
                             r'(?P<status_code>\d+)\s+(?P<content_length>\d+)\s+"(?P<referer>\S+)"\s+"(?P<agent>\S+)"$')

RE_PATTERN_RESOURCE_PATH = re.compile(r'/(?P<ver>\S+)/accounts/(?P<gpid>\S+)/(?P<resource>\S+)[?](?P<parameters>\S+)')

def strptime(timestr):
    elem = timestr.split()
    dt = datetime.datetime.strptime(elem[0], '%d/%b/%Y:%H:%M:%S')
    if len(elem) > 1:
        dt.replace(tzinfo=datetime.tzinfo(elem[1]))
    return dt


class SeriesMap(dict):
    def __init__(self, dimension, **kwargs):
        super(SeriesMap, self).__init__()
        self.base_dimension = dimension
        self.kwargs = kwargs

    def __getitem__(self, item):
        series = self.get(item)
        if series is None:
            self[item] = series = Series(self.base_dimension + [item,], **self.kwargs)
            series.last_called = None
        return series


class HttpResourceIntervalParser(object):
    default_interval = datetime.timedelta(seconds=30.0)
    def __init__(self, base_dimension):
        self.series_map = SeriesMap(base_dimension + ['interval'], unit='seconds')

        self.test_time = None
        self.cur_block = None
        self.last_bps = 0
        self.elapsed_time = 0
        self.errors = []

    def feed(self, l):
        m = RE_PATTERN_HTTP.match(l)
        if m is None:
            raise ValueError("Incorrect format")

        r = RE_PATTERN_RESOURCE_PATH.match(m.group('path'))
        if r is None:
            raise ValueError("Incorrect format")

        series = self.series_map[r.group('gpid')]

        called = strptime(m.group('datetime'))
        if m.group('status_code') == '500':
            called -= datetime.timedelta(seconds=3)  # Apply timeout

        if series.last_called:
            delay = (called - series.last_called).total_seconds()
            if delay > 300:  # Change
                delay = numpy.nan
            elif delay < 0:  # Not periodic
                return None
            series.last_called += HttpResourceIntervalParser.default_interval
        else:
            delay = 0
            series.last_called = called

        series.append(series.last_called, delay)

        return m


class HttpAccessLogParser(LogParser):
    def parse_file(self, filepath):
        filename = os.path.basename(filepath)
        dimension = filename.strip('.log').split('_')

        parser = HttpResourceIntervalParser(dimension)

        with open(filepath, 'r') as f:
            ln = 0
            for l in f.readlines():  # Some lines are ended with CR without LB
                ln += 1
                try:
                    parser.feed(l)
                except ValueError:
                    logger.warn("Line has incorrect format on %s(%d): %s", filepath, ln, l)
                except BaseException as e:
                    sys.stderr.write('ERROR: %s\n%s\n' % (e.message, traceback.format_exc()))
                    logger.error("Parse error on %s(%d): %s", filepath, ln, e.message)

        return parser.series_map.values()

