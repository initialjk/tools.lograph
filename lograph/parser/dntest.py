import datetime
import logging
import os
import re

from lograph.parse import LogParser, Series, UnsupportedLogError
from lograph.parser.erftest import RE_PATTERN_ERFTEST_TIME

logger = logging.getLogger(__name__)

RE_PATTERN_DNTEST_TIME = RE_PATTERN_ERFTEST_TIME
RE_PATTERN_DNTEST_FINISHED = re.compile(
    r'^(?P<time>\d+-\d+-\d+\s+\d+:\d+:\d+)\s*[(](?P<bps>\d+[.]?\d*)\s*(?P<unit>[/A-Za-z]+)[)]\s*-\s*\xe2\x80\x9c(?P<file>.*)\xe2\x80\x9d\s*(?P<status>\S+)\s*\[(?P<trans>\d+)/(?P<size>\d+)\].*')
RE_PATTERN_DNTEST_RETRY = re.compile(
    r'^(?P<detail>(?P<time>\d+-\d+-\d+\s+\d+:\d+:\d+)\s*[(](?P<bps>\d+[.]?\d*)\s*(?P<unit>[/A-Za-z]+)[)]\s*-\s*(?P<reason>.*[.]))?\s*Retrying.')

def strptime(time):
    return  datetime.datetime.strptime(time, '%Y-%m-%d %H:%M:%S')


def normalize_bps(bps, unit):
    bps = float(bps or 0)
    if unit == 'KB/s':
        bps *= 1024
    elif unit == 'MB/s':
        bps *= (1024 * 1024)
    elif unit == 'GB/s':
        bps *= (1024 * 1024 * 1024)
    elif unit == 'B/s':
        pass  # Do nothing
    elif unit:
        logger.warn("Invalid unit format '%s'. Use figure %s by literal", unit, bps)
    return bps


class DownloadTestLogParser(LogParser):
    def parse_file(self, filepath):
        filename = os.path.basename(filepath)
        if not filename.startswith('dntest'):
            raise UnsupportedLogError()

        dimension = filename.strip('.log').split('_')
        series_error = Series(dimension + ['retry'], unit='count', is_continuous=False)
        series_map = {}

        def get_series_for(size):
            try:
                return series_map[size]
            except KeyError:
                series_map[size] = s = Series(dimension + ['speed', size], unit='bps')
                return s

        with open(filepath, 'r') as f:
            test_time = last_bps = None

            for l in f.readlines():
                m = RE_PATTERN_DNTEST_TIME.match(l)
                if m:
                    test_time = datetime.datetime.fromtimestamp(long(m.group('timestamp')))
                    continue

                m = RE_PATTERN_DNTEST_RETRY.match(l)
                if m:
                    event_time = test_time
                    if m.group('detail'):
                        event_time = strptime(m.group('time'))
                        last_bps = normalize_bps(m.group('bps'), m.group('unit'))
                    series_error.append(event_time, 1)
                    continue

                m = RE_PATTERN_DNTEST_FINISHED.match(l)
                if m:
                    bps = normalize_bps(m.group('bps'), m.group('unit'))
                    event_time = strptime(m.group('time'))

                    series = get_series_for(m.group('size'))
                    series.append(event_time, min(last_bps, bps) if last_bps else bps)
                    last_bps = None

        return series_map.values() + [series_error,]
