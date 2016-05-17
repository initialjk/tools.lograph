import datetime
import logging
import os
import re

from lograph.parse import LogParser, Series, UnsupportedLogError, Sample
from lograph.parser.erftest import RE_PATTERN_ERFTEST_TIME

logger = logging.getLogger(__name__)

RE_PATTERN_DNTEST_TIME = RE_PATTERN_ERFTEST_TIME
RE_PATTERN_DNTEST_PROGRESS = re.compile(
    r'^\s*(?P<block_size>(?P<block_num>\d+)(?P<block_unit>[A-Z]))\s*[ .]{54}\s*(?P<progress>\d+)%\s*(?P<size>\d+[.]?\d*)(?P<size_unit>[A-Z]?)(?P<time_type>[ =])((?P<minutes>\d+)m)?(?P<seconds>\d+[.]?\d*)s')
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


class SeriesMap(dict):
    def __init__(self, dimension, **kwargs):
        super(SeriesMap, self).__init__()
        self.base_dimension = dimension
        self.kwargs = kwargs

    def __getitem__(self, item):
        series = self.get(item)
        if series is None:
            self[item] = series = Series(self.base_dimension + [item,], **self.kwargs)
        return series


class DownloadTestLogParser(LogParser):
    def parse_file(self, filepath):
        filename = os.path.basename(filepath)
        if not filename.startswith('dntest'):
            raise UnsupportedLogError()

        dimension = filename.strip('.log').split('_')
        series_map_speed = SeriesMap(dimension + ['speed'], unit='bps')
        series_map_elapsed = SeriesMap(dimension + ['elapsed'], unit='seconds')
        series_map_error = SeriesMap(dimension + ['error'], unit='count', is_continuous=False)

        with open(filepath, 'r') as f:
            test_time = last_bps = None
            elapsed_time = 0
            errors = []

            for l in f.readlines():
                m = RE_PATTERN_DNTEST_PROGRESS.match(l)
                if m:
                    if m.group('time_type') == '=':
                        minutes = m.group('minutes')
                        elapsed_time += float(60 * int(minutes) if minutes else 0) + float(m.group('seconds'))
                        if m.group('progress') == '100':
                            block_size = m.group('block_size')
                            assert block_size
                            series = series_map_elapsed[block_size]
                            series.append(test_time, elapsed_time)
                            if errors:
                                error_series = series_map_error[block_size]
                                if error_series not in series.subordinates_series:
                                    series.subordinates_series.append(error_series)
                                for e in errors:
                                    error_series.append(e.key, e)
                                errors = []
                            elapsed_time = 0
                    continue

                m = RE_PATTERN_DNTEST_TIME.match(l)
                if m:
                    test_time = datetime.datetime.fromtimestamp(long(m.group('timestamp')))
                    elapsed_time = 0
                    continue

                m = RE_PATTERN_DNTEST_RETRY.match(l)
                if m:
                    event_time = test_time
                    if m.group('detail'):
                        event_time = strptime(m.group('time'))
                        last_bps = normalize_bps(m.group('bps'), m.group('unit'))
                    errors.append(Sample(event_time, 1))
                    continue

                m = RE_PATTERN_DNTEST_FINISHED.match(l)
                if m:
                    bps = normalize_bps(m.group('bps'), m.group('unit'))
                    event_time = strptime(m.group('time'))

                    series = series_map_speed[m.group('size')]
                    series.append(event_time, min(last_bps, bps) if last_bps else bps)
                    last_bps = None
        return series_map_speed.values() + series_map_elapsed.values() + series_map_error.values()
