import datetime
import logging
import os
import re

import sys
import traceback

from lograph.parse import UnsupportedLogError, LogParser, Series

logger = logging.getLogger(__name__)

RE_PATTERN_ERFTEST_TIME = re.compile(
    r'^\[(?P<timestamp>\d+)\]\s+(?P<dow>[A-Za-z]{3})\s+(?P<month>[A-Za-z]{3})\s+(?P<date>\d+)\s+(?P<time>\d\d:\d\d:\d\d)\s+(?P<timezone>[A-Z]{3})\s+(?P<year>\d+)$')
RE_PATTERN_ERFTEST_BANDWIDTH = re.compile(
    r'^\[\s*(?P<id>\d+)]\s*(?P<start>\d+\.\d+)\s*-\s*(?P<finish>\d+\.\d+)\s*sec\s+(?P<sent>\d+\.?\d*)\s*(?P<sent_unit>[A-Z]Bytes)\s+(?P<bps>\d+\.?\d*)\s*(?P<bps_unit>[A-Z]bits/sec)')


class StraySample(object):
    def __init__(self, finish_time, value, line):
        self.finish_time = finish_time
        self.value = value
        self.line = line


def normalize_bps(bps, unit):
    bps = float(bps or 0) * 8
    if unit == 'Kbits/sec':
        bps *= 1024
    elif unit == 'Mbits/sec':
        bps *= (1024 * 1024)
    elif unit == 'Gbits/sec':
        bps *= (1024 * 1024 * 1024)
    elif unit:
        logger.warn("Invalid unit format '%s'. Use figure %s by literal", unit, bps)
    return bps


class ErfTestTolerantParser(object):
    class Sample(object):
        def __init__(self, bps, end_time):
            self.bps = bps
            self.end_time = end_time

    def __init__(self, base_dimension):
        self.series = Series(base_dimension + ['speed'], unit='bps')
        self.time_index = None
        self.candidates = []

    def get_series_list(self):
        return (self.series,)

    def mark_time(self, time):
        self.time_index = time

    def add_candidates(self, bps, end_time):
        if self.candidates:
            self.candidates.append(self.Sample(bps, end_time))
        else:
            self.candidates = [self.Sample(bps, end_time),]

    def flush_candidates(self):
        if not self.candidates:
            return 0

        if not self.time_index:
            raise ValueError("No time index for candidates.", self.candidates)

        # This is most close point to estimate
        count = len(self.candidates)
        if count == 1:
            self.series.append(self.time_index, self.candidates[0].bps)
        elif count > 1:
            candidates = sorted(self.candidates, key=lambda s: s.end_time)
            self.series.append(self.time_index, candidates[0].bps)

            for s in candidates[1:]:
                approximated_position = self.time_index - datetime.timedelta(seconds=s.end_time)
                logger.info("Insert stray sample to approximated position: %s", approximated_position.isoformat())
                self.series.append(approximated_position, s.bps)
        self.time_index = None
        self.candidates = []
        return count

    def feed(self, l):
        m = RE_PATTERN_ERFTEST_TIME.match(l)
        if m:
            try:
                if self.flush_candidates() == 0 and self.time_index:
                    logger.info("There is no samples for %s", self.time_index.isoformat())
            finally:
                self.mark_time(datetime.datetime.fromtimestamp(long(m.group('timestamp'))))
            return m

        m = RE_PATTERN_ERFTEST_BANDWIDTH.match(l)
        if m:
            bps = normalize_bps(m.group('bps'), m.group('bps_unit'))
            end_time = float(m.group('finish') or 0)
            self.add_candidates(bps, end_time)
            return m

        return None


class ErfTestParser(ErfTestTolerantParser):
    def __init__(self, base_dimension):
        super(ErfTestParser, self).__init__(base_dimension)
        self.error_series = Series(base_dimension + ['error'], unit='count', is_continuous=False)
        self.series.subordinates_series = [self.error_series,]

    def get_series_list(self):
        return (self.series, self.error_series)

    def flush_candidates(self):
        if not self.time_index:
            if self.candidates:
                raise ValueError("No time index for candidates.", self.candidates)
            return 0

        # This is most close point to estimate
        count = len(self.candidates)
        if count == 0:
            self.error_series.append(self.time_index, 1)
        elif count == 1:
            self.series.append(self.time_index, self.candidates[0].bps)
        elif count > 1:
            candidates = sorted(self.candidates, key=lambda s: s.end_time)
            self.series.append(self.time_index, candidates[0].bps)
        self.time_index = None
        self.candidates = []
        return count


class ErfTestLogParser(LogParser):
    def __init__(self, approximate_stray_samples=False):
        self.approximate_stray_samples = approximate_stray_samples

    def parse_file(self, filepath):
        filename = os.path.basename(filepath)
        if not filename.startswith('erftest'):
            raise UnsupportedLogError()

        parser = ErfTestParser(filename.strip('.log').split('_') + ['Bandwidth'])
        with open(filepath, 'r') as f:
            for n, l in enumerate(f.readlines()):
                try:
                    parser.feed(l)
                except BaseException as e:
                    sys.stderr.write('ERROR: %s\n%s\n' % (e.message, traceback.format_exc()))
                    logger.error("Parse error on %s(%d): %s", filepath, n, e.message)

        return parser.get_series_list()
