import datetime
import logging
import os
import re

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


class ErfTestLogParser(LogParser):
    def parse_file(self, filepath):
        filename = os.path.basename(filepath)
        if not filename.startswith('erftest'):
            raise UnsupportedLogError()

        dimension = filename.strip('.log').split('_') + ['Bandwidth']
        series = Series(dimension, unit='bps')

        with open(filepath, 'r') as f:
            time_index = None
            stray_samples = []

            for n, l in enumerate(f.readlines()):
                m = RE_PATTERN_ERFTEST_TIME.match(l)
                if m:
                    timestamp = datetime.datetime.fromtimestamp(long(m.group('timestamp')))
                    if time_index:
                        logger.warn("Can't find sample from %s to %s: %s(%d)", time_index.isoformat(), timestamp.isoformat(), filepath, n)
                    time_index = timestamp

                    # This is most close point to estimate
                    for s in stray_samples:
                        approximated_position = time_index - datetime.timedelta(seconds=s.finish_time)
                        logger.info("Insert stray sample to approximated position %s from line %d ", approximated_position.isoformat(), s.line)
                        series.append(approximated_position, s.value)
                    stray_samples = []

                    continue

                m = RE_PATTERN_ERFTEST_BANDWIDTH.match(l)
                if m:
                    bps = float(m.group('bps'))
                    unit = m.group('bps_unit')
                    if unit == 'Kbits/sec':
                        bps *= 1024
                    elif unit == 'Mbits/sec':
                        bps *= (1024 * 1024)
                    elif unit == 'Gbits/sec':
                        bps *= (1024 * 1024 * 1024)
                    elif unit:
                        logger.warn("Invalid unit format '%s'. Use figure %s by literal", unit, bps)

                    if time_index:
                        series.append(time_index, bps)
                        time_index = None
                    else:
                        # Insert this to offset of next known point later
                        stray_samples.append(StraySample(float(m.group('finish')), bps, n))
                        logger.warn("Sample is not on valid location. : %s at %s(%d)", l, filepath, n)

        return (series,)
