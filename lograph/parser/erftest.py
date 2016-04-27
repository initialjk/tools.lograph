import datetime
import logging
import os
import re

from lograph.parse import UnsupportedLogError, LogParser, Series

logger = logging.getLogger(__name__)

RE_PATTERN_ERFTEST_TIME = re.compile(
    r'^\[(?P<timestamp>\d+)\]\s+(?P<dow>[A-Za-z]{3})\s+(?P<month>[A-Za-z]{3})\s+(?P<date>\d+)\s+(?P<time>\d\d:\d\d:\d\d)\s+(?P<timezone>[A-Z]{3})\s+(?P<year>\d+)$')
RE_PATTERN_ERFTEST_BANDWIDTH = re.compile(
    r'^\[\s*(?P<id>\d+)]\s*(?P<start>\d+\.\d+)-(?P<finish>\d+\.\d+)\s*sec\s+(?P<sent>\d+)\s*(?P<sent_unit>[A-Z]Bytes)\s+(?P<bps>\d+)\s*(?P<bps_unit>[A-Z]bits/sec)')


class ErfTestLogParser(LogParser):
    def parse_file(self, filepath):
        filename = os.path.basename(filepath)
        if not filename.startswith('erftest'):
            raise UnsupportedLogError()

        dimension = filename.strip('.log').split('_') + ['Bandwidth']
        series = Series(dimension, unit='bps')

        with open(filepath, 'r') as f:
            time_index = None

            for l in f.readlines():
                if time_index:
                    m = RE_PATTERN_ERFTEST_BANDWIDTH.match(l)
                    if m:
                        bps = long(m.group('bps'))
                        unit = m.group('bps_unit')
                        if unit == 'Kbits/sec':
                            bps *= 1024
                        elif unit == 'Mbits/sec':
                            bps *= (1024 * 1024)
                        elif unit == 'Gbits/sec':
                            bps *= (1024 * 1024 * 1024)
                        elif unit:
                            logger.warn("Invalid unit format '%s'. Use figure %s by literal", unit, bps)

                        series.append(time_index, bps)
                        time_index = None
                    else:
                        m = RE_PATTERN_ERFTEST_TIME.match(l)
                        if m:
                            logger.warn("Can't determine time for record which will be ignored: %s", l)
                else:
                    m = RE_PATTERN_ERFTEST_TIME.match(l)
                    if m:
                        time_index = datetime.datetime.fromtimestamp(long(m.group('timestamp')))
        return (series,)
