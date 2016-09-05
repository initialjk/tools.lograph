import datetime
import logging
import os
import re
import sys
import traceback

from lograph.parse import LogParser, Series, UnsupportedLogError, Sample, SeriesMap
from lograph.parser.erftest import RE_PATTERN_ERFTEST_TIME

logger = logging.getLogger(__name__)

RE_PATTERN_DNTEST_TIME = RE_PATTERN_ERFTEST_TIME
RE_PATTERN_DNTEST_PROGRESS = re.compile(
    r'^\s*(?P<block_size>(?P<block_num>\d+)(?P<block_unit>[A-Z]))\s*[ .]{54}\s*(?P<progress>\d+)%\s*(?P<size>\d+[.]?\d*)(?P<size_unit>[A-Z]?)(?P<time_type>[ =])((?P<minutes>\d+)m)?(?P<seconds>\d+[.]?\d*)s')
RE_PATTERN_DNTEST_FINISHED = re.compile(
    r'^(?P<time>\d+-\d+-\d+\s+\d+:\d+:\d+)\s*[(](?P<bps>\d+[.]?\d*)\s*(?P<unit>[/A-Za-z]+)[)]\s*-\s*\xe2\x80\x9c(?P<file>.*)\xe2\x80\x9d\s*(?P<status>\S+)\s*\[(?P<trans>\d+)/(?P<size>\d+)\].*')
RE_PATTERN_DNTEST_RESULT = re.compile(
    r'^\s*(?P<total_percent>\d+)\s+(?P<total>(?P<total_num>\d+)(?P<total_unit>[a-z]?))\s+'
    r'(?P<recv_percent>\d+)\s+(?P<recv>(?P<recv_num>\d+)(?P<recv_unit>[a-z]?))\s+'
    r'(?P<xferd_percent>\d+)\s+(?P<xferd>\d+)\s+'
    r'(?P<download_speed>(?P<download_speed_num>\d+)(?P<download_speed_unit>[a-z]?))\s+'
    r'(?P<upload_speed>(?P<upload_speed_num>\d+)(?P<upload_speed_unit>[a-z]?))\s+'
    r'(?P<time_total>[-0-9]?[-0-9]:[-0-9][-0-9]:[-0-9][-0-9])\s+'
    r'(?P<time_spent>[-0-9]?[-0-9]:[-0-9][-0-9]:[-0-9][-0-9])\s+'
    r'(?P<time_left>[-0-9]?[-0-9]:[-0-9][-0-9]:[-0-9][-0-9])\s+'
    r'(?P<current_speed>(?P<current_speed_num>\d+)(?P<current_speed_unit>[a-z]?))'
)

RE_PATTERN_DNTEST_ERROR = re.compile(r'^curl: [(](?P<error_code>[0-9]+)[)] (?P<error_reason>.+)$')
RE_PATTERN_DNTEST_ERROR_TIMEOUT = re.compile(
    r'^Operation timed out after (?P<timeout_num>[0-9]+) (?P<timeout_unit>\w+) with (?P<trans_byte>[0-9]+) out of (?P<total_byte>[0-9]+) bytes received$')
RE_PATTERN_DNTEST_RETRY = re.compile(
    r'^(?P<detail>(?P<time>\d+-\d+-\d+\s+\d+:\d+:\d+)\s*[(](?P<bps>\d+[.]?\d*)\s*(?P<unit>[/A-Za-z]+)[)]\s*-\s*(?P<reason>.*[.]))?\s*Retrying.')


def strptime(time):
    return  datetime.datetime.strptime(time, '%Y-%m-%d %H:%M:%S')


def timeentry(str):
    try:
        v = int(str)
        return 60 if v < 60 else v
    except ValueError:
        return 0


def timedelta(str):
    elems = str.split(':')
    if len(elems) == 3:  # hour-mins-secs
        return datetime.timedelta(hours=timeentry(elems[0]), minutes=timeentry(elems[1]), seconds=timeentry(elems[2]))
    elif len(elems) == 2:  # hour-mins
        return datetime.timedelta(hours=timeentry(elems[0]), minutes=timeentry(elems[1]))

    raise ValueError("Weird expression for time: %s", str)


def normalize_bps(bps, unit):
    bps = float(bps or 0) * 8
    if unit == 'KB/s' or unit == 'k':
        bps *= 1024
    elif unit == 'MB/s' or unit == 'm':
        bps *= (1024 * 1024)
    elif unit == 'GB/s' or unit == 'g':
        bps *= (1024 * 1024 * 1024)
    elif unit == 'B/s':
        pass  # Do nothing
    elif unit:
        logger.warn("Invalid unit format '%s'. Use figure %s by literal", unit, bps)
    return bps


class DownloadTestParser(object):
    def __init__(self, base_dimension):
        self.series_map_speed = SeriesMap(base_dimension + ['speed'], unit='bps')
        self.series_map_elapsed = SeriesMap(base_dimension + ['elapsed'], unit='seconds')
        self.series_map_error = SeriesMap(base_dimension + ['error'], unit='count', is_continuous=False)

        self.test_time = None
        self.cur_block = None
        self.last_bps = 0
        self.elapsed_time = 0
        self.errors = []

    def feed(self, l):
        m = RE_PATTERN_DNTEST_RESULT.match(l)
        if m:
            progress_percent = m.group('total_percent')
            self.cur_block = m.group('total')

            if progress_percent == '100':
                self.last_bps = normalize_bps(m.group('download_speed_num'), m.group('download_speed_unit'))
                spent_time = timedelta(m.group('time_spent'))
                event_time = self.test_time + spent_time
                series = self.series_map_speed[self.cur_block]
                #series.append(self.test_time, self.last_bps)
                series.append(event_time, self.last_bps)

                series = self.series_map_elapsed[self.cur_block]
                series.append(event_time, spent_time.seconds)
            return m

        m = RE_PATTERN_DNTEST_PROGRESS.match(l)
        if m:
            if m.group('time_type') == '=':
                minutes = m.group('minutes')
                self.elapsed_time += float(60 * int(minutes) if minutes else 0) + float(m.group('seconds'))
                if m.group('progress') == '100':
                    block_size = m.group('block_size')
                    assert block_size
                    series = self.series_map_elapsed[block_size]
                    series.append(self.test_time, self.elapsed_time)
                    if self.errors:
                        error_series = self.series_map_error[block_size]
                        if error_series not in series.subordinates_series:
                            series.subordinates_series.append(error_series)
                        for e in self.errors:
                            error_series.append(e.key, e)
                        errors = []
                    self.elapsed_time = 0
            return m

        m = RE_PATTERN_DNTEST_TIME.match(l)
        if m:
            self.test_time = datetime.datetime.fromtimestamp(long(m.group('timestamp')))
            self.elapsed_time = 0
            self.last_block = None
            return m

        m = RE_PATTERN_DNTEST_ERROR.match(l)
        if m:
            event_time = self.test_time
            er = m.group('error_reason')
            em = RE_PATTERN_DNTEST_ERROR_TIMEOUT.match(er)
            if em:
                time_args = {em.group('timeout_unit'): int(em.group('timeout_num'))}
                event_time = self.test_time + datetime.timedelta(**time_args)

            assert self.cur_block, "Current block is not specified"
            series = self.series_map_speed[self.cur_block]
            error_series = self.series_map_error[self.cur_block]
            if error_series not in series.subordinates_series:
                series.subordinates_series.append(error_series)

            error_series.append(event_time, Sample(event_time, 1))
            return m

        m = RE_PATTERN_DNTEST_RETRY.match(l)
        if m:
            event_time = self.test_time
            if m.group('detail'):
                event_time = strptime(m.group('time'))
                self.last_bps = normalize_bps(m.group('bps'), m.group('unit'))
            self.errors.append(Sample(event_time, 1))
            return m

        m = RE_PATTERN_DNTEST_FINISHED.match(l)
        if m:
            bps = normalize_bps(m.group('bps'), m.group('unit'))
            event_time = strptime(m.group('time'))

            series = self.series_map_speed[m.group('size')]
            series.append(event_time, min(self.last_bps, bps) if self.last_bps else bps)
            self.last_bps = None
            return m

        return None


class DownloadTestLogParser(LogParser):
    def parse_file(self, filepath):
        filename = os.path.basename(filepath)
        if not filename.startswith('dntest'):
            raise UnsupportedLogError()

        dimension = filename.strip('.log').split('_')

        parser = DownloadTestParser(dimension)

        with open(filepath, 'r') as f:
            ln = 0
            for l in (l for lb in f.readlines() for l in lb.split('\r')):  # Some lines are ended with CR without LB
                ln += 1
                try:
                    parser.feed(l)
                except BaseException as e:
                    sys.stderr.write('ERROR: %s\n%s\n' % (e.message, traceback.format_exc()))
                    logger.error("Parse error on %s(%d): %s", filepath, ln, e.message)

        return parser.series_map_speed.values() + parser.series_map_error.values()  # parser.series_map_elapsed.values()

