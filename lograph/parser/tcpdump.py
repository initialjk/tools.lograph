import datetime
import logging
import os
import re
import sys
import traceback

import geoip2.database

from lograph.parse import LogParser, SeriesMap

logger = logging.getLogger(__name__)

reader = geoip2.database.Reader(os.path.join(os.path.dirname(os.path.abspath(__file__)), './GeoLite2-City.mmdb'))

RE_PATTERN_TCPDUMP_HEADER = re.compile(
    r'^(?P<time>\S+)\s+IP\s+'
    r'(?P<src_host>\S+)[.](?P<src_port>[-/\w]+)\s+(?P<direction>[<>])\s+(?P<dst_host>\S+)[.](?P<dst_port>[-/\w]+)\s*:\s+'
    r'Flags\s+\[(?P<flags>\S+)\]((?P<delim>,\s*)'
    r'((seq\s+(?P<seq>[:\d]+))'
    r'|(ack\s+(?P<ack>\d+))'
    r'|(win\s+(?P<win>\d+))'
    r'|(length\s+(?P<length>\d+))'
    r'|(options\s+\[(?P<options>.+)\]))'
    r')*$')


def strptime(timestr):
    return datetime.datetime.strptime(timestr, '%H:%M:%S.%f').time()

class TcpDumpGroupCountParser(object):
    default_interval = 60
    def __init__(self, interval = None):
        self.interval = interval or self.default_interval
        self.sample_map_dict = dict()
        self.last_time = datetime.datetime.utcnow()
        self.day = datetime.date.today()

    def feed(self, l):
        m = RE_PATTERN_TCPDUMP_HEADER.match(l)
        if m is None:
            raise ValueError("Incorrect format")

        if m.group('src_host').startswith('10.0.98'):
            item_name = 's2c:' + m.group('src_port')
        elif m.group('dst_host').startswith('10.0.98'):
            item_name = 's2c:' + m.group('src_port')
        else:
            item_name = 'stray'

        flags = m.group('flags')
        if 'F' in flags:
            item_name += ":FIN"
        elif 'R' in flags:
            item_name += ":RST"

        try:
            sample_map = self.sample_map_dict[item_name]
        except KeyError:
            sample_map = self.sample_map_dict[item_name] = dict()

        time = strptime(m.group('time'))
        time = time.replace(second=(time.second/self.interval)*self.interval, microsecond=0)
        dt = datetime.datetime.combine(self.day, time)

        try:
            sample_map[dt] += 1
        except KeyError:
            sample_map[dt] = 1

        return m


class TcpDumpRegionalGroupCountParser(object):
    default_interval = 60
    def __init__(self, interval = None):
        self.interval = interval or self.default_interval
        self.sample_map_dict = dict()
        self.last_time = datetime.datetime.utcnow()
        self.day = datetime.date.today()

    def feed(self, l):
        m = RE_PATTERN_TCPDUMP_HEADER.match(l)
        if m is None:
            raise ValueError("Incorrect format")

        if not m.group('src_host').startswith('10.0.98'):
            return None

        item_name = "%s:%s" % (m.group('src_port'), reader.city(m.group('dst_host')).city)

        try:
            sample_map = self.sample_map_dict[item_name]
        except KeyError:
            sample_map = self.sample_map_dict[item_name] = dict()

        time = strptime(m.group('time'))
        time = time.replace(second=(time.second/self.interval)*self.interval, microsecond=0)
        dt = datetime.datetime.combine(self.day, time)

        try:
            sample_map[dt] += 1
        except KeyError:
            sample_map[dt] = 1

        return m


class TcpDumpLogParser(LogParser):
    def parse_file(self, filepath):
        filename = os.path.basename(filepath)
        dimension = filename.strip('.log').split('_')

        parser = TcpDumpGroupCountParser()

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

        series_map = SeriesMap(dimension, unit='count')
        for k, v in parser.sample_map_dict.iteritems():
            series = series_map[k]
            for dt, sample in v.iteritems():
                series.append(dt, sample)

        return series_map.values()

