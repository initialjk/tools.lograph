import datetime
import logging
import os
import re
import sys
import traceback

import geoip2.database
import itertools
from geoip2.errors import AddressNotFoundError

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
    def __init__(self, interval = None, func_is_server_addr=lambda x: x and x.startswith('10.0.')):
        self.interval = interval or self.default_interval
        self.is_server_addr = func_is_server_addr
        self.sample_map_dict = dict()
        self.last_time = datetime.time()
        self.day = datetime.datetime(2016, 8, 29)

    def feed(self, l):
        m = RE_PATTERN_TCPDUMP_HEADER.match(l)
        if m is None:
            raise ValueError("Incorrect format")

        if self.is_server_addr(m.group('src_host')):
            item_name = m.group('src_port')
        elif self.is_server_addr(m.group('dst_host')):
            item_name = m.group('dst_port')
        else:
            return None

        if item_name == 'pnbs':
            item_name = '6124'

        flags = m.group('flags')
        if 'F' in flags:
            item_name += ":FIN"
        elif 'R' in flags:
            item_name += ":RST"
        else:
            return None

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


class TcpDumpRegionalGroupCountParser(TcpDumpGroupCountParser):
    def feed(self, l):
        m = RE_PATTERN_TCPDUMP_HEADER.match(l)
        if m is None:
            raise ValueError("Incorrect format")

        if self.is_server_addr(m.group('src_host')):
            host = m.group('dst_host')
            port = m.group('src_port')
        elif self.is_server_addr(m.group('dst_host')):
            host = m.group('src_host')
            port = m.group('dst_port')
        else:
            return None

        if port == 'pnbs':
            port = '6124'
        elif port == 'ssh':
            return None
        else:
            return None

        flags = m.group('flags')
        if 'F' in flags:
            flag = "FIN"
            return None
        elif 'R' in flags:
            flag = "RST"
        elif 'S' in flags:
            flag = "SYN"
            return None
        else:
            return None

        try:
            city = reader.city(host).country.name or 'unknown'
        except AddressNotFoundError:
            city = "unknown"

        #item_name = ':'.join((port, flag, city,))
        item_name = city

        try:
            sample_map = self.sample_map_dict[item_name]
        except KeyError:
            sample_map = self.sample_map_dict[item_name] = dict()

        time = strptime(m.group('time'))
        if time < self.last_time:
            self.day += datetime.timedelta(days=1)
        self.last_time = time
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
        parser = TcpDumpRegionalGroupCountParser()

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

        series_map = SeriesMap([], unit='count')
        for k, v in parser.sample_map_dict.iteritems():
            series = series_map[k]
            for dt, sample in v.iteritems():
                series.append(dt, sample)

        series_to_return = sorted(series_map.values(), key=lambda s: len(s.samples), reverse=True)
        series_to_return = itertools.islice(series_to_return, 15)
        return series_to_return

