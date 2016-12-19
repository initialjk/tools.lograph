import datetime
import logging
import os
import re
import sys
import traceback

import geoip2.database
import itertools
from geoip2.errors import AddressNotFoundError

from lograph.parse import LogParser, SeriesMap, SlidingCache

logger = logging.getLogger(__name__)

reader = geoip2.database.Reader(os.path.join(os.path.dirname(os.path.abspath(__file__)), './GeoLite2-City.mmdb'))

RE_PATTERN_TCPDUMP_HEADER = re.compile(
    r'^(?P<time>\S+)\s+IP\s+'
    r'(?P<src_host>\d+\.\d+\.\d+\.\d+)\.(?P<src_port>[-./\w]+)\s+(?P<direction>[<>])\s+'
    r'(?P<dst_host>\d+\.\d+\.\d+\.\d+)\.(?P<dst_port>[-./\w]+)\s*:\s+'
    r'Flags\s+\[(?P<flags>\S+)\]((?P<delim>,\s*)'
    r'((seq\s+(?P<seq>[:\d]+))'
    r'|(ack\s+(?P<ack>\d+))'
    r'|(win\s+(?P<win>\d+))'
    r'|(urg\s+(?P<urg>\d+))'
    r'|(length\s+(?P<length>\d+)(\s+update\+(?P<updates>(\s+(?P<update>\S+))+))?)'
    r'|(options\s+\[(?P<options>.+)[\]>])'
    r'))*$')


def strptime(timestr):
    return datetime.datetime.strptime(timestr, '%H:%M:%S.%f').time()


def estimate_time_from_file(filename):
    for t in re.findall(r"[\w']+", str(filename)):
        try:
            return datetime.datetime.strptime(t, "%Y%m%d")
        except ValueError:
            pass
    return None


class TcpDumpGroupCountParser(object):
    TRANSMIT_ID_ENTRIES = ['src_host', 'src_port', 'dst_host', 'dst_port', 'seq']
    RETRANSMIT_CACHE_SIZE = 600
    default_interval = 60

    def __init__(self,
                 interval=None,
                 start_date=datetime.date.today(),
                 func_is_server_addr=lambda x: x and x.startswith('10.0.')):
        self.interval = interval or self.default_interval
        self.is_server_addr = func_is_server_addr
        self.sample_map_dict = dict()
        self.last_time = datetime.time()
        self.retransmit_cache = SlidingCache(self.RETRANSMIT_CACHE_SIZE)
        self.day = start_date

    def feed(self, l):
        m = RE_PATTERN_TCPDUMP_HEADER.match(l)
        if m is None:
            raise ValueError("Incorrect format")

        transmit_id = ':'.join(m.group(x) for x in self.TRANSMIT_ID_ENTRIES)
        if self.retransmit_cache.hit(transmit_id):
            logger.debug("Found retransmission record. This will be ignored: %s", l)
            return m

        if self.is_server_addr(m.group('src_host')):
            item_name = m.group('src_port')
        elif self.is_server_addr(m.group('dst_host')):
            item_name = m.group('dst_port')
        else:
            return None

        if item_name == 'pnbs':
            item_name = '6124'

        # Temporary exclusive for speed
        if item_name != '6124':  # , '27021', '27022']:
            return None

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
            host_port = m.group('dst_port')
            service_port = m.group('src_port')
        elif self.is_server_addr(m.group('dst_host')):
            host = m.group('src_host')
            host_port = m.group('src_port')
            service_port = m.group('dst_port')
            # return None
        else:
            return None

        transmit_id = "%s:%s" % (host, host_port)
        if self.retransmit_cache.hit(transmit_id):
            return m

        if service_port == 'pnbs':
            service_port = '6124'

        # Temporary exclusive for speed
        if service_port != '6124':
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
        if time.hour < self.last_time.hour:
            if self.last_time.hour - time.hour > 20:
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
    def __init__(self, start_date=None):
        self.start_date = start_date

    def parse_file(self, filepath):
        filename = os.path.basename(filepath)
        start_date = self.start_date or estimate_time_from_file(filename) or datetime.date.today()
        parser = TcpDumpRegionalGroupCountParser(start_date=start_date)

        with open(filepath, 'r') as f:
            ln = 0
            for l in f.readlines():  # Some lines are ended with CR without LB
                ln += 1
                try:
                    parser.feed(l)
                except KeyboardInterrupt:
                    raise
                except ValueError:
                    logger.warn("Line has incorrect format on %s(%d): %s", filepath, ln, l and l.rstrip('\n'))
                except BaseException as e:
                    sys.stderr.write('ERROR: %s\n%s\n' % (e.message, traceback.format_exc()))
                    logger.error("Parse error on %s(%d): %s", filepath, ln, e.message)

        series_map = SeriesMap([], unit='count')
        for k, v in parser.sample_map_dict.iteritems():
            series = series_map[k]
            for dt, sample in v.iteritems():
                if sample:
                    series.append(dt, sample)

        return series_map.values()

