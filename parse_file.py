import datetime
import logging
import os
import re

logger = logging.getLogger(__name__)


class Sample:
    def __init__(self, key, value):
        self.key = key
        self.value = value


class Series:
    def __init__(self, dimension, unit, priority=0, is_continuous=True):
        self.unit = unit
        self.priority = priority
        self.is_continuous = is_continuous
        try:
            self.dimension = tuple(dimension)
        except TypeError:
            self.dimension = (unicode(dimension), )
        self.samples = []

    def append(self, key, value):
        self.samples.append(Sample(key, value))

    def keys(self):
        return (r.key for r in self.samples)

    def values(self):
        return (r.value for r in self.samples)

    def sort(self):
        self.samples.sort(key=lambda x: x.key)

    def __add__(self, other):
        assert other.dimension == self.dimension and other.unit == self.unit
        self.samples.append(other.records)

    def __len__(self):
        return len(self.samples)

    def __unicode__(self):
        return "%s (%d records)" % (unicode(self.dimension), len(self))


RE_PATTERN_ERFTEST_TIME = re.compile(r'^\[(?P<timestamp>\d+)\]\s+(?P<dow>[A-Za-z]{3})\s+(?P<month>[A-Za-z]{3})\s+(?P<date>\d+)\s+(?P<time>\d\d:\d\d:\d\d)\s+(?P<timezone>[A-Z]{3})\s+(?P<year>\d+)$')
RE_PATTERN_ERFTEST_BANDWIDTH = re.compile(r'^\[\s*(?P<id>\d+)]\s*(?P<start>\d+\.\d+)-(?P<finish>\d+\.\d+)\s*sec\s+(?P<sent>\d+)\s*(?P<sent_unit>[A-Z]Bytes)\s+(?P<bps>\d+)\s*(?P<bps_unit>[A-Z]bits/sec)')

def parse_erftest_file(file):
    dimension = os.path.basename(file).strip('.log').split('_')
    dimension.append('Bandwidth')
    series = Series(dimension, 'bps')
    with open(file,'r') as f:
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
                        logger.warn("Can't determine time for record '%s'. It will be ignored", l)
            else:
                m = RE_PATTERN_ERFTEST_TIME.match(l)
                if m:
                    time_index = datetime.datetime.fromtimestamp(long(m.group('timestamp')))
    return (series,)

RE_PATTERN_PINGTEST_TIME = RE_PATTERN_ERFTEST_TIME
RE_PATTERN_PINGTEST_PACKETS= re.compile(r'^\s*(?P<sent>\d+)\s*packets transmitted,\s*(?P<received>\d+)\s*received,\s*(?P<loss_percent>\d+)%\s*packet loss,\s*time\s*(?P<time>\d+)ms')
RE_PATTERN_PINGTEST_RTT= re.compile(r'^rtt\s+min/avg/max/mdev\s*=\s*(?P<min>\d+.\d+)/(?P<avg>\d+.\d+)/(?P<max>\d+.\d+)/(?P<mdev>\d+.\d+)\s*ms')


def parse_pingtest_file(file):
    dimension = os.path.basename(file).strip('.log').split('_')
    loss_series = Series(dimension + ['loss'], 'percent', is_continuous=False)
    min_series = Series(dimension + ['min'], 'ms')
    avg_series = Series(dimension + ['avg'], 'ms')
    max_series = Series(dimension + ['max'], 'ms')
    mdev_series = Series(dimension + ['mdev'], 'ms')

    def feed_loss_series(index, line):
        m = RE_PATTERN_PINGTEST_PACKETS.match(line)
        if m:
            loss = int(RE_PATTERN_PINGTEST_PACKETS.match(line).group('loss_percent'))
            if loss:
                loss_series.append(index, loss)
            return True
        else:
            return False

    def feed_stat_series(index, line):
        m = RE_PATTERN_PINGTEST_RTT.match(line)
        if m:
            min_series.append(index, float(m.group('min')))
            avg_series.append(index, float(m.group('avg')))
            max_series.append(index, float(m.group('max')))
            mdev_series.append(index, float(m.group('mdev')))
            return True
        else:
            return False


    with open(file,'r') as f:
        time_index = None

        for l in f.readlines():
            if time_index:
                if feed_loss_series(time_index, l):
                    pass
                elif feed_stat_series(time_index, l):  # This expression is last one of segment
                    time_index = None
                else:
                    pass  # Just meaningless line
            else:
                m = RE_PATTERN_PINGTEST_TIME.match(l)
                if m:
                    time_index = datetime.datetime.fromtimestamp(long(m.group('timestamp')))

    return (loss_series, min_series, avg_series, max_series, mdev_series)


class Data:
    def __init__(self, source_path):
        self.series_set = {}
        self.sources = []
        self.load_from(source_path)

    def load_from(self, source_path):
        if not source_path:
            return None

        if os.path.isdir(source_path):
            return tuple(self.load_from_file(os.path.join(source_path, f)) for f in os.listdir(source_path))
        elif os.path.isfile(source_path):
            return tuple(self.load_from_file(source_path))

    def load_from_file(self, filepath):
        filename = os.path.basename(filepath)
        if filename.startswith('erftest'):
            series = parse_erftest_file(filepath)
        elif filename.startswith('pingtest'):
            series = parse_pingtest_file(filepath)
        else:
            logger.info("Unknown file (ignore): %s", filepath)
            series = None

        if series:
            self.sources.append(filepath)
            for s in series:
                self.merge(s)

        return series

    def merge(self, new_series):
        if not isinstance(new_series, Series):
            raise TypeError("Only Series type can be merged", new_series)
        series = self.series_set.get(new_series.dimension)
        if series:
            series += new_series
            series.sort()
        else:
            # Actually I better copy series as new instance but it's unnecessary now
            self.series_set[new_series.dimension] = new_series
            new_series.sort()

    def filter(self, filter_func):
        self.filter = filter_func

    def __iter__(self):
        for x in self.series_set.values():
            if self.filter is None or self.filter(x):
                yield  x

