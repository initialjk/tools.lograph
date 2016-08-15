import bisect
import datetime
import logging
import os
import re

from lograph.parse import LogParser, Series, UnsupportedLogError, MeanSample, Sample
from lograph.parser.erftest import RE_PATTERN_ERFTEST_TIME

logger = logging.getLogger(__name__)

RE_PATTERN_PINGTEST_TIME = RE_PATTERN_ERFTEST_TIME
RE_PATTERN_PINGTEST_PACKETS = re.compile(
    r'^\s*(?P<sent>\d+)\s*packets transmitted,\s*(?P<received>\d+)\s*received,\s*(?P<loss_percent>\d+)%\s*packet loss,\s*time\s*(?P<time>\d+)ms')
RE_PATTERN_PINGTEST_RTT = re.compile(
    r'^rtt\s+min/avg/max/mdev\s*=\s*(?P<min>\d+.\d+)/(?P<avg>\d+.\d+)/(?P<max>\d+.\d+)/(?P<mdev>\d+.\d+)\s*ms')


class LossSample(Sample):
    def __init__(self, key, loss_percent, sent, received):
        Sample.__init__(self, key, int(loss_percent))
        self.sent = int(sent)
        self.received = int(received)

def arrange_loss_event_to_rtt(rtt_series, loss_series):
    rtt_series.sort()
    keys = list(rtt_series.keys())
    values = list(rtt_series.values())
    arranged_series = Series(loss_series.dimension, rtt_series.unit, loss_series.priority, loss_series.is_continuous)
    for s in loss_series.samples:
        i = bisect.bisect(keys, s.key)
        if i <= 0:
            arranged_series.append(keys[0], values[0])
        elif s.sent == s.received:
            arranged_series.append(keys[i - 1], values[i - 1])
        else:
            steps = (s.sent - s.received) + 1
            left_key = keys[i - 1]
            left_value = values[i - 1]
            try:
                step_key = (keys[i] - left_key) / steps
                step_value = (values[i] - left_value) / steps

                for n in xrange(1, steps):
                    arranged_series.append(left_key + (step_key * n), left_value + (step_value * n))
            except IndexError:  # Last item
                arranged_series.append(left_key, left_value)

    return arranged_series


class PingTestLogParser(LogParser):
    def parse_file(self, filepath):
        filename = os.path.basename(filepath)
        if not filename.startswith('pingtest'):
            raise UnsupportedLogError()

        dimension = filename.strip('.log').split('_')
        loss_series = Series(dimension + ['loss'], unit='%', is_continuous=False)
        rtt_series = Series(dimension + ['rtt'], unit='ms')

        def feed_loss_series(index, line):
            m = RE_PATTERN_PINGTEST_PACKETS.match(line)
            if m:
                loss_sample = LossSample(index, m.group('loss_percent'), m.group('sent'), m.group('received'))
                if loss_sample.value and loss_sample.value > 20:
                    loss_series.append(index, loss_sample)
                return True
            else:
                return False

        def feed_stat_series(index, line):
            m = RE_PATTERN_PINGTEST_RTT.match(line)
            if m:
                rtt_series.append(index, MeanSample(index, *(float(m.group(k)) for k in ['avg', 'min', 'max', 'mdev'])))
                return True
            else:
                return False

        with open(filepath, 'r') as f:
            time_index = None

            for l in f.readlines():
                if time_index:
                    if feed_loss_series(time_index, l):
                        pass
                    elif feed_stat_series(time_index, l):
                        time_index = None  # This expression is last one of segment
                    else:
                        pass  # Just meaningless line
                else:
                    m = RE_PATTERN_PINGTEST_TIME.match(l)
                    if m:
                        time_index = datetime.datetime.fromtimestamp(long(m.group('timestamp')))

        arranged_loss_series = arrange_loss_event_to_rtt(rtt_series, loss_series)
        rtt_series.subordinates_series.append(arranged_loss_series)

        return (rtt_series, loss_series,)
