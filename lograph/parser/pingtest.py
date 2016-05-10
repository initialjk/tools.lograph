import datetime
import logging
import os
import re

from lograph.parse import LogParser, Series, UnsupportedLogError, MeanSample
from lograph.parser.erftest import RE_PATTERN_ERFTEST_TIME

logger = logging.getLogger(__name__)

RE_PATTERN_PINGTEST_TIME = RE_PATTERN_ERFTEST_TIME
RE_PATTERN_PINGTEST_PACKETS = re.compile(
    r'^\s*(?P<sent>\d+)\s*packets transmitted,\s*(?P<received>\d+)\s*received,\s*(?P<loss_percent>\d+)%\s*packet loss,\s*time\s*(?P<time>\d+)ms')
RE_PATTERN_PINGTEST_RTT = re.compile(
    r'^rtt\s+min/avg/max/mdev\s*=\s*(?P<min>\d+.\d+)/(?P<avg>\d+.\d+)/(?P<max>\d+.\d+)/(?P<mdev>\d+.\d+)\s*ms')


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
                loss = int(RE_PATTERN_PINGTEST_PACKETS.match(line).group('loss_percent'))
                if loss:
                    loss_series.append(index, loss)
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

        rtt_series.subordinates_series.append(loss_series)

        return (rtt_series,)
