import csv
import datetime
import itertools
import logging
import os
import re
import sys
import traceback
from glob import glob
from optparse import OptionParser

from enum import Enum

logging.basicConfig(level='INFO')
logger = logging.getLogger(__name__)

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


class ConnectionPair(object):
    def __init__(self, client_host, client_port, server_host, server_port):
        self.client_host = client_host
        self.client_port = client_port
        self.server_host = server_host
        self.server_port = server_port

    def __hash__(self):
        return ':'.join(sorted([self.client_host, self.server_host])).__hash__()

    def __eq__(self, other):
        return (
            self.client_host == other.client_host and
            self.client_port == other.client_port and
            self.server_host == other.server_host and
            self.server_port == other.server_port
               ) or (
            self.client_host == other.server_host and
            self.client_port == other.server_port and
            self.server_host == other.client_host and
            self.server_port == other.client_port
        )

    def __unicode__(self):
        return self.__str__()

    def __str__(self):
        return '-'.join([
            ':'.join([self.client_host, self.client_port]),
            ':'.join([self.server_host, self.server_port])])


class ConnectionLastEvent(Enum):
    SYN = 1
    SYN_ACK = 2
    ACK_1 = 3  # Established
    FIN = 4
    RST = 5
    CLOSE = 6

class DisconnectionType(Enum):
    undefined = 0
    fin_by_client =   0b0010
    fin_by_server =   0b0011
    reset_by_client = 0b0100
    reset_by_server = 0b0101


class ConnectionEntry(object):
    def __init__(self, pair, seq, last_event=ConnectionLastEvent.SYN, handshake_time=None):
        self.pair = pair
        self.seq = seq
        self.last_event = last_event
        self.handshake_time = handshake_time
        self.connected_time = None
        self.disconnected_time = None
        self.disconnected_reason = None


class TcpConnectionEventCsvWriter(object):
    FIELDNAMES = (
        'Service',
        'Client',
        'Handshake Time',
        'Connected Time',
        'Disconnected Time',
        'Disconnected Reason'
    )

    SERVICE_MAP = {
        '6124': 'Session',
        'pnbs': 'Session',
        '27021': 'Table',
        '27022': 'Table',
    }

    def __init__(self, out_file):
        self.out_file = out_file

    def __enter__(self):
        self.out = open(self.out_file, 'wb').__enter__()
        self.csv = csv.writer(self.out)
        self.csv.writerow(self.FIELDNAMES)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.out.__exit__(exc_type, exc_val, exc_tb)
        self.csv = self.out = None

    def put(self, entry):
        if not self.csv:
            raise IOError("'csv' file is not ready")

        self.csv.writerow((
            self.SERVICE_MAP.get(entry.pair.server_port, entry.pair.server_port),
            entry.pair.client_host,
            entry.handshake_time,
            entry.connected_time,
            entry.disconnected_time,
            entry.disconnected_reason and entry.disconnected_reason.name,
        ))


class TcpDumpConnectionParser(object):
    default_interval = 60
    def __init__(self, db, func_is_server_addr, start_date=datetime.date.today()):
        self.currents = dict()
        self.db = db
        self.is_server_addr = func_is_server_addr

        self.last_time = datetime.time()
        self.day = start_date

    def on_syn(self, time, pair, seq):
        entry = self.currents.get(pair)
        if entry:
            if entry.seq == seq:
                pass  # This is retransmission. Skip it
            else:
                entry.disconnected_reason = DisconnectionType.undefined
                self.db.put(entry)  # This can be timed out connection but didn't send RST yet
        self.currents[pair] = ConnectionEntry(pair, seq, ConnectionLastEvent.SYN, time)

    def on_syn_ack(self, time, pair, ack):
        try:
            entry = self.currents[pair]
            if int(entry.seq)+1 != int(ack):
                logger.info("Unmatched seq number %s:%s. This SYN-ACK can be delayed for: %s", entry.seq, ack, pair)
                pass
            else:
                entry.connected_time = time  # Actually, this should be set when it received first ACK, but I missed record
                entry.last_event = ConnectionLastEvent.SYN_ACK
        except KeyError:
            logger.info("Unmatched SYN-ACK for connection pair: %s %s", time.isoformat(), pair)

    def on_first_ack(self, time, pair):
        # I missed to record this entry .. It might never called
        try:
            entry = self.currents[pair]
            entry.connected_time = time
            entry.last_event = ConnectionLastEvent.ACK_1
        except KeyError:
            logger.info("Unmatched ACK for connection pair: %s %s", time.isoformat(), pair)

    def on_disconnect(self, time, pair, src_host, flag):
        try:
            entry = self.currents.pop(pair)
            entry.disconnected_time = time
            entry.disconnected_reason = flag
            entry.last_event = ConnectionLastEvent.RST if flag == 'R' else ConnectionLastEvent.FIN
            if src_host == pair.client_host:
                entry.disconnected_reason = DisconnectionType.reset_by_client if flag == 'R' else DisconnectionType.fin_by_client
            else:
                entry.disconnected_reason = DisconnectionType.reset_by_server if flag == 'R' else DisconnectionType.fin_by_server
            self.db.put(entry)
        except KeyError:
            pass  #logger.info("Unmatched disconnection for connection pair: %s %s", time.isoformat(), pair)


    def feed(self, l):
        m = RE_PATTERN_TCPDUMP_HEADER.match(l)
        if m is None:
            raise ValueError("Incorrect format")

        if self.is_server_addr(m.group('src_host')):
            pair = ConnectionPair(*(m.group(x) for x in ('dst_host', 'dst_port', 'src_host', 'src_port',)))
        elif self.is_server_addr(m.group('dst_host')):
            pair = ConnectionPair(*(m.group(x) for x in ('src_host', 'src_port', 'dst_host', 'dst_port',)))
        else:
            return None

        t = strptime(m.group('time'))
        if t < self.last_time:
            self.day += datetime.timedelta(days=1)
        self.last_time = t
        dt = datetime.datetime.combine(self.day, t)

        flags = m.group('flags')
        if 'S.' in flags:
            self.on_syn_ack(dt, pair, m.group('ack'))
        elif 'S' in flags:
            self.on_syn(dt, pair, m.group('seq'))
        elif 'R' in flags:
            self.on_disconnect(dt, pair, m.group('src_host'), 'R')
        elif 'F' in flags:
            self.on_disconnect(dt, pair, m.group('src_host'), 'F')
        elif '1' == m.group('ack'):
            self.on_syn_ack(dt, pair, m.group('ack'))
        else:
            return None

        return m

    def flush(self):
        for v in self.currents.itervalues():
            self.db.put(v)


def is_server_addr(addr):
    return addr and addr.startswith('10.0.98')


def estimate_time_from_file(filename):
    for t in str(filename).split('.'):
        try:
            return datetime.datetime.strptime(t, "%Y%m%d")
        except ValueError:
            pass
    return None


optparser = OptionParser()

def main():
    (options, args) = optparser.parse_args()

    if not args:
        args = ['*.dump']
    files = itertools.chain(*(glob(arg) for arg in args))

    for f in (f for f in files if os.path.isfile(f)):
        filepath = os.path.abspath(f)
        with TcpConnectionEventCsvWriter(filepath + '.csv') as w:
            start_date = estimate_time_from_file(os.path.basename(filepath)) or datetime.datetime.now()
            parser = TcpDumpConnectionParser(w, is_server_addr, start_date)

            with open(filepath, 'r') as f:
                for n, l in enumerate(f.readlines()):  # Some lines are ended with CR without LB
                    try:
                        parser.feed(l)
                    except ValueError:
                        logger.warn("Line has incorrect format on %s(%d): %s", filepath, n, l)
                    except BaseException as e:
                        sys.stderr.write('ERROR: %s\n%s\n' % (e.message, traceback.format_exc()))
                        logger.error("Parse error on %s(%d): %s", filepath, n, e.message)

            parser.flush()

if __name__ == '__main__':
    main()

