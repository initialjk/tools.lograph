from lograph.parse import Data
from lograph.parser.erftest import ErfTestLogParser
from lograph.parser.pingtest import PingTestLogParser
from lograph.render import plot_data

def main():
    data = Data()
    data.load(source_path='./log/', parsers=[ErfTestLogParser(), PingTestLogParser()])
    data.filter(lambda s: 'pingtest' in s.dimension)
    for series in data:
        if 'loss' in series.dimension:
            series.priority = -1
    plot_data(data).savefig("pingtest.png", bbox_inches="tight")

    data.filter(lambda s: 'erftest' in s.dimension)
    plot_data(data).savefig("erftest.png", bbox_inches="tight")

if __name__ == '__main__':
    main()

