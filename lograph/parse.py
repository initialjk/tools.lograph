import logging
import os
import copy
import datetime
from abc import abstractmethod
from collections import OrderedDict

from collections import deque

import math

logger = logging.getLogger(__name__)


class Sample:
    def __init__(self, key, value):
        self.key = key
        self.value = value

    def merge(self, other):
        assert self.key == other.key
        self.value += other.value
        return self


class MeanSample(Sample):
    def __init__(self, key, mean, value, min=None, max=None, std=0):
        Sample.__init__(self, key, value)
        self.min = min or mean
        self.max = max or mean
        self.std = std

    def merge(self, other):
        super(self, MeanSample).merge(other)
        self.min = min(self.min, other.min)
        self.max = max(self.max, other.max)
        self.std = math.sqrt(self.std * other.std)
        return self


class Series:
    def __init__(self, dimension, unit, priority=0, is_continuous=True):
        self.unit = unit
        self.priority = priority
        self.is_continuous = is_continuous
        self.graph_hint = None
        try:
            self.dimension = tuple(dimension)
        except TypeError:
            self.dimension = (unicode(dimension),)
        self.clear()

    def clear(self):
        self.samples = []
        self.subordinates_series = []

    def make_subset(self, func):
        r = copy.copy(self)
        r.samples = list(s for s in self.samples if func(s))
        return r

    def split_by(self, map_func):
        result = dict()
        for s in self.samples:
            k = map_func(s)
            r = result.get(k, None)
            if r is None:
                r = result[k] = copy.copy(self)
                r.clear()
            r.samples.append(s)

        # TODO: handle subordinates_series
        return result

    def append(self, key, sample):
        if isinstance(sample, Sample):
            self.samples.append(sample)
        else:
            self.samples.append(Sample(key, sample))

    def keys(self):
        return (r.key for r in self.samples)

    def values(self):
        return (r.value for r in self.samples)

    def sort(self):
        self.samples.sort(key=lambda x: x.key)
        return self

    def consolidate(self):
        lasts = samples = None
        for s in self.samples:
            if not samples:
                samples = [s]
                lasts = s.key
            elif lasts == s.key:
                samples[-1].merge(s)
            else:
                samples.append(s)
                lasts = s.key

        self.samples = samples
        return self

    def __add__(self, other):
        assert other.dimension == self.dimension and other.unit == self.unit
        self.samples += other.samples
        # TODO: handle subordinates_series
        return self

    def __len__(self):
        return len(self.samples)

    def __unicode__(self):
        return u"%s (%d records)" % (unicode(self.dimension), len(self))


def get_period_of_series_list(series_list):
    end_date = start_date = None
    for s in series_list:
        keys = s.keys()
        try:
            key_first = next(keys)
            key_last = deque(keys or [key_first], maxlen=1).pop()
            if not start_date or start_date > key_first:
                start_date = key_first
            if not end_date or end_date < key_last:
                end_date = key_last
        except (StopIteration, IndexError):
            pass

    return (start_date, end_date)


class SeriesMap(dict):
    def __init__(self, dimension, **kwargs):
        super(SeriesMap, self).__init__()
        self.base_dimension = dimension
        self.kwargs = kwargs

    def __getitem__(self, item):
        series = self.get(item)
        if series is None:
            self[item] = series = Series(self.base_dimension + [item,], **self.kwargs)
        return series


class UnsupportedLogError(Exception):
    pass


class LogParser:
    @abstractmethod
    def parse_file(self, filepath):
        return (Series(['notinitialized'], unit='unknown'),)


class SeriesSet:
    def __init__(self):
        self.sources = []
        self.clear()

    def clear(self):
        self.series_set = OrderedDict()

    def load(self, parsers, source_path=None):
        if source_path:
            if os.path.isdir(source_path):
                return tuple(self.load_from_file(os.path.join(source_path, f), parsers=parsers)
                             for f in os.listdir(source_path))
            elif os.path.isfile(source_path):
                return tuple(self.load_from_file(source_path, parsers=parsers))
        return tuple()

    def load_from_file(self, filepath, parsers):
        if os.path.isdir(filepath):
            logger.info("Target path is directory: %s", filepath)
            return

        for parser in parsers:
            try:
                series_list = parser.parse_file(filepath)
                for s in series_list:
                    self.merge(s)

                self.sources.append(filepath)
                return series_list
            except UnsupportedLogError:
                pass

        logger.info("Unrecognized file: %s", filepath)

    def get_period(self):
        return get_period_of_series_list(self.series_set.values())

    def merge(self, new_series, distinct=False):
        if not isinstance(new_series, Series):
            raise TypeError("Only Series type can be merged", new_series)

        series = self.series_set.get(new_series.dimension)
        if series:
            series += new_series
            series.sort()
        else:
            # Actually I better copy series as new instance but it's unnecessary now
            self.series_set[new_series.dimension] = series = new_series.sort()

        if distinct and series:
            series.consolidate()

        return self

    def consolidate(self):
        for s in self.series_set.values():
            s.consolidate()

    def split_by(self, map_func):
        result = dict()
        for k, v in self.series_set.iteritems():
            for sk, sv in v.split_by(map_func).iteritems():
                r = result.get(sk, None)
                if r is None:
                    r = result[sk] = copy.copy(self)
                    r.clear()
                r.merge(sv)
        return result

    def filter(self, filter_func):
        return (x for x in self.series_set.values() if x is None or filter_func(x))

    def __iter__(self):
        for x in self.series_set.values():
            yield x


class SlidingCache(set):
    def __init__(self, size):
        super(SlidingCache, self).__init__()
        self.size = size
        self.order = deque()

    def hit(self, entry):
        if entry in self:
            return True
        else:
            self.add(entry)
            return False

    def add(self, entry):
        r = super(SlidingCache, self).add(entry)
        self.order.append(entry)

        i = self.__len__() - self.size
        while i > 0:
            self.remove(self.order.popleft())
            i -= 1

        return r

