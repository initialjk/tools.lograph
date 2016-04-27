import datetime
import logging
import os
import re
from abc import abstractmethod

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
            self.dimension = (unicode(dimension),)
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


class UnsupportedLogError(Exception):
    pass


class LogParser:
    @abstractmethod
    def parse_file(self, filepath):
        return (Series(['notinitialized'], unit='unknown'),)


class Data:
    def __init__(self):
        self.series_set = {}
        self.sources = []
        self._filter = None

    def load(self, parsers, source_path="./"):
        if source_path:
            if os.path.isdir(source_path):
                return tuple(self.load_from_file(os.path.join(source_path, f), parsers=parsers)
                             for f in os.listdir(source_path))
            elif os.path.isfile(source_path):
                return tuple(self.load_from_file(source_path, parsers=parsers))

    def load_from_file(self, filepath, parsers):
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
        self._filter = filter_func

    def __iter__(self):
        for x in self.series_set.values():
            if self._filter is None or self._filter(x):
                yield x
