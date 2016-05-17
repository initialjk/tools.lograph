import datetime
import logging
import os
import re
from abc import abstractmethod
from collections import OrderedDict

import itertools

logger = logging.getLogger(__name__)


class Sample:
    def __init__(self, key, value):
        self.key = key
        self.value = value


class MeanSample(Sample):
    def __init__(self, key, mean, min=None, max=None, std=0):
        Sample.__init__(self, key, mean)
        self.min = min or mean
        self.max = max or mean
        self.std = std


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
        self.subordinates_series = []

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

    def __add__(self, other):
        assert other.dimension == self.dimension and other.unit == self.unit
        self.samples += other.samples
        return self

    def __len__(self):
        return len(self.samples)

    def __unicode__(self):
        return u"%s (%d records)" % (unicode(self.dimension), len(self))


class UnsupportedLogError(Exception):
    pass


class LogParser:
    @abstractmethod
    def parse_file(self, filepath):
        return (Series(['notinitialized'], unit='unknown'),)


class SeriesSet:
    def __init__(self):
        self.series_set = OrderedDict()
        self.sources = []

    def load(self, parsers, source_path="./"):
        if source_path:
            if os.path.isdir(source_path):
                return tuple(self.load_from_file(os.path.join(source_path, f), parsers=parsers)
                             for f in os.listdir(source_path))
            elif os.path.isfile(source_path):
                return tuple(self.load_from_file(source_path, parsers=parsers))

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

    def merge(self, new_series):
        if not isinstance(new_series, Series):
            raise TypeError("Only Series type can be merged", new_series)
        series = self.series_set.get(new_series.dimension)
        if series:
            series += new_series
            series.sort()
        else:
            # Actually I better copy series as new instance but it's unnecessary now
            self.series_set[new_series.dimension] = new_series.sort()

    def filter(self, filter_func):
        return (x for x in self.series_set.values() if x is None or filter_func(x))

    def __iter__(self):
        for x in self.series_set.values():
            yield x
