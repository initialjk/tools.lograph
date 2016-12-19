import random
import time
import logging
from collections import Counter
from collections import defaultdict

import matplotlib.pyplot as plt
import matplotlib.ticker
from matplotlib.dates import DateFormatter, HourLocator
from numpy import arange

from lograph.parse import MeanSample, get_period_of_series_list

logger = logging.getLogger(__name__)

tableau20 = [((31, 119, 180), (174, 199, 232)),
             ((255, 127, 14), (255, 187, 120)),
             ((44, 160, 44), (152, 223, 138)),
             ((214, 39, 40), (255, 152, 150)),
             ((148, 103, 189), (197, 176, 213)),
             ((140, 86, 75), (196, 156, 148)),
             ((227, 119, 194), (247, 182, 210)),
             ((127, 127, 127), (199, 199, 199)),
             ((188, 189, 34), (219, 219, 141)),
             ((23, 190, 207), (158, 218, 229))]

# Scale the RGB values to the [0, 1] range, which is the format matplotlib accepts.
tableau20 = map(lambda y: map(lambda x: (x[0] / 255., x[1] / 255., x[2] / 255.), y), tableau20)

class UnitOption(object):
    def __init__(self, grid=None, scale=None, scope=None):
        self.grid = grid
        self.scale = scale
        self.scope = scope


class RightAdditiveDatePlot(object):
    def __init__(self, figsize, dpi=None):
        self.axes = {}
        self.arts = []
        self.bottom_for_stack = defaultdict(lambda: 0)
        self.stack_history = defaultdict(lambda: [])
        self.fig, self.pax = plt.subplots(figsize=figsize, dpi=dpi)

        self.pax.spines["top"].set_visible(False)
        self.pax.spines["bottom"].set_visible(False)
        self.pax.spines["right"].set_visible(False)
        self.pax.spines["left"].set_visible(False)

        self.pax.get_xaxis().tick_bottom()
        self.pax.get_yaxis().tick_left()

        self.pax.xaxis.set_major_locator(HourLocator(arange(0, 25, 6)))
        self.pax.xaxis.set_minor_locator(HourLocator(arange(0, 25, 1)))
        self.pax.xaxis.set_major_formatter(DateFormatter('%m/%d-%Hh'))

    def get_ax(self, unit):
        ax = self.axes.get(unit)
        if ax:
            return ax

        ax = self.axes[unit] = self.pax if len(self.axes) == 0 else self.pax.twinx()
        ax.set_ylabel(unit)
        ax.xaxis_date()

        ax_count = len(self.axes)
        if ax_count > 2:
            adjust = pow(0.9, (ax_count - 2))
            self.fig.subplots_adjust(right=adjust)
            right_additive = (0.98 - adjust) / float(ax_count)

            for i, a in enumerate(axes for axes in self.axes.values() if axes != self.pax):
                a.spines['right'].set_position(('axes', 1. + right_additive * i))
                a.set_frame_on(True)
                a.patch.set_visible(False)

        return ax

    def draw_series(self, series, **kwargs):
        ax = self.get_ax(series.unit)

        ax.set_zorder(10 + series.priority)
        ax.patch.set_facecolor('none')

        draw_args = kwargs.copy()
        label = "%s (%s)" % ('-'.join(series.dimension), series.unit)
        draw_args.setdefault('label', label)

        keys = list(series.keys())
        values = list(series.values())
        if series.is_continuous:
            draw_args.setdefault('marker', None)
            draw_args.setdefault('linestyle', '-')
            art, = ax.plot_date(keys, values, **draw_args)
            if series.samples and isinstance(series.samples[0], MeanSample):  # Draw range
                ax.fill_between(keys,
                                list(getattr(s, 'min', s.value) for s in series.samples),
                                list(getattr(s, 'max', s.value) for s in series.samples),
                                facecolor=kwargs.get('color'), edgecolor='none', alpha=0.5)
        elif series.graph_hint == 'stacked':
            #  logger.debug("Print series %s on: %s", series.dimension, ', '.join("%s-%s" % (k, self.bottom_for_stack[time.mktime(k.timetuple())]) for k in keys))
            art = ax.bar(keys, values,
                         bottom=list(self.bottom_for_stack[time.mktime(k.timetuple())] for k in keys),
                         **draw_args)
            for s in series.samples:
                self.bottom_for_stack[time.mktime(s.key.timetuple())] += s.value
                self.stack_history[time.mktime(s.key.timetuple())].append("%d (%s)" % (s.value, series.dimension[0]))

        else:
            draw_args.setdefault('marker', '.')
            art = ax.scatter(keys, values, **draw_args)

        for s in series.subordinates_series:
            self.draw_series(s, **kwargs)

        if art:
            self.arts.append(art)
        return art

    def set_title(self, *args, **kwargs):
        return self.pax.set_title(*args, **kwargs)

    def text(self, *args, **kwargs):
        return self.pax.text(*args, **kwargs)

    def savefig(self, *args, **kwargs):
        self.fig.savefig(*args, **kwargs)

    def legend(self, **kwargs):
        self.pax.legend(handles=self.arts, labels=(l.get_label() for l in self.arts), **kwargs)
        pass


def si_prefix_func(x, pos):
    if x >= 1e9:
        return '%1.1fG' % (x * 1e-6)
    if x >= 1e6:
        return '%1.1fM' % (x * 1e-6)
    if x >= 1e3:
        return '%1.1fK' % (x * 1e-3)
    return '%1.1f' % x


def get_default_color(series, rank=None):
    colour = getattr(series, 'color', None) or getattr(series, 'colour', None)
    if colour:
        return colour
    elif isinstance(rank, int) and len(tableau20) > rank:
        return tableau20[rank][0]
    else:
        return random.randrange(1, 256) / 255., random.randrange(1, 256) / 255., random.randrange(1, 256) / 255.

def calculate_fig_width(start_date, end_date, width_per_day):
    if end_date and start_date:
        return ((end_date - start_date).days + 1) * width_per_day
    else:
        return width_per_day

DEFAULT_FIG_WIDTH_PER_DAY = 24
DEFAULT_FIG_HEIGHT = 16
def plot_series(title, series_list, figsize=None, unit_options=None, legend=None, draw_options=None, dpi=None):
    if legend is None:
        legend = dict(loc=0)

    if draw_options is None:
        draw_options = dict()

    if figsize is None:
        start_date, end_date = get_period_of_series_list(series_list)
        figsize = (calculate_fig_width(start_date, end_date, DEFAULT_FIG_WIDTH_PER_DAY), DEFAULT_FIG_HEIGHT)

    plot = RightAdditiveDatePlot(figsize=figsize, dpi=dpi)

    # Preponderance unit first
    all_subordinates_series = list(s for ss in series_list for s in ss.subordinates_series)
    all_units = (x.unit for sl in (series_list, all_subordinates_series) for x in sl)
    units = sorted(Counter(all_units).iteritems(), key=lambda (k, v): v, reverse=True)
    for unit, count in units:
        ax = plot.get_ax(unit)
        ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(si_prefix_func))
        unit_option = unit_options and unit_options.get(unit)
        if unit_option:
            unit_option.scope and ax.set_xlim(unit_option.scope)
            unit_option.scale and ax.set_ylim(unit_option.scale)
            unit_option.grid and ax.grid(True)

    if unit_options is None:
        plot.get_ax(units[0][0]).grid(True)

    for rank, series in enumerate(series_list):
        plot.draw_series(series, color=get_default_color(series, rank), **draw_options)

    plot.legend(**legend)
    plot.set_title(title)

    if logger.isEnabledFor(logging.DEBUG):
        for k, v in plot.stack_history.iteritems():
            logger.debug("%s: %s", k, v)

    return plot
