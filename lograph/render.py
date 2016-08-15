import random
from collections import Counter

import matplotlib.pyplot as plt
import matplotlib.ticker
from matplotlib.dates import DateFormatter, HourLocator
from numpy import arange

# These are the "Tableau 20" colors as RGB.
from lograph.parse import MeanSample

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


class RightAdditivePlot:
    def __init__(self, figsize):
        self.axes = {}
        self.arts = []
        self.fig, self.pax = plt.subplots(figsize=figsize)

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
        return (random.randrange(1,256)/255., random.randrange(1,256)/255., random.randrange(1,256)/255.)


def plot_series(title, series_list, figsize=None, scales={}):
    if not figsize:
        end_date = start_date = None
        for s in series_list:
            keys = list(s.keys())
            if keys:
                if not start_date or start_date < keys[0]:
                    start_date = keys[0]
                if not end_date or end_date < keys[-1]:
                    end_date = keys[-1]
            else:
                print("Empty series", title, s.__dict__)
        if end_date and start_date:
            figsize = ((end_date - start_date).days * 20, 14)
        else:
            raise ValueError("No data", title, series_list)

    plot = RightAdditivePlot(figsize=figsize)

    # Preponderance unit first
    units = sorted(Counter(x.unit for x in series_list).iteritems(), key=lambda (k, v): v, reverse=True)
    for unit, count in units:
        ax = plot.get_ax(unit)
        ax.yaxis.set_major_formatter(matplotlib.ticker.FuncFormatter(si_prefix_func))
        limits = scales.get(unit) if scales else None
        if limits:
            ax.set_ylim(limits)

    plot.get_ax(units[0][0]).grid(True)

    for rank, series in enumerate(series_list):
        plot.draw_series(series, color=get_default_color(series, rank))

    plot.legend(loc=0)
    plot.set_title(title)

    return plot
