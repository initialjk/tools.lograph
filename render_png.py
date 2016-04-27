from collections import Counter

import matplotlib.pyplot as plt
#import pandas as pd

# Read the data into a pandas DataFrame.
#gender_degree_data = pd.read_csv("./log/percent-bachelors-degrees-women-usa.csv")
from matplotlib.dates import DateFormatter, HourLocator, AutoDateLocator
from numpy import arange

# These are the "Tableau 20" colors as RGB.
tableau20 = [(174, 199, 232), (31, 119, 180), (255, 127, 14), (255, 187, 120),
             (44, 160, 44), (152, 223, 138), (214, 39, 40), (255, 152, 150),
             (148, 103, 189), (197, 176, 213), (140, 86, 75), (196, 156, 148),
             (227, 119, 194), (247, 182, 210), (127, 127, 127), (199, 199, 199),
             (188, 189, 34), (219, 219, 141), (23, 190, 207), (158, 218, 229)]

# Scale the RGB values to the [0, 1] range, which is the format matplotlib accepts.
tableau20 = map(lambda x: (x[0]/255., x[1]/255., x[2]/255.), tableau20)

class RightAdditivePlot:
    def __init__(self):
        self.axes = {}
        self.fig, self.pax = plt.subplots(figsize=(23, 15))

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
            right_additive = (0.98-adjust)/float(ax_count)

            for i, a in enumerate(axes for axes in self.axes.values() if axes != self.pax):
                a.spines['right'].set_position(('axes', 1.+right_additive*i))
                a.set_frame_on(True)
                a.patch.set_visible(False)

        return ax

    def set_title(self, *args, **kwargs):
        return self.pax.set_title(*args, **kwargs)

    def text(self, *args, **kwargs):
        return self.pax.text(*args, **kwargs)

    def savefig(self, *args, **kwargs):
        self.fig.savefig(*args, **kwargs)


def render_data(data, filename):
    main_plot = RightAdditivePlot()

    for unit, count in sorted(Counter(x.unit for x in data).iteritems(), key=lambda(k, v): v, reverse=True):
        ax = main_plot.get_ax(unit)

    arts = []
    for rank, series in enumerate(data):
        ax = main_plot.get_ax(series.unit)

        keys = list(series.keys())
        values = list(series.values())

        ax.set_zorder(10+series.priority)
        ax.patch.set_facecolor('none')

        label = "%s (%s)" % ('-'.join(series.dimension), series.unit)
        if series.is_continuous:
            art, = ax.plot_date(keys, values,
                                linestyle='-', marker=None, color=tableau20[rank], label=label)
        else:
            art = ax.bar(keys, values,
                         width=0.005, edgecolor='', color=tableau20[rank], label=label)
        arts.append(art)

    main_plot.pax.legend(handles=arts, labels=(l.get_label() for l in arts), loc=0)
    main_plot.set_title("Network statistics")

    main_plot.savefig(filename, bbox_inches="tight")

