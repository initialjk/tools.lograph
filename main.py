from parse_file import Data
from render_png import render_data, RightAdditivePlot


import matplotlib.pyplot as plt

def main():
    data = Data('./log/')
    data.filter(lambda s: 'erftest' not in s.dimension)
    for series in data:
        if 'loss' in series.dimension:
            series.priority = -1
    render_data(data, "pingtest.png")

if __name__ == '__main__':
    main()


