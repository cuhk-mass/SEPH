# !/usr/bin/env python3
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
import string
from scipy.signal import find_peaks
from functools import reduce
from math import floor
import scienceplots

textwidth = 7
columnwidth = 3.34
# circle, triangle_up, square, pentagon, plus (filled), star, diamond
markers = ["o", "^", "s", "p", "P", "*", "D"]
colors = ['#474747E0', '#0C5DA5E0', '#845B97E0',
          '#00B945E0', '#FF9500E0', '#FF2C00E0', '#9E9E9EE0']
plt.style.use(["default", "science", "nature", "../jlhu.mplstyle"])


if __name__ == "__main__":
    # read all sheets, one sheet for one subplot
    data = pd.read_excel("data.xlsx", sheet_name=None)
    fig, axes = plt.subplots(1, len(data))
    fig.set_size_inches(textwidth, textwidth/6)
    laxes, raxes = axes, list(map(matplotlib.axes.Axes.twinx, axes))
    lylim, rylim = pd.DataFrame(
        map(pd.DataFrame.max, data.values())).max().values.tolist()[1:3]
    lcolor, rcolor = colors[5], colors[1]
    handles = None
    for i, (name, lax, rax) in enumerate(zip(data.keys(), laxes, raxes)):
        sheet = data[name]
        sheet = sheet.iloc[:-3, :]  # remove some dropping tail
        x = sheet.iloc[:, 0]  # the first column is the time point
        throughput, rehash = sheet.columns.tolist()[1:]
        # draw throughput
        lax.set_zorder(lax.get_zorder()+1)
        lax.set_frame_on(False)
        lax.plot(x, sheet[throughput], color=lcolor)
        lax.set_xlabel(f"Time (s)\n({string.ascii_lowercase[i]}) {name}")
        lax.ticklabel_format(axis='y', style='sci', scilimits=(
            6, 6), useMathText=True)  # set SCI format in Y axis
        lax.set_ylim(0, lylim)  # the maximum value in across sheets
        # draw the mean dashed line
        meanthroughput = sheet[throughput].describe()["mean"]
        lax.axhline(meanthroughput, linestyle='dashed', color=lcolor)
        lax.text(lax.get_xlim()[-1], meanthroughput,
                 '\n{:.2f}\n\n'.format(meanthroughput/1e6), color=lcolor, fontsize='medium',  va='center' if meanthroughput < lax.get_ylim()[-1]/2 else 'top', ha='right')
        # draw # of rehash
        rax.plot(x, sheet[rehash], color=rcolor)
        rax.fill_between(x, sheet[rehash], color=rcolor, alpha=.25)
        rax.ticklabel_format(axis='y', style='sci', scilimits=(
            6, 6), useMathText=True)  # set SCI format in Y axis
        rax.set_ylim(0, rylim)  # the maximum value in across sheets
        peaks, properties = find_peaks(sheet[throughput], prominence=1)
        peaks = pd.DataFrame(
            {"time": x[peaks], rehash: sheet[throughput][peaks], "prominence": properties["prominences"]}).sort_values(by="prominence", ascending=False)
        # use first 3 prominent peaks as breaking point
        npeaks = 3
        brkx = sorted(peaks.iloc[:npeaks, 0].tolist() +
                      [x.iloc[0], x.iloc[-1]])
        if i == 0 or i == 2:
            brkx = brkx[1:]
        # sum each interval
        for left, right in zip(brkx[:-1], brkx[1:]):
            interval = sheet[rehash][x.between(left, right)].sum() / 10
            xyx = (3*right+2*left)/5  # round to multiple of 0.1
            xyy = sheet[rehash][x.between(
                xyx, xyx + 0.1)].iloc[0] / (3 if i == 2 else 2)
            if i == 0:
                xyy = sheet[rehash][x.between(xyx, xyx + 0.1)].iloc[0]
            rax.annotate(f"{interval/1e6:.0f}", (xyx, xyy/8), color=rcolor,
                         fontsize='medium', va='bottom', ha='center')
        handles = lax.get_lines()[:1] + rax.get_lines()
    laxes[0].set_ylabel('Real-time Throughput (ops)')
    raxes[-1].set_ylabel('Real-time Rehashing (ops)')
    fig.legend(handles, ["Real-time Throughput", "Real-time Rehashing"],
               bbox_to_anchor=(0.5, .95), loc='lower center', ncol=2)
    fig.tight_layout(pad=.25)
    fig.savefig('plot.pdf', dpi=1000, bbox_inches='tight')
    plt.cla()
