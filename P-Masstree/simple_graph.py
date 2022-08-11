import os

import matplotlib.pyplot as plt
# import os
import subprocess
import argparse
import numpy as np
from termcolor import colored

parser = argparse.ArgumentParser()
parser.add_argument("--t", default=[], nargs='+', help="title")
parser.add_argument("--x", default=['iteration'], nargs='+', help="x title")
parser.add_argument("--y", default=['latency'], nargs='+', help="y title")
parser.add_argument("--r", default=[], nargs='+', help="read results")
parser.add_argument("--xlim", default=0, type=int, help="x range")
parser.add_argument("--ylim", default=0, type=int, help="y range")
parser.add_argument("--fn", default="out.png", type=str, help="y range")

args = parser.parse_args()

plt.figure(1, figsize=(19, 12))

for fn in args.r:
    with open(fn, "r") as data_file:
        data_read = list(map(lambda x: float(x), data_file.read().splitlines()))
        plt.plot(data_read, label=fn)

xs = []
script_index = 0

temp_title = ' '.join(args.t if len(args.t) > 0 else args.f)
plt.title(temp_title, fontsize=40)
if args.ylim != 0:
    plt.ylim(0, args.ylim)
if args.xlim != 0:
    plt.xlim(0, args.xlim)

plt.xticks(fontsize=30)
plt.yticks(fontsize=30)

plt.ylabel(' '.join(args.y), fontsize=40)
plt.xlabel(' '.join(args.x), fontsize=40)
plt.savefig(args.fn, bbox_inches='tight')
