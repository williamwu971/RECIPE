import os

import matplotlib.pyplot as plt
# import os
import subprocess
import argparse
import numpy as np
from termcolor import colored

parser = argparse.ArgumentParser()
parser.add_argument("--t", default=[], nargs='+', help="title")
parser.add_argument("--f", default=[], nargs='+', help="executables")
parser.add_argument("--F", default=False, type=bool, help="treat executables as sources")
parser.add_argument("--a", default=['1', '1000000', '8'], nargs='+', help="arguments")
parser.add_argument("--i", default=['0', '1', '1'], nargs='+', help="argument to change")
parser.add_argument("--T", default="0", type=str, help="cores")
parser.add_argument("--v", default=0, type=int, help="preview data")
parser.add_argument("--c", default="", help="files to delete for NUMA 0")
parser.add_argument("--x", default=['iteration'], nargs='+', help="x title")
parser.add_argument("--y", default=['latency'], nargs='+', help="y title")
parser.add_argument("--s", default=None, help="save results")
parser.add_argument("--S", default=[], nargs="+", help="pre-run scripts")
parser.add_argument("--r", default=[], nargs='+', help="read results")
parser.add_argument("--xlim", default=0, type=int, help="x range")
parser.add_argument("--ylim", default=0, type=int, help="y range")

args = parser.parse_args()

plt.figure(1, figsize=(19, 12))

if len(args.r) > 0:
    for fn in args.r:
        with open(fn, "r") as data_file:
            data_read = list(map(lambda x: float(x), data_file.read().splitlines()))
            plt.plot(data_read, label=fn)

increment_target = int(args.i[0])
increment_size = int(args.i[1])
increment_until = int(args.i[2])

# print(increment_target,increment_size,increment_until)
# for inc in range(0, increment_until, increment_size):
#     print(inc)
# quit()

xs = []
script_index = 0


def commander(command: str):
    print(colored(command, "yellow"))
    os.system(command)


def my_float(s: str):
    try:
        return float(s)
    except Exception:
        return 0


def str_list_to_float_list(ll):
    output_ll = []
    for l_string in ll:
        try:
            output_ll.append(float(l_string))
        except Exception:
            print("cannot decode {}".format(l_string))

    return output_ll


for fn in args.f:

    all_data = []

    if not args.F:
        args_copy = [ar for ar in args.a]
        write_to_x = False
        if len(xs) == 0:
            write_to_x = True

        for inc in range(0, increment_until, increment_size):

            if args.c != "":
                for pmem_index in range(4):
                    commander("rm -rf /pmem{}/{}".format(pmem_index, args.c))

            if len(args.S) > 0:
                os.system(args.S[script_index])
                script_index += 1
                script_index = script_index % len(args.S)

            cmd = 'taskset -c {} ./{} {}'.format(args.T, fn, ' '.join(map(lambda x: str(x), args_copy)))
            # if 'v' in fn:
            #     cmd = "LD_PRELOAD=libvmmalloc.so.1 " + cmd
            print(colored(cmd, "yellow"))

            result = subprocess.run(cmd, shell=True, capture_output=True)
            print(result.stdout.decode('utf-8'))
            # if result.returncode != 0:
            #     print(result.stderr.decode('utf-8'))
            #     quit()
            data = str_list_to_float_list(result.stderr.decode('utf-8').splitlines())
            all_data += data

            if write_to_x:
                xs.append(int(args_copy[increment_target]))

            args_copy[increment_target] = str(int(args_copy[increment_target]) + increment_size)

    else:
        with open(fn, "r") as given_file:
            all_data = list(map(lambda x: float(x), given_file.read().splitlines()))

    if len(xs) > 1:
        xi = list(range(len(xs)))
        plt.plot(xi, all_data, label=fn)
        plt.xticks(xi, xs)
    else:
        plt.plot(all_data, label=fn)
        print("avg:{} max:{} min:{} std:{}".format(
            np.mean(all_data), np.max(all_data), np.min(all_data), np.std(all_data)))

    if args.v:
        print(fn)
        for i in range(args.v):
            if i >= len(all_data):
                break
            print(all_data[i])

    if args.s is not None:
        with open(args.s, "w") as s_file:
            for dp in all_data:
                s_file.write("{}\n".format(dp))

# plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left', fontsize='xx-small')
temp_title = ' '.join(args.t if len(args.t) > 0 else args.f)
# temp_title += ' iterations={} size={}'.format(*args.a[1:])
plt.title(temp_title, fontsize=40)
if args.ylim != 0:
    plt.ylim(0, args.ylim)
if args.xlim != 0:
    plt.xlim(0, args.xlim)

plt.xticks(fontsize=30)
plt.yticks(fontsize=30)

plt.ylabel(' '.join(args.y), fontsize=40)
plt.xlabel(' '.join(args.x), fontsize=40)
plt.savefig('out.png', bbox_inches='tight')
