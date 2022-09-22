import matplotlib.pyplot as plt
import argparse

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

# for fn in args.r:
#     with open(fn, "r") as data_file:
#         data_read = list(map(lambda x: float(x), data_file.read().splitlines()))
#
#         if args.ylim != 0:
#             max_value = max(data_read)
#             if max_value > args.ylim * 0.9:
#                 raise Exception("adjust ylim to {}".format(max_value / 0.9))
#
#         plt.plot(data_read, label=fn)

for fn in args.r:
    with open(fn, "r") as data_file:
        data_read = list(map(lambda x: int(x), data_file.read().splitlines()))
        final_data = []

        last_time = data_read[0]
        nb_requests = 0

        for time in data_read:
            diff = time - last_time
            nb_requests += 1

            if diff > 2000000:
                last_time = time
                op_per_second = nb_requests / (diff / 2000000)

                if args.ylim != 0 and op_per_second > args.ylim:
                    raise Exception("adjust ylim to {}".format(op_per_second))

                # for i in range(0, diff, 2000000):
                #     final_data.append(op_per_second)

                final_data.append(op_per_second)
                nb_requests = 0

        # print("{} total cycle: {}".format(fn, data_read[-1] - data_read[0]))

        plt.plot(final_data, label=fn)

# temp_title = ' '.join(args.t)
# plt.title(temp_title, fontsize=40)
plt.title(args.fn, fontsize=40)
if args.ylim != 0:
    plt.ylim(0, args.ylim)
if args.xlim != 0:
    plt.xlim(0, args.xlim)

plt.xticks(fontsize=30)
plt.yticks(fontsize=30)

plt.ylabel(' '.join(args.y), fontsize=40)
plt.xlabel(' '.join(args.x), fontsize=40)
plt.savefig(args.fn + ".png", bbox_inches='tight')
