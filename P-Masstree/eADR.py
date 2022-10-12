import matplotlib.pyplot as plt
import sys

plt.figure(1, figsize=(19, 12))

with open("perf.csv", "r") as data_file:
    data_read = list(map(lambda x: x.split(","), data_file.read().splitlines()))
    data_read.pop(0)
    data_read.pop(0)

    line_0 = []
    line_1 = []
    xs = []

    offset = int(len(data_read) / 2)
    for idx in range(offset):
        xs.append(float(data_read[idx][6]))
        line_0.append(float(data_read[idx][int(sys.argv[2])]))
        line_1.append(float(data_read[idx + offset][int(sys.argv[2])]))

    plt.plot(xs, line_0, label="non-temporal")
    plt.plot(xs, line_1, label="eADR")

    # print(line_0)

plt.title("Flush vs eADR (Ralloc-{})".format(sys.argv[1]), fontsize=40)

# to change
plt.ylim(0, 40)
# plt.ylim(0, 3)
plt.xlim(16, 256)

plt.xticks(fontsize=30)
plt.yticks(fontsize=30)

plt.ylabel("Throughput (ops/us)", fontsize=40)
plt.xlabel("Total Size (Bytes)", fontsize=40)

plt.legend(loc="upper right", fontsize=40)
plt.grid()
plt.axvline(x=21, color='r', label='axvline - full height')

plt.savefig("eADR-{}.png".format(sys.argv[1]), bbox_inches='tight')
