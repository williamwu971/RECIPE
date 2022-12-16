file_maps = {
    "l1": open("stalled_l1.txt", "w"),
    "l2": open("stalled_l2.txt", "w"),
    "l3": open("stalled_l3.txt", "w"),
    "mem": open("stalled_mem.txt", "w"),
    "total": open("stalled_total.txt", "w"),
    "sb": open("stalled_sb.txt", "w"),
}

for size in range(48, 1025, 16):

    for file in file_maps.values():
        print("{}".format(size), file=file, end=" ")

    with open("../stat/update/dram-log_best-19-8-NF0-0b-{}B-N-flush-10000000n-update.perf.stat".format(
            size)) as stat_file:

        for line in stat_file.read().splitlines():

            for key in file_maps.keys():
                if key in line:
                    print("{}".format(int(line.split()[0].replace(",", ""))), file=file_maps[key], end=" ")

    with open("../stat/update/dram-log_best-19-8-NF1-0b-{}B-N-flush-10000000n-update.perf.stat".format(
            size)) as stat_file:

        for line in stat_file.read().splitlines():

            for key in file_maps.keys():
                if key in line:
                    print("{}".format(int(line.split()[0].replace(",", ""))), file=file_maps[key])
