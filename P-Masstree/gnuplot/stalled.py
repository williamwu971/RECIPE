grand_file = open("stalled.txt", "w")

for size in range(40, 257, 24):

    print("{}".format(size), file=grand_file, end=" ")

    with open("../stat/insert/dram-log-best-19-8-NF0-0b-{}B-N-flush-43000000n-insert.perf.stat".format(
            size)) as stat_file:

        misses = 0

        for line in stat_file.read().splitlines():

            if "l1d" in line or "l2" in line:
                # print(line.split()[0].replace(",", ""))
                misses += int(line.split()[0].replace(",", ""))

        # print("misses: {}".format(misses), end=" ### ")
        print("{}".format(misses), file=grand_file, end=" ")

    with open("../stat/insert/dram-log-best-19-8-NF1-0b-{}B-N-flush-43000000n-insert.perf.stat".format(
            size)) as stat_file:

        misses = 0

        for line in stat_file.read().splitlines():

            if "l1d" in line or "l2" in line:
                # print(line.split()[0].replace(",", ""))
                misses += int(line.split()[0].replace(",", ""))

        # print("misses: {}".format(misses))
        print("{}".format(misses), file=grand_file)
