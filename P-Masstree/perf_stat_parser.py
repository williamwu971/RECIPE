print("resource_stalls,NF=0 stalled,NF=0 total,NF=1 stalled,NF=1 total,")

for j in range(48, 257, 8):
    print(j, end=",")
    for i in range(2):
        fn = "stats/dram-log-19-8-NF{}-256b-{}B-N-flush-insert.perf.stat".format(i, j)
        # print(fn)
        with open(fn, "r") as file:
            lines = file.read().splitlines()
            for line in lines:
                if "resource_stalls" in line:
                    print(line.split()[0].replace(",", ""), end=",")

                if "time" in line:
                    elapsed = (float(line.split()[0]) - 1) * 2000000000 * 19
                    print(elapsed, end=",")
    print()
