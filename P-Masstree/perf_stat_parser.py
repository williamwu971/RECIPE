print("\t\t\tNF=0\t\t\t\t\t\t\t\t\t\t\t\tNF=1")

for j in range(0, 241, 8):
    print(j + 16, end="\t")
    for i in range(2):
        fn = "stats/dram-ralloc-24-8-NF{}-{}b-0B-N-insert.perf.stat".format(i, j)
        # print(fn)
        with open(fn, "r") as file:
            lines = file.read().splitlines()
            for line in lines:
                if "resource_stalls" in line:
                    print(line, end=" ")
    print()
