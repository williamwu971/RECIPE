import sys

times = {
    "tree_time": [],
    "alloc_time": [],
    "free_time": [],
    "value_write_time": [],
    "sum_time": [],
}

for fn in sys.argv[1:]:
    with open(fn, "r") as f:
        for line in f.read().splitlines():
            tokens = line.split(",")

            times[tokens[0]].append(float(tokens[1]))


print(times)
