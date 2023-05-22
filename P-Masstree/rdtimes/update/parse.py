import sys

times = {
    "tree_time": [],
    "alloc_time": [],
    "free_time": [],
    "value_write_time": [],
    "sum_time": [],
}

headers = []

for fn in sys.argv[1:]:

    parts = fn.split("-")
    for p in parts:
        if "B" in p:
            headers.append(p)

    with open(fn, "r") as f:
        for line in f.read().splitlines():
            tokens = line.split(",")

            times[tokens[0]].append(float(tokens[1]))

# zip 3
target = ["alloc_time", "free_time", "sum_time"]
new_list = [0.0 for i in range(len(times[target[0]]))]
for t in target:
    for i in range(len(times[t])):
        new_list[i] += times[t][i]
    times.pop(t)
times["rest_time"] = new_list

# normalize
sum_list = [0.0 for i in range(len(times["tree_time"]))]
for k, v in times.items():
    for i in range(len(v)):
        sum_list[i] += v[i]

for k, v in times.items():
    for i in range(len(v)):
        # v[i] = v[i] / sum_list[i] * 100
        v[i] *= 100

# outfile = open("out.csv", "w")
outfile = sys.stdout
t = open("template.txt", "r")
template = t.read()

print(",{}".format(",".join(headers)), file=outfile)

order = ["tree_time", "rest_time", "value_write_time"]

for k in order:
    print(k, end=",", file=outfile)

    v = times[k]
    base = 0.0

    for number in v:
        print("{:.2f}".format(number), end=",", file=outfile)

        if base == 0.0:
            # template = template.replace("PPP", "{:.2f}".format(number), 1)
            template = template.replace("PPP", "{}".format(int(number)), 1)
            base = number
        else:
            # template = template.replace("PPP", "{:.2f} ({:+d}\%)".format(number, int((number - base) / base * 100)), 1)
            template = template.replace("PPP", "{} ({:+d}\%)".format(int(number), int((number - base) / base * 100)), 1)


print("", file=outfile)

with open("template.txt", "w") as out_f:
    out_f.write(template)

# print(sum_list)
# print(times)
