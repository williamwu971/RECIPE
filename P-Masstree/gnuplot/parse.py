import sys

with open(sys.argv[1], "r") as data_file:
    data_read = list(map(lambda x: int(x), data_file.read().splitlines()))
    final_data = []

    last_time = data_read[0]
    nb_requests = 0

    count = 0

    for time in data_read:
        diff = time - last_time
        nb_requests += 1

        if diff > 2000000:
            last_time = time
            op_per_second = nb_requests / (diff / 2000000)

            for i in range(0, diff - 2000000, 2000000):
                final_data.append(str(op_per_second))
                count += 1

            nb_requests = 0

    with open("parsed.txt", "w") as out_file:
        print("\n".join(final_data), file=out_file)
