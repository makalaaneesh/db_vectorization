from random import randint

range_start = 1
range_end = 100
lines = 10000000
with open("sample_table", "w") as f:
    for line in range(lines):
        rand1 = randint(range_start, range_end)
        # rand2 = randint(range_start, range_end)
        # s = f"{rand1} {rand2}\n"
        s = f"{rand1}\n"
        # print(s)
        f.write(s)