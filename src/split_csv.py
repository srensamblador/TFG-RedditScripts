import csv

def write_to_csv(lines, index):
    with open("splitted_csv/hourly_posts_" + str(index) + ".csv", "w") as f:
                    for tuple in cache:
                        f.write(";".join([str(x) for x in tuple]) + "\n")

with open("hourly_posts.csv") as file:
    csv_file = csv.reader(file, delimiter=";")
    cache = []
    num_docs = 0
    index = 0

    for line in csv_file:
        cache.append(line)
        num_docs += int(line[2])
        if num_docs > 250000:
            write_to_csv(cache, index)
            cache = []
            num_docs = 0
            index +=1

    write_to_csv(cache, index)


