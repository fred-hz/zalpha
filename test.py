fp = open(r'./data/listing_date.csv')
content = fp.read().splitlines()
fp.close()

for line in content[1:]:
    items = line.split(',')
    if items[2] == '1':
        date = items[1][1:5]+items[1][6:8]+items[1][9:11]
        print(date)


