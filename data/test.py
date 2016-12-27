import os, re
data_path = 'F:\zalpha\zalpha\data'
di_list = []
self.ii_list = []

address = data_path + '\\raw_stock_daily_data'
files = os.listdir(address)

for file_ in files:
    print(file_)
    with open(address + '\\' + file_) as fp:
        content = fp.read().splitlines()
    for line in content[1:]:
        items = line.replace('"', '').split(',')
        items[1] = re.sub('\D','',items[1])
        if items[1] not in self.ii_list:
            self.ii_list.append(items[1])

print(self.ii_list)