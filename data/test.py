import os, re
address = r'F:\zalpha\zalpha\data\raw_stock_daily_data\20060104.csv'
try:
    with open(address) as fp:
        content = fp.read().splitlines()
except IOError:
    print("file is missing!")
else:
    print("do next")

'''
address = data_path + '\\raw_stock_daily_data'
files = os.listdir(address)
output = open('STname.csv','w')
name_list = []

for file_ in files:
    print(file_)
    with open(address + '\\' + file_) as fp:
        content = fp.read().splitlines()
    for line in content[1:]:
        items = line.replace('"', '').split(',')
        if 's' in items[2] or 'S' in items[2]:
            if items[2] not in name_list:
                name_list.append(items[2])
                output.write(('%s' % items[2]) + '\n')
output.close()
'''