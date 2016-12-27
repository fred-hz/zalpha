import numpy as np


def quickSort(alist, amap):
    quickSortHelper(alist, amap, 0, len(alist) - 1)


def quickSortHelper(alist, amap, first, last):
    if first < last:
        splitpoint = partition(alist, amap, first, last)
        quickSortHelper(alist, amap, first, splitpoint - 1)
        quickSortHelper(alist, amap, splitpoint + 1, last)


def partition(alist, amap, first, last):
    pivotvalue = alist[first]

    leftmark = first + 1
    rightmark = last

    done = False
    while not done:

        while leftmark <= rightmark and alist[leftmark] <= pivotvalue:
            leftmark = leftmark + 1

        while alist[rightmark] >= pivotvalue and rightmark >= leftmark:
            rightmark = rightmark - 1

        if rightmark < leftmark:
            done = True
        else:
            temp = alist[leftmark]
            alist[leftmark] = alist[rightmark]
            alist[rightmark] = temp
            temp = amap[leftmark]
            amap[leftmark] = amap[rightmark]
            amap[rightmark] = temp

    temp = alist[first]
    alist[first] = alist[rightmark]
    alist[rightmark] = temp
    temp = amap[first]
    amap[first] = amap[rightmark]
    amap[rightmark] = temp

    return rightmark


def rank(x):
    n = np.sum(-np.isnan(x))
    if n <= 1:
        if n == 1:
            x[-np.isnan(x)] = 0.5
        return n
    x1 = x[-np.isnan(x)]
    xmap = np.where(-np.isnan(x))

    quickSort(x1, xmap)
    i = 0
    while i < n:
        j = i+1
        while j < n and (x1[j] == x1[i] or abs(x1[j]-x1[i])/(abs(x1[j])+abs(x1[i])) < 1e-9):
            j += 1
        j -= 1
        val = (i+j)/(n-1)/2
        for k in range(i, j+1):
            x[xmap[k]] = val
        i = j+1
    return n


def sign(x):
    if x > 0:
        return 1
    elif x < 0:
        return -1
    else:
        return 0


def power(x, num):
    rank(x)
    for i in range(len(x)):
        if np.isnan(x[i]):
            continue
        x[i] -= 0.5
        x[i] = sign(x[i]) * pow(abs(x[i]), num)

def powerNoRank(x, num):
    for i in range(len(x)):
        if np.isnan(x[i]):
            continue
        x[i] = sign(x[i]) * pow(abs(x[i]), num)

def truncate(x, maxPercent = 0.1, maxIter = 1):
    for i in range(maxIter):
        sum_p = np.sum(x[x > 0])
        sum_n = np.sum(x[x < 0])
        sum_p = 0.
        sum_n = 0.
        for item in x:
            if np.isnan(item):
                continue
            if item > 0:
                sum_p += item
            else:
                sum_n += item
        iUpdate = 0
        #print(sum_p * maxPercent)
        for k in range(len(x)):
            if np.isnan(x[k]):
                continue
            if x[k] > sum_p * maxPercent:
                x[k] = sum_p * maxPercent
                iUpdate += 1
            if x[k] < sum_n * maxPercent:
                x[k] = sum_n * maxPercent
                iUpdate += 1
        #print(x)
        if iUpdate == 0:
            return
        if i == maxIter - 1 and iUpdate > 0:
            print("*** warning ***: "+str(iUpdate)+" extreme value in last iter in truncate! ")
