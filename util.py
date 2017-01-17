import numpy as np
import math


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
    xmap = np.where(-np.isnan(x))[0]
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


def power(x, num):
    rank(x)
    x[:] -= 0.5
    x[:] = np.sign(x[:]) * np.power(np.abs(x[:]),num)


def powerNoRank(x, num):
    x[:] = np.sign(x[:]) * np.power(np.abs(x[:]), num)


def truncate(x, maxPercent = 0.1, maxIter = 1):
    for i in range(maxIter):
        sum_p = np.sum(x[x > 0])
        sum_n = np.sum(x[x < 0])
        x[x > sum_p * maxPercent] = sum_p * maxPercent
        x[x < sum_n * maxPercent] = sum_n * maxPercent
        iUpdate = np.sum(x > sum_p * maxPercent) + np.sum(x < sum_n * maxPercent)
        if iUpdate == 0:
            return
        if i == maxIter - 1 and iUpdate > 0:
            print("*** warning ***: "+str(iUpdate)+" extreme value in last iter in truncate! ")


def corr(x, y):
    if not len(x) == len(y):
        return np.nan
    iNonNanNum = 0
    sum_x = 0
    sum_x2 = 0
    sum_y = 0
    sum_y2 = 0
    sum_xy = 0

    for i in range(len(x)):
        if not np.isnan(x[i]) and not np.isnan(y[i]):
            sum_x += x[i]
            sum_x2 += x[i] * x[i]
            sum_y += y[i]
            sum_y2 += y[i] * y[i]
            sum_xy += x[i] * y[i]
            iNonNanNum += 1

    if iNonNanNum < 3:
        return np.nan

    devX = abs(iNonNanNum * sum_x2 - sum_x * sum_x)
    devY = abs(iNonNanNum * sum_y2 - sum_y * sum_y)

    stdX = 0
    if abs(devX) < 1e-4:
        stdX = 0
    else:
        stdX = math.sqrt(devX)

    stdY = 0
    if abs(devY) < 1e-4:
        stdY = 0
    else:
        stdY = math.sqrt(devY)

    if abs(stdX) < 1e-4 or abs(stdY) < 1e-4:
        return np.nan

    ret = (iNonNanNum * sum_xy - sum_x * sum_y) / (stdX * stdY)
    return ret