import numpy as np
import math
import sys
from cutils.rank import rank_func

sys.setrecursionlimit(5000)

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


# def rank(x):
#     n = np.sum(-np.isnan(x))
#     if n <= 1:
#         if n == 1:
#             x[-np.isnan(x)] = 0.5
#         return n
#     x1 = x[-np.isnan(x)]
#     xmap = np.where(-np.isnan(x))[0]
#     quickSort(x1, xmap)
#     i = 0
#     while i < n:
#         j = i+1
#         while j < n and (x1[j] == x1[i] or abs(x1[j]-x1[i])/(abs(x1[j])+abs(x1[i])) < 1e-9):
#             j += 1
#         j -= 1
#         val = (i+j)/(n-1)/2
#         x[xmap[np.arange(i, j+1)]] = val
#         i = j+1
#     return n


def rank(x):
    rank_func(x)

def power(x, num):
    rank_func(x)
    x[:] -= 0.5
    x[:] = np.sign(x[:]) * np.power(np.abs(x[:]),num)


def powerNoRank(x, num):
    x[:] = np.sign(x[:]) * np.power(np.abs(x[:]), num)


def truncate(x, maxPercent = 0.1, maxIter = 3):
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
    if not x.size == y.size:
        return np.nan
    tmp = np.where(-np.isnan(x) & -np.isnan(y))[0]
    iNonNanNum = tmp.size
    if iNonNanNum < 3:
        return np.nan
    sum_x = np.sum(x[tmp])
    sum_x2 = np.sum(x[tmp] * x[tmp])
    sum_y = np.sum(y[tmp])
    sum_y2 = np.sum(y[tmp] * y[tmp])
    sum_xy = np.sum(x[tmp] * y[tmp])
    devX = abs(iNonNanNum * sum_x2 - sum_x * sum_x)
    devY = abs(iNonNanNum * sum_y2 - sum_y * sum_y)
    if abs(devX) < 1e-4:
        stdX = 0
    else:
        stdX = math.sqrt(devX)
    if abs(devY) < 1e-4:
        stdY = 0
    else:
        stdY = math.sqrt(devY)
    if abs(stdX) < 1e-4 or abs(stdY) < 1e-4:
        return np.nan
    return (iNonNanNum * sum_xy - sum_x * sum_y) / (stdX * stdY)


def EffiRatio(x):
    if x.size <= 2 or abs(x[0] - x[-1]) <= 1e-5:
        return np.nan
    vola = 0
    for i in range(x.size-1):
        if np.isnan(x[i]) or np.isnan(x[i+1]):
            continue
        vola += abs(x[i] - x[i+1])
    return vola/abs(x[0] - x[-1])


def mabsdv(x):
    m = np.nanmean(x)
    if np.isnan(m):
        return np.nan
    return np.nansum(np.abs(x-m))/np.sum(-np.isnan(x))

def wmean(x):
    sum = 0.
    cnt = 0.
    for i in range(x.size):
        if not np.isnan(x[i]):
            sum += x[i] * (x.size - i)
            cnt += x.size - i
    if cnt > 0.5:
        return sum/cnt
    else:
        return np.nan

def downstddv(x):
    if x.size < 4:
        return np.nan
    median = np.nanpercentile(x, 50)
    return np.nanstd(x[x < median])
