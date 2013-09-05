#! /usr/bin/python

import random


def is_sorted(list_to_test):
    if list_to_test == sorted(list_to_test):
        return True
    else:
        return False

def bubble_sort(list_to_mix):
    for i,item in enumerate(list_to_mix):
        if (i + 1) >= len(list_to_mix):
            break
        if list_to_mix[i] > list_to_mix[i + 1]:
            to_move = list_to_mix.pop(i + 1)
            list_to_mix.insert(i, to_move)
    return list_to_mix

def faster_bubble_sort(list_to_mix):
    current_sum = 0
    current_avg = 0
    for i,item in enumerate(list_to_mix):
        current_sum += item
        current_avg = current_sum / (i + 1)
        if (i + 1) >= len(list_to_mix):
            break
        if list_to_mix[i] > list_to_mix[i + 1]:
            to_move = list_to_mix.pop(i + 1)
            if to_move < current_avg:
                list_to_mix.insert((i / 2) , to_move)
            else:
                list_to_mix.insert(i, to_move)
    return list_to_mix

sort_list = [ random.randrange(10000) for i in range(10000) ]
#sort_list = [ 11, 3, 2, 80, 10, 67, 3, 1, 56, 93, 592, 3049, 23, 34.7 ,-1, 3.558 ]

iteration = 0

while iteration < 10000000:
    if is_sorted(sort_list):
        print sort_list
        print "Iterations: %s" % iteration
        break
    else:
        sort_list = faster_bubble_sort(sort_list)
        iteration += 1
        print sort_list
