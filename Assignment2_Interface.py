#!/usr/bin/python2.7
#
# Assignment2 Interface
#roundrobinratingsmetadata'
#partitionnum, rangeratingsmetadata >= minrating and <= maxrating.
#partitionnum, tablenextinsert

import psycopg2
import os
import sys
import math
# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingMinValue, ratingMaxValue, openconnection, outputPath):
    cur = openconnection.cursor()

    cur.execute( "SELECT * FROM rangeratingsmetadata;")
    range_meta= cur.fetchall()
    range_part=[]
    max_r=0

    for i in range(0, len(range_meta)):
        range_part.append(range_meta[i][1])
        max_r=range_meta[i][1]
    #print max_r

        #cur.execute('SELECT * FROM rangeratingspart{};'.format(i))
        #result=cur.fetchall()
        #print result



    diff1, diff2 = range_part[0] - ratingMinValue, range_part[0] - ratingMaxValue
    start_part, end_part = -1, -1

    i=0
    while i!=len(range_meta):
        if start_part==-1:
            diff1=range_part[i]-ratingMinValue
           # print "range_part[i]", range_part[i],"-","ratingMinValue",ratingMinValue,'=diff1', diff1, "(i", i, ")"
            if int(diff1)>=0:
                start_part=i
        if end_part == -1:
            diff2=range_part[i]-ratingMaxValue
           # print "range_part[i]", range_part[i], "-", "ratingMaxValue", ratingMaxValue, '=diff1', int(math.ceil(diff2)), "(i", i, ")"
            if int(math.ceil(diff2))>=0:
                end_part = i
                break
            if ratingMaxValue>max_r:
                end_part=int(max_r)
        i+=1
    #print ratingMinValue, ratingMaxValue
    #print start_part, end_part

    rating_=[]
    print outputPath
    start_part-=1
    print start_part
    f = open(outputPath, "w+")
    for i in range(start_part, end_part+1):
        cur.execute("select  * from rangeratingspart{} where rating>={} and rating<={} ;".format(i, ratingMinValue,
                                                                                                 ratingMaxValue))
        count = cur.fetchall()
        #print count
        for rating in count:
            f.write("rangeratingspart{},{},{},{}\n".format(i,rating[0],rating[1],rating[2]))
            r=[rating[0],rating[1],rating[2]]
            rating_.append(r)
    f.close()
    cur.close()

    #print rating_
    point_q(rating_, openconnection, outputPath)




def point_q(ratingValues, openconnection, outputPath):
    cur = openconnection.cursor()
    cur.execute("SELECT partitionnum FROM roundrobinratingsmetadata;")
    count = cur.fetchone()[0]
    #print count

    f= open(outputPath, "a+")

    for i in ratingValues:
        for tno in range(count):
            cur.execute("select  * from roundrobinratingspart{} where rating={} and userID={} and movieID={};".format(tno, i[2],i[0],i[1]))
            res = cur.fetchone()
            if res>0:
                #print res
                f.write("roundrobinratingspart{},{},{},{}\n".format(tno, i[0], i[1], i[2]))
    f.close()
    cur.close()



def PointQuery(ratingValue, openconnection, outputPath):
    cur = openconnection.cursor()

    #round robin
    cur.execute("SELECT partitionnum FROM roundrobinratingsmetadata;")
    count_rr = cur.fetchone()[0]

    f = open(outputPath, "w+")

    rating_rr = []


    for tno in range(count_rr):
        cur.execute("select  * from roundrobinratingspart{} where rating={};".format(tno, ratingValue))
        res = cur.fetchall()
        # print res
        for itm in res:
            f.write("roundrobinratingspart{},{},{},{}\n".format(tno, itm[0], itm[1], itm[2]))
            r=[itm[0], itm[1], itm[2]]
            rating_rr.append(r)
    f.close()
    cur.close()

    range_q(rating_rr, openconnection, outputPath)



def range_q(ratingValues, openconnection, outputPath):
    cur = openconnection.cursor()
    cur.execute("SELECT * FROM rangeratingsmetadata;")
    range_meta = cur.fetchall() #count of all the partitions
    range_part = []
   # print 'meta', range_meta

    #print 'over'
    f = open(outputPath, "a+")
    for i in ratingValues:
        for rating in range_meta:
            if i[2]>rating[1] and i[2]<=rating[2]:

                f.write("rangeratingspart{},{},{},{}\n".format(rating[0], i[0], i[1], i[2]))
    f.close()
    cur.close()
