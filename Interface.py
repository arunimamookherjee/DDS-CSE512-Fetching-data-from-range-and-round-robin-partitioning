#!/usr/bin/python2.7
#
# Interface for the assignement
#
import psycopg2

DATABASE_NAME = 'dds_assgn1'


def getopenconnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadratings(ratingstablename, ratingsfilepath, openconnection):
    rating_file=open(ratingsfilepath,'r')
    rating_rows=rating_file.read().split('\n')
    cur=openconnection.cursor()
    cur.execute('CREATE TABLE ratings (userid INT, movieid INT, rating FLOAT );')
    insert_rating=''
    for row in rating_rows:
        r=row.split('::')
        insert_rating+='INSERT INTO ratings VALUES ({}, {}, {});'.format(r[0],r[1],r[2])
    cur.execute(insert_rating)


def rangepartition(ratingstablename, numberofpartitions, openconnection):
    cur = openconnection.cursor()
    cur.execute('SELECT * from {0}'.format(ratingstablename))
    res = cur.fetchall()
    i=0
    while i!=numberofpartitions:
        cur.execute('CREATE TABLE range_part{} (userid INT, movieid INT, rating FLOAT );'.format(i))
        i += 1

    rating_range = 5.0 / numberofpartitions
    range, i = [], 0
    while i != numberofpartitions:
        i += 1
        range.append(rating_range * i)

    insert_command = ''
    for item in enumerate(res):
        iter_range = 0
        diff = range[iter_range] - item[1][2]

        while (diff < 0):
            iter_range += 1
            diff = range[iter_range] - item[1][2]


        insert_command += 'INSERT INTO range_part{} VALUES ({}, {}, {});'.format(iter_range, item[1][0], item[1][1],
                                                                                 item[1][2])

    cur.execute(insert_command)



def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    cur = openconnection.cursor()
    cur.execute('SELECT * from {0}'.format(ratingstablename))
    res = cur.fetchall()
    i=0
    while i!=numberofpartitions:
        cur.execute('CREATE TABLE rrobin_part{} (userid INT, movieid INT, rating FLOAT );'.format(i))
        i += 1
    cur.execute('CREATE TABLE meta_rr(id INT, counter INT, partition INT);')
    part = 0
    insert_command=''

    for item in res:

        if part>=numberofpartitions:
            part=0
        print(part)
        insert_command+='INSERT INTO rrobin_part{} VALUES ({}, {}, {});'.format(part, item[0], item[1], item[2])
        part+=1



    cur.execute(insert_command)

    if part >= numberofpartitions:
        part = 0
    print(part)
    cur.execute('INSERT INTO meta_rr VALUES ({},{},{});'.format(1, part, numberofpartitions))


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    cur = openconnection.cursor()
    cur.execute('select counter, partition from meta_rr where id=1;')
    res=cur.fetchone()
    part, numberofpartitions =res[0],res[1]

    cur.execute('INSERT INTO rrobin_part{} VALUES ({},{},{});'.format(part,userid,itemid,rating))

    part+=1
    if part >= numberofpartitions:
        part = 0
    cur.execute('UPDATE meta_rr SET counter={} where id=1;'.format(part))


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    cur = openconnection.cursor()
    cur.execute(
        "SELECT COUNT(table_name) FROM information_schema.tables WHERE table_schema = 'public' AND table_name LIKE 'range%';")
    numberofpartitions = int(cur.fetchone()[0])

    rating_range = 5.0 / numberofpartitions
    range, i = [], 0
    while i != numberofpartitions:
        i += 1
        range.append(rating_range * i)


    iter_range = 0
    diff = range[iter_range] - rating

    while (diff < 0):
        iter_range += 1
        diff = range[iter_range] - rating
    insert_command='INSERT INTO range_part{} VALUES({}, {}, {});'.format(iter_range, userid, itemid, rating)
    cur.execute(insert_command)



def create_db(dbname):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getopenconnection(dbname=dbname)
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.close()



