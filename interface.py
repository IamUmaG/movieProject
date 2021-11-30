import psycopg2

openconnection = psycopg2.connect(host='localhost', dbname='postgres', user='postgres', password='sa', port=5432);


def Load_Ratings(ratingsfilepath):
    createratingsquery = """
          CREATE TABLE IF NOT EXISTS ratings
          (
              UserID integer NOT NULL,
              MovieID integer NOT NULL,
              Rating numeric NOT NULL,        
              primary key (UserID,MovieID)     
          );
      """
    db_cursor = openconnection.cursor()
    db_cursor.execute(createratingsquery)

    # load data
    with open(ratingsfilepath, 'r') as file:
        for line in file:
            [UserID, MovieID, Rating, Timestamp] = line.split("::")
            db_cursor.execute("INSERT INTO ratings values({},{},{})".format(UserID, MovieID, Rating))
    openconnection.commit()

def Range_Partition(ratingstablename, numberofpartitions):
    # create meta data table
    cur = openconnection.cursor()
    partitiontableprefix = 'range_part'
    interim = "CREATE TABLE ratings_range_meta(partition INT, startrating numeric, endrating numeric,primary key(partition))"
    cur.execute(interim)

    # Iterating number of partitions
    partitioncounter = 0
    while (partitioncounter < numberofpartitions):

        temp = float(5 / numberofpartitions)
        start = partitioncounter * temp
        end = (partitioncounter + 1) * temp
        partitiontablename = partitiontableprefix + str(partitioncounter)

        cur.execute("CREATE TABLE IF NOT EXISTS {part} (UserID INT, movieID INT, Rating numeric)".format(part=partitiontablename))
        openconnection.commit()

        if (partitioncounter != 0):
            tableinsert = "INSERT INTO {part} select * from {r}  where {r}.rating > {fro} AND  {r}.rating <= {to} order by rating asc".format(
                part=partitiontablename, r=ratingstablename, fro=start, to=end)
        else:
            tableinsert = "INSERT INTO {part} select * from {r}  where {r}.rating >= {fro} AND {r}.rating <={to} order by rating asc ".format(
                part=partitiontablename, r=ratingstablename, fro=start, to=end)

        cur.execute(tableinsert)
        openconnection.commit()

        insert = "INSERT INTO ratings_range_meta VALUES ({partition},{fro},{to})".format(partition=partitioncounter,
                                                                                        fro=start,
                                                                                        to=end)
        partitioncounter += 1
        cur.execute(insert)
        openconnection.commit()


def Range_Insert(ratingstablename, userid, movieid, rating):
    db_cursor = openconnection.cursor()

    # Select the partition to store data
    db_cursor.execute("SELECT min(partition) FROM ratings_range_meta where startrating<= {r} and endrating >= {r} ".format(
        r=rating))

    partition = db_cursor.fetchone()
    initialpartition = partition[0]

    #insert data into ratings
    db_cursor.execute("Insert into {} values ({},{},{})".format(ratingstablename, userid,movieid, rating))
    openconnection.commit()

    #insert data into partition
    db_cursor.execute("Insert into range_part{} values ({},{},{})".format(initialpartition, userid, movieid, rating))
    openconnection.commit()

def Delete_Range_Partitions():
    db_cursor = openconnection.cursor()
    db_cursor.execute("DROP TABLE ratings")
    db_cursor.execute("SELECT partition from ratings_range_meta")
    partitions = db_cursor.fetchall()
    for partition in partitions:
        db_cursor.execute("DROP TABLE {}".format("range_part" + str(partition[0])))
    db_cursor.execute("DROP TABLE ratings_range_meta")
    openconnection.commit()

#Delete_Range_Partitions()
#Load_Ratings("M:/test_data.txt")
#Range_Partition("ratings",5)
#Range_Insert("ratings",2,2333,2.4)


def RoundRobin_Partition(ratingstablename, numberofpartitions):

    db_cursor = openconnection.cursor()
    partitiontableprefix = 'rrobin_part'

    # Iterating and creating partition tables
    partitioncounter=0
    while (partitioncounter < numberofpartitions):
        partitiontablename = partitiontableprefix + str(partitioncounter)
        db_cursor.execute("CREATE TABLE IF NOT EXISTS {partition} (UserID INT, movieID INT, Rating numeric, primary key(userid,movieid))".format(partition=partitiontablename))
        openconnection.commit()
        partitioncounter += 1

    #insert ratings data into partition tables
    db_cursor.execute("SELECT * from ratings")
    result=db_cursor.fetchall();

    for row_num,row_data in enumerate(result):
        partitiontablesuffix = str(row_num%numberofpartitions)
        tablename= partitiontableprefix + partitiontablesuffix
        db_cursor.execute("INSERT INTO {t}(userid,movieid,rating) VALUES({c1},{c2},{c3})".format(t=tablename,c1=row_data[0],c2=row_data[1],c3=row_data[2]))
    openconnection.commit()


def RoundRobin_Insert(ratingstablename, userid, movieid, rating):
    db_cursor = openconnection.cursor()
    db_cursor.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name LIKE 'rrobin_part%'")
    num_partitions = int(db_cursor.fetchall()[0][0])
    print(num_partitions)

    db_cursor.execute("SELECT COUNT(*) FROM rrobin_part0")
    min_partition = int(db_cursor.fetchall()[0][0])
    print(min_partition)

    min_partition_idx = 0
    for partition_idx in range(1, num_partitions):
            db_cursor.execute("SELECT COUNT(*) FROM rrobin_part{}".format(partition_idx))
            num_rows = int(db_cursor.fetchall()[0][0])
            if num_rows < min_partition:
                min_partition = num_rows
                min_partition_idx = partition_idx

    # insert data into ratings
    db_cursor.execute("Insert into {} values ({},{},{})".format(ratingstablename, userid, movieid, rating))
    openconnection.commit()

    # insert into minimally sized partition table
    db_cursor.execute("INSERT INTO rrobin_part{}(UserID, MovieID, Rating) VALUES ({},{},{})"
               .format(min_partition_idx, userid, movieid, rating))

    openconnection.commit()


def Delete_RoundRobin_Partitions():
    db_cursor = openconnection.cursor()
    db_cursor.execute("DROP TABLE ratings")
    db_cursor.execute("SELECT table_name FROM information_schema.tables where table_name like 'rrobin%'")
    partitions = db_cursor.fetchall()
    for partition in partitions:
        db_cursor.execute("DROP TABLE {}".format(partition[0]))
    openconnection.commit()

'''Delete_RoundRobin_Partitions()
Load_Ratings("M:/test_data.txt")
RoundRobin_Partition("ratings",2)

'''
#RoundRobin_Insert("Ratings",122,363,1.5)


def RangeQuery(ratingstablename,ratingminvalue,ratingmaxvalue,openconnection):
    db_cursor = openconnection.cursor()
    rrobinpartitiondata(db_cursor, ratingminvalue, ratingmaxvalue)
    rangepartitiondata(db_cursor,ratingminvalue,ratingmaxvalue)


def rangepartitiondata(db_cursor,ratingminvalue,ratingmaxvalue):
    db_cursor.execute("SELECT partitionnum from rangeratingsmetadata where minrating<={}".format(ratingmaxvalue))
    tablenames = db_cursor.fetchall()
    finalresult = []
    file = open("RangeQueryOut.txt", "a")
    for table in tablenames:
        print(table[0])
        db_cursor.execute(
        "select userid,movieid,rating from rangeratingspart{} where rating between {} and {}".format(table[0], ratingminvalue,ratingmaxvalue))
        result = db_cursor.fetchall()
        for data in result:
            finalresult.append(["rangeratingspart" + str(table[0]), data[0], data[1], float(data[2])])
    writeToFile("RangeQueryOut.txt",finalresult)


def rrobinpartitiondata(db_cursor,ratingminvalue,ratingmaxvalue):
    db_cursor.execute("SELECT table_name FROM information_schema.tables where table_name like 'roundrobinratingspart%' order by table_name asc")
    tablenames = db_cursor.fetchall()
    finalresult=[]
    for table in tablenames:
        print(table[0])
        db_cursor.execute(
            "select userid,movieid,rating from {} where rating between {} and {} ".format(table[0], ratingminvalue, ratingmaxvalue))
        result = db_cursor.fetchall()
        for data in result:
            finalresult.append([table[0], data[0], data[1], float(data[2])])
    writeToFile("RangeQueryOut.txt",finalresult)

def writeToFile(filename, rows):
    f = open(filename, 'a')
    for line in rows:
        f.write(','.join(str(s) for s in line))
        f.write('\n')
    f.close()


def PointQuery(ratingstablename,ratingvalue,openconnection):
    db_cursor = openconnection.cursor()
    rrobinpointdata(db_cursor, ratingvalue)
    rangepointdata(db_cursor, ratingvalue)

def rangepointdata(db_cursor,ratingvalue):
    db_cursor.execute("SELECT partitionnum from rangeratingsmetadata where minrating<={}".format(ratingvalue))
    tablenames = db_cursor.fetchall()
    finalresult = []
    file = open("PointQueryOut.txt", "a")
    for table in tablenames:
        print(table[0])
        db_cursor.execute(
        "select userid,movieid,rating from rangeratingspart{} where rating={}".format(table[0], ratingvalue))
        result = db_cursor.fetchall()
        for data in result:
            finalresult.append(["rangeratingspart" + str(table[0]), data[0], data[1], float(data[2])])
    writeToFile("PointQueryOut.txt",finalresult)


def rrobinpointdata(db_cursor,ratingvalue):
    db_cursor.execute("SELECT table_name FROM information_schema.tables where table_name like 'roundrobinratingspart%' order by table_name asc")
    tablenames = db_cursor.fetchall()
    finalresult=[]
    for table in tablenames:
        print(table[0])
        db_cursor.execute(
            "select userid,movieid,rating from {} where rating = {} ".format(table[0], ratingvalue))
        result = db_cursor.fetchall()
        for data in result:
            finalresult.append([table[0], data[0], data[1], float(data[2])])
    writeToFile("PointQueryOut.txt",finalresult)
