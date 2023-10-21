# !/bin/python
import datetime
import psycopg2
from time import time
from time import sleep
import sys

def main():
    zvals = ['0', '1', '1_5']
    shuffles = ['1', '2', '3']
    ks =  [10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000, 1500, 2000, 3000, 4000, 5000, 10000, 20000, 30000, 40000, 50000, 60000, 70000, 80000, 90000, 100000]
    sigma = .99

    # Open connection
    conn = psycopg2.connect(host="/tmp/", database="XXXX", user="XXXX",
                             port="XXXX")
    cur = conn.cursor()
    # If only one argument, clean is provided, drop and remake tables, then exit
    if (len(sys.argv) == 2 and sys.argv[1] == "clean"):
        createTables(shuffles, zvals, conn)
        exit()

    # Otherwise, expect two arguments
    if (len(sys.argv) != 3):
        print("Expecting 2 arguments, verbose output filename and \
                                                             summary filename")
        exit()

    # Open files for writing
    data_filename = str(sys.argv[1])
    summary_filename = str(sys.argv[2])
    summary = open(summary_filename, 'w+')

    # Build queries for testing
    joinQueries = constructQueries(zvals)

    # Begin iterations
    loop = 1
    for joinQuery in joinQueries:
        timeForKs = []
        cur.execute('set statement_timeout = 0;')

        for i in range(loop):
            print("Running this qury for the " + str(i) + " time(s)")
            timeForKs.append(measureTimeForKs(conn, joinQuery[0], ks, sigma, data_filename, i))
        summary.write("\tQuery: %s\n" % (joinQuery[0]))
        minLenRun = sys.maxsize

        for i in range(len(timeForKs)):
            minLenRun = min(minLenRun, len(timeForKs[i]))
        for j in range(minLenRun):
            normalsum = 0
            weightedsum = 0
            for i in range(len(timeForKs)):
                print(j, i, timeForKs[i][j], timeForKs[i][j][1])
                normalsum += timeForKs[i][j][1]
                weightedsum += timeForKs[i][j][2]
                kCurr = timeForKs[i][j][0]
            summary.write("K val:%i\tAverage time (unweighted): %f\t Average time (weighted): %f\n" % (
            kCurr, normalsum / len(timeForKs), weightedsum / len(timeForKs))) # loop = 5
            print(kCurr, normalsum / len(timeForKs), weightedsum / len(timeForKs))
        summary.flush()
    print("This run for all the queries and z-values has finished!")
    summary.close()
    exit()

def constructQueries(zvals):
    result = []
    sch_val = '1'
    for val in zvals:
        # equality joins
        #result.append(("select o_orderkey, l_orderkey from order%s%s join lineitem%s%s on o_orderkey = l_orderkey limit 100000;" % (sch_val, val, sch_val, val), val, 'order', 'lineitem'))
        #result.append(("select p_partkey, l_partkey from part%s%s join lineitem%s%s on p_partkey = l_partkey limit 100000;" % (sch_val, val, sch_val, val), val, 'part', 'lineitem'))
        #result.append(("select s_suppkey, l_suppkey from supplier%s%s join lineitem%s%s on s_suppkey = l_suppkey limit 100000;" % (sch_val, val, sch_val, val), val, 'supplier', 'lineitem'))
        #result.append(("select * from lineitem%s%s, partsupp%s%s where ps_partkey = l_partkey limit 100000;" % (sch_val, val, sch_val, val), val, 'partsupp', 'lineitem'))
        #result.append(("select * from order%s%s, lineitem%s%s where o_orderdate=l_shipdate limit 100000;" % (sch_val, val, sch_val, val), val, 'order', 'lineitem'))
        #result.append(("select * from customer%s%s, order%s%s where c_custkey = o_custkey limit 100000;" % (sch_val, val, sch_val, val), val, 'customer', 'order'))

        #FLIPPED
        #result.append(("select l_orderkey, o_orderkey from lineitem%s join order%s on l_orderkey = o_orderkey limit 100000;" % (sch_val, val, sch_val, val), val, 'lineitem', 'order'))
        #result.append(("select l_partkey, p_partkey from lineitem%s join part%s on l_partkey = p_partkey limit 100000;" % (sch_val, val, sch_val, val), val, 'lineitem', 'part'))
        #result.append(("select l_suppkey, s_suppkey from lineitem%s join supplier%s on l_suppkey = s_suppkey limit 100000;" % (sch_val, val, sch_val, val), val, 'lineitem', 'supplier'))
        #result.append(("select * from partsupp%s, lineitem%s where l_partkey = ps_partkey limit 100000;" % (sch_val, val, sch_val, val), val, 'lineitem', 'partsupp'))
        #result.append(("select * from lineitem%s, order%s where l_shipdate=o_orderdate limit 100000;" % (sch_val, val, sch_val, val), val, 'lineitem', 'order'))
        #result.append(("select * from order%s, customer%s where o_custkey = c_custkey limit 100000;" % (sch_val, val, sch_val, val), val, 'order', 'customer'))

        # non-equality joins
        #result.append(("select s_suppkey, l_suppkey from supplier%s%s join lineitem%s%s on levenshtein(trim(s_suppkey::varchar(10)), trim(l_suppkey::varchar(10))) <= 1 limit 100000;" % (sch_val, val, sch_val, val), val, 'supplier', 'lineitem'))
        #result.append(("select * from customer%s%s, order%s%s where levenshtein(trim(c_custkey::varchar(10)), trim(o_custkey::varchar(10))) <= 1 limit 100000;" % (sch_val, val, sch_val, val), val, 'customer', 'order'))
        
        #FLIPPED
        #result.append(("select s_suppkey, l_suppkey from supplier%s%s join lineitem%s%s on levenshtein(trim(s_suppkey::varchar(10)), trim(l_suppkey::varchar(10))) <= 1 limit 100000;" % (sch_val, val, sch_val, val), val, 'supplier', 'lin    eitem'))
        #result.append(("select * from customer%s%s, order%s%s where levenshtein(trim(c_custkey::varchar(10)), trim(o_custkey::varchar(10))) <= 1 limit 100000;" % (sch_val, val, sch_val, val), val, 'customer', 'order'))

    return result


def measureTimeForKs(conn, joinQuery, ks, sigma, data_filename, iteration):
    f = open(data_filename, 'a')
    cur = conn.cursor()

    # Database config options
    cur.execute('set enable_material=off;')
    cur.execute('set max_parallel_workers_per_gather=0;')
    cur.execute('set enable_hashjoin=off;')
    cur.execute('set enable_mergejoin=off;')  
    cur.execute('set enable_indexonlyscan=off;')
    cur.execute('set enable_indexscan=off;')
    cur.execute('set enable_block=off;')         # Block based Nested loop
    cur.execute('set enable_bitmapscan=off;')
    cur.execute('set enable_fastjoin=on;')       # Bandit Join
    cur.execute('set enable_seqscan=off;')
    cur.execute('set enable_nestloop=off;')
    cur.execute('set work_mem = "64kB";') 
    cur.execute('set statement_timeout = 1800000;') 

    f.write("======================================================== \n")
    f.write("Time of the test run: " + str(datetime.datetime.now()) + '\n')
    f.write("Bandit-Join: ")
    f.write(joinQuery + " #" + str(iteration + 1) + '\n')

    # Make the cursor server-side by naming it
    cur = conn.cursor('cur_uniq')

    cur.itersize = 1
    start = time()

    cur.execute(joinQuery)

    f.write('  time before fetch: %f sec' % (time() - start))
    fetched = int(0)
    start = time()
    prev = start
    factor = sigma
    weightedTime = 0
    res = []

    barrier = int(2)
    print("Query started: " + joinQuery)

    for _ in cur:
        fetched += 1
        current = time()
        weightedTime += (current - prev) * factor
        prev = current
        factor *= sigma
        joinTime = current - start
        if fetched == barrier:
            barrier *= 2
            f.write("%d, %f, %f\n" % (fetched, joinTime, weightedTime))
            print("TEST, fetched: ", fetched)
        if fetched in ks:
            res.append([fetched, joinTime, weightedTime])
        if joinTime >= 1500.0:  
            print("Fetched , JoinTime: ", fetched, joinTime)
            print("Query took more than time limit, continue with next\n")
            break

    joinTime = time() - start
    f.write("Total joined tuples fetched: %d,%f\n" % (fetched, joinTime))
    f.write('Time of current query run: %.2f sec\n' % (joinTime) + '\n')

    cur.close()

    print("Query Complete: " + joinQuery + '\n')
    print("Time to complete the query: %f\n", joinTime)
    sys.stdout.flush()

    # Close files before exiting
    f.flush()
    f.close()

    if not res:
        res.append([0, 0, 0])
    return res


def makeCopyString(scf, val, table):
    print("Time of data reload start: " + str(datetime.datetime.now()) + '\n')
    if (val != '0'):
        infile = "1_5" if val == "1_5" else val
        return "copy %s%s%s from '/PATH/%s.tbl' WITH DELIMITER AS '|';" % (
        table, scf, val, scf, infile, table)
    else:
        return "copy %s%s%s from '/PATH/%s.tbl' WITH DELIMITER AS '|';" % (
        table, scf, val, scf, val, table)

def createTables(shuffles, zvals, conn):
    cur = conn.cursor()

    for scf in shuffles:
        print("Recreating shuffle: %s" % scf)
        for val in zvals:
            print("Recreating zval: %s" % val)
            cur.execute("DROP TABLE IF EXISTS PART" + scf + val + ";")
            cur.execute("DROP TABLE IF EXISTS SUPPLIER" + scf + val + ";")
            cur.execute("DROP TABLE IF EXISTS ORDER" + scf + val + ";")
            cur.execute("DROP TABLE IF EXISTS LINEITEM" + scf + val + ";")
            cur.execute("DROP TABLE IF EXISTS CUSTOMER" + scf + val + ";")
            cur.execute("DROP TABLE IF EXISTS PARTSUPP" + scf + val + ";")

            cur.execute("CREATE TABLE PART" + scf + val + """ ( P_PARTKEY     INTEGER NOT NULL,
                              P_NAME        VARCHAR(55) NOT NULL,
                              P_MFGR        CHAR(25) NOT NULL,
                              P_BRAND       CHAR(10) NOT NULL,
                              P_TYPE        VARCHAR(25) NOT NULL,
                              P_SIZE        INTEGER NOT NULL,
                              P_CONTAINER   CHAR(10) NOT NULL,
                              P_RETAILPRICE DECIMAL(15,2) NOT NULL,
                              P_COMMENT     VARCHAR(23) NOT NULL );""")
            cur.execute("CREATE TABLE SUPPLIER" + scf + val + """ ( S_SUPPKEY     INTEGER NOT NULL,
                                 S_NAME        CHAR(25) NOT NULL,
                                 S_ADDRESS     VARCHAR(40) NOT NULL,
                                 S_NATIONKEY   INTEGER NOT NULL,
                                 S_PHONE       CHAR(15) NOT NULL,
                                 S_ACCTBAL     DECIMAL(15,2) NOT NULL,
                                 S_COMMENT     VARCHAR(101) NOT NULL);""")
            cur.execute("CREATE TABLE LINEITEM" + scf + val + """ ( L_ORDERKEY    INTEGER NOT NULL,
                                 L_PARTKEY     INTEGER NOT NULL,
                                 L_SUPPKEY     INTEGER NOT NULL,
                                 L_LINENUMBER  INTEGER NOT NULL,
                                 L_QUANTITY    DECIMAL(15,2) NOT NULL,
                                 L_EXTENDEDPRICE  DECIMAL(15,2) NOT NULL,
                                 L_DISCOUNT    DECIMAL(15,2) NOT NULL,
                                 L_TAX         DECIMAL(15,2) NOT NULL,
                                 L_RETURNFLAG  CHAR(1) NOT NULL,
                                 L_LINESTATUS  CHAR(1) NOT NULL,
                                 L_SHIPDATE    DATE NOT NULL,
                                 L_COMMITDATE  DATE NOT NULL,
                                 L_RECEIPTDATE DATE NOT NULL,
                                 L_SHIPINSTRUCT CHAR(25) NOT NULL,
                                 L_SHIPMODE     CHAR(10) NOT NULL,
                                 L_COMMENT      VARCHAR(44) NOT NULL);""")
            cur.execute("CREATE TABLE ORDER" + scf + val + """ ( O_ORDERKEY       INTEGER NOT NULL,
                               O_CUSTKEY        INTEGER NOT NULL,
                               O_ORDERSTATUS    CHAR(1) NOT NULL,
                               O_TOTALPRICE     DECIMAL(15,2) NOT NULL,
                               O_ORDERDATE      DATE NOT NULL,
                               O_ORDERPRIORITY  CHAR(15) NOT NULL,
                               O_CLERK          CHAR(15) NOT NULL,
                               O_SHIPPRIORITY   INTEGER NOT NULL,
                               O_COMMENT        VARCHAR(79) NOT NULL);""")
            cur.execute("CREATE TABLE CUSTOMER" + scf + val + """( C_CUSTKEY     INTEGER NOT NULL,
                                 C_NAME        VARCHAR(25) NOT NULL,
                                 C_ADDRESS     VARCHAR(40) NOT NULL,
                                 C_NATIONKEY   INTEGER NOT NULL,
                                 C_PHONE       CHAR(15) NOT NULL,
                                 C_ACCTBAL     DECIMAL(15,2)   NOT NULL,
                                 C_MKTSEGMENT  CHAR(10) NOT NULL,
                                 C_COMMENT     VARCHAR(117) NOT NULL);""")
            cur.execute("CREATE TABLE PARTSUPP" + scf + val + """ ( PS_PARTKEY     INTEGER NOT NULL,
                                 PS_SUPPKEY     INTEGER NOT NULL,
                                 PS_AVAILQTY    INTEGER NOT NULL,
                                 PS_SUPPLYCOST  DECIMAL(15,2)  NOT NULL,
                                 PS_COMMENT     VARCHAR(199) NOT NULL );""")

            cur.execute(makeCopyString(scf, val, "part"))
            cur.execute(makeCopyString(scf, val, "order"))
            cur.execute(makeCopyString(scf, val, "lineitem"))
            cur.execute(makeCopyString(scf, val, "supplier"))
            cur.execute(makeCopyString(scf, val, "customer"))
            cur.execute(makeCopyString(scf, val, "partsupp"))
            conn.commit()

if __name__ == '__main__':
    main()
