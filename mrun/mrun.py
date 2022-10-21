# !/bin/python
import psycopg2
from time import time
import sys
# import pandas


def main():
    zvals = ['0', '1', '1_5', '2']
    ks = [20, 50, 100, 500, 1000, 2000, 30000, 60000]
    sigma = .99
    # Open connection
    conn = psycopg2.connect(host="/tmp/", database="xxxxxx", user="xxxxx", port="xxxx")

    # If only one argument, clean is provided, drop and remake tables, then exit
    if (len(sys.argv) == 2 and sys.argv[1] == "clean"):
        createTables(zvals, conn)
        exit()

    # Otherwise, expect two arguments
    if (len(sys.argv) != 3):
        print("Expecting 2 arguments, verbose output filename and summary filename")
        exit()

    # Open files for writing
    data_filename = str(sys.argv[1])
    summary_filename = str(sys.argv[2])
    f = open(data_filename, 'w+')
    summary = open(summary_filename, 'w+')

    # Build queries for testing
    joinQueries = constructQueries(zvals)

    # Begin iterations
    loop = 5
    for joinQuery in joinQueries:
        timeForKs = []
        for i in range(loop):
            timeForKs.append(measureTimeForKs(conn, joinQuery, ks, sigma, f, i))
        summary.write("\tQuery: %s\n" % (joinQuery))
        for j in range(len(timeForKs[0])):
            normalsum = 0
            weightedsum = 0
            for i in range(loop):
                normalsum += timeForKs[i][j][1]
                weightedsum += timeForKs[i][j][2]
            summary.write("K val:%i\tAverage time (unweighted): %f\t Average time (weighted): %f\n" % (
            timeForKs[i][j][0], normalsum / 5, weightedsum / 5))
        summary.flush()

    # Close files before exiting
    f.close()
    summary.close()
    exit()


def constructQueries(zvals):
    result = []
    for val in zvals:
        result.append(
            "select o_orderkey, l_orderkey from order%s join lineitem%s on o_orderkey = l_orderkey limit 100000;" % (
            val, val))
        result.append(
            "select p_partkey, l_partkey from part%s join lineitem%s on p_partkey = l_partkey limit 100000;" % (
            val, val))
        result.append(
            "select s_suppkey, l_suppkey from supplier%s join lineitem%s on s_suppkey = l_suppkey limit 100000;" % (
            val, val))
    	result.append("select * from lineitem%s, partsupp%s where ps_partkey = l_partkey limit 100000;" % (val, val))
    	result.append("select * from order%s, lineitem%s where o_orderdate=l_shipdate limit 100000;" % (val, val))
        result.append("select * from customer%s, order%s where c_custkey = o_custkey limit 100000;" % (val, val))
    return result


# measures the running time of a join query for a given value of k
def measureTimeForKs(conn, joinQuery, ks, sigma, f, iteration):
    cur = conn.cursor()

    # Database config options
    cur.execute('set enable_material=off;')
    cur.execute('set max_parallel_workers_per_gather=0;')
    cur.execute('set enable_hashjoin=off;')
    cur.execute('set enable_mergejoin=off;')
    cur.execute('set enable_indexonlyscan=off;')
    cur.execute('set enable_indexscan=off;')
    cur.execute('set enable_block=off;')
    cur.execute('set enable_bitmapscan=off;')
    cur.execute('set enable_fastjoin=on;')
    cur.execute('set enable_seqscan=off;')
    cur.execute('set enable_fliporder=off;')
    cur.execute('set work_mem = "64kB";')

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
        if fetched in ks:
            f.write("%d, %f, %f\n" % (fetched, joinTime, weightedTime))
            res.append([fetched, joinTime, weightedTime])

    joinTime = time() - start
    f.write("%d,%f\n" % (fetched, joinTime))
    f.write('  time %.2f sec\n' % (joinTime) + '\n')
    f.write("%f\n" % joinTime)
    cur.close()
    print("Query Complete: " + joinQuery + '\n')
    sys.stdout.flush()
    return res


def makeCopyString(val, table):
    if (val != '0'):
        infile = "1.5" if val == "1_5" else val
        return "copy %s%s from '/data/tpchSkewedData/z_%s_orderkey_partkey_zipf/%s.tbl' WITH DELIMITER AS '|';" % (
        table, val, infile, table)
    else:
        return "copy %s%s from '/data/tpchSkewedData/z_%s_original/%s.tbl' WITH DELIMITER AS '|';" % (
        table, val, val, table)


def createTables(zvals, conn):
    cur = conn.cursor()

    for val in zvals:
        print("Recreating zval: %s" % val)
        cur.execute("DROP TABLE IF EXISTS PART" + val + ";")
        cur.execute("DROP TABLE IF EXISTS SUPPLIER" + val + ";")
        cur.execute("DROP TABLE IF EXISTS ORDER" + val + ";")
        cur.execute("DROP TABLE IF EXISTS LINEITEM" + val + ";")
        cur.execute("DROP TABLE IF EXISTS CUSTOMER" + val + ";")
        cur.execute("DROP TABLE IF EXISTS PARTSUPP" + val + ";")

        cur.execute("CREATE TABLE PART" + val + """ ( P_PARTKEY     INTEGER NOT NULL,
                          P_NAME        VARCHAR(55) NOT NULL,
                          P_MFGR        CHAR(25) NOT NULL,
                          P_BRAND       CHAR(10) NOT NULL,
                          P_TYPE        VARCHAR(25) NOT NULL,
                          P_SIZE        INTEGER NOT NULL,
                          P_CONTAINER   CHAR(10) NOT NULL,
                          P_RETAILPRICE DECIMAL(15,2) NOT NULL,
                          P_COMMENT     VARCHAR(23) NOT NULL );""")
        cur.execute("CREATE TABLE SUPPLIER" + val + """ ( S_SUPPKEY     INTEGER NOT NULL,
                             S_NAME        CHAR(25) NOT NULL,
                             S_ADDRESS     VARCHAR(40) NOT NULL,
                             S_NATIONKEY   INTEGER NOT NULL,
                             S_PHONE       CHAR(15) NOT NULL,
                             S_ACCTBAL     DECIMAL(15,2) NOT NULL,
                             S_COMMENT     VARCHAR(101) NOT NULL);""")
        cur.execute("CREATE TABLE LINEITEM" + val + """ ( L_ORDERKEY    INTEGER NOT NULL,
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
        cur.execute("CREATE TABLE ORDER" + val + """ ( O_ORDERKEY       INTEGER NOT NULL,
                           O_CUSTKEY        INTEGER NOT NULL,
                           O_ORDERSTATUS    CHAR(1) NOT NULL,
                           O_TOTALPRICE     DECIMAL(15,2) NOT NULL,
                           O_ORDERDATE      DATE NOT NULL,
                           O_ORDERPRIORITY  CHAR(15) NOT NULL,  
                           O_CLERK          CHAR(15) NOT NULL, 
                           O_SHIPPRIORITY   INTEGER NOT NULL,
                           O_COMMENT        VARCHAR(79) NOT NULL);""")
        cur.execute("CREATE TABLE CUSTOMER" + val + """( C_CUSTKEY     INTEGER NOT NULL,
                             C_NAME        VARCHAR(25) NOT NULL,
                             C_ADDRESS     VARCHAR(40) NOT NULL,
                             C_NATIONKEY   INTEGER NOT NULL,
                             C_PHONE       CHAR(15) NOT NULL,
                             C_ACCTBAL     DECIMAL(15,2)   NOT NULL,
                             C_MKTSEGMENT  CHAR(10) NOT NULL,
                             C_COMMENT     VARCHAR(117) NOT NULL);""")
        cur.execute("CREATE TABLE PARTSUPP" + val + """ ( PS_PARTKEY     INTEGER NOT NULL,
                             PS_SUPPKEY     INTEGER NOT NULL,
                             PS_AVAILQTY    INTEGER NOT NULL,
                             PS_SUPPLYCOST  DECIMAL(15,2)  NOT NULL,
                             PS_COMMENT     VARCHAR(199) NOT NULL );""")

        cur.execute(makeCopyString(val, "part"))
        cur.execute(makeCopyString(val, "order"))
        cur.execute(makeCopyString(val, "lineitem"))
        cur.execute(makeCopyString(val, "supplier"))
        cur.execute(makeCopyString(val, "customer"))
        cur.execute(makeCopyString(val, "partsupp"))
        conn.commit()

if __name__ == '__main__':
    main()
