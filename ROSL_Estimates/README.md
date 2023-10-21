Randomized Online Single Learning(ROSL_Estimates): Steps to follow for running the algorithm
1.	Install the PostgreSQL source code on the machine.
2.	Copy the "nodeNestloop.c" from the ROSL_Estimates folder to the PostgreSQL path - src/backend/executor
3.	Copy the "execnodes.h" from the ROSL_Estimates folder to the PostgreSQL path - src/include/nodes
4.	Copy the "postgresql.conf" from the ROSL_Estimates folder to the PostgreSQL path - src/demodir
5.	make
6.	make install
7.	start the server
8.	Refer to the Python script - run_test.py and make the following changes in the script: "database", "user" and "port" to be modified in accordance with the user who is running the experiment with PostgreSQL configuration. (currently in the Python script - these values are "xxxx")
9.	1st run the Python script using the command: python run_test.py clean
10.	Once the clean step is completed, run the Python script using the command: python run_test.py fullDataFilename summaryFilename
11.	The queries will run and store the running time results in the files "fullDataFilename" and "summaryFilename"
12.	Based on the postgresql.conf file, the logs with estimated results will be created in the "demodir/log" path. 
13.	Please note that, since we are focusing on estimates, the query "COUNT(*)" will focus on estimation results only and will not capture the running times in the above full and summary files.
14.	If we want to observe the running times as well as estimates, then the query can be changed to "SELECT *" instead of "COUNT(*)".
