Implicit Collaborative Learning (equi-join or non equi-join): Steps to follow for running the algorithm
1.	Install the postgresql source code on the machine.
2.	Copy the "nodeNestloop.c" from implicit co-learn (ICL) folder(Github) to postgresql path - src/backend/executor
3.	Copy the "execnodes.h" from implicit co-learn (ICL) folder(Github) to postgresql path - src/include/nodes
4.	make
5.	make install
6.	start the server
7.	Refer to the python script - run_test.py and make the following changes in the script: "database", "user", "limit" and "port" to be modified in accordance with the user who is running the experiment with postgresql configuration. (currently in the python script - these values are "xxxx")
8.	The script run_test.py contains all the equi-join and non equi-join queries. Please make sure to comment out the "equi-join" or "non-equi-join" queries according to the requirement of the script.
9.	1st run the python script using the command: python run_test.py clean
10.	Once step-8 is completed, then run the python script using the command: python run_test.py fullDataFilename summaryFilename
11.	The queries will run and stores the running time results in the files "fullDataFilename" and "summaryFilename"
