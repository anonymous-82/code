# Bandit Join(mrun), randomized mrun, SMS join(Optimized Sort Merge), collaborative learning, Block-based Nested Loop: Algorithms

    Bandit Join(mrun): Steps to follow for running the algorithm
  1. Install the postgresql source code on the machine.
  2. Copy the "nodeNestloop.c" from mrun folder to postgresql path - src/backend/executor
  3. Copy the "execnodes.h" from mrun folder to postgresql path - src/include/nodes
  4. make
  5. make install
  6. start the server
  7. Refer to the python script - mrun.py and make the following changes in the script:
     "database", "user" and "port" to be modified in accordance with the user who is running the experiment with postgresql configuration.
     (currently in the python script - these values are "xxxx")
  8. 1st run the python script using the command: python mrun.py clean
  9. Once step-8 is completed, then run the python script using the command: python mrun.py fullDataFilename summaryFilename
  10. The queries will run and stores the running time results in the files "fullDataFilename" and "summaryFilename"
  
  
    randomized mrun: Steps to follow for running the algorithm
  1. Install the postgresql source code on the machine.
  2. Copy the "nodeNestloop.c" from randomized mrun folder(Github) to postgresql path - src/backend/executor
  3. Copy the "execnodes.h" from randomized mrun folder(Github) to postgresql path - src/include/nodes
  4. make
  5. make install
  6. start the server
  7. Refer to the python script - random_mrun.py and make the following changes in the script:
     "database", "user" and "port" to be modified in accordance with the user who is running the experiment with postgresql configuration.
     (currently in the python script - these values are "xxxx")
  8. 1st run the python script using the command: python random_mrun.py clean
  9. Once step-8 is completed, then run the python script using the command: python random_mrun.py fullDataFilename summaryFilename
  10. The queries will run and stores the running time results in the files "fullDataFilename" and "summaryFilename"
  
  
    SMS Join (Optimized Sort Merge): Steps to follow for running the algorithm
  1. Install the postgresql source code on the machine.
  2. Copy the "nodeMergejoin.c" and "nodeSort.c" from sort merge shrink (SMS) folder(Github) to postgresql path - src/backend/executor
  3. Copy the "execnodes.h" from sort merge shrink (SMS) folder(Github) to postgresql path - src/include/nodes
  4. make
  5. make install
  6. start the server
  7. Refer to the python script - sms.py and make the following changes in the script:
     "database", "user" and "port" to be modified in accordance with the user who is running the experiment with postgresql configuration.
     (currently in the python script - these values are "xxxx")
  8. 1st run the python script using the command: python sms.py clean
  9. Once step-8 is completed, then run the python script using the command: python sms.py fullDataFilename summaryFilename
  10. The queries will run and stores the running time results in the files "fullDataFilename" and "summaryFilename"
  
  
    Collaborative Learning (equi-join or non equi-join): Steps to follow for running the algorithm
  1. Install the postgresql source code on the machine.
  2. Copy the "nodeNestloop.c" from co-learn folder(Github) to postgresql path - src/backend/executor
  3. Copy the "execnodes.h" from co-learn folder(Github) to postgresql path - src/include/nodes
  4. make
  5. make install
  6. start the server
  7. Refer to the python script - colearn.py and make the following changes in the script:
     "database", "user", "limit" and "port" to be modified in accordance with the user who is running the experiment with postgresql configuration.
     (currently in the python script - these values are "xxxx")
  8. The script colearn.py contains all the equi-join and non equi-join queries.
      Please make sure to comment out the "equi-join" or "non-equi-join" queries according to the requirement of the script.
  8. 1st run the python script using the command: python colearn.py clean
  9. Once step-8 is completed, then run the python script using the command: python colearn.py fullDataFilename summaryFilename
  10. The queries will run and stores the running time results in the files "fullDataFilename" and "summaryFilename"
  
  
    Block Based Nested loop (BNL):
  1. Take similar source code above mentioned "mrun" folder
  2. In the python script, change the below flags
      set enable_block=on;
      set enable_fastjoin=off;
  3. And follow the same steps of make, make install and running the script with the queries.
