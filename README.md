# Bandit Join(mrun), randomized mrun, SMS join(Optimized Sort Merge) Algorithms

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
