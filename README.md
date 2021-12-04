# Scala-spark-Demo
Using scala spark to develop an ETL job and unit tests.


Dataset
-------


Data acquired from 
``
https://www.kaggle.com/usdeptofag/pesticide-data-program-2015
``

Create a folder **data** in the source directory and create two subdirectories 

- csvFiles
- database

Create a directory **data** in and within it create two sub-directories **csvFiles**  and **database**.

Paste all the CSV files in the sub-directory **csvFiles** and the only sqlite file database.sqlite file in **database** sub-directory.

````
./Scala-spark-Demo
|
|-------data
          |
          |------csvFiles
          |          |
          |          |---------all csv files here
          |
          |------database
                   |
                   |---------database.sqlite


````
Task Description
-----

- In the first task, I calculated the frequency with which each pesticide was found in descending order.
- In the second task, I calculated the most used pesticide in each state.

To Install
-------

I have created a script called **installer.cmd** that would create a jar file in **target** directory. You need to have **Maven 3.8.x** and **Java 1.8** for the script to work.

You also need to have **spark 2.4.5** installed on your system and have **spark-submit** configured in the environment variables.


To execute
------

I have created a script run.cmd to execute the spark jobs, simply open command prompt in windows, go to the root directory of this project and type 

``./run.cmd task1``

where task1 is the first task, for executing second task, simply replace **task1** with **task2**.

The output will be written in the **output** sub-directory, output of each task will be written in a csv file named after the task. 

Output
-------

You can find the output generated in the **output** sub-directory in the root.