Distributed Computing
Naive Bayes, Regression, aggregation, reducejoin
=========
# MapReduceTutorial
# JDK Link in pom.xml

#C:\Program Files\Java\jdk1.8.0_60\lib\tools.jar
# or
#/usr/lib/jvm/java-6-openjdk-amd64/lib/tools.jar


=========
##### How to run reducejoin  ##########

>##### Step.1: copy all files in a local folder such as 
rootPath=/home/mnaeem/workspace/data/
Every file in this folder must contain column name 

>##### Step.2:
Remove the column name from every file and copy them in folder
rootPath=/home/mnaeem/workspace/data/data2

>##### Step.3: 
upload folder of step 1 into hdfs such as
rootPath=/home/mnaeem/workspace/data/

>##### Step.4: 
upload folder of step 2 into hdfs such as
rootPath=/home/mnaeem/workspace/data2/

>##### Step.5: 
create the following folder
bashfolder=/home/mnaeem/workspace/bashscripts/

>##### Step.6: 
Run the jave file reducejoin.PrepareBashSingleKey.java
It will create the file schemaFilePath
upload this file at the root of the hdfs 
hadoop fs -put schemaFilePath schemaFilePath

>##### Step.7: 
Fix the paths in configuration file (config.properties)
as a first step, we need to run it locally so switch to 
rootPath=/home/mnaeem/workspace/data/
one the step 6 is finished, now switch to option
rootPath=hdfs://localhost:54310/user/mnaeem/

>##### Step.8: 
Compile the program. Run the jar such as
bash t.sh
It will generate all of the bash scripts in the 
bashscript folder

>##### Step.8: 
run each bash script one by one


About Me
---------
```
Muhammad Naeem
naeem.lyon@gmail.com
```
