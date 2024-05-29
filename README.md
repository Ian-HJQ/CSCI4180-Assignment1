# CSCI4180 Introduction to Cloud Computing and Storage 

## Assignment 1: WordCount using MapReduce

Compile:
- $ hadoop com.sun.tools.javac.Main WordCount.java
- $ jar cf wc.jar WordCount*.class

Run:
- $ hadoop jar wc.jar WordCount /user/hadoop/input /user/hadoop/output <args>


HADOOP Operation:

List files 	
- $ hadoop fs -ls <hdfs URI>

Make directory 	
- $ hadoop fs -mkdir -p <hdfs URI>

Remove file 	
- $ hadoop fs -rm <hdfs URI>

write into hdfs 	
- $ hadoop fs -put <local file> <hdfs URI>

read from hdfs 	
- $ hadoop fs -get <hdfs URI> <local file>

show contents 	
- $ hadoop fs -cat <hdfs URI>

Reformat hdfs:

- $ rm -rf $HADOOP_HOME/tmp
- $ rm -rf $HADOOP_HOME/logs
- $ mkdir -p $HADOOP_HOME/logs
- $ hadoop namenode -format
