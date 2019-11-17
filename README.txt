USAGE:

#Hadoop should be running already before executing the following commands.
#The input file needs to be stored on the HDFS. This can be done by:
$hdfs dfs -put <local input file> <HDFS output directory>

#To create the jar file, change the directry to KNN and enter:
$mvn clean package

#To execute the jar file, enter:
$hadoop jar target/KNN-1.0-SNAPSHOT.jar edu.ucr.cs.cs226.aayac001.KNN <hdfs input file> <query point x> <query point y> <value of > <hdfs output directory>

#'input and output' contains the directory starting from root folder of hdfs.

eg., $hadoop jar target/KNN-1.0-SNAPSHOT.jar edu.ucr.cs.cs226.aayac001.KNN /points 11 12 5 /output

#To read the output file run following command:
$hdfs dfs -cat <output directory>/part-r-00000
