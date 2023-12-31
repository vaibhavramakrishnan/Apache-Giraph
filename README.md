# Apache-Giraph

Giraph brute-force implementation of the Travelling Salesman Problem

In order to run this example :

1/Download Giraph via Apache's website

2/Add TSP.java file to the src folder of Giraph

3/compile with maven 3, via :
$ mvn compile

4/place the input file 'tiny_graph' in the HDFS, via :
$ hadoop fs -put /.../input /

5/run the Giraph jar with Hadoop, via :
$ hadoop jar /.../target/giraph-0.1-jar-with-dependencies.jar giraph.TSP /input /output
