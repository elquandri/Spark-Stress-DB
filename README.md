# Spark-Stress-DB
SparkStressDB is an application to do stress test and database overload (Cassandra, MongoDB, Redis, PostgreSQL) and also to stress Kafka and HDFS Hadoop

# Version compatibility and branching

| SparkStress  |   Spark        |    Cassandra  | MongoDB    |   Redis    |  Kafka    | 
|     ---      |     ---        |     ---       |  ---       |   ---      |   ---     |
| 1.0          | 2.3.x and 2.4.x| >= 2.1        |>= 3.0      | >=4.0      | > = 1.1.0 |

# How to use 

- ** Example : **




  ``` spark-submit --class SparkStressDB --master spark://centos-vm-01.localdomain:7077 target/scala-2.11/spark-stress-assembly-0.1.jar 1000000 MongoDB ```
