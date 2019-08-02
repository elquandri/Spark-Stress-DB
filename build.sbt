name := "spark-generator"

version := "0.1"

scalaVersion := "2.11.8"

//libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.0" % "provided"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.0" % "provided"
libraryDependencies += "com.google.guava" % "guava" % "14.0.1"
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "3.1.0" exclude("com.google.guava", "guava")

libraryDependencies += "com.redislabs" % "spark-redis" % "2.4.0"
libraryDependencies += "redis.clients" % "jedis" % "3.1.0-m1"

resolvers += "Spark Packages Repo" at "https://dl.bintray.com/spark-packages/maven"
libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.4.0"

libraryDependencies += "org.mongodb.spark" %% "mongo-spark-connector" % "2.4.0"






test in assembly := {}


assemblyMergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf")          => MergeStrategy.discard
  case m if m.toLowerCase.matches("meta-inf.*\\.sf$")      => MergeStrategy.discard
  case "log4j.properties"                                  => MergeStrategy.discard
  case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
  case "reference.conf"                                    => MergeStrategy.concat
  case _                                                   => MergeStrategy.first
}
