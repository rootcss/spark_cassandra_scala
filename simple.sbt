name := "Simple Project"

version := "1.0"

scalaVersion := "2.10.5"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.0"
libraryDependencies += "org.apache.spark" %% "spark-sql"  % "1.6.0"

libraryDependencies += "org.apache.spark" %% "spark-hive" % "1.6.0"

// https://mvnrepository.com/artifact/org.apache.cassandra/cassandra-all
libraryDependencies += "org.apache.cassandra" % "cassandra-all" % "3.5"

// https://mvnrepository.com/artifact/com.datastax.spark/spark-cassandra-connector_2.10
libraryDependencies += "com.datastax.spark" % "spark-cassandra-connector_2.10" % "1.5.0-M2"

resolvers += "Akka Repository" at "http://repo.akka.io/releases/"
