// Project Name
// ------------------
name := "spark-cassandra"
 
 
// Project Version
// -------------------
version := "0.0.1"


// Scala Version
// ------------------
scalaVersion := "2.9.3"


// Repositories
// ----------------

resolvers += "Local Maven Repository" at "file:///"+Path.userHome+"/.m2/repository"

resolvers += "Akka Maven Repository" at "http://repo.akka.io/releases"

resolvers += "Spray Maven Repository" at "http://repo.spray.io"
 
 
// Dependencies
// ------------------

libraryDependencies += "org.spark-project" % "spark-core_2.9.3" % "0.7.3" % "compile"

libraryDependencies += "org.eclipse.jetty" % "jetty-server" % "8.1.0.v20120127" % "compile"

libraryDependencies += "com.datastax.cassandra" % "cassandra-driver-core" % "1.0.2" % "compile"

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "1.2.1" % "compile"