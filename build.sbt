// Set the project name to the string 'My Project'
name := "spark-cassandra"
 
// The := method used in Name and Version is one of two fundamental methods.
// The other method is <<=
// All other initialization methods are implemented in terms of these.
version := "1.0"

scalaVersion := "2.9.3"

//Add Repository Path
//resolvers += "db4o-repo" at "http://source.db4o.com/maven"
resolvers += "Local Maven Repository" at "file:///"+Path.userHome+"/.m2/repository"

resolvers += "Akka Maven Repository" at "http://repo.akka.io/releases"

resolvers += "Spray Maven Repository" at "http://repo.spray.io"

 
// Add a single dependency
libraryDependencies += "org.spark-project" % "spark-core_2.9.3" % "0.7.3" % "compile"

libraryDependencies += "org.eclipse.jetty" % "jetty-server" % "8.1.0.v20120127" % "compile"

libraryDependencies += "com.datastax.cassandra" % "cassandra-driver-core" % "1.0.2" % "compile"

libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "1.2.1" % "compile"