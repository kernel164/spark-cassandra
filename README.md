spark-cassandra
===============

Integrating Spark &amp; Cassandra

Warning: Consider this as pre alpha -> Work in progress - API might change very frequently.

```java

import spark.SparkContext
import spark.SparkContext._
import spark.cassandra.Implicits._
import spark.cassandra.CassEnum._

val sc = new SparkContext("local", "appName")
val vars = sc.broadcast(Map(("host", "cassandra-host")))
val cql3 = CassConfig.cql3
  .host("cassandra-host")
  .randomPartitioner()
  .table("keyspace", "columnfamily", true)
  .columns("col2, col3, col4")
  .limit(10000)

val dataList = sc.fetchFromCassandra(cql3).asList(Map((3, LONG))).filter(l => l(2) == "")
dataList.cache
dataList.storeToCassandra(vars.value("host"), "INSERT keyspace.columnfamily1 (col1, col2, col3, col4) VALUES (?, ?, ?, ?);");

val reducedKVMap = dataList.map(l => (l(2).toInt / 100, l(3).toInt)).reduceByKey(_ + _)
reducedKVMap.cache
reducedKVMap.storeToCassandra(vars.value("host"), "UPDATE keyspace.columnfamily2 SET col2 = ? WHERE col1 = ?;");

val sortedListResult = reducedKVMap.map(x => (x._2, x._1)).sortByKey(false)
sortedListResult.foreach(x => println(x))

```