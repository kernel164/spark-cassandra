package spark.cassandra

import scala.Enumeration

object CassEnum extends Enumeration {
  type CassDataType = Value
  val STRING, INT, LONG, DOUBLE, FLOAT = Value
}