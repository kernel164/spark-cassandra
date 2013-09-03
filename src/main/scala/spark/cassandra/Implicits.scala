package spark.cassandra

import spark.RDD
import spark.SparkContext

object Implicits {
  implicit def rddToCassPairRDDFunctions[K: ClassManifest, V: ClassManifest](rdd: RDD[(K, V)]) = new CassPairRDDFunctions(rdd)
  implicit def rddToCassListRDDFunctions[T <: List[Any]: ClassManifest](rdd: RDD[(T)]) = new CassListRDDFunctions(rdd)
  implicit def rddToCassKVListRDDFunctions[K <: List[Any]: ClassManifest, V <: List[Any]: ClassManifest](rdd: RDD[(K, V)]) = new CassKVListRDDFunctions(rdd)
  implicit def rddToCassKVMapRDDFunctions[K <: Map[String, Any]: ClassManifest, V <: Map[String, Any]: ClassManifest](rdd: RDD[(K, V)]) = new CassKVMapRDDFunctions(rdd)
  implicit def sparkContextToCassContext(sc: SparkContext) = new CassContext(sc)
}