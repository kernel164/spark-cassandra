package spark.cassandra

import spark.SparkContext
import spark.SparkContext._
import spark.Logging
import java.nio.ByteBuffer
import spark.RDD
import spark.SparkException
import spark.TaskContext
import org.apache.hadoop.mapred.HadoopWriter

class CassContext(sc_ : SparkContext) extends Logging {

  initLogging()

  if (sc_ == null) {
    throw new Exception("CassContext cannot be initialized with SparkContext as null")
  }

  private val clazz = classOf[java.util.Map[java.lang.String, ByteBuffer]];

  def fetchFromCassandra(keyspace: String, columnFamily: String): CassResult = {
    fetchFromCassandra(CassConfig.cql3.table(keyspace, columnFamily));
  }

  def fetchFromCassandra(keyspace: String, columnFamily: String, where: String): CassResult = {
    fetchFromCassandra(CassConfig.cql3.table(keyspace, columnFamily).where(where));
  }

  def fetchFromCassandra(host: String, port: Int, keyspace: String, columnFamily: String, columns: String = null, where: String = null): CassResult = {
    fetchFromCassandra(CassConfig.cql3.host(host).port(port).table(keyspace, columnFamily).columns(columns).where(where));
  }

  def fetchFromCassandra(config: Cql3Config): CassResult = {
    new CassResult(sc_.newAPIHadoopRDD(config.getConf(), config.getInputFormatClass(), clazz, clazz));
  }
}
