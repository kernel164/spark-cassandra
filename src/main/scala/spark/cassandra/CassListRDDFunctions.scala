package spark.cassandra

import org.apache.hadoop.mapreduce.HadoopMapReduceUtil
import org.apache.hadoop.mapred.HadoopWriter
import spark.Logging
import spark.RDD
import spark.SparkException
import spark.TaskContext
import java.nio.ByteBuffer
import spark.cassandra.CassEnum._
import spark.cassandra.hadoop._
import org.apache.hadoop.mapred.JobConf
import org.apache.cassandra.hadoop.ConfigHelper
import org.apache.cassandra.hadoop.cql3.CqlConfigHelper
import org.apache.hadoop.io.NullWritable

class CassListRDDFunctions[T <: List[Any]: ClassManifest](self: RDD[(T)])
  extends Logging
  with HadoopMapReduceUtil
  with Serializable {

  private val keyClass = classOf[java.util.Map[java.lang.String, ByteBuffer]];
  private val valueClass = classOf[java.util.List[ByteBuffer]];

  def debugCql3RDDFunctions() = {
    println("[T] = [(" + getKeyClass + ")]")
  }

  def updateToCassandra(conf: Cql3Config, primaryKeys: List[String]): Unit = {
    val outputFormatClass = conf.getOutputFormatClass()
    if (outputFormatClass == null)
      throw new SparkException("Output format class not set")

    logInfo("Saving data of type (" + keyClass.getSimpleName + ", " + valueClass.getSimpleName + ")")

    val jobConf = conf.getJobConf();
    jobConf.setOutputKeyClass(keyClass);
    jobConf.setOutputValueClass(valueClass);

    val writer = new HadoopWriter(jobConf)
    writer.preSetup()

    def writeToCass(context: TaskContext, iter: Iterator[T]) {
      // Hadoop wants a 32-bit task attempt ID, so if ours is bigger than Int.MaxValue, roll it
      // around by taking a mod. We expect that no task will be attempted 2 billion times.
      val attemptNumber = (context.attemptId % Int.MaxValue).toInt

      writer.setup(context.stageId, context.splitId, attemptNumber)
      writer.open()

      var count = 0
      while (iter.hasNext) {
        val record = iter.next()
        count += 1
        val key = primaryKeys.zip(record.take(primaryKeys.size).map((v) => CassUtil.toByteBuffer(v)).toList).toMap
        //val key = record.take(keyNames.size).zipWithIndex.flatMap((v) => CassUtil.toByteBuffer(keyNames, dataTypes, v._2, v._1)).toMap
        val value = record.takeRight(primaryKeys.size).map((v) => CassUtil.toByteBuffer(v)).toList
        writer.write(key, value)
      }

      writer.close()
      writer.commit()
    }

    self.context.runJob(self, writeToCass _)
    writer.commitJob()
    writer.cleanup()
  }

  def updateToCassandra(conf: Cql3Config): Unit = {
    updateToCassandra(conf.host, conf.cql);
  }

  def updateToCassandra(host: String, cql: String): Unit = {
    insertToCassandra(host, cql);
  }

  def insertToCassandra(conf: Cql3Config): Unit = {
    insertToCassandra(conf.host, conf.cql);
  }

  def insertToCassandra(host: String, cql: String): Unit = {
    storeToCassandra(host, cql);
  }

  def storeToCassandra(host: String, cql: String): Unit = {
    val outputFormatClass = classOf[ListTypeCql3OutputFormat]

    logInfo("Saving data using (" + outputFormatClass.getSimpleName + ")");

    val jobConf = new JobConf();
    jobConf.setOutputFormat(outputFormatClass);
    //jobConf.setOutputKeyClass(classOf[List[Object]]);
    //jobConf.setOutputValueClass(classOf[NullWritable]);
    ConfigHelper.setOutputInitialAddress(jobConf, host);
    CqlConfigHelper.setOutputCql(jobConf, cql);

    val writer = new HadoopWriter(jobConf)
    writer.preSetup()

    def writeToCass(context: TaskContext, iter: Iterator[T]) {
      // Hadoop wants a 32-bit task attempt ID, so if ours is bigger than Int.MaxValue, roll it
      // around by taking a mod. We expect that no task will be attempted 2 billion times.
      val attemptNumber = (context.attemptId % Int.MaxValue).toInt

      writer.setup(context.stageId, context.splitId, attemptNumber)
      writer.open()

      var count = 0
      while (iter.hasNext) {
        val record = iter.next()
        count += 1
        writer.write(record, null)
      }

      writer.close()
      writer.commit()
    }

    self.context.runJob(self, writeToCass _)
    writer.commitJob()
    writer.cleanup()
  }

  private[spark] def getKeyClass() = implicitly[ClassManifest[T]].erasure
}
