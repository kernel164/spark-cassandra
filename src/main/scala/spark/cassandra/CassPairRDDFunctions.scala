package spark.cassandra

import org.apache.hadoop.mapreduce.HadoopMapReduceUtil
import org.apache.hadoop.mapred.HadoopWriter
import spark.Logging
import spark.RDD
import spark.SparkException
import spark.TaskContext
import java.nio.ByteBuffer
import spark.cassandra.hadoop._
import org.apache.hadoop.mapred.JobConf
import org.apache.cassandra.hadoop.ConfigHelper
import org.apache.cassandra.hadoop.cql3.CqlConfigHelper

class CassPairRDDFunctions[K: ClassManifest, V: ClassManifest](self: RDD[(K, V)])
  extends Logging
  with HadoopMapReduceUtil
  with Serializable {

  def debugCql3PairRDDFunctionsKeyClassValueClass() = {
    println("[(K, V)] = [(" + getKeyClass + ", " + getValueClass + ")]")
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

    def writeToCass(context: TaskContext, iter: Iterator[(K, V)]) {
      // Hadoop wants a 32-bit task attempt ID, so if ours is bigger than Int.MaxValue, roll it
      // around by taking a mod. We expect that no task will be attempted 2 billion times.
      val attemptNumber = (context.attemptId % Int.MaxValue).toInt

      writer.setup(context.stageId, context.splitId, attemptNumber)
      writer.open()

      var count = 0
      while (iter.hasNext) {
        val record = iter.next()
        count += 1
        writer.write(List(record._1), List(record._2))
      }

      writer.close()
      writer.commit()
    }

    self.context.runJob(self, writeToCass _)
    writer.commitJob()
    writer.cleanup()
  }

  private[spark] def getKeyClass() = implicitly[ClassManifest[K]].erasure
  private[spark] def getValueClass() = implicitly[ClassManifest[V]].erasure
}
