package spark.cassandra

import org.apache.hadoop.mapreduce.Job
import org.apache.cassandra.hadoop.ConfigHelper
import org.apache.cassandra.hadoop.cql3.{ CqlOutputFormat, CqlConfigHelper, CqlPagingInputFormat }
import scala.collection.JavaConversions._
import org.apache.hadoop.mapred.JobConf

object CassConfig {
  def cql3 = new Cql3Config()
}

class Cql3Config() {
  val inputFormatClazz = classOf[CqlPagingInputFormat];
  val outputFormatClazz = classOf[CqlOutputFormat];
  val job = new Job()
  murmur3Partitioner()
  job.setInputFormatClass(inputFormatClazz)
  job.setOutputFormatClass(outputFormatClazz)
  readConsistencyLevel("ONE")
  writeConsistencyLevel("TWO")

  def getInputFormatClass() = inputFormatClazz
  def getOutputFormatClass() = outputFormatClazz

  def keyspace(keyspace: String) = {
    columnFamily(keyspace, ConfigHelper.getInputColumnFamily(getConf()), ConfigHelper.getInputIsWide(getConf()));
    this
  }

  def table(table: String) = {
    columnFamily(ConfigHelper.getInputKeyspace(getConf()), table, ConfigHelper.getInputIsWide(getConf()));
    this
  }

  def wideRows() = {
    columnFamily(ConfigHelper.getInputKeyspace(getConf()), ConfigHelper.getInputColumnFamily(getConf()), true);
    this
  }

  def table(schema: String, table: String) = {
    columnFamily(schema, table);
    this
  }

  def table(schema: String, table: String, widerows: Boolean) = {
    columnFamily(schema, table, widerows);
    this
  }

  def columnFamily(keyspace: String, columnFamily: String) = {
    ConfigHelper.setInputColumnFamily(getConf(), keyspace, columnFamily)
    ConfigHelper.setOutputColumnFamily(getConf(), keyspace, columnFamily)
    this
  }

  def columnFamily(keyspace: String, columnFamily: String, widerows: Boolean) = {
    ConfigHelper.setInputColumnFamily(getConf(), keyspace, columnFamily, widerows)
    ConfigHelper.setOutputColumnFamily(getConf(), keyspace, columnFamily)
    this
  }

  def splitSize(splitSize: Int) = {
    ConfigHelper.setInputSplitSize(getConf(), splitSize)
    this
  }

  def readConsistencyLevel(consistencyLevel: String) = {
    getConf().set("cassandra.consistencylevel.read", consistencyLevel);
    this
  }

  def writeConsistencyLevel(consistencyLevel: String) = {
    getConf().set("cassandra.consistencylevel.write", consistencyLevel);
    this
  }

  def columns(columns: String) = {
    if (columns != null)
      CqlConfigHelper.setInputColumns(getConf(), columns)
    this
  }

  def limit(limit: Int) = {
    CqlConfigHelper.setInputCQLPageRowSize(getConf(), limit.toString)
    this
  }

  def where(clauses: String) = {
    if (clauses != null)
      CqlConfigHelper.setInputWhereClauses(getConf(), clauses)
    this
  }

  def randomPartitioner() = {
    partitioner("RandomPartitioner")
    this
  }

  def murmur3Partitioner() = {
    partitioner("Murmur3Partitioner")
    this
  }

  def partitioner(partitioner: String) = {
    ConfigHelper.setInputPartitioner(getConf(), partitioner)
    ConfigHelper.setOutputPartitioner(getConf(), partitioner)
    this
  }

  def cql(cql: String) = {
    CqlConfigHelper.setOutputCql(getConf(), cql)
    this
  }

  def cql() = CqlConfigHelper.getOutputCql(getConf())

  def userName(username: String) = {
    ConfigHelper.setInputKeyspaceUserName(getConf(), username)
    ConfigHelper.setOutputKeyspaceUserName(getConf(), username)
    this
  }

  def password(password: String) = {
    ConfigHelper.setInputKeyspacePassword(getConf(), password)
    ConfigHelper.setInputKeyspacePassword(getConf(), password)
    this
  }

  def host(host: String) = {
    ConfigHelper.setInputInitialAddress(getConf(), host)
    ConfigHelper.setOutputInitialAddress(getConf(), host)
    this
  }

  def host() = ConfigHelper.getOutputInitialAddress(getConf())

  def port(port: Int) = {
    ConfigHelper.setInputRpcPort(getConf(), port.toString)
    ConfigHelper.setOutputRpcPort(getConf(), port.toString)
    this
  }

  def getConf() = job.getConfiguration()
  def getJobConf() = new JobConf(getConf())
}
