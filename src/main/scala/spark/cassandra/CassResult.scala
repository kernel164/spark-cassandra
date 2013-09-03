package spark.cassandra

import scala.collection.JavaConversions._
import spark._
import java.nio.ByteBuffer

import spark.cassandra.CassEnum._

class CassResult(result: RDD[(java.util.Map[String, ByteBuffer], java.util.Map[String, ByteBuffer])]) {
  def rdd() = result

  def asList(typeList: List[CassDataType] = null): RDD[List[String]] = {
    rdd.map {
      case (keys, values) => {
        val keys1 = keys.values.zipWithIndex.map((v) => CassUtil.stringValue(typeList, v._2, v._1)).toList
        val values1 = values.values.zipWithIndex.map((v) => CassUtil.stringValue(typeList, keys1.size + v._2, v._1)).toList
        keys1 ::: values1
      }
    }
  }

  def asList(indexTypeMap: Map[Int, CassDataType]): RDD[List[String]] = {
    rdd.map {
      case (keys, values) => {
        val keys1 = keys.values.zipWithIndex.map((v) => CassUtil.stringValue(indexTypeMap, v._2, v._1)).toList
        val values1 = values.values.zipWithIndex.map((v) => CassUtil.stringValue(indexTypeMap, keys1.size + v._2, v._1)).toList
        keys1 ::: values1
      }
    }
  }

  def asMap(): RDD[(java.util.Map[String, String])] = {
    rdd.map {
      case (keys, values) => {
        val keys1 = keys.map {
          case (colName, colValue) => {
            (colName -> CassUtil.stringValue(colValue))
          }
        }
        val values1 = values.map {
          case (colName, colValue) => {
            (colName -> CassUtil.stringValue(colValue))
          }
        }
        keys1 ++ values1
      }
    }
  }

  def asKVMap: RDD[(java.util.Map[String, String], java.util.Map[String, String])] = {
    rdd.map {
      case (keys, values) => {
        val keys1 = keys.map {
          case (colName, colValue) => {
            (colName -> CassUtil.stringValue(colValue))
          }
        }
        val values1 = values.map {
          case (colName, colValue) => {
            (colName -> CassUtil.stringValue(colValue))
          }
        }
        (keys1, values1)
      }
    }
  }
}