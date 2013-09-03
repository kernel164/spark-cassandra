package spark.cassandra

import java.nio.ByteBuffer
import org.apache.cassandra.utils.ByteBufferUtil
import java.nio.charset.CharacterCodingException

import CassEnum._

object CassUtil {
  def stringValue(types: List[CassDataType], idx: Int, bytes: ByteBuffer): String = {
    if (types == null)
      stringValue(bytes)
    else
      stringValue(types(idx), bytes)
  }

  def stringValue(types: Map[Int, CassDataType], idx: Int, bytes: ByteBuffer): String = {
    if (types == null || !types.contains(idx))
      stringValue(bytes)
    else
      stringValue(types(idx), bytes)
  }

  def stringValue(typ: CassDataType, bytes: ByteBuffer): String = {
    typ match {
      case STRING => stringValue(bytes)
      case INT => intValue(bytes).toString
      case LONG => longValue(bytes).toString
      case DOUBLE => doubleValue(bytes).toString
      case FLOAT => floatValue(bytes).toString
    }
  }

  def stringValue(bytes: ByteBuffer): String = {
    try {
      return ByteBufferUtil.string(bytes);
    } catch {
      case e: CharacterCodingException => throw new RuntimeException(e);
    }
  }

  def longValue(bytes: ByteBuffer): Long = {
    ByteBufferUtil.toLong(bytes)
  }

  def intValue(bytes: ByteBuffer): Int = {
    ByteBufferUtil.toInt(bytes)
  }

  def doubleValue(bytes: ByteBuffer): Double = {
    ByteBufferUtil.toDouble(bytes)
  }

  def floatValue(bytes: ByteBuffer): Float = {
    ByteBufferUtil.toFloat(bytes)
  }

  def toByteBuffer(value: Any): ByteBuffer = {
    value match {
      case d: Double => ByteBufferUtil.bytes(d)
      case f: Float => ByteBufferUtil.bytes(f)
      case i: Int => ByteBufferUtil.bytes(i)
      case l: Long => ByteBufferUtil.bytes(l)
      case s: String => ByteBufferUtil.bytes(s)
      case x: Any => throw new RuntimeException("data type not supported. value=" + x + ", type: " + x.getClass)
    }
  }

}
