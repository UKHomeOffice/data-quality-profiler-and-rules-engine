package uk.gov.ipt.das.dataprofiler.value

import uk.gov.ipt.das.dataprofiler.profiler.input.record.ProfilableRecord
import scala.collection.JavaConverters._
@SuppressWarnings(Array("org.wartremover.warts.Any","org.wartremover.warts.Null","org.wartremover.warts.Throw"))
abstract class RecordValue extends Serializable {
  def valueType: ValueType

  def asRecord: ProfilableRecord

  def asArray: Seq[RecordValue]

  def asString: String

  def asBoolean: Boolean

  def asInt: Int

  def asLong: Long

  def asFloat: Float

  def asDouble: Double

  def isPrimitive: Boolean

  /**
   * Get the value as a string. i.e. not the .toString() version, and don't assume it is a string, it can be any type.
   * @return value, as a string
   */
  def valueAsString: String = {
    valueType match {
      case NULL => null
      case STRING => asString
      case BOOLEAN => asBoolean.toString
      case INT => asInt.toString
      case LONG => asLong.toString
      case FLOAT => asFloat.toString
      case DOUBLE => asDouble.toString
      case ARRAY => throw new Exception("Could not return value as a String, it is an Array.")
      case RECORD => throw new Exception("Could not return value as a String, it is a Record.")
      case _ =>
        throw new Exception("Could not return value as a String - unknown type")
    }
  }

  override def toString: String = {
    valueType match {
      case NULL =>
        "NullValue()"
      case STRING =>
        s"StringValue($asString)"
      case BOOLEAN =>
        s"BooleanValue(${asBoolean.toString})"
      case INT =>
        s"IntValue(${asInt.toString})"
      case LONG =>
        s"LongValue(${asLong.toString})"
      case FLOAT =>
        s"FloatValue(${asFloat.toString})"
      case DOUBLE =>
        s"DoubleValue(${asDouble.toString})"
      case ARRAY =>
        s"ArrayValue([${asArray.map { v => v.toString }.mkString(",")}])"
      case RECORD =>
        s"RecordValue(${asRecord.toString})"
      case _ =>
        "UnknownValue()"
    }
  }

}
@SuppressWarnings(Array("org.wartremover.warts.Any","org.wartremover.warts.Null","org.wartremover.warts.Throw"))
object RecordValue {
  def fromAny(value: Any): RecordValue = {
    if (value == null) {
      NullValue()
    } else {
      value match {
        case v: String => StringValue(v)
        case v: Long => LongValue(v)
        case v: Int => LongValue(v)
        case v: Double => DoubleValue(v)
        case v: Float => DoubleValue(v)
        case v: Boolean => BooleanValue(v)
        case v: java.util.LinkedHashMap[String, Any] => ProfilableRecord.fromLinkedHashMap(linkedHashMap = v)
        case v: java.math.BigDecimal => DoubleValue(v.doubleValue()) // TODO do better, write tests against this
        case v: java.util.ArrayList[_] => ArrayValue(v.asScala.map{ RecordValue.fromAny })
        case foo =>
          throw new Exception(s"Unknown primtive type of in RecordValue.fromPrimitive: $foo, class: ${foo.getClass}")
      }
    }
  }
}