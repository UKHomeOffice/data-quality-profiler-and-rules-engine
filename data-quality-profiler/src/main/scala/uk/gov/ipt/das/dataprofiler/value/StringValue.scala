package uk.gov.ipt.das.dataprofiler.value

import uk.gov.ipt.das.dataprofiler.profiler.input.record.ProfilableRecord
import java.time.{LocalDate, LocalDateTime, OffsetDateTime, ZoneOffset}

@SuppressWarnings(Array("org.wartremover.warts.Throw"))
case class StringValue(value: String) extends RecordValue with Serializable {

  if (value == null) {
    throw new Exception("Cant have a null within a StringValue use NullValue instead")
  }

  override def valueType: ValueType = STRING

  override def asRecord: ProfilableRecord =
    throw new InvalidRecordValueTypeException(actual = valueType, attempted = RECORD, value = value)

  override def asArray: Seq[RecordValue] =
    throw new InvalidRecordValueTypeException(actual = valueType, attempted = ARRAY, value = value)

  override def asString: String = value

  override def asBoolean: Boolean =
    throw new InvalidRecordValueTypeException(actual = valueType, attempted = BOOLEAN, value = value)

  override def asInt: Int =
    throw new InvalidRecordValueTypeException(actual = valueType, attempted = INT, value = value)

  override def asLong: Long =
    throw new InvalidRecordValueTypeException(actual = valueType, attempted = LONG, value = value)

  override def asFloat: Float =
    throw new InvalidRecordValueTypeException(actual = valueType, attempted = FLOAT, value = value)

  override def asDouble: Double =
    throw new InvalidRecordValueTypeException(actual = valueType, attempted = DOUBLE, value = value)

  override def isPrimitive: Boolean = true

  def parseISO8601DateTime: Long = {
    OffsetDateTime.parse(value, java.time.format.DateTimeFormatter.ISO_DATE_TIME).toEpochSecond
  }
  def parseISO8601Date: Long = {
    LocalDate.parse(value, java.time.format.DateTimeFormatter.ISO_DATE).toEpochDay
  }
}
