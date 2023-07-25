package uk.gov.ipt.das.dataprofiler.value

import uk.gov.ipt.das.dataprofiler.profiler.input.record.ProfilableRecord
@SuppressWarnings(Array("org.wartremover.warts.Throw","org.wartremover.warts.Null" ))
case class NullValue() extends RecordValue with Serializable {
  override def valueType: ValueType = NULL

  override def asRecord: ProfilableRecord =
    throw new InvalidRecordValueTypeException(actual = valueType, attempted = RECORD, value = null)

  override def asArray: Seq[RecordValue] =
    throw new InvalidRecordValueTypeException(actual = valueType, attempted = ARRAY, value = null)

  override def asString: String =
    throw new InvalidRecordValueTypeException(actual = valueType, attempted = STRING, value = null)

  override def asBoolean: Boolean =
    throw new InvalidRecordValueTypeException(actual = valueType, attempted = BOOLEAN, value = null)

  override def asInt: Int =
    throw new InvalidRecordValueTypeException(actual = valueType, attempted = INT, value = null)

  override def asLong: Long =
    throw new InvalidRecordValueTypeException(actual = valueType, attempted = LONG, value = null)

  override def asFloat: Float =
    throw new InvalidRecordValueTypeException(actual = valueType, attempted = FLOAT, value = null)

  override def asDouble: Double =
    throw new InvalidRecordValueTypeException(actual = valueType, attempted = DOUBLE, value = null)

  override def isPrimitive: Boolean = false
}
