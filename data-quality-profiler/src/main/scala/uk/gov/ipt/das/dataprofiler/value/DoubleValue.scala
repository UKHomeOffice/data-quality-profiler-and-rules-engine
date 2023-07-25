package uk.gov.ipt.das.dataprofiler.value

import uk.gov.ipt.das.dataprofiler.profiler.input.record.ProfilableRecord
@SuppressWarnings(Array("org.wartremover.warts.Throw"))
case class DoubleValue(value: Double) extends RecordValue with Serializable {
  override def valueType: ValueType = DOUBLE

  override def asRecord: ProfilableRecord =
    throw new InvalidRecordValueTypeException(actual = valueType, attempted = RECORD, value = value)

  override def asArray: Seq[RecordValue] =
    throw new InvalidRecordValueTypeException(actual = valueType, attempted = ARRAY, value = value)

  override def asString: String =
    throw new InvalidRecordValueTypeException(actual = valueType, attempted = STRING, value = value)

  override def asBoolean: Boolean =
    throw new InvalidRecordValueTypeException(actual = valueType, attempted = BOOLEAN, value = value)

  override def asInt: Int =
    throw new InvalidRecordValueTypeException(actual = valueType, attempted = INT, value = value)

  override def asLong: Long =
    throw new InvalidRecordValueTypeException(actual = valueType, attempted = LONG, value = value)

  override def asFloat: Float =
    throw new InvalidRecordValueTypeException(actual = valueType, attempted = FLOAT, value = value)

  override def asDouble: Double = value

  override def isPrimitive: Boolean = true
}

