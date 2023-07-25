package uk.gov.ipt.das.dataprofiler.value

import uk.gov.ipt.das.dataprofiler.profiler.input.record.ProfilableRecord
@SuppressWarnings(Array("org.wartremover.warts.Throw"))
case class ArrayValue(value: Seq[RecordValue]) extends RecordValue with Serializable {

  def filterArray: ArrayValue = ArrayValue(
    value.distinct match {
      case allNull if allNull.length == 1 && allNull.contains(NullValue()) =>
        Seq(StringValue(NotFoundValue.toString))
      case distinctMix =>
        distinctMix.filter(recordValue => recordValue != NullValue())
    })

  override def valueType: ValueType = ARRAY

  override def asRecord: ProfilableRecord =
    throw new InvalidRecordValueTypeException(actual = valueType, attempted = RECORD, value = value)

  override def asArray: Seq[RecordValue] = value

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

  override def asDouble: Double =
    throw new InvalidRecordValueTypeException(actual = valueType, attempted = DOUBLE, value = value)

  override def isPrimitive: Boolean = false
}
