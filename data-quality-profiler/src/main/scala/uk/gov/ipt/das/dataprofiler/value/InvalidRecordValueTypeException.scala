package uk.gov.ipt.das.dataprofiler.value

class InvalidRecordValueTypeException(actual: ValueType, attempted: ValueType, value: Any)
  extends Exception(
    s"""Invalid record value type Exception,
       |tried to retrieve [${if (value == null) "null" else value.toString}] as a [$attempted], but it is a [$actual].""".stripMargin)
