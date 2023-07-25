package uk.gov.ipt.das.dataprofiler.value

sealed trait ValueType

case object NULL extends ValueType

case object RECORD extends ValueType
case object ARRAY extends ValueType

case object STRING extends ValueType
case object BOOLEAN extends ValueType
case object INT extends ValueType
case object LONG extends ValueType
case object FLOAT extends ValueType
case object DOUBLE extends ValueType
