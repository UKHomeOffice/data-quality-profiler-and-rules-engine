package uk.gov.ipt.das.dataprofiler.profiler.rule.mask.logic

import uk.gov.ipt.das.dataprofiler.profiler.rule.mask.BuiltIn
import uk.gov.ipt.das.dataprofiler.profiler.rule.mask.value.{DateString, TimestampString}
import uk.gov.ipt.das.dataprofiler.value.{NullValue, RecordValue, StringValue}

import java.time.LocalDate
@SuppressWarnings(Array("org.wartremover.warts.Null"))
case class StringDateAfter(name: String,
                           afterDate: LocalDate,
                           nullAllowed: Boolean) extends BuiltIn {
  override def rule: RecordValue => Boolean = {
    case null | NullValue() => nullAllowed
    case StringValue(DateString(d)) => d.isAfter(afterDate)
    case _ => false
  }
}