package uk.gov.ipt.das.dataprofiler.profiler.rule.mask.logic

import uk.gov.ipt.das.dataprofiler.profiler.rule.mask.BuiltIn
import uk.gov.ipt.das.dataprofiler.profiler.rule.mask.value.DateString
import uk.gov.ipt.das.dataprofiler.value.{NullValue, RecordValue, StringValue}

import java.time.LocalDate
@SuppressWarnings(Array("org.wartremover.warts.Null"))
case class StringDateBetween(name: String,
                             afterDate: LocalDate,
                             beforeDate: LocalDate,
                             nullAllowed: Boolean) extends BuiltIn {
  override def rule: RecordValue => Boolean = {
    case null | NullValue() => nullAllowed
    case StringValue(DateString(d)) => d.isAfter(afterDate) && d.isBefore(beforeDate)
    case _ => false
  }
}