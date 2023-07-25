package uk.gov.ipt.das.dataprofiler.profiler.rule

import Comparator.{AFTER, BEFORE}
import uk.gov.ipt.das.dataprofiler.profiler.rule.mask.logic.StringMatchesPattern
import uk.gov.ipt.das.dataprofiler.value.{RecordValue, StringValue}

import java.time.format.DateTimeParseException
import java.util.regex.Pattern

object CompareDateToEventDate {
  def apply(definitionName: String,
            datePath: String,
            comparator: Comparator,
            eventDatePath: String = "event_date"): ProfileRule =
    CompareTwoFields(
      definitionName = definitionName,
      pathOne = datePath,
      pathTwo = eventDatePath,
      comparisonFunction = (dateValue, eventDateValue) =>
        try {
          comparator match {
            case BEFORE => (parseDateToNumber(dateValue) < parseDateToNumber(eventDateValue)).toString
            case AFTER => (parseDateToNumber(dateValue) > parseDateToNumber(eventDateValue)).toString
          }
        } catch {
            case e: StringIndexOutOfBoundsException => "ERROR"
            case e: DateTimeParseException => "ERROR"
        }
    )
  @SuppressWarnings(Array("org.wartremover.warts.Throw"))
  private def parseDateToNumber(dateValue: RecordValue): Long = {
    dateValue match {
      case s: StringValue => StringValue(s.asString.substring(0, 10)).parseISO8601Date // take the date portion of the timestamp and parseDate
      case _ => throw new Exception(s"Could not parse (${dateValue.valueAsString}) - it was not a StringValue")
    }
  }

}