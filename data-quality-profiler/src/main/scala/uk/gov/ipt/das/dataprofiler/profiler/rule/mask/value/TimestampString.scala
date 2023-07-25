package uk.gov.ipt.das.dataprofiler.profiler.rule.mask.value

import java.time.OffsetDateTime
import scala.util.control.Exception.allCatch

object TimestampString {
  def unapply(s: String): Option[OffsetDateTime] =
    allCatch.opt(OffsetDateTime.parse(s, java.time.format.DateTimeFormatter.ISO_DATE_TIME))
}