package uk.gov.ipt.das.dataprofiler.profiler.rule.mask.value

import java.time.LocalDate
import scala.util.control.Exception.allCatch

object DateString {
  def unapply(s: String): Option[LocalDate] = {
    val yearFirstFormat = allCatch.opt(LocalDate.parse(s,
      java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd")))
    // check that its not dd/MM/yyyy
    yearFirstFormat match {
      case Some(d) => Some(d)
      case None => allCatch.opt(LocalDate.parse(s,
        java.time.format.DateTimeFormatter.ofPattern("dd-MM-yyyy")))
    }
  }
}