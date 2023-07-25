package uk.gov.ipt.das.dataprofiler.profiler.rule.mask.value

import scala.util.control.Exception.allCatch

object IntString {
  def unapply(s: String): Option[Int] =
    allCatch.opt(s.toInt)
}