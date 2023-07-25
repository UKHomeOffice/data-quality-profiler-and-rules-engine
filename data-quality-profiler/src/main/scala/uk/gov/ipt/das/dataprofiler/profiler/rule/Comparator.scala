package uk.gov.ipt.das.dataprofiler.profiler.rule

sealed trait Comparator
object Comparator {
  case object BEFORE extends Comparator
  case object AFTER extends Comparator
}