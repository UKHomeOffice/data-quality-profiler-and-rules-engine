package uk.gov.ipt.das.dataprofiler.profiler.rule.mask

import uk.gov.ipt.das.dataprofiler.value.RecordValue

trait BuiltIn {
  def name: String
  def rule: RecordValue => Boolean

  def mapPair: (String, RecordValue => Boolean) = name -> rule
}