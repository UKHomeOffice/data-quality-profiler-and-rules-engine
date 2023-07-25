package uk.gov.ipt.das.dataprofiler.feature

import uk.gov.ipt.das.dataprofiler.value.RecordValue

case class FeatureOutput (feature: FeatureDefinition, value: RecordValue) {
  def getValueAsString: String =
    value.valueAsString
}