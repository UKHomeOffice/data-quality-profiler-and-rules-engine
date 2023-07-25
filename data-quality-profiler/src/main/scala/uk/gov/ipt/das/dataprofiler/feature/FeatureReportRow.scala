package uk.gov.ipt.das.dataprofiler.feature

case class FeatureReportRow(featurePoint: ComparableFeaturePoint,
                            sampleMin: String,
                            sampleMax: String,
                            sampleFirst: String,
                            sampleLast: String,
                            count: Long
                           ) {

  // from Tabular report creator - not really used for anything much
  def getSortKey: (String, Long, String) = {
    val comparableValue =
      if (featurePoint.featureValue == null) "" else featurePoint.featureValue

    (featurePoint.featureDefinition, 0 - count, comparableValue)
  }

}
