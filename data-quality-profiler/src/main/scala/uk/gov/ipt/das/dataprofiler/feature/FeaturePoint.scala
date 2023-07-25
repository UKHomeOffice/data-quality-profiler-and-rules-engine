package uk.gov.ipt.das.dataprofiler.feature

import uk.gov.ipt.das.dataprofiler.identifier.AdditionalIdentifiers
@SuppressWarnings(Array("org.wartremover.warts.Throw"))
case class FeaturePoint private (recordId: String,
                                 path: String,
                                 originalValue: String,
                                 feature: FeatureOutput,
                                 recordSet: String,
                                 additionalIdentifiers: AdditionalIdentifiers) {

  def toComparable(additionalGroupByColumns: Seq[String]): ComparableFeaturePoint =
    ComparableFeaturePoint(
      flatPath = path,
      featureDefinition = feature.feature.getFieldName,
      featureValue = feature.getValueAsString,
      additionalGroupByValues = additionalGroupByColumns.map { colName =>
        additionalIdentifiers.values.find(ai => ai.name == colName)
          .getOrElse(throw new Exception(s"Could not get AdditionalGroupByColumn with name: $colName"))
          .value
      }
    )

}
object FeaturePoint {

  def apply(recordId: String,
            path: String,
            originalValue: String,
            feature: FeatureOutput,
            recordSet: String,
            additionalIdentifiers: AdditionalIdentifiers = AdditionalIdentifiers()): FeaturePoint =
    new FeaturePoint(
      recordId = recordId,
      path = path,
      originalValue = originalValue,
      feature = feature,
      recordSet = recordSet,
      additionalIdentifiers = additionalIdentifiers)

}
