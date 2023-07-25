package uk.gov.ipt.das.dataprofiler.feature

case class FeatureDefinition(
                              `type`: String, // "DQ"
                              name: String // "HIGHGRAIN"
                            ) {

  private val fieldName: String = s"${`type`}_$name" // DQ_HIGHGRAIN

  def getFieldName: String = fieldName
}
object FeatureDefinition {
  def fromName(name: String): FeatureDefinition =
    FeatureDefinition(`type` = "DQ", name = name)
}