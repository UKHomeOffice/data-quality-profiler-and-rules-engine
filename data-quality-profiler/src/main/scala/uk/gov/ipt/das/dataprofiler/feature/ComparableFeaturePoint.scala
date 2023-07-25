package uk.gov.ipt.das.dataprofiler.feature

/** FeaturePoint without OriginalValue, suitable for reducing FeaturePoint for counting etc. */
case class ComparableFeaturePoint(
                                   flatPath: String, // e.g.: key.subkey.array[].subArraykey
                                   featureDefinition: String,
                                   featureValue: String,
                                   additionalGroupByValues: Seq[String]
                                 )