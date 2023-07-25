package uk.gov.ipt.das.dataprofiler.profiler.rule.mask

import OriginalValuePassthrough.definition
import uk.gov.ipt.das.dataprofiler.feature.FeatureDefinition
import uk.gov.ipt.das.dataprofiler.value.RecordValue

class OriginalValuePassthrough private extends MaskProfiler with Serializable {

  override def profile(value: RecordValue): RecordValue = value

  override def getDefinition: FeatureDefinition = definition
}
object OriginalValuePassthrough {
  val INSTANCE = new OriginalValuePassthrough

  def apply(): OriginalValuePassthrough = INSTANCE

  val definition: FeatureDefinition = FeatureDefinition(`type` = "DQ", name = "ORIGINALVALUE")
}