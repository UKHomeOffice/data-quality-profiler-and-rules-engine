package uk.gov.ipt.das.dataprofiler.profiler.rule.mask

import uk.gov.ipt.das.dataprofiler.feature.{FeatureDefinition, FeatureOutput}
import uk.gov.ipt.das.dataprofiler.profiler.rule.FieldBasedMask
import uk.gov.ipt.das.dataprofiler.value.{NullValue, RecordValue}
trait MaskProfiler {
  def profile(value: RecordValue): RecordValue

  def getDefinition: FeatureDefinition

  def getFeatureOutput(value: RecordValue): FeatureOutput = {
    val featureValue = profile(value)
    if (featureValue == null) {
      FeatureOutput(feature = getDefinition, value = NullValue())
    } else {
      FeatureOutput(feature = getDefinition, value = featureValue)
    }
  }

  def asMask: FieldBasedMask =
    FieldBasedMask(this)
}

@SuppressWarnings(Array("org.wartremover.warts.Serializable"))
object MaskProfiler {
  val defaultProfilers: Seq[MaskProfiler] = Seq(
    HighGrainProfile(),
    LowGrainProfile(),
  )
}