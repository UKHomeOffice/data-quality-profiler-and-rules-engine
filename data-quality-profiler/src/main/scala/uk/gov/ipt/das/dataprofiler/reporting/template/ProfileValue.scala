package uk.gov.ipt.das.dataprofiler.reporting.template

import uk.gov.ipt.das.dataprofiler.reporting.template.mixin.SplitLongWords

case class ProfileValue private (featureValue: String, sample: String, count: String, comma: String) {
  def withNoComma: ProfileValue = this.copy(comma = "")
}
object ProfileValue extends SplitLongWords {
  def apply(featureValue: String, sample: String, count: String, comma: String = ", "): ProfileValue =
    new ProfileValue(slw(featureValue), slw(sample), count, comma)
}