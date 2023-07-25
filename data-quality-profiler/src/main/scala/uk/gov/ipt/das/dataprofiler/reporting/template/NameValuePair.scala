package uk.gov.ipt.das.dataprofiler.reporting.template

import uk.gov.ipt.das.dataprofiler.reporting.template.mixin.SplitLongWords

case class NameValuePair private (name: String, value: String, comma: String) {
  def withNoComma: NameValuePair = this.copy(comma = "")
}
object NameValuePair extends SplitLongWords {
  def apply(name: String, value: String): NameValuePair =
    new NameValuePair(slw(name), slw(value), ", ")
}