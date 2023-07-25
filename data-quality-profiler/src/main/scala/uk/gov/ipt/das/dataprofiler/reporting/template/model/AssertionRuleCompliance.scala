package uk.gov.ipt.das.dataprofiler.reporting.template.model

case class AssertionRuleCompliance private (featureName: String,
                                            trueCount: String,
                                            falseCount: String,
                                            percentTrue: String,
                                            backgroundcolor: String)

object AssertionRuleCompliance {
  def apply(featureName: String, trueCount: Long, falseCount: Long): AssertionRuleCompliance = {
    val percentRaw = ((trueCount.toDouble / (trueCount + falseCount).toDouble) * 100)
    val percentStr = (Math.round(percentRaw*100.0)/100.0).toString // round to 2 decimal places

    val backgroundColour = percentRaw match {
      case x if x > 95 => "69B34C"
      case x if x > 85  => "ACB334"
      case x if x > 66 => "FAB733"
      case x if x > 45 => "FF8E15"
      case x if x > 33.33 => "FF4E11"
      case _ => "FF0D0D" // 0 - 16.66
    }

    new AssertionRuleCompliance(
      featureName = featureName,
      trueCount = trueCount.toString,
      falseCount = falseCount.toString,
      percentTrue = percentStr,
      backgroundcolor = backgroundColour
    )
  }
}
