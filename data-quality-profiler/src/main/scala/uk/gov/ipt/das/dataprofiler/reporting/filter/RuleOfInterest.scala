package uk.gov.ipt.das.dataprofiler.reporting.filter

import uk.gov.ipt.das.dataprofiler.reporting.MetricsFilter

case class RuleOfInterest(ruleName: String) extends MetricsFilter {
  override def getFilterExpression: String = s"`featureName` == '$ruleName'"
}
