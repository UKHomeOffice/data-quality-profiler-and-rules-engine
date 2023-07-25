package uk.gov.ipt.das.dataprofiler.reporting.filter

import uk.gov.ipt.das.dataprofiler.reporting.MetricsFilter

case class IdentifierOfInterest(identifierName: String, identifierValue: String) extends MetricsFilter {
  override def getFilterExpression: String = s"`$identifierName` == '$identifierValue'"
}
