package uk.gov.ipt.das.dataprofiler.reporting.filter

import uk.gov.ipt.das.dataprofiler.reporting.MetricsFilter

case class PathOfInterest(path: String) extends MetricsFilter {
  override def getFilterExpression: String = s"`featurePath` == '$path'"
}
