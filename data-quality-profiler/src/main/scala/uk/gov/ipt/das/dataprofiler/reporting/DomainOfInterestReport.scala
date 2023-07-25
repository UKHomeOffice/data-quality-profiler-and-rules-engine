package uk.gov.ipt.das.dataprofiler.reporting

import org.slf4j.LoggerFactory
import DomainOfInterestReport.logger
import uk.gov.ipt.das.dataprofiler.dataframe.profiler.output.MetricsDataFrame

class DomainOfInterestReport private (domainOfInterest: DomainOfInterest,
                                      reportData: MetricsDataFrame) extends ReportingDataSource {

  override def getReportData: MetricsDataFrame =
    MetricsDataFrame(reportData.dataFrame.map { dataFrame =>
      val query = domainOfInterest.filters.map{ _.getFilterExpression }.mkString("(", ") OR (", ")")
      logger.debug(s"DomainOfInterestReport, getReportData, query: $query")
      dataFrame.filter(query).toDF()
    })

}
object DomainOfInterestReport {

  private val logger = LoggerFactory.getLogger(getClass)

  def apply(domainOfInterest: DomainOfInterest, reportData: MetricsDataFrame): DomainOfInterestReport =
    new DomainOfInterestReport(domainOfInterest = domainOfInterest, reportData = reportData)
}