package uk.gov.ipt.das.dataprofiler.reporting

import uk.gov.ipt.das.dataprofiler.dataframe.profiler.output.MetricsDataFrame

trait ReportingDataSource {

  def getReportData: MetricsDataFrame

}
