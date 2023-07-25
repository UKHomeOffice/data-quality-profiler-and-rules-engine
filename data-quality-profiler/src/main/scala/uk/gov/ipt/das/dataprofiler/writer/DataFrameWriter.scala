package uk.gov.ipt.das.dataprofiler.writer

import uk.gov.ipt.das.dataprofiler.dataframe.VersionedDataFrame

trait DataFrameWriter {
  def writeDataFrame(dataFrame: VersionedDataFrame, partitionOn: Option[Seq[String]]): Unit
}