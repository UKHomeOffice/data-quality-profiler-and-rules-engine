package uk.gov.ipt.das.dataprofiler.dataframe.profiler.output

import org.apache.spark.sql.DataFrame
import uk.gov.ipt.das.dataprofiler.dataframe.VersionedDataFrame

class GenericDataFrame private(dataFrameIn: Option[DataFrame], nameIn:String) extends VersionedDataFrame {

  override val dataFrame: Option[DataFrame] = dataFrameIn
  override val schemaVersion: String = "schemaV2"
  override val name: String = nameIn

}

object GenericDataFrame {
  def apply(dataFrame: Option[DataFrame], name:String): GenericDataFrame =
    new GenericDataFrame(dataFrameIn = dataFrame, nameIn = name)
}