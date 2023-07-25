package uk.gov.ipt.das.dataprofiler.dataframe.summary

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, date_format}
import org.apache.spark.sql.types.DateType
import uk.gov.ipt.das.dataprofiler.dataframe.VersionedDataFrame
import uk.gov.ipt.das.dataprofiler.dataframe.profiler.output.ProfiledDataFrame
@SuppressWarnings(Array("org.wartremover.warts.OptionPartial"))
class SummaryDataFrame private(fieldToSummarise: String,
                               dataFrameIn: ProfiledDataFrame,
                               transformDateField: Boolean) extends VersionedDataFrame {
//TODO build a empty data frame to use when folding
  def summaryStatistics: DataFrame =
    if (!transformDateField){
      dataFrameIn.dataFrame.get.groupBy(fieldToSummarise).count()
    } else {
      val yearMonthColumn: String = s"$fieldToSummarise-YearMonth"
      dataFrameIn
        .dataFrame
        .get
        .withColumn(yearMonthColumn, date_format(col(fieldToSummarise).cast(DateType), "yyyy-MM"))
        .groupBy(yearMonthColumn)
        .count()
    }

  override lazy val dataFrame: Option[DataFrame] =  Option(summaryStatistics)
  override val schemaVersion: String = "schemaV1"
  override val name: String = s"$fieldToSummarise-summary-stats"

}

object SummaryDataFrame {

  def apply(fieldToSummarise: String, dataFrameIn: ProfiledDataFrame, transformDateField: Boolean): SummaryDataFrame =
    new SummaryDataFrame(
      fieldToSummarise = fieldToSummarise,
      dataFrameIn = dataFrameIn,
      transformDateField = transformDateField)

  def apply(fieldToSummarise: String, dataFrameIn: DataFrame, transformDateField: Boolean): SummaryDataFrame =
    new SummaryDataFrame(
      fieldToSummarise = fieldToSummarise,
      dataFrameIn = ProfiledDataFrame(dataFrameIn),
      transformDateField = transformDateField)
}
