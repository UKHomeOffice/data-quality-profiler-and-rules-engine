package uk.gov.ipt.das.dataprofiler.dataframe.analysis

import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, lit, row_number}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import uk.gov.ipt.das.dataprofiler.feature.FeatureCollectionEncoder._
import uk.gov.ipt.das.dataprofiler.assertion.dataset.ReferenceDataset
import uk.gov.ipt.das.dataprofiler.dataframe.VersionedDataFrame
import uk.gov.ipt.das.dataprofiler.dataframe.profiler.output.ProfiledDataFrame

import scala.collection.JavaConverters.seqAsJavaListConverter

class InferRefDatasetDataFrame(dataFrameIn: ProfiledDataFrame,
                               referenceData: Map[String, ReferenceDataset],
                               maxRank: Int)(implicit spark: SparkSession)
  extends VersionedDataFrame with Serializable {

  val ROW_NUMBER: String = "rowNumber"
  val REF_DATASET: String = "refDataset"
  val COUNT: String = "count"

  private lazy val window = Window.partitionBy(FEATURE_PATH).orderBy(col(COUNT).desc)
  import spark.implicits._
  val baseDataFrame = baseColumnOrder.toDF
  private lazy val aggregateDataFrame =
    dataFrameIn.dataFrame.getOrElse(baseDataFrame)
      .groupBy(FEATURE_PATH, ORIGINAL_VALUE)
      .count()
      .withColumn(ROW_NUMBER, row_number.over(window) )
      .filter(col(ROW_NUMBER) <= maxRank)
      .drop(ROW_NUMBER)


  override lazy val dataFrame: Option[DataFrame] = Option(aggregateDataFrame
    .join(right = referenceDataFrame,
      joinExprs = aggregateDataFrame(ORIGINAL_VALUE) === referenceDataFrame(ReferenceDataset.FIELD_REFVALUE),
      joinType = "left")
    .drop(ReferenceDataset.FIELD_REFVALUE))


  override val schemaVersion: String = "schemaV1"
  override val name: String = "profiler-ref-data-inference"

  val nilDataSet: DataFrame = dataFrameIn.dataFrameIn.sparkSession.createDataFrame(List.empty[Row].asJava,
    StructType(Seq(StructField(ReferenceDataset.FIELD_REFVALUE, StringType), StructField(REF_DATASET, StringType))))


  private lazy val referenceDataFrame: DataFrame =
    referenceData.collect { case (refValue: String, refData: ReferenceDataset) => (refValue, refData) }
      .map { refData =>
        refData._2.df.withColumn(REF_DATASET, lit(refData._1))
      }.fold(nilDataSet)(_.union(_))
}

object InferRefDatasetDataFrame {
  def apply(dataFrameIn: ProfiledDataFrame
            ,referenceData: Map[String, ReferenceDataset]
            ,maxRank: Int)(implicit sparkSession: SparkSession): InferRefDatasetDataFrame =
    new InferRefDatasetDataFrame(dataFrameIn = dataFrameIn, referenceData = referenceData, maxRank = maxRank)
}




