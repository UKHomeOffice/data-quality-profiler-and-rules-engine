package uk.gov.ipt.das.dataprofiler.wrapper

import org.apache.spark.sql.{DataFrame, SparkSession}
import uk.gov.ipt.das.dataprofiler.assertion.dataset.ReferenceDataset

trait SparkSessionTestWrapper {

  lazy implicit val spark: SparkSession = {

    val sparkSession = SparkSession
      .builder()
      .master("local[8]")
      .appName("spark session")
      .config("spark.default.parallelism", "8")
      .config("spark.sql.shuffle.partitions", "8")
      .getOrCreate()

    sparkSession.sparkContext.setLogLevel("OFF")
    sparkSession.sparkContext.setCheckpointDir("/tmp/checkpoints")

    sparkSession
  }

  def datasetView(dataFrame: DataFrame): String = {
    val viewName = ReferenceDataset.randomViewName
    dataFrame.createOrReplaceTempView(viewName)
    viewName
  }

}
