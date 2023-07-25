package uk.gov.ipt.das.dataprofiler.writer

import com.amazonaws.services.s3.AmazonS3
import org.apache.spark.sql.SparkSession
import uk.gov.ipt.das.dataprofiler.dataframe.profiler.output.GenericDataFrame
import uk.gov.ipt.das.dataprofiler.feature.FeatureCollection

case class JsonInjectedDQWriter(featureCollection: FeatureCollection,
                                path: String,
                                sparkSession: SparkSession) {
  def writeToJson(): Unit = {
    import sparkSession.implicits._
    featureCollection.asJSONStrings().map { record => record._2}.write.json(path)
  }

  def writeToIdJsonDataFrame(outputBucket: String, outputPrefix: String)(implicit amazonS3: AmazonS3): Unit = {
    GenericDataFrame(Option(featureCollection.asJSONStrings().toDF()), "id-json-profiled-data")
      .write(S3Writer(outputBucket, outputPrefix), partitionOn = None)
  }
}
