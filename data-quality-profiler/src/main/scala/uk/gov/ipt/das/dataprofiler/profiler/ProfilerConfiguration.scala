package uk.gov.ipt.das.dataprofiler.profiler

import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory
import uk.gov.ipt.das.dataprofiler.feature
import uk.gov.ipt.das.dataprofiler.feature.{FeatureCollection, FeaturePoint}
import uk.gov.ipt.das.dataprofiler.spark.Implicits._
import uk.gov.ipt.das.dataprofiler.profiler.rule.{ProfileRule, RecordSets}

case class ProfilerConfiguration (recordSets: RecordSets,
                                  rules: ProfileRule*) {

  private val logger = LoggerFactory.getLogger(getClass)

  def executeMulti(): Seq[(String, FeatureCollection)] = {
    rules.zipWithIndex.flatMap { case(rule, index) =>
      logger.info(s"Executing rule in ProfilerConfiguration called: ${rule.getClass.getSimpleName}")

      rule.profile(recordSets).map { case (resultName, results) =>
        s"${rule.getClass.getSimpleName}($index)-$resultName" -> feature.FeatureCollection(results)
      }
    }
  }

  /**
   * Execute all rules and group the results by path, suitable for use in report generation
   *
   * @return featurecollections by path
   */
  def executeByPath()(implicit sparkSession: SparkSession): Map[String, FeatureCollection] =
    executeMulti().flatMap { case (ruleName, results) =>
      results.featurePoints.collect().map{ fp => (fp.path, fp) }
    }
    .groupBy{ case (path: String, v: FeaturePoint) => path }
    .map{ case (path: String, v: Seq[(String, FeaturePoint)]) =>
      val fps = v.map { case (k: String, v: FeaturePoint) => v }
      path -> FeatureCollection(sparkSession.createDataset(fps))
    }

  /**
   * Execute multi and union the datasets
   *
   * @return featurecollection of all rules
   */
  @SuppressWarnings(Array("org.wartremover.warts.IterableOps"))
  def executeAll()(implicit sparkSession: SparkSession): FeatureCollection =
    FeatureCollection(executeMulti().map { case (_, results) => results.featurePoints }.reduce(_ union _))

}