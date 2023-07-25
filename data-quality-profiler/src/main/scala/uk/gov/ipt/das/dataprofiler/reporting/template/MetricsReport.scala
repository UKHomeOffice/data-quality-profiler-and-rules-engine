package uk.gov.ipt.das.dataprofiler.reporting.template

import org.apache.spark.sql.functions.countDistinct
import uk.gov.ipt.das.dataprofiler.feature.FeatureReportRowEncoder._
import uk.gov.ipt.das.dataprofiler.dataframe.profiler.output.MetricsDataFrame
import uk.gov.ipt.das.dataprofiler.profiler.rule.mask.{HighGrainProfile, LowGrainProfile}
import uk.gov.ipt.das.dataprofiler.reporting.charts.HistogramChart
import uk.gov.ipt.das.dataprofiler.reporting.template.mixin.ImageToDataURL
import uk.gov.ipt.das.dataprofiler.reporting.template.model.{AssertionRuleCompliance, BasicReportRowTemplate}

object MetricsReport extends ImageToDataURL {

  private val logger = org.apache.log4j.Logger.getLogger(this.getClass)

  def replaceNull(value: String): String = if (value == null) "[null]" else value
  def replaceNulls(values: Seq[String]): Seq[String] = values.map(replaceNull)

  def uniqueFieldCount(dataFrameIn: MetricsDataFrame): Option[Long] =
    dataFrameIn.dataFrame.map { dataFrame =>
      dataFrame.select(countDistinct(FEATURE_PATH)).first().getLong(0)
    }

  def generateReportTemplate(dataFrameIn: MetricsDataFrame): Option[Iterable[BasicReportRowTemplate]] = {
    // Schema is: (FEATURE_PATH, FEATURE_NAME, FEATURE_VALUE, SAMPLE_MIN, SAMPLE_MAX, SAMPLE_FIRST, SAMPLE_LAST, COUNT)

    case class SummarySource(count: Long, sampleMin: String, sampleMax: String) {
      def toSummaryArray: Array[NameValuePair] =
        Array(
          NameValuePair("Min", sampleMin),
          NameValuePair("Max", sampleMax),
          NameValuePair("Count", count.toString)
        )
    }

    case class Top10Source(counts: Seq[Long], perPathPerGrainDistinctCount: Int, featureValues: Seq[String], samples: Seq[String]) {
      def toTemplateArray: Array[ProfileValue] =
        featureValues
          .zip(counts)
          .zip(samples)
          .map { case ((featureValue: String, count: Long), sample: String) =>
            ProfileValue(featureValue = featureValue, sample = sample, count = count.toString) }
          .toArray
    }

    dataFrameIn.dataFrame.map { dataFrame =>
      import dataFrame.sparkSession.implicits._

      val VIEW_NAME = "metrics"
      val DQ_HIGHGRAIN = HighGrainProfile().getDefinition.getFieldName
      val DQ_LOWGRAIN = LowGrainProfile().getDefinition.getFieldName

      dataFrame.createOrReplaceTempView(VIEW_NAME)

      /**
       * Population size, i.e. the denominator for the percentage calculation
       *
       * Calculated by looking at the largest count, in reality
       * this will be something "required" like an identifier.
       *
       */
      val populationSize = dataFrame.sqlContext.sql(
        s"""SELECT
           |    MAX($COUNT)
           |
           |FROM
           |    $VIEW_NAME
           |
           |""".stripMargin
      ).collect().head.getLong(0)

      logger.debug(s"Population size: $populationSize")

      /**
       * Count of populated fields per path, used as the nominator for the percentage calculation.
       *
       * Min and Max original values for the summary.
       */
      val summaryPerFeaturePath = dataFrame.sqlContext.sql(
        s"""SELECT
           |    $FEATURE_PATH,
           |    SUM($COUNT), -- count up all the counts for a feature path
           |    MIN($SAMPLE_MIN),
           |    MAX($SAMPLE_MAX)
           |
           |FROM
           |    $VIEW_NAME
           |
           |WHERE
           |    $FEATURE_NAME == "$DQ_HIGHGRAIN" -- we just need a single feature name so the count is accurate
           |
           |GROUP BY
           |    $FEATURE_PATH
           |
           |""".stripMargin
      ).collect().map{ row =>
        (row.getString(0), SummarySource(row.getLong(1), row.getString(2), row.getString(3)))
      }.toMap


      def getHistogramImages(highGrain: Boolean): Map[String, String] =
        dataFrame
          .filter(row => row.getAs[String](FEATURE_NAME) == (if (highGrain) DQ_HIGHGRAIN else DQ_LOWGRAIN))
          .groupByKey(row => row.getAs[String](FEATURE_PATH))
          .mapGroups { case (featurePath, rows) =>
            val values = rows.map(_.getAs[Long](COUNT))
            val chart = new HistogramChart(values.toSeq)
            (featurePath, imageToDataURL(chart.asPNG, "png"))
          }
          .collect()
          .toMap

      def getAssertionRuleCompliance: Map[String, Array[AssertionRuleCompliance]] =
        dataFrame
          .filter(row => row.getAs[String](FEATURE_NAME) != DQ_LOWGRAIN && row.getAs[String](FEATURE_NAME) != DQ_HIGHGRAIN)
          .groupByKey(row => row.getAs[String](FEATURE_PATH))
          .mapGroups { case (featurePath, pathRows) =>
            featurePath -> pathRows.toList
              .groupBy(row => row.getAs[String](FEATURE_NAME))
              .map{ case (featureName, assertionRows) =>
                val trueCount = assertionRows.filter(row => row.getAs[String](FEATURE_VALUE) == "true").map(_.getAs[Long](COUNT)).sum
                val falseCount = assertionRows.filter(row => row.getAs[String](FEATURE_VALUE) == "false").map(_.getAs[Long](COUNT)).sum
                (featureName, trueCount, falseCount)
              }.toList
          }.collect()
          .map{ case (featurePath: String, compliance: List[(String, Long, Long)]) =>
            featurePath -> compliance.map(c => AssertionRuleCompliance(c._1, c._2, c._3)).toArray
          }
          .toMap

      def get10List(top: Boolean, highGrain: Boolean): Map[String, Top10Source] =
        dataFrame
          .filter(row => row.getAs[String](FEATURE_NAME) == (if (highGrain) DQ_HIGHGRAIN else DQ_LOWGRAIN))
          .groupByKey(row => row.getAs[String](FEATURE_PATH))
          .mapGroups { case (featurePath, rows) =>
            val orderedByCount = rows.toList.sortBy(row => row.getAs[Long](COUNT))
            val orientedOrdered = if (!top) orderedByCount else orderedByCount.reverse

            val top10rows = orientedOrdered.take(10)

            val perPathPerGrainDistinctCount = orientedOrdered.size

            (featurePath, perPathPerGrainDistinctCount, top10rows.map(row => row.getAs[Long](COUNT)), top10rows.map(row => row.getAs[String](FEATURE_VALUE)), top10rows.map(row => row.getAs[String](SAMPLE_FIRST)))
          }
          .collect()
          .map { case (featurePath, perPathPerGrainDistinctCount, counts, featureValues, sampleFirsts) =>
            // to this outside of Spark to avoid having to serialise Top10Source
            featurePath -> Top10Source(counts, perPathPerGrainDistinctCount, replaceNulls(featureValues), replaceNulls(sampleFirsts))
          }
          .toMap

      val top10HG = get10List(top = true, highGrain = true)
      val top10LG = get10List(top = true, highGrain = false)
      val bottom10HG = get10List(top = false, highGrain = true)
      val bottom10LG = get10List(top = false, highGrain = false)

      val assertionRuleCompliance = getAssertionRuleCompliance

      val highGrainHistograms = getHistogramImages(highGrain = true)
      val lowGrainHistograms = getHistogramImages(highGrain = false)

      def calcPercentage(count: Long): String = {
        val percent = (count.toDouble / populationSize.toDouble) * 100
        val rounded = Math.round(percent * 100.0) / 100.0 // round to 2 DP
        rounded.toString
      }

      summaryPerFeaturePath.map { case ( featurePath, summary ) =>
        model.BasicReportRowTemplate(
          json_path = featurePath,
          percent_populated = calcPercentage(summary.count),
          summary = summary.toSummaryArray,
          assertionRuleCompliance = assertionRuleCompliance.getOrElse(featurePath, Array()),
          highGrainHistogram = highGrainHistograms(featurePath),
          lowGrainHistogram = lowGrainHistograms(featurePath),
          hg_count = top10HG(featurePath).perPathPerGrainDistinctCount.toString,
          lg_count = top10LG(featurePath).perPathPerGrainDistinctCount.toString,
          top10_hg = top10HG(featurePath).toTemplateArray,
          top10_lg = top10LG(featurePath).toTemplateArray,
          bottom10_hg = bottom10HG(featurePath).toTemplateArray,
          bottom10_lg = bottom10LG(featurePath).toTemplateArray
        )
      }

    }
  }

}
