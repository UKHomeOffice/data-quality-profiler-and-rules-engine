package uk.gov.ipt.das.dataprofiler.performance

import org.apache.spark.sql.functions.{array, explode, lit}
import org.scalatest.funspec.AnyFunSpec
import uk.gov.ipt.das.dataprofiler.wrapper.{FileOutputWrapper, SparkSessionTestWrapper}

import java.time.{Duration, LocalDateTime}

class PerformanceTest extends AnyFunSpec with SparkSessionTestWrapper with FileOutputWrapper {

  it("benchmarks performance") {
    import spark.implicits._
    val start = LocalDateTime.now()
    val startDf = Seq(("Start_Timestamp", start.toString)).toDF("ID","Value")
    val df = spark.read.csv("src/test/resources/test-data/Sample_BasicCompanyData-2021-08-01.csv")

    // duplicate dataframe N times for performance testing
    val N = 10
    val dup_df = df.withColumn("new_col", explode(array((1 until N+1).map(lit): _*)))
    val new_df = dup_df.drop("new_col")
    new_df.show(false)

    val stop = LocalDateTime.now()
    val stopDf = Seq(("Stop_Timestamp", stop.toString)).toDF("ID","Value")
    val df1 = startDf.union(stopDf)


    val diff = Duration.between(start, stop).getSeconds
    val diffDf = Seq(("Run_Time(seconds)", diff.toString)).toDF("ID","Value")
    val df2 = df1.union(diffDf)

    val recordCount = new_df.count()
    val recordCountDf = Seq(("Records Processed", recordCount.toInt)).toDF("ID","Value")
    val df3 = df2.union(recordCountDf)

    df3.show(false)

  }

}
