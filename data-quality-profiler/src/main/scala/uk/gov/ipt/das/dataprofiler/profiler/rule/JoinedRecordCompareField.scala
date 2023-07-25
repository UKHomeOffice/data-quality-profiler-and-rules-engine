package uk.gov.ipt.das.dataprofiler.profiler.rule

import org.apache.spark.sql.Dataset
import uk.gov.ipt.das.dataprofiler.feature.{FeatureDefinition, FeatureOutput, FeaturePoint}
import uk.gov.ipt.das.dataprofiler.spark.Implicits._
import uk.gov.ipt.das.dataprofiler.identifier.AdditionalIdentifiers
import uk.gov.ipt.das.dataprofiler.value.BooleanValue

import java.util.regex.Pattern

/**
 * "Type C" profile rule
 */
@SuppressWarnings(Array("org.wartremover.warts.Throw"))
//TODO can we replace this with log.errors instead of an exception
case class JoinedRecordCompareField(definitionName: String,
                                    primaryRecordSet: String,
                                    joinedRecordSet: String,
                                    primaryJoinPath: String,
                                    joinedJoinPath: String,
                                    primaryComparisonPath: String,
                                    joinedComparisonPath: String) extends ProfileRule {

  private val definition: FeatureDefinition =
    FeatureDefinition(
      `type` = "DQ", name = definitionName
    )

  override def profile(profilableRecordSets: RecordSets): Seq[(String, Dataset[FeaturePoint])] = {

    def filterPath(path: String): Option[Seq[Pattern]] = {
      Option(List(
        Pattern.compile("^" + path + "$"),
      ))
    }

    val primaryRecordsJoin = profilableRecordSets.recordSets.getOrElse(primaryRecordSet, { throw new Exception(s"Could not find primaryRecordSet: $primaryRecordSet in JoinedRecordCompareField") })
      .filterByPaths(filterPath(primaryJoinPath))

    val joinedRecordsJoin = profilableRecordSets.recordSets.getOrElse(joinedRecordSet, { throw new Exception(s"Could not find joinedRecordSet: $joinedRecordSet in JoinedRecordCompareField") })
      .filterByPaths(filterPath(joinedJoinPath))

    val primaryRecordsComparison = profilableRecordSets.recordSets.getOrElse(primaryRecordSet, { throw new Exception(s"Could not find primaryRecordSet: $primaryRecordSet in JoinedRecordCompareField") })
      .filterByPaths(filterPath(primaryComparisonPath))

    val joinedRecordsComparison = profilableRecordSets.recordSets.getOrElse(joinedRecordSet, { throw new Exception(s"Could not find joinedRecordSet: $joinedRecordSet in JoinedRecordCompareField") })
      .filterByPaths(filterPath(joinedComparisonPath))

    val joinedRecordIdsSubquery =
      s"""SELECT ${primaryRecordsJoin.viewName}.${primaryRecordsJoin.idColumn} AS primary,
         |       ${joinedRecordsJoin.viewName}.${joinedRecordsJoin.idColumn} AS joined
         |
         | FROM ${primaryRecordsJoin.viewName}
         |
         | JOIN ${joinedRecordsJoin.viewName} ON
         |      (${primaryRecordsJoin.viewName}.${primaryRecordsJoin.valueColumn} =
         |           ${joinedRecordsJoin.viewName}.${joinedRecordsJoin.valueColumn})
         |""".stripMargin

    val comparisonQuery =
      s"""SELECT ${primaryRecordsComparison.viewName}.${primaryRecordsComparison.idColumn},
         |       ${primaryRecordsComparison.viewName}.${primaryRecordsComparison.valueColumn},
         |       ${joinedRecordsComparison.viewName}.${primaryRecordsComparison.valueColumn},
         |       ${primaryRecordsComparison.viewName}.${primaryRecordsComparison.flatPathColumn},
         |       ${primaryRecordsComparison.viewName}.${primaryRecordsComparison.additionalIdentifiersColumn}
         |
         | FROM ($joinedRecordIdsSubquery) AS joinedView
         |
         | LEFT JOIN ${primaryRecordsComparison.viewName} ON
         |           (${primaryRecordsComparison.viewName}.${primaryRecordsComparison.idColumn} =
         |                joinedView.primary)
         |
         | LEFT JOIN ${joinedRecordsComparison.viewName} ON
         |           (${joinedRecordsComparison.viewName}.${joinedRecordsComparison.idColumn} =
         |                joinedView.joined)
         |""".stripMargin
    @SuppressWarnings(Array("org.wartremover.warts.IterableOps"))
    //TODO is it possible to implicitly get the session??
    val spark = profilableRecordSets.recordSets.head._2.records.sparkSession
    Seq(definitionName -> spark.sql(comparisonQuery).map { row =>

      val (recordId, originalValue, joinedValue, flatPath, additionalIdentifiers: AdditionalIdentifiers) =
        (row.getString(0), row.getString(1), row.getString(2), row.getString(3), AdditionalIdentifiers.decodeFromString(row.getString(4)))

      // NB: this FeaturePoint is attached to the primary, so all of the metadata is from there.
      FeaturePoint(
        recordId = recordId,
        path = flatPath,
        originalValue = originalValue,
        feature = FeatureOutput(
          feature = definition,
          value = BooleanValue(joinedValue == originalValue)
        ),
        recordSet = primaryRecordSet,
        additionalIdentifiers = additionalIdentifiers
      )
    })
  }

  override def allowsFilterByRecordSets: Boolean = false
  override def allowsFilterByPaths: Boolean = false
  override def allowsArrayQueryPaths:Boolean = false
}
