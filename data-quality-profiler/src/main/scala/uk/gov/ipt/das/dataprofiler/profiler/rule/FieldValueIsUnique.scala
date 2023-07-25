package uk.gov.ipt.das.dataprofiler.profiler.rule

import org.apache.spark.sql.Dataset
import uk.gov.ipt.das.dataprofiler.feature.{FeatureDefinition, FeatureOutput, FeaturePoint}
import uk.gov.ipt.das.dataprofiler.spark.Implicits._
import uk.gov.ipt.das.dataprofiler.identifier.AdditionalIdentifiers
import uk.gov.ipt.das.dataprofiler.profiler.input.record.FlattenedRecords
import uk.gov.ipt.das.dataprofiler.value.BooleanValue

import java.util.regex.Pattern

/**
 * "Type B" profile rule
 */
case class FieldValueIsUnique(definitionName: String,
                              path: String) extends ProfileRule {

  private val definition: FeatureDefinition =
    FeatureDefinition(
      `type` = "DQ", name = definitionName
    )

  private val filterPath: Option[Seq[Pattern]] = {
    Option(List(
      Pattern.compile("^" + path + "$"),
    ))
  }

  override def profile(profilableRecordSets: RecordSets): Seq[(String, Dataset[FeaturePoint])] = {

    profilableRecordSets.filter(filterByRecordSets).recordSets.toSeq.map {
      case (recordSet: String, records: FlattenedRecords) =>
        val filteredRecords = records.filterByPaths(filterPath)

        val subQuery = s"""SELECT
                          |    ${filteredRecords.viewName}.${filteredRecords.valueColumn} AS value,
                          |    COUNT(*) AS valueCount
                          |    FROM ${filteredRecords.viewName}
                          |    GROUP BY value""".stripMargin

        val query =
          s"""SELECT ${filteredRecords.viewName}.${filteredRecords.idColumn},
             |       ${filteredRecords.viewName}.${filteredRecords.valueColumn},
             |       ${filteredRecords.viewName}.${filteredRecords.flatPathColumn},
             |       subQuery.valueCount,
             |       ${filteredRecords.viewName}.${filteredRecords.additionalIdentifiersColumn}
             |
             | FROM (
             |    $subQuery
             | ) AS subQuery
             |
             | LEFT JOIN ${filteredRecords.viewName} ON
             |           (${filteredRecords.viewName}.${filteredRecords.valueColumn} = subQuery.value)
             |""".stripMargin

        @SuppressWarnings(Array("org.wartremover.warts.IterableOps"))
        //TODO is it possible to implicitly get the session??
        val spark = profilableRecordSets.recordSets.head._2.records.sparkSession

        recordSet -> spark.sql(query).map { row =>

          val (recordId, originalValue, flatPath, valueCount: Long, additionalIdentifiers: AdditionalIdentifiers ) =
            (row.getString(0), row.getString(1), row.getString(2), row.getLong(3), AdditionalIdentifiers.decodeFromString(row.getString(4)))

          FeaturePoint(
            recordId = recordId,
            path = flatPath,
            originalValue = originalValue,
            feature = FeatureOutput(
              feature = definition,
              value = BooleanValue(valueCount == 1)
            ),
            recordSet = recordSet,
            additionalIdentifiers = additionalIdentifiers
          )
        }

    }
  }

  override def allowsFilterByRecordSets: Boolean = true
  override def allowsFilterByPaths: Boolean = false
  override def allowsArrayQueryPaths:Boolean = false
}
