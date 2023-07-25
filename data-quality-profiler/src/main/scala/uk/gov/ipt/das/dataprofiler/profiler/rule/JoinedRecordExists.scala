package uk.gov.ipt.das.dataprofiler.profiler.rule

import com.sun.xml.bind.v2.TODO
import org.apache.spark.sql.Dataset
import uk.gov.ipt.das.dataprofiler.feature.{FeatureDefinition, FeatureOutput, FeaturePoint}

import java.util.regex.Pattern
import uk.gov.ipt.das.dataprofiler.spark.Implicits._
import uk.gov.ipt.das.dataprofiler.identifier.AdditionalIdentifiers
import uk.gov.ipt.das.dataprofiler.value.BooleanValue

/**
 * "Type C" profile rule
 */
@SuppressWarnings(Array("org.wartremover.warts.Throw"))
case class JoinedRecordExists(definitionName: String,
                              primaryRecordSet: String,
                              joinedRecordSet: String,
                              primaryJoinPath: String,
                              joinedJoinPath: String) extends ProfileRule {

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

    //TODO is it necessary to throw exception here or can we just log.error this?
    val primaryRecordsJoin = profilableRecordSets.recordSets.getOrElse(primaryRecordSet, { throw new Exception(s"Could not find primaryRecordSet: $primaryRecordSet in JoinedRecordCompareField") })
      .filterByPaths(filterPath(primaryJoinPath))

    val joinedRecordsJoin = profilableRecordSets.recordSets.getOrElse(joinedRecordSet, { throw new Exception(s"Could not find joinedRecordSet: $joinedRecordSet in JoinedRecordCompareField") })
      .filterByPaths(filterPath(joinedJoinPath))

    val query =
      s"""SELECT ${primaryRecordsJoin.viewName}.${primaryRecordsJoin.idColumn},
         |       ${primaryRecordsJoin.viewName}.${primaryRecordsJoin.valueColumn},
         |       ${joinedRecordsJoin.viewName}.${joinedRecordsJoin.valueColumn},
         |       ${primaryRecordsJoin.viewName}.${primaryRecordsJoin.flatPathColumn},
         |       ${primaryRecordsJoin.viewName}.${primaryRecordsJoin.additionalIdentifiersColumn}
         |
         | FROM ${primaryRecordsJoin.viewName}
         |
         | LEFT JOIN ${joinedRecordsJoin.viewName} ON
         |      (${primaryRecordsJoin.viewName}.${primaryRecordsJoin.valueColumn} =
         |           ${joinedRecordsJoin.viewName}.${joinedRecordsJoin.valueColumn})
         |""".stripMargin
    @SuppressWarnings(Array("org.wartremover.warts.IterableOps"))
    //TODO is it possible to implicitly get the session??
    val spark = profilableRecordSets.recordSets.head._2.records.sparkSession
    Seq(definitionName -> spark.sql(query).map { row =>

      val (recordId, originalValue, joinedValue, flatPath, additionalIdentifiers: AdditionalIdentifiers) =
        (row.getString(0), row.getString(1), row.getString(2), row.getString(3), AdditionalIdentifiers.decodeFromString(row.getString(4)))

      // NB: this FeaturePoint is attached to the primary, so all of the metadata is from there.
      FeaturePoint(
        recordId = recordId,
        path = flatPath,
        originalValue = originalValue,
        feature = FeatureOutput(
          feature = definition,
          value = BooleanValue(joinedValue != null)
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
