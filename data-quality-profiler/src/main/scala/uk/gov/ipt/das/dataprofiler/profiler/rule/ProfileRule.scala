package uk.gov.ipt.das.dataprofiler.profiler.rule

import org.apache.spark.sql.Dataset
import uk.gov.ipt.das.dataprofiler.feature.FeaturePoint

import java.util.regex.Pattern
@SuppressWarnings(Array("org.wartremover.warts.Throw"))
trait ProfileRule {

  def profile(profilableRecordSets: RecordSets): Seq[(String, Dataset[FeaturePoint])]

  def allowsFilterByRecordSets: Boolean
  def allowsFilterByPaths: Boolean
  def allowsArrayQueryPaths: Boolean

  protected var parameters: ProfileRuleParameters = ProfileRuleParameters()
  def filterByRecordSets: Option[Seq[String]] = parameters.filterByRecordSets
  def filterByPaths: Option[Seq[Pattern]] = parameters.filterByPaths
  def filterByQueryPath: Option[Seq[ArrayQueryPath]] = parameters.queryPaths

  def errorOnFilterByPathsNotAllowed: Unit =
    throw new Exception("FilterByPaths not allowed on this ProfileRule")

  def errorOnArrayQueryPaths: Unit =
    throw new Exception("FilterByArrayQueryPaths not allowed on this ProfileRule")

  def errorOnFilterByRecordSetsNotAllowed: Unit =
    throw new Exception("FilterByRecordSets not allowed on this ProfileRule")


  def withParameters(parameters: ProfileRuleParameters): ProfileRule = {

    if (!allowsFilterByPaths && parameters.filterByPaths.isDefined)
      errorOnFilterByPathsNotAllowed

    if (!allowsFilterByRecordSets && parameters.filterByRecordSets.isDefined)
      errorOnFilterByRecordSetsNotAllowed

    this.parameters = parameters
    this
  }

  def withFilterByRecordsSets(filterByRecordSets: String*): ProfileRule = {
    if (!allowsFilterByRecordSets)
      errorOnFilterByRecordSetsNotAllowed

    parameters = ProfileRuleParameters(
      filterByRecordSets = Option(filterByRecordSets),
      filterByPaths = filterByPaths
    )
    this
  }

  def withFilterByPaths(filterByPaths: String*): ProfileRule = {
    if (!allowsFilterByPaths)
      errorOnFilterByPathsNotAllowed

    parameters = ProfileRuleParameters(
      filterByRecordSets = filterByRecordSets,
      filterByPaths = Option(filterByPaths.map{ s => Pattern.compile(s.replace("[]", "\\[\\]")) })
    )
    this
  }

  def withFilterByExactPaths(filterByPaths: String*): ProfileRule =
    withFilterByPaths(filterByPaths.map(exactPath => s"^$exactPath$$"):_*)

  def withArrayQueryPaths(arrayQueryPath: ArrayQueryPath*): ProfileRule = {
    if (!allowsFilterByPaths)
      errorOnArrayQueryPaths

    parameters = ProfileRuleParameters(
      filterByRecordSets = filterByRecordSets,
      filterByPaths = filterByPaths,
      queryPaths = Option(arrayQueryPath)
    )
    this
  }


}
sealed case class ProfileRuleParameters private (filterByRecordSets: Option[Seq[String]] = None,
                                                 filterByPaths: Option[Seq[Pattern]] = None,
                                                 queryPaths: Option[Seq[ArrayQueryPath]] = None)

case class ArrayQueryPath(lookupPath: String, finalPath: String)
