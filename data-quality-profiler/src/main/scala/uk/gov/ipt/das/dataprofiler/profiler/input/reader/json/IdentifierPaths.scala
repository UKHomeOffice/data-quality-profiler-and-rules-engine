package uk.gov.ipt.das.dataprofiler.profiler.input.reader.json

import uk.gov.ipt.das.dataprofiler.feature.FeatureCollectionEncoder
import uk.gov.ipt.das.dataprofiler.identifier.IdentifierSource

import scala.collection.immutable.ListMap

/**
 * Define Paths to identifiers - captured in output as AdditionalIdentifiers
 */
object IdentifierPaths {
  @SuppressWarnings(Array("org.wartremover.warts.Throw"))
  private def validate(paths: ListMap[String, IdentifierSource]): ListMap[String, IdentifierSource] = {
    if (paths.keys.exists { FeatureCollectionEncoder.isReservedColumn })
      throw new Exception(
        s"""Identifier Path names cannot contain reserved names from list:
           | ${FeatureCollectionEncoder.baseColumnOrder}""".stripMargin)
   paths
  }

  def apply(elems: (String, IdentifierSource)*): ListMap[String, IdentifierSource] =
    validate(ListMap[String, IdentifierSource](elems: _*))

  val empty: ListMap[String, IdentifierSource] =
    ListMap.empty[String, IdentifierSource]
}