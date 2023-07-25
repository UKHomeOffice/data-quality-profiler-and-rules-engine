package uk.gov.ipt.das.dataprofiler.spark

import org.apache.spark.sql.{Encoder, Encoders}
import uk.gov.ipt.das.dataprofiler.feature.{ComparableFeaturePoint, FeatureOutput, FeaturePoint, FeatureReportRow}
import uk.gov.ipt.das.dataprofiler.identifier.{AdditionalIdentifier, AdditionalIdentifiers}
import uk.gov.ipt.das.dataprofiler.profiler.input.record.{FlatValue, FlattenedProfilableRecord, ProfilableRecord}
import uk.gov.ipt.das.dataprofiler.value.{BooleanValue, RecordValue}

object Implicits {

  implicit def profilableRecordEncoder: Encoder[ProfilableRecord] = Encoders.kryo[ProfilableRecord]
  implicit def flattenedProfilableRecordEncoder: Encoder[FlattenedProfilableRecord] = Encoders.kryo[FlattenedProfilableRecord]
  implicit def flatValueEncoder: Encoder[FlatValue] = Encoders.kryo[FlatValue]
  implicit def recordValueEncoder: Encoder[RecordValue] = Encoders.kryo[RecordValue]
  implicit def featureReportRowEncoder: Encoder[FeatureReportRow] = Encoders.kryo[FeatureReportRow]
  implicit def featureOutputEncoder: Encoder[FeatureOutput] = Encoders.kryo[FeatureOutput]
  implicit def comparableFeaturePointEncoder: Encoder[ComparableFeaturePoint] = Encoders.kryo[ComparableFeaturePoint]
  implicit def featurePointEncoder: Encoder[FeaturePoint] = Encoders.kryo[FeaturePoint]
  implicit def additionalIdentifiersEncoder: Encoder[AdditionalIdentifiers] = Encoders.kryo[AdditionalIdentifiers]
  implicit def additionalIdentifierEncoder: Encoder[AdditionalIdentifier] = Encoders.kryo[AdditionalIdentifier]
  implicit def booleanValueEncoder: Encoder[BooleanValue] = Encoders.kryo[BooleanValue]

}
