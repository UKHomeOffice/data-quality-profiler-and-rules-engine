package uk.gov.ipt.das.dataprofiler.identifier

import com.dslplatform.json._
import com.dslplatform.json.runtime.Settings

import java.io.ByteArrayInputStream

case class AdditionalIdentifiers(values: Seq[AdditionalIdentifier])

object AdditionalIdentifiers {
  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private implicit val dslJson: DslJson[Any] = new DslJson[Any](Settings
    .withRuntime()
    .includeServiceLoader()
    .`with`(new ConfigureScala))

  def apply(): AdditionalIdentifiers =
    new AdditionalIdentifiers(values = Seq.empty[AdditionalIdentifier])

  def decodeFromString(str: String): AdditionalIdentifiers =
    dslJson.decode[AdditionalIdentifiers](new ByteArrayInputStream(str.getBytes("UTF-8")))

}