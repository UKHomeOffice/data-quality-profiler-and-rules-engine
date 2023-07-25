package uk.gov.ipt.das.dataprofiler.feature.map

import com.dslplatform.json.runtime.Settings
import com.dslplatform.json.{ConfigureScala, DslJson, PrettifyOutputStream}
import org.apache.commons.io.output.ByteArrayOutputStream
import StringMapNode.dslJson

import java.nio.charset.StandardCharsets
import scala.collection.mutable
import scala.collection.JavaConverters._
@SuppressWarnings(Array("org.wartremover.warts.Null","org.wartremover.warts.Throw"))
//TODO parameters could be re written to deal with empty children cases.
case class StringMapNode(children: mutable.HashMap[String, StringMapNode], leafValue: String) {

  def asJSONString(pretty: Boolean = false): String = {
    val writer = dslJson.newWriter()
    val os = new ByteArrayOutputStream()

    dslJson.serializeMap(asMap.asJava, writer)

    if (pretty) {
      val pos = new PrettifyOutputStream(os)
      writer.toStream(pos)
    } else {
      writer.toStream(os)
    }

    os.toString(StandardCharsets.UTF_8)
  }

  def asMap: Map[String, AnyRef] = {
    if (leafValue != null) {
      throw new Exception("Cannot turn a StringMapNode leafValue into a Map")
    }

    children.map { case (key: String, value: StringMapNode) =>
      (key, if (value.children != null) value.asMap else value.leafValue)
    }.toMap[String, AnyRef]
  }
}
@SuppressWarnings(Array("org.wartremover.warts.Null"))
object StringMapNode {
  private val dslJson = new DslJson(Settings
    .withRuntime()
    .allowArrayFormat(true)
    .includeServiceLoader()
    .`with`(new ConfigureScala))


  def leaf(value: String): StringMapNode = StringMapNode(null, value)
  def children(children: Iterable[(String, StringMapNode)]): StringMapNode =
    StringMapNode(mutable.HashMap[String, StringMapNode](children.toSeq: _*), null)
}