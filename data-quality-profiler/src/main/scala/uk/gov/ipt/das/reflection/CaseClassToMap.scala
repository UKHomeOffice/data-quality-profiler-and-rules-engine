package uk.gov.ipt.das.reflection

import org.slf4j.LoggerFactory
import scala.collection.JavaConverters.mapAsJavaMapConverter

trait CaseClassToMap {

  private val logger = LoggerFactory.getLogger(getClass)

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private def convert(value: Any)(implicit javaMaps: Boolean = false): Any = {
    logger.debug("CaseClassToMap.convert: {}, javaMaps? {}", value, javaMaps)
    value match {
      case p: Product =>
        logger.debug("CaseClassToMap.convert: {}, matched Product", value)
        val map = caseClassToMap(p)
        if (javaMaps) map.asJava else map
      case a: Array[_] =>
        logger.debug("CaseClassToMap.convert: {}, matched Array", value)
        a.map(convert)
      case l: List[_] =>
        logger.debug("CaseClassToMap.convert: {}, matched List", value)
        l.map(convert)
      case s: Seq[_] =>
        logger.debug("CaseClassToMap.convert: {}, matched Seq", value)
        s.map(convert)
      case m: Map[_,_] =>
        logger.debug("CaseClassToMap.convert: {}, matched Map", value)
        val map = m.map { case (k,v) => (k, convert(v)) }
        if (javaMaps) map.asJava else map
      case o =>
        logger.debug("CaseClassToMap.convert: {}, matched o", value)
        o
    }
  }

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  def caseClassToMap(caseClass: Product)(implicit javaMaps: Boolean = false): Map[String, Any] = {
    logger.debug("CaseClassToMap.caseClassToMap: {}", caseClass)

    val values = caseClass.productIterator.to.toList

    logger.debug("CaseClassToMap.caseClassToMap, values: {}", values)

    val converted = values.map(convert)

    logger.debug("CaseClassToMap.caseClassToMap, converted: {}", converted)

    val convertedMap = caseClass.getClass.getDeclaredFields.map(_.getName).zip(converted).toMap

    logger.debug("CaseClassToMap.caseClassToMap, convertedMap: {}", convertedMap)

    convertedMap
  }

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  def caseClassToJavaMap(caseClass: Product): java.util.Map[String, Any] = {
    logger.debug("CaseClassToMap.caseClassToJavaMap: {}", caseClass)
    caseClassToMap(caseClass)(javaMaps = true).asJava
  }

}
