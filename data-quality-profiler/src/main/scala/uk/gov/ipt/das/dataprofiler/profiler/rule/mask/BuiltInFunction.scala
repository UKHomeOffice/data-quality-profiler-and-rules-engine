package uk.gov.ipt.das.dataprofiler.profiler.rule.mask

import org.reflections.Reflections
import org.reflections.scanners.Scanners
import uk.gov.ipt.das.dataprofiler.feature.FeatureDefinition
import uk.gov.ipt.das.dataprofiler.value.{BooleanValue, RecordValue}
import uk.gov.ipt.das.reflection.ClassReflection

import scala.collection.JavaConverters._

case class BuiltInFunction private (name: String, func: RecordValue => Boolean) extends MaskProfiler {
  override def profile(value: RecordValue): RecordValue = BooleanValue(func(value))

  override def getDefinition: FeatureDefinition =
    FeatureDefinition(`type` = "DQ", name = name)
}
@SuppressWarnings(Array("org.wartremover.warts.Throw"))
object BuiltInFunction extends ClassReflection {

  def apply(name: String): MaskProfiler =
    new BuiltInFunction(
      name = name,
      func = builtInFunctions.getOrElse(name, {
        throw new Exception(s"Could not find Function with name: $name")
      })
    )

  /**
   * Gets all objects defined in the "builtin" package and dynamically calls the "apply" method
   * on them to get the subclass of the BuiltIn trait that they create.
   *
   * Asks them to return the mapPair of the name (i.e. the function name) and the rule function
   * (i.e. String => Boolean) from the underlying BuiltIn case class.
   *
   */
  val builtInFunctions: Map[String, RecordValue => Boolean] =
    loadBuiltInsFromPackage("uk.gov.ipt.das.dataprofiler.profiler.rule.mask.builtin")

  def loadBuiltInsFromPackage(packageName: String): Map[String, RecordValue => Boolean] = {
    new Reflections(packageName)
      .getAll(Scanners.TypesAnnotated).asScala.filter { _.startsWith(packageName) }
      .map{ className => instanceByClassName[BuiltIn](className).mapPair }.toMap
  }
}