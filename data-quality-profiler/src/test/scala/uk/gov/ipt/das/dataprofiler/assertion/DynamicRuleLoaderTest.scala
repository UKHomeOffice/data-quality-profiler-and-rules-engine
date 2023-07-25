package uk.gov.ipt.das.dataprofiler.assertion

import org.reflections.Reflections
import org.reflections.scanners.Scanners
import org.scalatest.funspec.AnyFunSpec
import uk.gov.ipt.das.dataprofiler.profiler.rule.mask.BuiltIn
import uk.gov.ipt.das.reflection.ClassReflection

import scala.collection.JavaConverters._

class DynamicRuleLoaderTest extends AnyFunSpec with ClassReflection {

  it("finds some builtin rules") {

    val p = "uk.gov.ipt.das.dataprofiler.profiler.rule.mask.builtin"
    val t = new Reflections(p).getAll(Scanners.TypesAnnotated).asScala.filter { _.startsWith(p) }
    val builtInFunctions = t.map{ className => instanceByClassName[BuiltIn](name = className).mapPair }

    println(builtInFunctions)

    assert(builtInFunctions.nonEmpty)
  }

}