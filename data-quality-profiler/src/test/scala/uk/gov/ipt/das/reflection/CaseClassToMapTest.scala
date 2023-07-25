package uk.gov.ipt.das.reflection

import org.scalatest.funspec.AnyFunSpec

class CaseClassToMapTest extends AnyFunSpec with CaseClassToMap {

  it("converts a single depth case class to a kv map") {
    case class SimpleCC(a: String, b: Int)

    val values = SimpleCC("a", 1)

    val expectedMap = Map("a" -> "a", "b" -> 1)

    assert(caseClassToMap(values) === expectedMap)
  }

  it("converts nested case classes to nested kv maps") {
    case class NestedCC(a: SimpleCC)
    case class SimpleCC(a: String, b: Int)

    val values = NestedCC(a = SimpleCC("a", 1))

    val expectedMap = Map("a" -> Map("a" -> "a", "b" -> 1))

    assert(caseClassToMap(values) === expectedMap)
  }

  it("converts nested case classes with nested lists/arrays to nested kv maps") {
    case class NestedCC(a: Array[SimpleCC])
    case class SimpleCC(a: String, b: Int)

    val values = NestedCC(a = Array(SimpleCC("a", 1)))

    val expectedMap = Map("a" -> Array(Map("a" -> "a", "b" -> 1)))

    // can't do Array to Array comparison easily - TODO replace with more elegant check
    val output = caseClassToMap(values)
    assert(output.keys === Set("a"))
    assert(output("a").isInstanceOf[Array[_]])
    assert(output("a").asInstanceOf[Array[_]].length === 1)
    assert(output("a").asInstanceOf[Array[_]].head.isInstanceOf[Map[_,_]])
    assert(output("a").asInstanceOf[Array[_]].head.asInstanceOf[Map[_,_]] === expectedMap("a").head)
  }

}
