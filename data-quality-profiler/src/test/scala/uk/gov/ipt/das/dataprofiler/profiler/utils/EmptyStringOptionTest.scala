package uk.gov.ipt.das.dataprofiler.profiler.utils

import org.scalatest.funspec.AnyFunSpec

class EmptyStringOptionTest extends AnyFunSpec {

  def process(str: String): Option[String] = {
    Option(str).collect { case x if x.trim.nonEmpty => x }
  }

  val nullStr: Option[String] = process(null)
  val emptyStr: Option[String] = process("")
  val populatedStr: Option[String] = process("foo")

  it("replaces an empty string with an Option[String] correctly") {
    assert(nullStr === None)
    assert(emptyStr === None)
    assert(populatedStr === Option("foo"))
  }

  it ("adds a new named option only when Option is defined") {

    val baseOptions = Map("path" -> "FIXED_VALUE")

    def generateOptions(pKeyOpt: Option[String]): Map[String, String] = {
      val output = baseOptions ++ pKeyOpt.fold(Map[String, String]()){ pKey => Map("partitionKeys" -> pKey) }
      println(s"generateOptions with input: $pKeyOpt, output: $output")
      output
    }

    assert(generateOptions(nullStr) === baseOptions)
    assert(generateOptions(emptyStr) === baseOptions)
    assert(generateOptions(populatedStr) === baseOptions ++ Map("partitionKeys" -> populatedStr.get))
  }

  it("gets an optional argument") {
    val args = Map[String, String](
      "emptyArg" -> "",
      "populatedArg" -> "someValue",
      "needsATrimArg" -> "    someValue    ",
    )

    def getOptionalArg(argName: String): Option[String] = {
      Option(args.contains(argName)).collect {
        case true => args(argName)
      }.collect {
        case x if x.trim.nonEmpty => x.trim
      }
    }

    assert(getOptionalArg("missingArg") === None)
    assert(getOptionalArg("emptyArg") === None)
    assert(getOptionalArg("populatedArg") === Option("someValue"))
    assert(getOptionalArg("needsATrimArg") === Option("someValue"))
  }
}
