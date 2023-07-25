package uk.gov.ipt.das.dataprofiler.parser

import org.scalatest.funspec.AnyFunSpec
import uk.gov.ipt.das.dataprofiler.profiler.input.reader.json.JsonInputReader
import uk.gov.ipt.das.dataprofiler.wrapper.SparkSessionTestWrapper

class JsonPathLookupTest extends AnyFunSpec with SparkSessionTestWrapper {

  it("looks up jsonpath in a record") {

    val record = JsonInputReader.parseString(
      """{
        |  "a": {
        |    "nothere": "goodbye",
        |    "quite": {
        |      "notthis": "hello",
        |      "deep": {
        |        "value": "TESTVAL"
        |      },
        |      "nothereeither": "nope"
        |    }
        |  }
        |}
        |""".stripMargin)

    assert(record.lookup("a.nothere").orNull.asString === "goodbye")
    assert(record.lookup("a.quite.notthis").orNull.asString === "hello")
    assert(record.lookup("a.quite.deep.value").orNull.asString === "TESTVAL")
    assert(record.lookup("a.quite.nothereeither").orNull.asString === "nope")
  }

}
