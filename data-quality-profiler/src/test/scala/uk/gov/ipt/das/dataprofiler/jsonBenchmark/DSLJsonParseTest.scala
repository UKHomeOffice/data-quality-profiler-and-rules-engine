package uk.gov.ipt.das.dataprofiler.jsonBenchmark

import org.scalatest.funspec.AnyFunSpec
import uk.gov.ipt.das.dataprofiler.profiler.input.reader.json.JsonInputReader
import uk.gov.ipt.das.mixin.TimerWrapper

import java.nio.file.{Files, Paths}
import scala.collection.JavaConverters._

class DSLJsonParseTest extends AnyFunSpec with TimerWrapper {

  private val typesJson =
    """{"stringPopulated": "aString",
      | "stringEmpty": "",
      | "aNull": null,
      | "booleanTrue": true,
      | "booleanFalse": false,
      | "aDouble": 1.1111111,
      | "aLong": 9223372036854775807,
      | "aInt": 1,
      | "aObject": {"subKey": "subValue" },
      | "aArray": ["el1", "el2"]
      |}
      |""".stripMargin

  it("parses all JSON datatypes") {
    val record = JsonInputReader.parseString(typesJson).withId("A_ID")
    println(record)
  }

  it("runs in a timed loop for profiling") {
    // from: https://api.cqc.org.uk/public/v1/locations/
    val inputFile = "src/test/resources/test-data/cqc_locations.json"

    val jsonString = Files.readAllLines(Paths.get(inputFile)).asScala.mkString("\n")

    timer("DSL Loop test", iterations = 100000) {
      JsonInputReader.parseString(jsonString).withId("A_ID")
    }
  }
}
