package uk.gov.ipt.das.dataprofiler.parser

import com.dslplatform.json._
import com.dslplatform.json.runtime.Settings
import org.scalatest.funspec.AnyFunSpec
import uk.gov.ipt.das.dataprofiler.identifier.{AdditionalIdentifier, AdditionalIdentifiers, IdentifierSource}
import uk.gov.ipt.das.dataprofiler.profiler.ProfilerConfiguration
import uk.gov.ipt.das.dataprofiler.profiler.input.reader.json.{IdentifierPaths, JsonInputReader}
import uk.gov.ipt.das.dataprofiler.profiler.input.record.FlattenedRecords
import uk.gov.ipt.das.dataprofiler.profiler.rule.{FieldBasedMask, RecordSets}
import uk.gov.ipt.das.dataprofiler.profiler.rule.mask.HighGrainProfile
import uk.gov.ipt.das.dataprofiler.wrapper.SparkSessionTestWrapper

import java.io.ByteArrayOutputStream

class IdentifierTests extends AnyFunSpec with SparkSessionTestWrapper {

  it("Lookup single-depth JSONPaths in ProfilableRecords") {
    val jsonStrings = Seq(
      """{
        |  "ID": "1234",
        |  "name": "Edward"
        |}
        |""".stripMargin,
      """{
        |  "ID": "5678",
        |  "name": "Thomas"
        |}
        |""".stripMargin,
      """{
        |  "ID": "9012",
        |  "name": "John"
        |}
        |""".stripMargin,
    )

    val records = JsonInputReader.fromJsonStrings(spark = spark, jsonStrings = jsonStrings, idPath = Option("ID"))

    val results = records.records.collect()

    results.foreach({ record =>
      println(s"${record.getId} - >${record.toString()}")
    })

    assert(results.filter(_.getId.get == "1234").head.getEntries.filter(_._1 == "name").head._2.asString === "Edward")
    assert(results.filter(_.getId.get == "5678").head.getEntries.filter(_._1 == "name").head._2.asString === "Thomas")
    assert(results.filter(_.getId.get == "9012").head.getEntries.filter(_._1 == "name").head._2.asString === "John")
  }

  it("Lookup deep JSONPaths in ProfilableRecords") {
    val jsonStrings = Seq(
      """{
        |  "ID": "1234",
        |  "person": {
        |   "name": "Edward"
        |  }
        |}
        |""".stripMargin,
      """{
        |  "ID": "5678",
        |  "person": {
        |    "name": "Thomas"
        |  }
        |}
        |""".stripMargin,
      """{
        |  "ID": "9012",
        |  "person": {
        |    "name": "John"
        |  }
        |}
        |""".stripMargin,
    )

    val records = JsonInputReader.fromJsonStrings(spark = spark, jsonStrings = jsonStrings, idPath = Option("person.name"))

    val results = records.records.collect()

    results.foreach({ record =>
      println(s"${record.getId} - >${record.toString()}")
    })

    assert(results.filter(_.getId.get == "Edward").head.getEntries.filter(_._1 == "ID").head._2.asString === "1234")
    assert(results.filter(_.getId.get == "Thomas").head.getEntries.filter(_._1 == "ID").head._2.asString === "5678")
    assert(results.filter(_.getId.get == "John").head.getEntries.filter(_._1 == "ID").head._2.asString === "9012")
  }

  it("Resolved additional identifiers using the DSL-JSON parser") {

    val identifierPaths = IdentifierPaths(
      "recordType" -> IdentifierSource.direct("metadata.type"),
      "personCohort" -> IdentifierSource.direct("person.group.cohort")
    )

    val jsonStrings = Seq(
    """{
      |  "metadata": {
      |    "type": "person_record_update",
      |    "created": "2022-01-02"
      |  },
      |  "person": {
      |    "id": "PERSON1",
      |    "group": {
      |      "cohort": "interns",
      |      "age_group": "18-30"
      |    }
      |  },
      |  "status": "employed"
      |}
      |""".stripMargin,
      """{
        |  "metadata": {
        |    "type": "person_hired",
        |    "created": "2022-01-03"
        |  },
        |  "person": {
        |    "id": "PERSON2",
        |    "group": {
        |      "cohort": "managers",
        |      "age_group": "40-50"
        |    }
        |  },
        |  "status": "onboarding"
        |}
        |""".stripMargin,
      """{
        |  "metadata": {
        |    "type": "person_left",
        |    "created": "2022-01-04"
        |  },
        |  "person": {
        |    "id": "PERSON3",
        |    "group": {
        |      "cohort": "employees",
        |      "age_group": "50-60"
        |    }
        |  },
        |  "status": "offboarding"
        |}
        |""".stripMargin,
    )

    val records = JsonInputReader.fromJsonStrings(
      spark = spark,
      jsonStrings = jsonStrings,
      idPath = Option("person.id"),
      identifierPaths = identifierPaths
    )

    val results = records.records.collect()

    results.foreach({ record =>
      println(s"${record.getId} - >${record.toString()}")
    })

    assert(results.filter(_.getId.get == "PERSON1").head.additionalIdentifiers.values ===
      List(AdditionalIdentifier("recordType" , "person_record_update"), AdditionalIdentifier("personCohort" , "interns")))
    assert(results.filter(_.getId.get == "PERSON2").head.additionalIdentifiers.values ===
      List(AdditionalIdentifier("recordType" , "person_hired"), AdditionalIdentifier("personCohort" , "managers")))
    assert(results.filter(_.getId.get == "PERSON3").head.additionalIdentifiers.values ===
      List(AdditionalIdentifier("recordType" , "person_left"), AdditionalIdentifier("personCohort" , "employees")))

  }
  it("Check nulls are created when identifer does not exist") {

    val identifierPaths = IdentifierPaths(
      "recordType" -> IdentifierSource.direct("metadata.type"),
      "personCohort" -> IdentifierSource.direct("person.group.cohort")
    )

    val jsonStrings = Seq(
      """{
        |  "metadata": {
        |     "created": "2022-01-02"
        |  },
        |  "person": {
        |    "id": "PERSON1",
        |    "group": {
        |      "cohort": "interns",
        |      "age_group": "18-30"
        |    }
        |  },
        |  "status": "employed"
        |}
        |""".stripMargin
    )

    val records = JsonInputReader.fromJsonStrings(
      spark = spark,
      jsonStrings = jsonStrings,
      idPath = Option("person.id"),
      identifierPaths = identifierPaths
    )

    val results = records.records.collect()

    results.foreach({ record =>
      println(s"${record.getId} - >${record.toString()}")
    })

    // assertion to check recordType is null as it does not exist
    assert(results.filter(_.getId.get == "PERSON1").head.additionalIdentifiers.values ===
      List(AdditionalIdentifier("recordType" ,"_UNKNOWN"), AdditionalIdentifier("personCohort" , "interns")))

  }
  it("Check additional identifiers encode to string and decode from string") {

    val identifierPaths = IdentifierPaths(
      "recordType" -> IdentifierSource.direct("metadata.type"),
      "personCohort" -> IdentifierSource.direct("person.group.cohort")
    )

    val jsonStrings = Seq(
      """{
        |  "metadata": {
        |     "type": "person_left",
        |     "created": "2022-01-02"
        |  },
        |  "person": {
        |    "id": "PERSON1",
        |    "group": {
        |      "cohort": "interns",
        |      "age_group": "18-30"
        |    }
        |  },
        |  "status": "employed"
        |}
        |""".stripMargin
    )

    val records = JsonInputReader.fromJsonStrings(
      spark = spark,
      jsonStrings = jsonStrings,
      idPath = Option("person.id"),
      identifierPaths = identifierPaths
    )

    val results = records.records.collect()

    // serialise Additional Identifiers as Json
    implicit val dslJson: DslJson[Any] = new DslJson[Any](Settings.withRuntime().includeServiceLoader()
      .`with`(new ConfigureScala))
    val outputStream = new ByteArrayOutputStream()
    dslJson.encode[AdditionalIdentifiers](results.head.additionalIdentifiers,outputStream)
    val additionalIdentifiersStr = outputStream.toString("UTF-8")

    println(additionalIdentifiersStr)
    assert(additionalIdentifiersStr === "{\"values\":[{\"name\":\"recordType\",\"value\":\"person_left\"},{\"name\":\"personCohort\",\"value\":\"interns\"}]}")


    val JsonInput = AdditionalIdentifiers.decodeFromString(additionalIdentifiersStr)
    println(JsonInput)
    JsonInput.values.map{
      testList =>
        assert(testList.name === "recordType" || testList.name === "personCohort")
        assert(testList.value === "person_left" || testList.value === "interns")
    }
  }

  it("gets metrics also grouped by an additional identifier") {
    val jsonStrings = Seq(
      """{
        |  "id": "RECORD0",
        |  "value": "foo",
        |  "group": "group1"
        |}
        |""".stripMargin,
      """{
        |  "id": "RECORD1",
        |  "value": "bar",
        |  "group": "group1"
        |}
        |""".stripMargin,
      """{
        |  "id": "RECORD2",
        |  "value": "foo",
        |  "group": "group2"
        |}
        |""".stripMargin,
      """{
        |  "id": "RECORD3",
        |  "value": "qux",
        |  "group": "group2"
        |}
        |""".stripMargin,
      """{
        |  "id": "RECORD4",
        |  "value": "quxx",
        |  "group": "group2"
        |}
        |""".stripMargin,
      """{
        |  "id": "RECORD4",
        |  "value": "quxxx",
        |  "group": "group2"
        |}
        |""".stripMargin,
    )

    val identifierPaths = IdentifierPaths("group" -> IdentifierSource.direct("group"))

    val records = JsonInputReader.fromJsonStrings(
      spark = spark,
      jsonStrings = jsonStrings,
      idPath = Option("id"),
      identifierPaths = identifierPaths
    )

    val recordSets = RecordSets(
      "testSet" -> FlattenedRecords(records)
    )

    val results = ProfilerConfiguration(
      recordSets = recordSets,
      rules = FieldBasedMask(HighGrainProfile())
    ).executeMulti().head._2

    val metrics = results.getMetrics("group")

    metrics.dataFrame.get.show(numRows = 1000, truncate = false)
  }

}
