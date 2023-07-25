package uk.gov.ipt.das.dataprofiler.profiler.input.record

import uk.gov.ipt.das.dataprofiler.profiler.input.record.keypreprocessor.KeyPreProcessor
import uk.gov.ipt.das.dataprofiler.value.{ARRAY, BOOLEAN, DOUBLE, FLOAT, INT, LONG, NULL, RECORD, RecordValue, STRING}

case class RecordFlattener private (keyPreProcessor: KeyPreProcessor) {

  def flatten(record: ProfilableRecord): FlattenedProfilableRecord =
    FlattenedProfilableRecord(
      id = record.getId.getOrElse("_UNKNOWN"),
      flatValues = parseBranch(flatPath = "", fullyQualifiedPath = "", record = record),
      additionalIdentifiers = record.additionalIdentifiers
    )

  private def parseValue(flatPath: String, fullyQualifiedPath: String, v: RecordValue): Seq[FlatValue] =
    v.valueType match {
      case NULL | STRING | BOOLEAN | INT | LONG | FLOAT | DOUBLE => List(FlatValue(flatPath, fullyQualifiedPath, v))
      case ARRAY => v.asArray.zipWithIndex.flatMap { case (arrV: RecordValue, index: Int) =>
        parseValue(s"$flatPath[]", s"$fullyQualifiedPath[$index]", arrV)
      }
      case RECORD => parseBranch(flatPath, fullyQualifiedPath, v.asRecord)
    }

  private def stringFilter(key: String): String =
    keyPreProcessor.keyPreProcessor(key)

  private def parseBranch(flatPath: String, fullyQualifiedPath: String, record: ProfilableRecord): Seq[FlatValue] = {
    def genPath(path: String, key: String): String =
      if (path == "") stringFilter(key) else s"$path.${stringFilter(key)}"

    record.getEntries.flatMap { case (key: String, value: RecordValue) =>
      parseValue(genPath(flatPath, key), genPath(fullyQualifiedPath, key), value)
    }
  }

}
object RecordFlattener {
  def apply(keyPreProcessor: KeyPreProcessor): RecordFlattener =
    new RecordFlattener(keyPreProcessor)
}