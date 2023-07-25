package uk.gov.ipt.das.dataprofiler.profiler.input.record

import uk.gov.ipt.das.dataprofiler.identifier.AdditionalIdentifier
import uk.gov.ipt.das.dataprofiler.value.{NotFoundValue, NullValue, RecordValue, StringValue}

case class ArrayFinder(arrayPath: String,
                       valuePath: String,
                       lookupPath: String,
                       lookupValue: String = "",
                       getArray: Array[Seq[FlatValue]]) {

  private def findValue: Seq[FlatValue] = {
    getArray.flatMap { values =>
      if (ruleBasedLookup(lookupPath = lookupPath, values = values, lookupValue = lookupValue)) {
        values.find(fv => fv.flatPath == valuePath)
      } else {
        None
      }
    }.toSeq.distinct
  }

  def findValueAsIdentifier(identifierName: String): Seq[AdditionalIdentifier] = {
    val valueDerivedIDs = findValue.map { fv => AdditionalIdentifier(identifierName, fv.recordValue.valueAsString) }
    if (valueDerivedIDs.isEmpty) {
      Seq(AdditionalIdentifier(identifierName, NotFoundValue.toString))
    } else {
      valueDerivedIDs
    }
  }
  @SuppressWarnings(Array("org.wartremover.warts.OptionPartial"))
  def findValueWithQueryPath: Seq[FlatValue] = {
    val Array(arrayKey, restOfPath) = lookupPath split("\\[\\].", 2)
    getArray.flatMap { flatValues =>
      //TODO need default value for lookupFlatValue
      val lookupFlatValue: Option[FlatValue] = flatValues.find { fv => fv.flatPath == lookupPath }
      val finalFlatValue = flatValues.find { fv => fv.flatPath == valuePath }
      finalFlatValue match {
        case Some(flatValue) if lookupFlatValue.isDefined =>
          Option(FlatValue(flatValue.flatPath.replace("[]",
            s"[$restOfPath=${lookupFlatValue.get.recordValue.valueAsString}]"),
            flatValue.fullyQualifiedPath,
            flatValue.recordValue))
        case Some(flatValue) if lookupFlatValue.isEmpty =>
          Option(FlatValue(flatValue.flatPath.replace("[]", s"[$restOfPath=_UNKNOWN]"),
            flatValue.fullyQualifiedPath,
            flatValue.recordValue))
        case None => None
      }
    }
  }

  private def ruleBasedLookup(lookupPath: String, lookupValue: String, values: Seq[FlatValue]): Boolean = {
    values.exists { fv =>
      fv.flatPath == lookupPath && fv.recordValue == StringValue(lookupValue)
    }
  }
}
