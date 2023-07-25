package uk.gov.ipt.das.dataprofiler.profiler.input.record

import uk.gov.ipt.das.dataprofiler.identifier.{AdditionalIdentifier, AdditionalIdentifiers}
import uk.gov.ipt.das.dataprofiler.value.{ARRAY, ArrayValue, BOOLEAN, DOUBLE, FLOAT, INT, InvalidRecordValueTypeException, LONG, NullValue, RECORD, RecordValue, STRING, ValueType}

import scala.annotation.tailrec
import scala.collection.JavaConverters._
@SuppressWarnings(Array("org.wartremover.warts.Throw","org.wartremover.warts.Any","org.wartremover.warts.Null","org.wartremover.warts.Return"))
case class ProfilableRecord private (id: Option[String],
                                     entries: Seq[(String, RecordValue)],
                                     additionalIdentifiers: AdditionalIdentifiers) extends RecordValue {


  @tailrec
  private def arrayValueFinder(recordArray: Seq[RecordValue], searchKey: String): Seq[RecordValue] = {
    val keys = searchKey split("[.]", 2)
    val arrayKey = keys.head
    val restOfPath = keys.drop(1).headOption // .get will error if it doesn't exist, BUT shouldn't be a RECORD type if there is no restOfPath //TODO throw our own exception or return different type?
    val recordValues = recordArray.map { record =>
      record.asRecord.entries
        .find(profilableRecord => arrayKey == profilableRecord._1)
        .map { recordPair => recordPair._2 }
        .fold(NullValue(): RecordValue)(recordValue => recordValue)
    }
    restOfPath match {
      case Some(path) if path.contains(".") => arrayValueFinder(recordValues, searchKey = path)
      case Some(path) =>
        recordValues.map { recordEntry =>
          if (recordEntry.valueType == RECORD) {
            recordEntry.asRecord.entries
              .find { case (key: String, _: RecordValue) => key == path }
              .fold(NullValue(): RecordValue) { nestedRecord => nestedRecord._2 }
          }
          else recordEntry
        }
      case None => recordValues
    }
  }

  def nestedArrayLookup(profilableRecordArray: Seq[(String, RecordValue)],
                        jsonPath: String,
                        lastKey: String): Option[RecordValue] = {

    val keys = jsonPath split("\\[\\].", 2)
    val thisKey = keys.head

    keys.drop(1).headOption match {
      case Some(restOfPath) =>
        val values = profilableRecordArray
          .find { case (key: String, _: RecordValue) => key == lastKey }
          .flatMap { case (_: String, recordvalue: RecordValue) =>
            Option(
              recordvalue.asArray.map { record =>
                record.asRecord.entries
                  .find { case (str: String, _: RecordValue) => str == thisKey }
                  .getOrElse((thisKey, NullValue()))
              })
          }

        values match {
          case Some(subValues) =>
            nestedArrayLookup(profilableRecordArray = subValues,
              jsonPath = restOfPath,
              lastKey = thisKey)
          case None => Option(NullValue())
        }

      case None =>
        profilableRecordArray
          .find { case (key: String, _: RecordValue) => key == lastKey }
          .flatMap { case (_: String, recordvalue: RecordValue) =>
            Option(ArrayValue(arrayValueFinder(recordArray = recordvalue.asArray, searchKey = thisKey)).filterArray)
          }
    }
  }

  //TODO rewrite this method as there is a lot of duplication when doing find and flatmap
  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
  def lookup(jsonPath: String): Option[RecordValue] = {
    if (jsonPath.contains("[]")) {

      val Array(thisKey, restOfPath) = jsonPath split("\\[\\].", 2)

      if (!restOfPath.contains("[]"))
          return getEntries
          .find { case (key: String, _: RecordValue) => key == thisKey }
          .flatMap { case (_: String, recordvalue: RecordValue) =>
            Option(ArrayValue(arrayValueFinder(recordArray = recordvalue.asArray,
              searchKey = restOfPath )).filterArray)
          }
      else   return nestedArrayLookup(profilableRecordArray = getEntries,  jsonPath = restOfPath, lastKey = thisKey)
    }

    if (jsonPath.contains(".")) {
      val Array(thisKey, restOfPath) = jsonPath split("[.]", 2)
      getEntries
        .find { case(key: String, _: RecordValue) => key == thisKey }
        .flatMap { case (_: String, recordvalue: RecordValue) =>
          recordvalue match {
            case NullValue() => Option(RecordValue.fromAny(null))
            case _ => recordvalue.asRecord.lookup(restOfPath)
          }
        }
    } else {
      getEntries
        .find { case(key: String, _: RecordValue) => key == jsonPath }
        .map { case(_: String, recordvalue: RecordValue) => recordvalue }
    }
  }

  def withId(id: String): ProfilableRecord =
    copy(id = Option(id))

  def withAdditionalIdentifiers(additionalIdentifiers: AdditionalIdentifiers): ProfilableRecord =
    copy(additionalIdentifiers = additionalIdentifiers)

  def withDerivedIdentifier(func: ProfilableRecord => AdditionalIdentifier): ProfilableRecord =
    copy(additionalIdentifiers = AdditionalIdentifiers(this.additionalIdentifiers.values :+ func(this)))

  def getId: Option[String] = id

  def getEntries: Seq[(String, RecordValue)] = entries

  override def valueType: ValueType = RECORD

  override def asRecord: ProfilableRecord = this

  override def asArray: Seq[RecordValue] =
    throw new InvalidRecordValueTypeException(actual = valueType, attempted = ARRAY, value = this)

  override def asString: String =
    throw new InvalidRecordValueTypeException(actual = valueType, attempted = STRING, value = this)

  override def asBoolean: Boolean =
    throw new InvalidRecordValueTypeException(actual = valueType, attempted = BOOLEAN, value = this)

  override def asInt: Int =
    throw new InvalidRecordValueTypeException(actual = valueType, attempted = INT, value = this)

  override def asLong: Long =
    throw new InvalidRecordValueTypeException(actual = valueType, attempted = LONG, value = this)

  override def asFloat: Float =
    throw new InvalidRecordValueTypeException(actual = valueType, attempted = FLOAT, value = this)

  override def asDouble: Double =
    throw new InvalidRecordValueTypeException(actual = valueType, attempted = DOUBLE, value = this)

  override def isPrimitive: Boolean = false

  override def toString: String = {
    s"""ProfilableRecord(
       |id = ${getId.toString},
       | entries = ${getEntries.toString()},
       | additionalIdentifiers = ${additionalIdentifiers.toString})""".stripMargin
  }

}
@SuppressWarnings(Array("org.wartremover.warts.Any"))
object ProfilableRecord {

  def apply(id: Option[String] = None,
            entries: Seq[(String, RecordValue)],
            additionalIdentifiers: AdditionalIdentifiers = AdditionalIdentifiers()): ProfilableRecord =
    new ProfilableRecord(id = id, entries = entries, additionalIdentifiers = additionalIdentifiers)

  def fromIterableAny(id: Option[String] = None,
                      iterableAny: Iterable[(String, Any)],
                      additionalIdentifiers: AdditionalIdentifiers): ProfilableRecord = {
    new ProfilableRecord(
      id = id,
      entries = iterableAny.map { entry => (entry._1, RecordValue.fromAny(entry._2)) }.toList,
      additionalIdentifiers = additionalIdentifiers
    )
  }

  /**
   * LinkedHashMaps are sub-records, so don't have Ids or AdditionalIdentifiers
   */
  def fromLinkedHashMap(linkedHashMap: java.util.LinkedHashMap[String, Any]): ProfilableRecord = {
    new ProfilableRecord(
      id = None,
      entries = linkedHashMap.entrySet().iterator().asScala
        .map { entry => (entry.getKey, RecordValue.fromAny(entry.getValue)) }.toList,
      additionalIdentifiers = AdditionalIdentifiers()
    )
  }
}