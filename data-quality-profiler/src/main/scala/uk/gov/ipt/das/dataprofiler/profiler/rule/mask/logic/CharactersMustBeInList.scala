package uk.gov.ipt.das.dataprofiler.profiler.rule.mask.logic

import uk.gov.ipt.das.dataprofiler.profiler.rule.mask.BuiltIn
import uk.gov.ipt.das.dataprofiler.value.{NullValue, RecordValue, StringValue}

@SuppressWarnings(Array("org.wartremover.warts.Null"))
case class CharactersMustBeInList(name: String,
                                  charList: Seq[Char],
                                  nullAllowed: Boolean) extends BuiltIn {
  override def rule: RecordValue => Boolean = {
    case null | NullValue() => nullAllowed
    case s: StringValue => !s.value.exists{ fieldChar: Char => !charList.contains(fieldChar) }
    case _ => false
  }
}
object CharactersMustBeInList {
  val lowerAlphaChars: Array[Char] = ('a' to 'z').toArray
  val upperAlphaChars: Array[Char] = ('A' to 'Z').toArray
  val allAlphaChars: Array[Char] = lowerAlphaChars ++ upperAlphaChars
  val numericChars: Array[Char] = ('0' to '9').toArray
  val allAlphaNumeric: Array[Char] = allAlphaChars ++ numericChars
  val allAlphaNumericAndChevrons :Array[Char] = allAlphaNumeric ++ Array('<','>')
}