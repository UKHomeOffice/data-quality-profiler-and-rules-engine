package uk.gov.ipt.das.dataprofiler.profiler.rule.mask.builtin

import uk.gov.ipt.das.dataprofiler.profiler.rule.mask.BuiltIn
import uk.gov.ipt.das.dataprofiler.profiler.rule.mask.logic.CharactersMustBeInList

object OnlyPermittedCharacters_numericChars {
  def apply(): BuiltIn =
    CharactersMustBeInList(
      name = "OnlyPermittedCharacters-numericChars",
      charList = CharactersMustBeInList.numericChars,
      nullAllowed = true
    )
}