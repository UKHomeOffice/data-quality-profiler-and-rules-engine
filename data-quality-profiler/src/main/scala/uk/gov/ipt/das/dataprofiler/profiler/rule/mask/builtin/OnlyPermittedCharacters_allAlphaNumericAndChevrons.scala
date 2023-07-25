package uk.gov.ipt.das.dataprofiler.profiler.rule.mask.builtin

import uk.gov.ipt.das.dataprofiler.profiler.rule.mask.BuiltIn
import uk.gov.ipt.das.dataprofiler.profiler.rule.mask.logic.CharactersMustBeInList

object OnlyPermittedCharacters_allAlphaNumericAndChevrons {
  def apply(): BuiltIn =
    CharactersMustBeInList(
      name = "OnlyPermittedCharacters-allAlphaNumericAndChevrons",
      charList = CharactersMustBeInList.allAlphaNumericAndChevrons,
      nullAllowed = true
    )
}