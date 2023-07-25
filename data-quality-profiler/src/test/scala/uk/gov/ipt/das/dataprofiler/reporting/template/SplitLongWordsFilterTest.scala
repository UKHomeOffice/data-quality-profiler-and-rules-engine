package uk.gov.ipt.das.dataprofiler.reporting.template

import org.scalatest.funspec.AnyFunSpec
import uk.gov.ipt.das.dataprofiler.reporting.template.mixin.SplitLongWords

class SplitLongWordsFilterTest extends AnyFunSpec with SplitLongWords {

  it("splits long words up") {

    val normal = "This is a normal sentence"
    assert(slw(normal) === normal)

    val longword = "This is a word with a long worddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
    val expected = s"This is a word with a long wordddddddddddddddddddddd${ZERO_WIDTH_SPACE}ddddddddddddddddddddddddd${ZERO_WIDTH_SPACE}dddddddddddddddd"
    assert(slw(longword) === expected)
  }

}
