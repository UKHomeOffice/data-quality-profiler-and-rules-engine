package uk.gov.ipt.das.dataprofiler.profiler.rule.mask

import org.scalatest.funspec.AnyFunSpec

class LowGraphCollapsibleCharsTest extends AnyFunSpec {

  it("collapses only a, A, 9 chars in high grain profiles") {
    Seq(
      ("hello", "a"),
      ("hello there", "a a"),
      ("hello  there", "a  a"),
      ("    hi", "    a")
    ).foreach { case (in, out) =>
      assert(LowGrainProfile.charByChar(in) === out)
    }
  }

}
