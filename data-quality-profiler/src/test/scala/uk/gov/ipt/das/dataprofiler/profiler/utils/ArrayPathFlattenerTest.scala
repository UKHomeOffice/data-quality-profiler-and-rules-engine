package uk.gov.ipt.das.dataprofiler.profiler.utils

import org.scalatest.funspec.AnyFunSpec
import uk.gov.ipt.das.mixin.TimerWrapper

import java.util.regex.Pattern

class ArrayPathFlattenerTest extends AnyFunSpec with TimerWrapper {

  it("benchmark different methods") {

    val loopCount = 1000000

    val arrayIndexPattern = Pattern.compile("\\[[0-9]+\\]")

    val sampleInput = "foobar[1].baz[2].baaaaa"
    val expectedOutput = "foobar[].baz[].baaaaa"

    def replaceAll(s: String) = s.replaceAll("\\[[0-9]+\\]", "[]")
    assert(replaceAll(sampleInput) === expectedOutput)
    def compiledRegex(s: String) = arrayIndexPattern.matcher(s).replaceAll("[]")
    assert(compiledRegex(sampleInput) === expectedOutput)
    def stepOver(s: String): String = {
      var inArr = false
      val sb = new StringBuilder
      var i = 0
      while (i < s.length) {
        if (inArr) {
          if (s(i) == ']') {
            inArr = false
            sb.append(s(i))
          }
        } else {
          if (s(i) == '[') {
            inArr = true
          }
          sb.append(s(i))
        }
        i += 1
      }
      sb.mkString
    }
    assert(stepOver(sampleInput) === expectedOutput)

    timer("replaceAll", iterations = loopCount){ replaceAll(sampleInput) }
    timer("compiled regex", iterations = loopCount){ compiledRegex(sampleInput) }
    timer("stepover", iterations = loopCount){ stepOver(sampleInput) }
  }

}
