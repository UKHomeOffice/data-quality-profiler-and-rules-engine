package uk.gov.ipt.das.dataprofiler.profiler.utils

import org.scalatest.funspec.AnyFunSpec
import uk.gov.ipt.das.mixin.TimerWrapper

import java.util.regex.Pattern

class LowGrainProfilerTest extends AnyFunSpec with TimerWrapper {

  it("compares performance of low grain algorithms") {

    /**
     * Run different low-grain masking algorithms over a loop of N loops
     *
     * Sample benchmarks:
     * Milliseconds ASCIICLASS_LOWGRAIN: 1557
       Milliseconds charByChar: 221
     *
     */

    val loopCount = 1000000

    val sampleInput = "Democratic People's Republic of 12345670 Land"
    val expectedOutput = "Aa Aa'a Aa a 9 Aa"

    {
      val replacements = List(
        (Pattern.compile("[a-z]+"), "a"),
        (Pattern.compile("[A-Z]+"), "A"),
        (Pattern.compile("[0-9]+"), "9"),
        (Pattern.compile("\t"), "T"),
      )

      def ASCIICLASS_LOWGRAIN(str: String): String = {
        if (str == null)
          null
        else {
          var replaced = str
          replacements.foreach { case (pattern, replacement) =>
            replaced = pattern.matcher(replaced).replaceAll(replacement)
          }
          replaced
        }
      }

      assert(ASCIICLASS_LOWGRAIN(sampleInput) === expectedOutput)

      timer("ASCIICLASS_LOWGRAIN", iterations = loopCount){ ASCIICLASS_LOWGRAIN(sampleInput) }
    }

    {
      def charByChar(s: String): String = {
        val sb = new StringBuilder
        var i = 0
        var last: Char = ' '
        while (i < s.length) {
          val c = s(i)
          val toAdd = {
            if (c >= 'A' && c <= 'Z') {
              'A'
            } else if (c >= 'a' && c <= 'z') {
              'a'
            } else if (c >= '0' && c <= '9') {
              '9'
            } else if (c == '\t') {
              'T'
            } else {
              c
            }
          }

          if (toAdd != last) sb.append(toAdd)

          i += 1
          last = toAdd
        }
        sb.mkString
      }

      assert(charByChar(sampleInput) === expectedOutput)

      timer("charByChar", iterations = loopCount){ charByChar(sampleInput) }
    }
  }
}
