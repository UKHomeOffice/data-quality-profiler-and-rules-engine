package uk.gov.ipt.das.dataprofiler.profiler.utils

import org.scalatest.funspec.AnyFunSpec
import uk.gov.ipt.das.mixin.TimerWrapper

import java.util.regex.Pattern

class HighGrainProfilerTest extends AnyFunSpec with TimerWrapper {

  it("compares performance of high grain algorithms") {

    /**
     * Run different high-grain masking algorithms over a loop of N loops
     *
     * Sample benchmarks:
     *
     *  Milliseconds ASCIICLASS_HIGHGRAIN: 2621
        Milliseconds charByChar: 500
        Milliseconds charByCharNoVar: 506
        Milliseconds inplaceChange: 90
     */

    val loopCount = 1000000

    val sampleInput = "Democratic People's Republic of 12345670 Land"
    val expectedOutput = "Aaaaaaaaaa Aaaaaa'a Aaaaaaaa aa 99999999 Aaaa"

    {
      val replacements = List(
        (Pattern.compile("[a-z]"), "a"),
        (Pattern.compile("[A-Z]"), "A"),
        (Pattern.compile("[0-9]"), "9"),
        (Pattern.compile("\t"), "T"),
      )

      /**
       * UPPER ascii characters => “A”,
       * LOWER ascii characters => “a”,
       * numbers => “9”,
       * [TAB] => "T"
       * all else left as is.
       */
      def ASCIICLASS_HIGHGRAIN(str: String): String = {
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

      assert(ASCIICLASS_HIGHGRAIN(sampleInput) === expectedOutput)

      timer("ASCIICLASS_HIGHGRAIN", iterations = loopCount){ ASCIICLASS_HIGHGRAIN(sampleInput) }
    }

   {
      def charByChar(s: String): String = {
        val sb = new StringBuilder
        var i = 0
        while (i < s.length) {
          val c = s(i)
          if (c >= 'A' && c <= 'Z') {
            sb.append("A")
          } else if (c >= 'a' && c <= 'z') {
            sb.append("a")
          } else if (c >= '0' && c <= '9') {
            sb.append("9")
          } else if (c == '\t') {
            sb.append("T")
          } else {
            sb.append(c)
          }
          i += 1
        }
        sb.mkString
      }

      assert(charByChar(sampleInput) === expectedOutput)

      timer("charByChar", iterations = loopCount){ charByChar(sampleInput) }
    }

    {
      def charByCharNoVar(s: String): String = {
        val sb = new StringBuilder
        var i = 0
        while (i < s.length) {
          if (s(i) >= 'A' && s(i) <= 'Z') {
            sb.append("A")
          } else if (s(i) >= 'a' && s(i) <= 'z') {
            sb.append("a")
          } else if (s(i) >= '0' && s(i) <= '9') {
            sb.append("9")
          } else if (s(i) == '\t') {
            sb.append("T")
          } else {
            sb.append(s(i))
          }
          i += 1
        }
        sb.mkString
      }

      assert(charByCharNoVar(sampleInput) === expectedOutput)

      timer("charByCharNoVar", iterations = loopCount){ charByCharNoVar(sampleInput) }
    }

    {
      def inplaceChange(s: String): String = {
        val inPlaceStr = s.toCharArray

        var i = 0
        while (i < s.length) {
          val c = s(i)
          if (c >= 'A' && c <= 'Z') {
            inPlaceStr(i) = 'A'
          } else if (c >= 'a' && c <= 'z') {
            inPlaceStr(i) = 'a'
          } else if (c >= '0' && c <= '9') {
            inPlaceStr(i) = '9'
          } else if (c == '\t') {
            inPlaceStr(i) = 'T'
          } else {
            // don't change the char
          }
          i += 1
        }
        new String(inPlaceStr)
      }

      assert(inplaceChange(sampleInput) === expectedOutput)

      timer("inplaceChange", iterations = loopCount){ inplaceChange(sampleInput) }
    }
  }
}
