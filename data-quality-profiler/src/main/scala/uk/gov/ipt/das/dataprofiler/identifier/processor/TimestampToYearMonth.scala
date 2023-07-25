package uk.gov.ipt.das.dataprofiler.identifier.processor

import org.slf4j.LoggerFactory
import TimestampToYearMonth.{inputFormat, inputFormatString, logger, outputFormat, outputFormatString}
import uk.gov.ipt.das.dataprofiler.identifier.IdentifierProcessor

import java.text.ParseException
import java.time.format.{DateTimeFormatter, DateTimeParseException}

class TimestampToYearMonth private () extends IdentifierProcessor {
  override def process(value: String): String =
    try {
      val reformatted = outputFormat.format(inputFormat.parse(value))
      logger.debug(s"TimestampToYearMonth parsing using inputFormat [ $inputFormatString ]: $value, into outputFormat [ $outputFormatString ]: $reformatted")
      reformatted
    } catch {
      case _: ParseException | _: NumberFormatException | _: DateTimeParseException => "INVALID_DATETIME"
    }
}
object TimestampToYearMonth {
  private lazy val logger = LoggerFactory.getLogger(getClass)

  private val inputFormatString = "yyyy-MM-dd'T'HH:mm:ss.SSS"
  private val outputFormatString = "yyyy-MM"

  private lazy val inputFormat = DateTimeFormatter.ofPattern(inputFormatString)
  private lazy val outputFormat = DateTimeFormatter.ofPattern(outputFormatString)

  lazy val INSTANCE = new TimestampToYearMonth()
  def apply(): TimestampToYearMonth = INSTANCE
}