package uk.gov.ipt.das.mixin

import org.apache.poi.ss.usermodel.{Cell, CellType}

trait ExcelMixins {

  def numericToString(n: Double): String = if ((BigDecimal(n) - BigDecimal(n.toInt)).precision > 1) {
    n.toString
  } else {
    n.toInt.toString  // to avoid ref ids becoming e.g 123.0
  }

  // TODO replace cell with Option[Cell] and write test to confirm null conversion works as expected (leave Throw as-is for now)
  @SuppressWarnings(Array("org.wartremover.warts.Null", "org.wartremover.warts.Throw"))
  def getCellValueAsString(cell: Cell, cellType: Option[CellType] = None): String = cell match {
    case null => ""
    case _ =>
      cellType.fold(cell.getCellType) { typ => typ } match {
        case CellType.STRING => cell.getStringCellValue
        case CellType.BOOLEAN => cell.getBooleanCellValue.toString
        case CellType.NUMERIC => numericToString(cell.getNumericCellValue)
        case CellType.BLANK => ""
        case CellType.FORMULA => getCellValueAsString(cell, Option(cell.getCachedFormulaResultType))
        case CellType.ERROR => throw new Exception("Formula Error in Excel Spreadsheet")
      }
  }

}