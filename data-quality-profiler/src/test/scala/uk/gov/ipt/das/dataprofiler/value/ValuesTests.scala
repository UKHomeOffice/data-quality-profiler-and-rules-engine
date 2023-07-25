package uk.gov.ipt.das.dataprofiler.value

import org.scalatest.funspec.AnyFunSpec

class ValuesTests extends AnyFunSpec {

  describe("values") {

    it("should only return boolean value for Boolean value") {

      val testValue = BooleanValue(true)

      assertThrows[InvalidRecordValueTypeException](testValue.asArray)
      assertThrows[InvalidRecordValueTypeException](testValue.asRecord)
      assertThrows[InvalidRecordValueTypeException](testValue.asInt)
      assertThrows[InvalidRecordValueTypeException](testValue.asLong)
      assertThrows[InvalidRecordValueTypeException](testValue.asDouble)
      assertThrows[InvalidRecordValueTypeException](testValue.asFloat)
      assertThrows[InvalidRecordValueTypeException](testValue.asString)

      assertResult(true)(testValue.asBoolean)
      assertResult(BOOLEAN)(testValue.valueType)
      assertResult(true)(testValue.isPrimitive)

    }
    it("should only return string for String value") {
      val testValue = StringValue("Foo")

      assertThrows[InvalidRecordValueTypeException](testValue.asArray)
      assertThrows[InvalidRecordValueTypeException](testValue.asRecord)
      assertThrows[InvalidRecordValueTypeException](testValue.asInt)
      assertThrows[InvalidRecordValueTypeException](testValue.asLong)
      assertThrows[InvalidRecordValueTypeException](testValue.asDouble)
      assertThrows[InvalidRecordValueTypeException](testValue.asFloat)
      assertThrows[InvalidRecordValueTypeException](testValue.asBoolean)

      assertResult("Foo")(testValue.asString)
      assertResult(STRING)(testValue.valueType)
      assertResult(true)(testValue.isPrimitive)

    }

    it("should only return double for Double Value") {
      val testValue = DoubleValue(0.2)

      assertThrows[InvalidRecordValueTypeException](testValue.asArray)
      assertThrows[InvalidRecordValueTypeException](testValue.asRecord)
      assertThrows[InvalidRecordValueTypeException](testValue.asInt)
      assertThrows[InvalidRecordValueTypeException](testValue.asLong)
      assertThrows[InvalidRecordValueTypeException](testValue.asString)
      assertThrows[InvalidRecordValueTypeException](testValue.asFloat)
      assertThrows[InvalidRecordValueTypeException](testValue.asBoolean)

      assertResult(0.2)(testValue.asDouble)
      assertResult(DOUBLE)(testValue.valueType)
      assertResult(true)(testValue.isPrimitive)

    }

    it("should only return long for Long value") {
      val testValue: LongValue = LongValue(2L)

      assertThrows[InvalidRecordValueTypeException](testValue.asArray)
      assertThrows[InvalidRecordValueTypeException](testValue.asRecord)
      assertThrows[InvalidRecordValueTypeException](testValue.asInt)
      assertThrows[InvalidRecordValueTypeException](testValue.asDouble)
      assertThrows[InvalidRecordValueTypeException](testValue.asString)
      assertThrows[InvalidRecordValueTypeException](testValue.asFloat)
      assertThrows[InvalidRecordValueTypeException](testValue.asBoolean)

      assertResult(2L)(testValue.asLong)
      assertResult(LONG)(testValue.valueType)
      assertResult(true)(testValue.isPrimitive)
    }

    it("should only return Array for Array Value") {
      val testValue = ArrayValue(Seq(BooleanValue(true), StringValue("foo")))

      assertThrows[InvalidRecordValueTypeException](testValue.asLong)
      assertThrows[InvalidRecordValueTypeException](testValue.asRecord)
      assertThrows[InvalidRecordValueTypeException](testValue.asInt)
      assertThrows[InvalidRecordValueTypeException](testValue.asDouble)
      assertThrows[InvalidRecordValueTypeException](testValue.asString)
      assertThrows[InvalidRecordValueTypeException](testValue.asFloat)
      assertThrows[InvalidRecordValueTypeException](testValue.asBoolean)

      assertResult(Seq(BooleanValue(true), StringValue("foo")))(testValue.asArray)
      assertResult(ARRAY)(testValue.valueType)
      assertResult(false)(testValue.isPrimitive)

    }

    it("should only return exception for all Null Value") {
      val testValue = NullValue()
      assertThrows[InvalidRecordValueTypeException](testValue.asLong)
      assertThrows[InvalidRecordValueTypeException](testValue.asRecord)
      assertThrows[InvalidRecordValueTypeException](testValue.asInt)
      assertThrows[InvalidRecordValueTypeException](testValue.asDouble)
      assertThrows[InvalidRecordValueTypeException](testValue.asString)
      assertThrows[InvalidRecordValueTypeException](testValue.asFloat)
      assertThrows[InvalidRecordValueTypeException](testValue.asBoolean)
      assertThrows[InvalidRecordValueTypeException](testValue.asArray)
      assertResult(NULL)(testValue.valueType)
      assertResult(false)(testValue.isPrimitive)

    }

  }

}
