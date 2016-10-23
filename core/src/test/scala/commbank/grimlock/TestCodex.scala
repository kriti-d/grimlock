// Copyright 2015,2016 Commonwealth Bank of Australia
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package commbank.grimlock

import commbank.grimlock.framework.encoding._

class TestDateCodec extends TestGrimlock {

  val dfmt = new java.text.SimpleDateFormat("yyyy-MM-dd hh:ss:mm")

  "A DateCodec" should "have a name" in {
    DateCodec("yyyy-MM-dd hh:ss:mm").toShortString shouldBe "date(yyyy-MM-dd hh:ss:mm)"
  }

  it should "decode a correct value" in {
    DateCodec("yyyy-MM-dd hh:ss:mm").decode("2001-01-01 01:01:01") shouldBe Option(
      DateValue(dfmt.parse("2001-01-01 01:01:01"), DateCodec("yyyy-MM-dd hh:ss:mm"))
    )
  }

  it should "not decode an incorrect value" in {
    DateCodec("yyyy-MM-dd hh:ss:mm").decode("a") shouldBe None
    DateCodec("yyyy-MM-dd hh:ss:mm").decode("1") shouldBe None
  }

  it should "encode a correct value" in {
    DateCodec("yyyy-MM-dd hh:ss:mm").encode(dfmt.parse("2001-01-01 01:01:01")) shouldBe "2001-01-01 01:01:01"
  }

  it should "compare a correct value" in {
    DateCodec("yyyy-MM-dd hh:ss:mm").compare(
      DateValue(dfmt.parse("2001-01-01 01:01:01"), DateCodec("yyyy-MM-dd hh:ss:mm")),
      DateValue(dfmt.parse("2002-01-01 01:01:01"), DateCodec("yyyy-MM-dd hh:ss:mm"))
    ) shouldBe Option(-1)
    DateCodec("yyyy-MM-dd hh:ss:mm").compare(
      DateValue(dfmt.parse("2001-01-01 01:01:01"), DateCodec("yyyy-MM-dd hh:ss:mm")),
      DateValue(dfmt.parse("2001-01-01 01:01:01"), DateCodec("yyyy-MM-dd hh:ss:mm"))
    ) shouldBe Option(0)
    DateCodec("yyyy-MM-dd hh:ss:mm").compare(
      DateValue(dfmt.parse("2002-01-01 01:01:01"), DateCodec("yyyy-MM-dd hh:ss:mm")),
      DateValue(dfmt.parse("2001-01-01 01:01:01"), DateCodec("yyyy-MM-dd hh:ss:mm"))
    ) shouldBe Option(1)
  }

  it should "not compare an incorrect value" in {
    DateCodec("yyyy-MM-dd hh:ss:mm").compare(
      DateValue(dfmt.parse("2001-01-01 01:01:01"), DateCodec("yyyy-MM-dd hh:ss:mm")),
      LongValue(1)
    ) shouldBe None
    DateCodec("yyyy-MM-dd hh:ss:mm").compare(
      LongValue(1),
      DateValue(dfmt.parse("2001-01-01 01:01:01"), DateCodec("yyyy-MM-dd hh:ss:mm"))
    ) shouldBe None
  }
}

class TestStringCodec extends TestGrimlock {

  "A StringCodec" should "have a name" in {
    StringCodec.toShortString shouldBe "string"
  }

  it should "decode a correct value" in {
    StringCodec.decode("abc") shouldBe Option(StringValue("abc"))
  }

  it should "encode a correct value" in {
    StringCodec.encode("abc") shouldBe "abc"
  }

  it should "compare a correct value" in {
    StringCodec.compare(StringValue("abc"), StringValue("bbc")) shouldBe Option(-1)
    StringCodec.compare(StringValue("abc"), StringValue("abc")) shouldBe Option(0)
    StringCodec.compare(StringValue("bbc"), StringValue("abc")) shouldBe Option(1)
  }

  it should "not compare an incorrect value" in {
    StringCodec.compare(StringValue("abc"), LongValue(1)) shouldBe None
    StringCodec.compare(LongValue(1), StringValue("abc")) shouldBe None
  }
}

class TestDoubleCodec extends TestGrimlock {

  "A DoubleCodec" should "have a name" in {
    DoubleCodec.toShortString shouldBe "double"
  }

  it should "decode a correct value" in {
    DoubleCodec.decode("3.14") shouldBe Option(DoubleValue(3.14))
  }

  it should "not decode an incorrect value" in {
    DoubleCodec.decode("a") shouldBe None
    DoubleCodec.decode("2001-01-01") shouldBe None
  }

  it should "encode a correct value" in {
    DoubleCodec.encode(3.14) shouldBe "3.14"
  }

  it should "compare a correct value" in {
    DoubleCodec.compare(DoubleValue(3.14), DoubleValue(4.14)) shouldBe Option(-1)
    DoubleCodec.compare(DoubleValue(3.14), DoubleValue(3.14)) shouldBe Option(0)
    DoubleCodec.compare(DoubleValue(4.14), DoubleValue(3.14)) shouldBe Option(1)

    DoubleCodec.compare(DoubleValue(3), LongValue(4)) shouldBe Option(-1)
    DoubleCodec.compare(DoubleValue(3), LongValue(3)) shouldBe Option(0)
    DoubleCodec.compare(DoubleValue(4), LongValue(3)) shouldBe Option(1)

    DoubleCodec.compare(LongValue(3), DoubleValue(4)) shouldBe Option(-1)
    DoubleCodec.compare(LongValue(3), DoubleValue(3)) shouldBe Option(0)
    DoubleCodec.compare(LongValue(4), DoubleValue(3)) shouldBe Option(1)

    DoubleCodec.compare(LongValue(3), LongValue(4)) shouldBe Option(-1)
    DoubleCodec.compare(LongValue(3), LongValue(3)) shouldBe Option(0)
    DoubleCodec.compare(LongValue(4), LongValue(3)) shouldBe Option(1)
  }

  it should "not compare an incorrect value" in {
    DoubleCodec.compare(DoubleValue(3.14), StringValue("abc")) shouldBe None
    DoubleCodec.compare(StringValue("abc"), DoubleValue(3.14)) shouldBe None
  }
}

class TestLongCodeCodec extends TestGrimlock {

  "A LongCodec" should "have a name" in {
    LongCodec.toShortString shouldBe "long"
  }

  it should "decode a correct value" in {
    LongCodec.decode("42") shouldBe Option(LongValue(42))
  }

  it should "not decode an incorrect value" in {
    LongCodec.decode("a") shouldBe None
    LongCodec.decode("2001-01-01") shouldBe None
  }

  it should "encode a correct value" in {
    LongCodec.encode(42) shouldBe "42"
  }

  it should "compare a correct value" in {
    LongCodec.compare(DoubleValue(42), DoubleValue(43)) shouldBe Option(-1)
    LongCodec.compare(DoubleValue(42), DoubleValue(42)) shouldBe Option(0)
    LongCodec.compare(DoubleValue(43), DoubleValue(42)) shouldBe Option(1)

    LongCodec.compare(DoubleValue(3), LongValue(4)) shouldBe Option(-1)
    LongCodec.compare(DoubleValue(3), LongValue(3)) shouldBe Option(0)
    LongCodec.compare(DoubleValue(4), LongValue(3)) shouldBe Option(1)

    LongCodec.compare(LongValue(3), DoubleValue(4)) shouldBe Option(-1)
    LongCodec.compare(LongValue(3), DoubleValue(3)) shouldBe Option(0)
    LongCodec.compare(LongValue(4), DoubleValue(3)) shouldBe Option(1)

    LongCodec.compare(LongValue(3), LongValue(4)) shouldBe Option(-1)
    LongCodec.compare(LongValue(3), LongValue(3)) shouldBe Option(0)
    LongCodec.compare(LongValue(4), LongValue(3)) shouldBe Option(1)
  }

  it should "not compare an incorrect value" in {
    LongCodec.compare(LongValue(42), StringValue("abc")) shouldBe None
    LongCodec.compare(StringValue("abc"), LongValue(42)) shouldBe None
  }
}

class TestBooleanCodec extends TestGrimlock {

  "A BooleanCodec" should "have a name" in {
    BooleanCodec.toShortString shouldBe "boolean"
  }

  it should "decode a correct value" in {
    BooleanCodec.decode("true") shouldBe Option(BooleanValue(true))
    BooleanCodec.decode("false") shouldBe Option(BooleanValue(false))
  }

  it should "not decode an incorrect value" in {
    BooleanCodec.decode("a") shouldBe None
    BooleanCodec.decode("2001-01-01") shouldBe None
  }

  it should "encode a correct value" in {
    BooleanCodec.encode(true) shouldBe "true"
  }

  it should "compare a correct value" in {
    BooleanCodec.compare(BooleanValue(false), BooleanValue(true)) shouldBe Option(-1)
    BooleanCodec.compare(BooleanValue(false), BooleanValue(false)) shouldBe Option(0)
    BooleanCodec.compare(BooleanValue(true), BooleanValue(false)) shouldBe Option(1)
  }

  it should "not compare an incorrect value" in {
    BooleanCodec.compare(BooleanValue(true), StringValue("abc")) shouldBe None
    BooleanCodec.compare(StringValue("abc"), BooleanValue(true)) shouldBe None
  }
}

