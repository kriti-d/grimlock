// Copyright 2015,2016,2017 Commonwealth Bank of Australia
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

package commbank.grimlock.test

import commbank.grimlock.framework.encoding._
import commbank.grimlock.framework.metadata._

class TestDateCodec extends TestGrimlock {

  val dfmt = new java.text.SimpleDateFormat("yyyy-MM-dd hh:ss:mm")

  "A DateCodec" should "have a name" in {
    DateCodec("yyyy-MM-dd hh:ss:mm").toShortString shouldBe "date(yyyy-MM-dd hh:ss:mm)"
  }

  it should "decode a correct value" in {
    DateCodec("yyyy-MM-dd hh:ss:mm").decode("2001-01-01 01:01:01") shouldBe Option(dfmt.parse("2001-01-01 01:01:01"))
  }

  it should "not decode an incorrect value" in {
    DateCodec("yyyy-MM-dd hh:ss:mm").decode("a") shouldBe None
    DateCodec("yyyy-MM-dd hh:ss:mm").decode("1") shouldBe None
  }

  it should "encode a correct value" in {
    DateCodec("yyyy-MM-dd hh:ss:mm").encode(dfmt.parse("2001-01-01 01:01:01")) shouldBe "2001-01-01 01:01:01"
  }

  it should "compare a correct value" in {
    DateCodec("yyyy-MM-dd hh:ss:mm")
      .compare(dfmt.parse("2001-01-01 01:01:01"), dfmt.parse("2002-01-01 01:01:01")) shouldBe -1
    DateCodec("yyyy-MM-dd hh:ss:mm")
      .compare(dfmt.parse("2001-01-01 01:01:01"), dfmt.parse("2001-01-01 01:01:01")) shouldBe 0
    DateCodec("yyyy-MM-dd hh:ss:mm")
      .compare(dfmt.parse("2002-01-01 01:01:01"), dfmt.parse("2001-01-01 01:01:01")) shouldBe 1
  }
}

class TestStringCodec extends TestGrimlock {

  "A StringCodec" should "have a name" in {
    StringCodec.toShortString shouldBe "string"
  }

  it should "decode a correct value" in {
    StringCodec.decode("abc") shouldBe Option("abc")
  }

  it should "encode a correct value" in {
    StringCodec.encode("abc") shouldBe "abc"
  }

  it should "compare a correct value" in {
    StringCodec.compare("abc", "bbc") shouldBe -1
    StringCodec.compare("abc", "abc") shouldBe 0
    StringCodec.compare("bbc", "abc") shouldBe 1
  }
}

class TestDoubleCodec extends TestGrimlock {

  "A DoubleCodec" should "have a name" in {
    DoubleCodec.toShortString shouldBe "double"
  }

  it should "decode a correct value" in {
    DoubleCodec.decode("3.14") shouldBe Option(3.14)
  }

  it should "not decode an incorrect value" in {
    DoubleCodec.decode("a") shouldBe None
    DoubleCodec.decode("2001-01-01") shouldBe None
  }

  it should "encode a correct value" in {
    DoubleCodec.encode(3.14) shouldBe "3.14"
  }

  it should "compare a correct value" in {
    DoubleCodec.compare(3.14, 4.14) shouldBe -1
    DoubleCodec.compare(3.14, 3.14) shouldBe 0
    DoubleCodec.compare(4.14, 3.14) shouldBe 1
  }
}

class TestIntCodec extends TestGrimlock {

  "A IntCodec" should "have a name" in {
    IntCodec.toShortString shouldBe "int"
  }

  it should "decode a correct value" in {
    IntCodec.decode("42") shouldBe Option(42)
  }

  it should "not decode an incorrect value" in {
    IntCodec.decode("a") shouldBe None
    IntCodec.decode("2001-01-01") shouldBe None
  }

  it should "encode a correct value" in {
    IntCodec.encode(42) shouldBe "42"
  }

  it should "compare a correct value" in {
    IntCodec.compare(3, 4) shouldBe -1
    IntCodec.compare(3, 3) shouldBe 0
    IntCodec.compare(4, 3) shouldBe 1
  }
}

class TestLongCodec extends TestGrimlock {

  "A LongCodec" should "have a name" in {
    LongCodec.toShortString shouldBe "long"
  }

  it should "decode a correct value" in {
    LongCodec.decode("42") shouldBe Option(42L)
  }

  it should "not decode an incorrect value" in {
    LongCodec.decode("a") shouldBe None
    LongCodec.decode("2001-01-01") shouldBe None
  }

  it should "encode a correct value" in {
    LongCodec.encode(42) shouldBe "42"
  }

  it should "compare a correct value" in {
    LongCodec.compare(3L, 4L) shouldBe -1
    LongCodec.compare(3L, 3L) shouldBe 0
    LongCodec.compare(4L, 3L) shouldBe 1
  }
}

class TestBooleanCodec extends TestGrimlock {

  "A BooleanCodec" should "have a name" in {
    BooleanCodec.toShortString shouldBe "boolean"
  }

  it should "decode a correct value" in {
    BooleanCodec.decode("true") shouldBe Option(true)
    BooleanCodec.decode("false") shouldBe Option(false)
  }

  it should "not decode an incorrect value" in {
    BooleanCodec.decode("a") shouldBe None
    BooleanCodec.decode("2001-01-01") shouldBe None
  }

  it should "encode a correct value" in {
    BooleanCodec.encode(true) shouldBe "true"
  }

  it should "compare a correct value" in {
    BooleanCodec.compare(false, true) shouldBe -1
    BooleanCodec.compare(false, false) shouldBe 0
    BooleanCodec.compare(true, false) shouldBe 1
  }
}

class TestTypeCodec extends TestGrimlock {

  "A TypeCodec" should "have a name" in {
    TypeCodec.toShortString shouldBe "type"
  }

  it should "decode a correct value" in {
    TypeCodec.decode("mixed") shouldBe Option(MixedType)
    TypeCodec.decode("categorical") shouldBe Option(CategoricalType)
    TypeCodec.decode("nominal") shouldBe Option(NominalType)
    TypeCodec.decode("ordinal") shouldBe Option(OrdinalType)
    TypeCodec.decode("numeric") shouldBe Option(NumericType)
    TypeCodec.decode("continuous") shouldBe Option(ContinuousType)
    TypeCodec.decode("discrete") shouldBe Option(DiscreteType)
    TypeCodec.decode("date") shouldBe Option(DateType)
  }

  it should "not decode an incorrect value" in {
    TypeCodec.decode("a") shouldBe None
    TypeCodec.decode("2001-01-01") shouldBe None
  }

  it should "encode a correct value" in {
    TypeCodec.encode(MixedType) shouldBe "mixed"
    TypeCodec.encode(CategoricalType) shouldBe "categorical"
    TypeCodec.encode(NominalType) shouldBe "nominal"
    TypeCodec.encode(OrdinalType) shouldBe "ordinal"
    TypeCodec.encode(NumericType) shouldBe "numeric"
    TypeCodec.encode(ContinuousType) shouldBe "continuous"
    TypeCodec.encode(DiscreteType) shouldBe "discrete"
    TypeCodec.encode(DateType) shouldBe "date"
  }

  it should "compare a correct value" in {
    TypeCodec.compare(DiscreteType, MixedType) < 0 shouldBe true
    TypeCodec.compare(DateType, DateType) shouldBe 0
    TypeCodec.compare(NominalType, ContinuousType) > 0 shouldBe true
  }
}

