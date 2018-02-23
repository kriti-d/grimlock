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

class TestContinuousSchema extends TestGrimlock {

  "A ContinuousSchema" should "return its string representation" in {
    ContinuousSchema[Double]().toString shouldBe "ContinuousSchema()"
    ContinuousSchema[Double](-3.1415, 1.4142).toString shouldBe "ContinuousSchema(-3.1415,1.4142)"

    ContinuousSchema[Long]().toString shouldBe "ContinuousSchema()"
    ContinuousSchema[Long](-1, 1).toString shouldBe "ContinuousSchema(-1,1)"
  }

  it should "validate a correct value" in {
    ContinuousSchema[Double]().validate(DoubleValue(1)) shouldBe true
    ContinuousSchema[Double](-3.1415, 1.4142).validate(DoubleValue(1)) shouldBe true

    ContinuousSchema[Long]().validate(LongValue(1)) shouldBe true
    ContinuousSchema[Long](-1, 1).validate(LongValue(1)) shouldBe true
  }

  it should "not validate an incorrect value" in {
    ContinuousSchema[Double](-3.1415, 1.4142).validate(DoubleValue(4)) shouldBe false
    ContinuousSchema[Double](-3.1415, 1.4142).validate(DoubleValue(-4)) shouldBe false

    ContinuousSchema[Long](-1, 1).validate(LongValue(4)) shouldBe false
    ContinuousSchema[Long](-1, 1).validate(LongValue(-4)) shouldBe false
  }

  it should "parse correctly" in {
    ContinuousSchema.fromShortString("continuous", DoubleCodec) shouldBe Option(ContinuousSchema[Double])
    ContinuousSchema.fromShortString("continuous()", DoubleCodec) shouldBe Option(ContinuousSchema[Double])
    ContinuousSchema.fromShortString("continuous(-3.1415:1.4142)", DoubleCodec) shouldBe Option(
      ContinuousSchema[Double](-3.1415, 1.4142)
    )

    ContinuousSchema.fromShortString("continuous", LongCodec) shouldBe Option(ContinuousSchema[Long])
    ContinuousSchema.fromShortString("continuous()", LongCodec) shouldBe Option(ContinuousSchema[Long])
    ContinuousSchema.fromShortString("continuous(-1:1)", LongCodec) shouldBe Option(ContinuousSchema[Long](-1, 1))

    ContinuousSchema.fromShortString("foo", DoubleCodec) shouldBe None

    ContinuousSchema.fromShortString("continuous(", DoubleCodec) shouldBe None
    ContinuousSchema.fromShortString("continuous)", DoubleCodec) shouldBe None
    ContinuousSchema.fromShortString("continuous:", DoubleCodec) shouldBe None
    ContinuousSchema.fromShortString("continuous:)", DoubleCodec) shouldBe None
    ContinuousSchema.fromShortString("continuous(:", DoubleCodec) shouldBe None
    ContinuousSchema.fromShortString("continuous(:)", DoubleCodec) shouldBe None
    ContinuousSchema.fromShortString("continuous(-3.1414:foo)", DoubleCodec) shouldBe None
    ContinuousSchema.fromShortString("continuous(foo:1.4142)", DoubleCodec) shouldBe None
    ContinuousSchema.fromShortString("continuous(-3.1414,1.4142)", DoubleCodec) shouldBe None

    ContinuousSchema.fromShortString("continuous(", LongCodec) shouldBe None
    ContinuousSchema.fromShortString("continuous)", LongCodec) shouldBe None
    ContinuousSchema.fromShortString("continuous:", LongCodec) shouldBe None
    ContinuousSchema.fromShortString("continuous:)", LongCodec) shouldBe None
    ContinuousSchema.fromShortString("continuous(:", LongCodec) shouldBe None
    ContinuousSchema.fromShortString("continuous(:)", LongCodec) shouldBe None
    ContinuousSchema.fromShortString("continuous(-1:foo)", LongCodec) shouldBe None
    ContinuousSchema.fromShortString("continuous(foo:1)", LongCodec) shouldBe None
    ContinuousSchema.fromShortString("continuous(-1,1)", LongCodec) shouldBe None
    ContinuousSchema.fromShortString("continuous(-1.2:1)", LongCodec) shouldBe None
    ContinuousSchema.fromShortString("continuous(-1:1.2)", LongCodec) shouldBe None
    ContinuousSchema.fromShortString("continuous(-1:1:2)", LongCodec) shouldBe None
  }
}

class TestDiscreteSchema extends TestGrimlock {

  "A DiscreteSchema" should "return its string representation" in {
    DiscreteSchema[Long]().toString shouldBe "DiscreteSchema()"
    DiscreteSchema[Long](-1, 1).toString shouldBe "DiscreteSchema(-1,1)"
    DiscreteSchema[Long](-1, 1, 1).toString shouldBe "DiscreteSchema(-1,1,1)"
  }

  it should "validate a correct value" in {
    DiscreteSchema[Long]().validate(LongValue(1)) shouldBe true
    DiscreteSchema[Long](-1, 1, 1).validate(LongValue(1)) shouldBe true
    DiscreteSchema[Long](-4, 4, 2).validate(LongValue(2)) shouldBe true
  }

  it should "not validate an incorrect value" in {
    DiscreteSchema[Long](-1, 1, 1).validate(LongValue(4)) shouldBe false
    DiscreteSchema[Long](-1, 1, 1).validate(LongValue(-4)) shouldBe false
    DiscreteSchema[Long](-4, 4, 2).validate(LongValue(3)) shouldBe false
  }

  it should "parse correctly" in {
    DiscreteSchema.fromShortString("discrete", LongCodec) shouldBe Option(DiscreteSchema[Long])
    DiscreteSchema.fromShortString("discrete()", LongCodec) shouldBe Option(DiscreteSchema[Long])
    DiscreteSchema.fromShortString("discrete(-1:2)", LongCodec) shouldBe Option(DiscreteSchema[Long](-1, 2))
    DiscreteSchema.fromShortString("discrete(-1:2,2)", LongCodec) shouldBe Option(DiscreteSchema[Long](-1, 2, 2))

    DiscreteSchema.fromShortString("foo", LongCodec) shouldBe None

    DiscreteSchema.fromShortString("discrete(", LongCodec) shouldBe None
    DiscreteSchema.fromShortString("discrete)", LongCodec) shouldBe None
    DiscreteSchema.fromShortString("discrete:", LongCodec) shouldBe None
    DiscreteSchema.fromShortString("discrete,", LongCodec) shouldBe None
    DiscreteSchema.fromShortString("discrete:)", LongCodec) shouldBe None
    DiscreteSchema.fromShortString("discrete(:", LongCodec) shouldBe None
    DiscreteSchema.fromShortString("discrete,)", LongCodec) shouldBe None
    DiscreteSchema.fromShortString("discrete(,", LongCodec) shouldBe None
    DiscreteSchema.fromShortString("discrete(:)", LongCodec) shouldBe None
    DiscreteSchema.fromShortString("discrete(,)", LongCodec) shouldBe None
    DiscreteSchema.fromShortString("discrete(:,)", LongCodec) shouldBe None
    DiscreteSchema.fromShortString("discrete(-1:foo,1)", LongCodec) shouldBe None
    DiscreteSchema.fromShortString("discrete(foo:2,1)", LongCodec) shouldBe None
    DiscreteSchema.fromShortString("discrete(-1:2,foo)", LongCodec) shouldBe None
    DiscreteSchema.fromShortString("discrete(-1,1)", LongCodec) shouldBe None
    DiscreteSchema.fromShortString("discrete(-1.2:2,1)", LongCodec) shouldBe None
    DiscreteSchema.fromShortString("discrete(-1:2.2,1)", LongCodec) shouldBe None
    DiscreteSchema.fromShortString("discrete(-1:2,1.2)", LongCodec) shouldBe None
    DiscreteSchema.fromShortString("discrete(-1:2:3,1)", LongCodec) shouldBe None
    DiscreteSchema.fromShortString("discrete(-1:2,1,3)", LongCodec) shouldBe None
  }
}

class TestNominalSchema extends TestGrimlock {

  "A NominalSchema" should "return its string representation" in {
    NominalSchema[Long]().toString shouldBe "NominalSchema()"
    NominalSchema[Long](Set[Long](1,2,3)).toString shouldBe "NominalSchema(Set(1,2,3))"

    NominalSchema[Double]().toString shouldBe "NominalSchema()"
    NominalSchema[Double](Set[Double](1,2,3)).toString shouldBe "NominalSchema(Set(1.0,2.0,3.0))"

    NominalSchema[String]().toString shouldBe "NominalSchema()"
    NominalSchema[String](Set("a","b","c")).toString shouldBe "NominalSchema(Set(a,b,c))"
  }

  it should "validate a correct value" in {
    NominalSchema[Long]().validate(LongValue(1)) shouldBe true
    NominalSchema[Long](Set[Long](1,2,3)).validate(LongValue(1)) shouldBe true

    NominalSchema[Double]().validate(DoubleValue(1)) shouldBe true
    NominalSchema[Double](Set[Double](1,2,3)).validate(DoubleValue(1)) shouldBe true

    NominalSchema[String]().validate(StringValue("a")) shouldBe true
    NominalSchema[String](Set("a","b","c")).validate(StringValue("a")) shouldBe true
  }

  it should "not validate an incorrect value" in {
    NominalSchema[Long](Set[Long](1,2,3)).validate(LongValue(4)) shouldBe false

    NominalSchema[Double](Set[Double](1,2,3)).validate(DoubleValue(4)) shouldBe false

    NominalSchema[String](Set("a","b","c")).validate(StringValue("d")) shouldBe false
  }

  it should "parse correctly" in {
    NominalSchema.fromShortString("nominal", StringCodec) shouldBe Option(NominalSchema[String]())
    NominalSchema.fromShortString("nominal()", StringCodec) shouldBe Option(NominalSchema[String](Set[String]()))
    NominalSchema.fromShortString("nominal(a,b,c)", StringCodec) shouldBe Option(
      NominalSchema(Set[String]("a", "b", "c"))
    )

    NominalSchema.fromShortString("nominal", DoubleCodec) shouldBe Option(NominalSchema[Double]())
    NominalSchema.fromShortString("nominal()", DoubleCodec) shouldBe Option(NominalSchema[Double]())
    NominalSchema.fromShortString("nominal(-3.1415,1.4142,42)", DoubleCodec) shouldBe Option(
      NominalSchema[Double](Set(-3.1415, 1.4142, 42))
    )

    NominalSchema.fromShortString("nominal", LongCodec) shouldBe Option(NominalSchema[Long]())
    NominalSchema.fromShortString("nominal()", LongCodec) shouldBe Option(NominalSchema[Long]())
    NominalSchema.fromShortString("nominal(-1,1,3)", LongCodec) shouldBe Option(NominalSchema(Set[Long](-1, 1, 3)))

    NominalSchema.fromShortString("foo", StringCodec) shouldBe None
    NominalSchema.fromShortString("foo", DoubleCodec) shouldBe None
    NominalSchema.fromShortString("foo", LongCodec) shouldBe None

    NominalSchema.fromShortString("nominal(", StringCodec) shouldBe None
    NominalSchema.fromShortString("nominal)", StringCodec) shouldBe None
    NominalSchema.fromShortString("nominal,", StringCodec) shouldBe None
    NominalSchema.fromShortString("nominal,)", StringCodec) shouldBe None
    NominalSchema.fromShortString("nominal(,", StringCodec) shouldBe None
    NominalSchema.fromShortString("nominal(,)", StringCodec) shouldBe Option(NominalSchema(Set[String]("")))

    NominalSchema.fromShortString("nominal(", DoubleCodec) shouldBe None
    NominalSchema.fromShortString("nominal)", DoubleCodec) shouldBe None
    NominalSchema.fromShortString("nominal,", DoubleCodec) shouldBe None
    NominalSchema.fromShortString("nominal,)", DoubleCodec) shouldBe None
    NominalSchema.fromShortString("nominal(,", DoubleCodec) shouldBe None
    NominalSchema.fromShortString("nominal(,)", DoubleCodec) shouldBe None
    NominalSchema.fromShortString("nominal(-1,foo)", DoubleCodec) shouldBe None
    NominalSchema.fromShortString("nominal(foo,1)", DoubleCodec) shouldBe None
    NominalSchema.fromShortString("nominal(-1:1)", DoubleCodec) shouldBe None

    NominalSchema.fromShortString("nominal(", LongCodec) shouldBe None
    NominalSchema.fromShortString("nominal)", LongCodec) shouldBe None
    NominalSchema.fromShortString("nominal,", LongCodec) shouldBe None
    NominalSchema.fromShortString("nominal,)", LongCodec) shouldBe None
    NominalSchema.fromShortString("nominal(,", LongCodec) shouldBe None
    NominalSchema.fromShortString("nominal(,)", LongCodec) shouldBe None
    NominalSchema.fromShortString("nominal(-1,foo)", LongCodec) shouldBe None
    NominalSchema.fromShortString("nominal(foo,1)", LongCodec) shouldBe None
    NominalSchema.fromShortString("nominal(-1:1)", LongCodec) shouldBe None
    NominalSchema.fromShortString("nominal(-1.2,1)", LongCodec) shouldBe None
    NominalSchema.fromShortString("nominal(-1,1.2)", LongCodec) shouldBe None
  }
}

class TestOrdinalSchema extends TestGrimlock {

  "A OrdinalSchema" should "return its string representation" in {
    OrdinalSchema[Long]().toString shouldBe "OrdinalSchema()"
    OrdinalSchema[Long](Set[Long](1,2,3)).toString shouldBe "OrdinalSchema(Set(1,2,3))"

    OrdinalSchema[Double]().toString shouldBe "OrdinalSchema()"
    OrdinalSchema[Double](Set[Double](1,2,3)).toString shouldBe "OrdinalSchema(Set(1.0,2.0,3.0))"

    OrdinalSchema[String]().toString shouldBe "OrdinalSchema()"
    OrdinalSchema[String](Set("a","b","c")).toString shouldBe "OrdinalSchema(Set(a,b,c))"
  }

  it should "validate a correct value" in {
    OrdinalSchema[Long]().validate(LongValue(1)) shouldBe true
    OrdinalSchema[Long](Set[Long](1,2,3)).validate(LongValue(1)) shouldBe true

    OrdinalSchema[Double]().validate(DoubleValue(1)) shouldBe true
    OrdinalSchema[Double](Set[Double](1,2,3)).validate(DoubleValue(1)) shouldBe true

    OrdinalSchema[String]().validate(StringValue("a")) shouldBe true
    OrdinalSchema[String](Set("a","b","c")).validate(StringValue("a")) shouldBe true
  }

  it should "not validate an incorrect value" in {
    OrdinalSchema[Long](Set[Long](1,2,3)).validate(LongValue(4)) shouldBe false

    OrdinalSchema[Double](Set[Double](1,2,3)).validate(DoubleValue(4)) shouldBe false

    OrdinalSchema[String](Set("a","b","c")).validate(StringValue("d")) shouldBe false
  }

  it should "parse correctly" in {
    OrdinalSchema.fromShortString("ordinal", StringCodec) shouldBe Option(OrdinalSchema[String]())
    OrdinalSchema.fromShortString("ordinal()", StringCodec) shouldBe Option(OrdinalSchema[String](Set[String]()))
    OrdinalSchema.fromShortString("ordinal(a,b,c)", StringCodec) shouldBe Option(
      OrdinalSchema(Set[String]("a", "b", "c"))
    )

    OrdinalSchema.fromShortString("ordinal", DoubleCodec) shouldBe Option(OrdinalSchema[Double]())
    OrdinalSchema.fromShortString("ordinal()", DoubleCodec) shouldBe Option(OrdinalSchema[Double]())
    OrdinalSchema.fromShortString("ordinal(-3.1415,1.4142,42)", DoubleCodec) shouldBe Option(
      OrdinalSchema[Double](Set(-3.1415, 1.4142, 42))
    )

    OrdinalSchema.fromShortString("ordinal", LongCodec) shouldBe Option(OrdinalSchema[Long]())
    OrdinalSchema.fromShortString("ordinal()", LongCodec) shouldBe Option(OrdinalSchema[Long]())
    OrdinalSchema.fromShortString("ordinal(-1,1,3)", LongCodec) shouldBe Option(OrdinalSchema(Set[Long](-1, 1, 3)))

    OrdinalSchema.fromShortString("foo", StringCodec) shouldBe None
    OrdinalSchema.fromShortString("foo", DoubleCodec) shouldBe None
    OrdinalSchema.fromShortString("foo", LongCodec) shouldBe None

    OrdinalSchema.fromShortString("ordinal(", StringCodec) shouldBe None
    OrdinalSchema.fromShortString("ordinal)", StringCodec) shouldBe None
    OrdinalSchema.fromShortString("ordinal,", StringCodec) shouldBe None
    OrdinalSchema.fromShortString("ordinal,)", StringCodec) shouldBe None
    OrdinalSchema.fromShortString("ordinal(,", StringCodec) shouldBe None
    OrdinalSchema.fromShortString("ordinal(,)", StringCodec) shouldBe Option(OrdinalSchema(Set[String]("")))

    OrdinalSchema.fromShortString("ordinal(", DoubleCodec) shouldBe None
    OrdinalSchema.fromShortString("ordinal)", DoubleCodec) shouldBe None
    OrdinalSchema.fromShortString("ordinal,", DoubleCodec) shouldBe None
    OrdinalSchema.fromShortString("ordinal,)", DoubleCodec) shouldBe None
    OrdinalSchema.fromShortString("ordinal(,", DoubleCodec) shouldBe None
    OrdinalSchema.fromShortString("ordinal(,)", DoubleCodec) shouldBe None
    OrdinalSchema.fromShortString("ordinal(-1,foo)", DoubleCodec) shouldBe None
    OrdinalSchema.fromShortString("ordinal(foo,1)", DoubleCodec) shouldBe None
    OrdinalSchema.fromShortString("ordinal(-1:1)", DoubleCodec) shouldBe None

    OrdinalSchema.fromShortString("ordinal(", LongCodec) shouldBe None
    OrdinalSchema.fromShortString("ordinal)", LongCodec) shouldBe None
    OrdinalSchema.fromShortString("ordinal,", LongCodec) shouldBe None
    OrdinalSchema.fromShortString("ordinal,)", LongCodec) shouldBe None
    OrdinalSchema.fromShortString("ordinal(,", LongCodec) shouldBe None
    OrdinalSchema.fromShortString("ordinal(,)", LongCodec) shouldBe None
    OrdinalSchema.fromShortString("ordinal(-1,foo)", LongCodec) shouldBe None
    OrdinalSchema.fromShortString("ordinal(foo,1)", LongCodec) shouldBe None
    OrdinalSchema.fromShortString("ordinal(-1:1)", LongCodec) shouldBe None
    OrdinalSchema.fromShortString("ordinal(-1.2,1)", LongCodec) shouldBe None
    OrdinalSchema.fromShortString("ordinal(-1,1.2)", LongCodec) shouldBe None
  }
}

class TestDateSchema extends TestGrimlock {

  val dfmt = new java.text.SimpleDateFormat("yyyy-MM-dd")
  val dtfmt = new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")

  "A DateSchema" should "return its string representation" in {
    DateSchema[java.util.Date]().toString shouldBe "DateSchema()"
  }

  it should "validate a correct value" in {
    DateSchema[java.util.Date]().validate(DateValue(dfmt.parse("2001-01-01"), DateCodec("yyyy-MM-dd"))) shouldBe true
    DateSchema[java.util.Date](dfmt.parse("2001-01-01"), dfmt.parse("2001-01-02"))
      .validate(DateValue(dfmt.parse("2001-01-01"), DateCodec("yyyy-MM-dd"))) shouldBe true
    DateSchema[java.util.Date](Set(dfmt.parse("2001-01-01"), dfmt.parse("2001-01-02")))
      .validate(DateValue(dfmt.parse("2001-01-01"), DateCodec("yyyy-MM-dd"))) shouldBe true

    DateSchema[java.util.Date]()
      .validate(DateValue(dtfmt.parse("2001-01-01 01:02:02"), DateCodec("yyyy-MM-dd hh:mm:ss"))) shouldBe true
    DateSchema[java.util.Date](dtfmt.parse("2001-01-01 01:02:02"), dtfmt.parse("2001-01-02 23:59:59"))
      .validate(DateValue(dtfmt.parse("2001-01-01 01:02:03"), DateCodec("yyyy-MM-dd hh:mm:ss"))) shouldBe true
    DateSchema[java.util.Date](Set(dtfmt.parse("2001-01-01 01:02:02"), dtfmt.parse("2001-01-02 23:59:59")))
      .validate(DateValue(dtfmt.parse("2001-01-01 01:02:02"), DateCodec("yyyy-MM-dd hh:mm:ss"))) shouldBe true
  }

  it should "not validate an incorrect value" in {
    DateSchema[java.util.Date](dfmt.parse("2001-01-01"), dfmt.parse("2001-01-02"))
      .validate(DateValue(dfmt.parse("2001-01-03"), DateCodec("yyyy-MM-dd"))) shouldBe false
    DateSchema[java.util.Date](Set(dfmt.parse("2001-01-01"), dfmt.parse("2001-01-02")))
      .validate(DateValue(dfmt.parse("2001-01-03"), DateCodec("yyyy-MM-dd"))) shouldBe false

    DateSchema[java.util.Date](dtfmt.parse("2001-01-01 01:02:02"), dtfmt.parse("2001-01-02 23:59:59"))
      .validate(DateValue(dtfmt.parse("2001-01-01 01:01:02"), DateCodec("yyyy-MM-dd hh:mm:ss"))) shouldBe false
    DateSchema[java.util.Date](Set(dtfmt.parse("2001-01-01 01:02:02"), dtfmt.parse("2001-01-02 23:59:59")))
      .validate(DateValue(dtfmt.parse("2001-01-01 01:02:03"), DateCodec("yyyy-MM-dd hh:mm:ss"))) shouldBe false
  }

  it should "parse correctly" in {
    DateSchema.fromShortString("date", DateCodec()) shouldBe Option(DateSchema[java.util.Date]())
    DateSchema.fromShortString("date()", DateCodec()) shouldBe Option(DateSchema[java.util.Date]())
    DateSchema.fromShortString("date(2001-01-01:2001-01-03)", DateCodec()) shouldBe Option(
      DateSchema[java.util.Date](dfmt.parse("2001-01-01"), dfmt.parse("2001-01-03"))
    )
    DateSchema.fromShortString("date(2001-01-01,2001-01-02,2001-01-03)", DateCodec()) shouldBe Option(
      DateSchema[java.util.Date](Set(dfmt.parse("2001-01-01"), dfmt.parse("2001-01-02"), dfmt.parse("2001-01-03")))
    )

    DateSchema.fromShortString("date(", DateCodec()) shouldBe None
    DateSchema.fromShortString("date)", DateCodec()) shouldBe None
    DateSchema.fromShortString("date,", DateCodec()) shouldBe None
    DateSchema.fromShortString("date:", DateCodec()) shouldBe None
    DateSchema.fromShortString("date(,", DateCodec()) shouldBe None
    DateSchema.fromShortString("date,)", DateCodec()) shouldBe None
    DateSchema.fromShortString("date(:", DateCodec()) shouldBe None
    DateSchema.fromShortString("date:)", DateCodec()) shouldBe None
    DateSchema.fromShortString("date:,", DateCodec()) shouldBe None
    DateSchema.fromShortString("date,:", DateCodec()) shouldBe None
    DateSchema.fromShortString("date(:,)", DateCodec()) shouldBe None
    DateSchema.fromShortString("date(,:)", DateCodec()) shouldBe None
    DateSchema.fromShortString("date(foo:2001-01-03)", DateCodec()) shouldBe None
    DateSchema.fromShortString("date(2001-01-01:foo)", DateCodec()) shouldBe None
    DateSchema.fromShortString("date(foo,2001-01-03)", DateCodec()) shouldBe None
    DateSchema.fromShortString("date(2001-01-01,foo)", DateCodec()) shouldBe None
    DateSchema.fromShortString("date(2001-01-01:2001-01-02:2001-01-03)", DateCodec()) shouldBe None

    // Unfortunately java date parsing is very lenient, so the following will succeed when it shouldn't.
    //DateSchema.fromShortString("date(2001-01-01,2001-01-02:2001-01-03)", DateCodec()) shouldBe None
    //DateSchema.fromShortString("date(2001-01-01:2001-01-02,2001-01-03)", DateCodec()) shouldBe None
  }
}

