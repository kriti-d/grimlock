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

import commbank.grimlock.framework.content._
import commbank.grimlock.framework.content.metadata._
import commbank.grimlock.framework.encoding._

class TestContent extends TestGrimlock {

  "A Continuous Double Content" should "return its string value" in {
    Content(ContinuousSchema[Double](), 3.14).toString shouldBe
      "Content(ContinuousSchema[Double](),DoubleValue(3.14,DoubleCodec))"
    Content(ContinuousSchema[Double](0, 10), 3.14).toString shouldBe
      "Content(ContinuousSchema[Double](0.0,10.0),DoubleValue(3.14,DoubleCodec))"
  }

  it should "return its short string value" in {
    Content(ContinuousSchema[Double](), 3.14).toShortString("|") shouldBe "double|continuous|3.14"
    Content(ContinuousSchema[Double](0, 10), 3.14).toShortString("|") shouldBe "double|continuous(0.0:10.0)|3.14"
  }

  it should "parse from string" in {
    Content.fromShortString("double|continuous|3.14") shouldBe Option(Content(ContinuousSchema[Double](), 3.14))
    Content.fromShortString("double|continuous(0.0:10.0)|3.14") shouldBe
      Option(Content(ContinuousSchema[Double](0, 10), 3.14))
    Content.fromShortString("string|continuous|3.14") shouldBe None
    Content.fromShortString("double|continuous|abc") shouldBe None
    Content.fromShortString("double|continuouz|3.14") shouldBe None
    Content.fromShortString("doulbe|continuous|3.14") shouldBe None
  }

  "A Continuous Long Content" should "return its string value" in {
    Content(ContinuousSchema[Long](), 42).toString shouldBe "Content(ContinuousSchema[Long](),LongValue(42,LongCodec))"
    Content(ContinuousSchema[Long](0, 100), 42).toString shouldBe
      "Content(ContinuousSchema[Long](0,100),LongValue(42,LongCodec))"
  }

  it should "return its short string value" in {
    Content(ContinuousSchema[Long](), 42).toShortString("|") shouldBe "long|continuous|42"
    Content(ContinuousSchema[Long](0, 100), 42).toShortString("|") shouldBe "long|continuous(0:100)|42"
  }

  it should "parse from string" in {
    Content.fromShortString("long|continuous|42") shouldBe Option(Content(ContinuousSchema[Long](), 42))
    Content.fromShortString("long|continuous(0:100)|42") shouldBe Option(Content(ContinuousSchema[Long](0, 100), 42))
    Content.fromShortString("string|continuous|42") shouldBe None
    Content.fromShortString("long|continuous|abc") shouldBe None
    Content.fromShortString("long|continuouz|42") shouldBe None
    Content.fromShortString("logn|continuous|42") shouldBe None
  }

  "A Discrete Long Content" should "return its string value" in {
    Content(DiscreteSchema[Long](), 42).toString shouldBe "Content(DiscreteSchema[Long](),LongValue(42,LongCodec))"
    Content(DiscreteSchema[Long](0, 100, 2), 42).toString shouldBe
      "Content(DiscreteSchema[Long](0,100,2),LongValue(42,LongCodec))"
  }

  it should "return its short string value" in {
    Content(DiscreteSchema[Long](), 42).toShortString("|") shouldBe "long|discrete|42"
    Content(DiscreteSchema[Long](0, 100, 2), 42).toShortString("|") shouldBe "long|discrete(0:100,2)|42"
  }

  it should "parse from string" in {
    Content.fromShortString("long|discrete|42") shouldBe Option(Content(DiscreteSchema[Long](), 42))
    Content.fromShortString("long|discrete(0:100,2)|42") shouldBe Option(Content(DiscreteSchema[Long](0, 100, 2), 42))
    Content.fromShortString("string|discrete|42") shouldBe None
    Content.fromShortString("long|discrete|abc") shouldBe None
    Content.fromShortString("long|discrets|42") shouldBe None
    Content.fromShortString("logn|discrete|42") shouldBe None
  }

  "A Nominal String Content" should "return its string value" in {
    Content(NominalSchema[String](), "a").toString shouldBe
      "Content(NominalSchema[String](),StringValue(a,StringCodec))"
    Content(NominalSchema[String](Set("a", "b", "c")), "a").toString shouldBe
      "Content(NominalSchema[String](Set(a,b,c)),StringValue(a,StringCodec))"
  }

  it should "return its short string value" in {
    Content(NominalSchema[String](), "a").toShortString("|") shouldBe "string|nominal|a"
    Content(NominalSchema[String](Set("a", "b", "c")), "a").toShortString("|") shouldBe "string|nominal(a,b,c)|a"
  }

  it should "parse from string" in {
    Content.fromShortString("string|nominal|a") shouldBe Option(Content(NominalSchema[String](), "a"))
    Content.fromShortString("string|nominal(a,b,c)|a") shouldBe
      Option(Content(NominalSchema[String](Set("a", "b", "c")), "a"))
    Content.fromShortString("string|nominas|a") shouldBe None
    Content.fromShortString("strign|nominal|a") shouldBe None
  }

  "A Nominal Double Content" should "return its string value" in {
    Content(NominalSchema[Double](), 1.0).toString shouldBe
      "Content(NominalSchema[Double](),DoubleValue(1.0,DoubleCodec))"
    Content(NominalSchema[Double](Set[Double](1, 2, 3)), 1.0).toString shouldBe
      "Content(NominalSchema[Double](Set(1.0,2.0,3.0)),DoubleValue(1.0,DoubleCodec))"
  }

  it should "return its short string value" in {
    Content(NominalSchema[Double](), 1.0).toShortString("|") shouldBe "double|nominal|1.0"
    Content(NominalSchema[Double](Set[Double](1, 2, 3)), 1.0).toShortString("|") shouldBe
      "double|nominal(1.0,2.0,3.0)|1.0"
  }

  it should "parse from string" in {
    Content.fromShortString("double|nominal|1.0") shouldBe Option(Content(NominalSchema[Double](), 1.0))
    Content.fromShortString("double|nominal(1.0,2.0,3.0)|1.0") shouldBe
      Option(Content(NominalSchema[Double](Set(1.0, 2.0, 3.0)), 1.0))
    Content.fromShortString("double|nominal|abc") shouldBe None
    Content.fromShortString("double|nominas|1.0") shouldBe None
    Content.fromShortString("doubel|nominal|1.0") shouldBe None
  }

  "A Nominal Long Content" should "return its string value" in {
    Content(NominalSchema[Long](), 1).toString shouldBe "Content(NominalSchema[Long](),LongValue(1,LongCodec))"
    Content(NominalSchema[Long](Set[Long](1, 2, 3)), 1).toString shouldBe
      "Content(NominalSchema[Long](Set(1,2,3)),LongValue(1,LongCodec))"
  }

  it should "return its short string value" in {
    Content(NominalSchema[Long](), 1).toShortString("|") shouldBe "long|nominal|1"
    Content(NominalSchema[Long](Set[Long](1, 2, 3)), 1).toShortString("|") shouldBe "long|nominal(1,2,3)|1"
  }

  it should "parse from string" in {
    Content.fromShortString("long|nominal|1") shouldBe Option(Content(NominalSchema[Long](), 1))
    Content.fromShortString("long|nominal(1,2,3)|1") shouldBe
      Option(Content(NominalSchema[Long](Set(1, 2, 3)), 1))
    Content.fromShortString("long|nominal|abc") shouldBe None
    Content.fromShortString("long|nominas|1") shouldBe None
    Content.fromShortString("logn|nominal|1") shouldBe None
  }

  "A Ordinal String Content" should "return its string value" in {
    Content(OrdinalSchema[String](), "a").toString shouldBe
      "Content(OrdinalSchema[String](),StringValue(a,StringCodec))"
    Content(OrdinalSchema[String](Set("a", "b", "c")), "a").toString shouldBe
      "Content(OrdinalSchema[String](Set(a,b,c)),StringValue(a,StringCodec))"
  }

  it should "return its short string value" in {
    Content(OrdinalSchema[String](), "a").toShortString("|") shouldBe "string|ordinal|a"
    Content(OrdinalSchema[String](Set("a", "b", "c")), "a").toShortString("|") shouldBe "string|ordinal(a,b,c)|a"
  }

  it should "parse from string" in {
    Content.fromShortString("string|ordinal|a") shouldBe Option(Content(OrdinalSchema[String](), "a"))
    Content.fromShortString("string|ordinal(a,b,c)|a") shouldBe
      Option(Content(OrdinalSchema[String](Set("a", "b", "c")), "a"))
    Content.fromShortString("string|ordinas|a") shouldBe None
    Content.fromShortString("strign|ordinal|a") shouldBe None
  }

  "A Ordinal Double Content" should "return its string value" in {
    Content(OrdinalSchema[Double](), 1.0).toString shouldBe
      "Content(OrdinalSchema[Double](),DoubleValue(1.0,DoubleCodec))"
    Content(OrdinalSchema[Double](Set[Double](1, 2, 3)), 1.0).toString shouldBe
      "Content(OrdinalSchema[Double](Set(1.0,2.0,3.0)),DoubleValue(1.0,DoubleCodec))"
  }

  it should "return its short string value" in {
    Content(OrdinalSchema[Double](), 1.0).toShortString("|") shouldBe "double|ordinal|1.0"
    Content(OrdinalSchema[Double](Set[Double](1, 2, 3)), 1.0).toShortString("|") shouldBe
      "double|ordinal(1.0,2.0,3.0)|1.0"
  }

  it should "parse from string" in {
    Content.fromShortString("double|ordinal|1.0") shouldBe Option(Content(OrdinalSchema[Double](), 1.0))
    Content.fromShortString("double|ordinal(1.0,2.0,3.0)|1.0") shouldBe
      Option(Content(OrdinalSchema[Double](Set(1.0, 2.0, 3.0)), 1.0))
    Content.fromShortString("double|ordinal|abc") shouldBe None
    Content.fromShortString("double|ordinas|1.0") shouldBe None
    Content.fromShortString("doubel|ordinal|1.0") shouldBe None
  }

  "A Ordinal Long Content" should "return its string value" in {
    Content(OrdinalSchema[Long](), 1).toString shouldBe "Content(OrdinalSchema[Long](),LongValue(1,LongCodec))"
    Content(OrdinalSchema[Long](Set[Long](1, 2, 3)), 1).toString shouldBe
      "Content(OrdinalSchema[Long](Set(1,2,3)),LongValue(1,LongCodec))"
  }

  it should "return its short string value" in {
    Content(OrdinalSchema[Long](), 1).toShortString("|") shouldBe "long|ordinal|1"
    Content(OrdinalSchema[Long](Set[Long](1, 2, 3)), 1).toShortString("|") shouldBe "long|ordinal(1,2,3)|1"
  }

  it should "parse from string" in {
    Content.fromShortString("long|ordinal|1") shouldBe Option(Content(OrdinalSchema[Long](), 1))
    Content.fromShortString("long|ordinal(1,2,3)|1") shouldBe
      Option(Content(OrdinalSchema[Long](Set(1, 2, 3)), 1))
    Content.fromShortString("long|ordinal|abc") shouldBe None
    Content.fromShortString("long|ordinas|1") shouldBe None
    Content.fromShortString("logn|ordinal|1") shouldBe None
  }

  val dfmt = new java.text.SimpleDateFormat("yyyy-MM-dd")

  "A Date Date Content" should "return its string value" in {
    Content(
      DateSchema[java.util.Date](),
      DateValue(dfmt.parse("2001-01-01"))
    ).toString shouldBe "Content(DateSchema[java.util.Date](),DateValue(" + dfmt.parse("2001-01-01").toString +
      ",DateCodec(yyyy-MM-dd)))"
  }

  it should "return its short string value" in {
    Content(DateSchema[java.util.Date](), DateValue(dfmt.parse("2001-01-01"))).toShortString("|") shouldBe
      "date(yyyy-MM-dd)|date|2001-01-01"
  }

  val dtfmt = new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")

  "A Date DateTime Content" should "return its string value" in {
    Content(
      DateSchema[java.util.Date](),
      DateValue(dtfmt.parse("2001-01-01 01:01:01"), DateCodec("yyyy-MM-dd hh:mm:ss"))
    ).toString shouldBe "Content(DateSchema[java.util.Date](),DateValue(" +
      dtfmt.parse("2001-01-01 01:01:01").toString + ",DateCodec(yyyy-MM-dd hh:mm:ss)))"
  }

  it should "return its short string value" in {
    Content(
      DateSchema[java.util.Date](),
      DateValue(dtfmt.parse("2001-01-01 01:01:01"), DateCodec("yyyy-MM-dd hh:mm:ss"))
    ).toShortString("|") shouldBe "date(yyyy-MM-dd hh:mm:ss)|date|2001-01-01 01:01:01"
  }

  it should "parse from string" in {
    Content.fromShortString("date(yyyy-MM-dd)|date|2001-01-01") shouldBe
      Option(Content(DateSchema[java.util.Date](), DateValue(dfmt.parse("2001-01-01"))))
    Content.fromShortString("string|date|2001-01-01") shouldBe None
    Content.fromShortString("date(yyyy-MM-dd)|date|abc") shouldBe None
    Content.fromShortString("date(yyyy-MM-dd)|dats|2001-01-01") shouldBe None
    Content.fromShortString("daet(yyyy-MM-dd)|date|2001-01-01") shouldBe None
  }
}

