// Copyright 2015,2016,2017,2018,2019 Commonwealth Bank of Australia
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

import commbank.grimlock.framework.content._
import commbank.grimlock.framework.encoding._
import commbank.grimlock.framework.environment.implicits._
import commbank.grimlock.framework.metadata._

class TestContent extends TestGrimlock {
  "A Continuous Double Content" should "return its string value" in {
    Content(ContinuousSchema[Double](), 3.14).toString shouldBe "Content(ContinuousType,DoubleValue(3.14,DoubleCodec))"
    Content(ContinuousSchema[Double](0, 10), 3.14).toString shouldBe
      "Content(ContinuousType,DoubleValue(3.14,DoubleCodec))"
  }

  it should "return its short string value" in {
    Content(ContinuousSchema[Double](), 3.14).toShortString("|") shouldBe "double|continuous|3.14"
    Content(ContinuousSchema[Double](0, 10), 3.14).toShortString("|") shouldBe "double|continuous|3.14"
  }

  it should "parse from string" in {
    Content.fromShortString("double|continuous|3.14", "|") shouldBe Option(Content(ContinuousSchema[Double](), 3.14))
    Content.fromShortString("double|continuous(min=0.0,max=10.0)|3.14", "|") shouldBe
      Option(Content(ContinuousSchema[Double](), 3.14))
    Content.fromShortString("string|continuous|3.14", "|") shouldBe None
    Content.fromShortString("double|continuous|abc", "|") shouldBe None
    Content.fromShortString("double|continuouz|3.14", "|") shouldBe None
    Content.fromShortString("doulbe|continuous|3.14", "|") shouldBe None
  }

  "A Continuous Float Content" should "return its string value" in {
    Content(ContinuousSchema[Float](), 3.14f).toString shouldBe "Content(ContinuousType,FloatValue(3.14,FloatCodec))"
    Content(ContinuousSchema[Float](0, 10), 3.14f).toString shouldBe
      "Content(ContinuousType,FloatValue(3.14,FloatCodec))"
  }

  it should "return its short string value" in {
    Content(ContinuousSchema[Float](), 3.14f).toShortString("|") shouldBe "float|continuous|3.14"
    Content(ContinuousSchema[Float](0, 10), 3.14f).toShortString("|") shouldBe "float|continuous|3.14"
  }

  it should "parse from string" in {
    Content.fromShortString("float|continuous|3.14", "|") shouldBe Option(Content(ContinuousSchema[Float](), 3.14f))
    Content.fromShortString("float|continuous(min=0.0,max=10.0)|3.14", "|") shouldBe
      Option(Content(ContinuousSchema[Float](), 3.14f))
    Content.fromShortString("string|continuous|3.14", "|") shouldBe None
    Content.fromShortString("float|continuous|abc", "|") shouldBe None
    Content.fromShortString("float|continuouz|3.14", "|") shouldBe None
    Content.fromShortString("flota|continuous|3.14", "|") shouldBe None
  }

  "A Continuous Long Content" should "return its string value" in {
    Content(ContinuousSchema[Long](), 42L).toString shouldBe "Content(ContinuousType,LongValue(42,LongCodec))"
    Content(ContinuousSchema[Long](0, 100), 42L).toString shouldBe
      "Content(ContinuousType,LongValue(42,LongCodec))"
  }

  it should "return its short string value" in {
    Content(ContinuousSchema[Long](), 42L).toShortString("|") shouldBe "long|continuous|42"
    Content(ContinuousSchema[Long](0, 100), 42L).toShortString("|") shouldBe "long|continuous|42"
  }

  it should "parse from string" in {
    Content.fromShortString("long|continuous|42", "|") shouldBe Option(Content(ContinuousSchema[Long](), 42L))
    Content.fromShortString("long|continuous(min=0,max=100)|42", "|") shouldBe Option(
      Content(ContinuousSchema[Long](), 42L)
    )
    Content.fromShortString("string|continuous|42", "|") shouldBe None
    Content.fromShortString("long|continuous|abc", "|") shouldBe None
    Content.fromShortString("long|continuouz|42", "|") shouldBe None
    Content.fromShortString("logn|continuous|42", "|") shouldBe None
  }

  "A Discrete Long Content" should "return its string value" in {
    Content(DiscreteSchema[Long](), 42L).toString shouldBe "Content(DiscreteType,LongValue(42,LongCodec))"
    Content(DiscreteSchema[Long](0, 100, 2), 42L).toString shouldBe "Content(DiscreteType,LongValue(42,LongCodec))"
  }

  it should "return its short string value" in {
    Content(DiscreteSchema[Long](), 42L).toShortString("|") shouldBe "long|discrete|42"
    Content(DiscreteSchema[Long](0, 100, 2), 42L).toShortString("|") shouldBe "long|discrete|42"
  }

  it should "parse from string" in {
    Content.fromShortString("long|discrete|42", "|") shouldBe Option(Content(DiscreteSchema[Long](), 42L))
    Content.fromShortString("long|discrete(min=0,max=100,step=2)|42", "|") shouldBe Option(
      Content(DiscreteSchema[Long](), 42L)
    )
    Content.fromShortString("string|discrete|42", "|") shouldBe None
    Content.fromShortString("long|discrete|abc", "|") shouldBe None
    Content.fromShortString("long|discrets|42", "|") shouldBe None
    Content.fromShortString("logn|discrete|42", "|") shouldBe None
  }

  "A Nominal String Content" should "return its string value" in {
    Content(NominalSchema[String](), "a").toString shouldBe "Content(NominalType,StringValue(a,StringCodec))"
    Content(NominalSchema[String](Set("a", "b", "c")), "a").toString shouldBe
      "Content(NominalType,StringValue(a,StringCodec))"
  }

  it should "return its short string value" in {
    Content(NominalSchema[String](), "a").toShortString("|") shouldBe "string|nominal|a"
    Content(NominalSchema[String](Set("a", "b", "c")), "a").toShortString("|") shouldBe "string|nominal|a"
  }

  it should "parse from string" in {
    Content.fromShortString("string|nominal|a", "|") shouldBe Option(Content(NominalSchema[String](), "a"))
    Content.fromShortString("string|nominal(set={a,b,c})|a", "|") shouldBe Option(Content(NominalSchema[String](), "a"))
    Content.fromShortString("string|nominas|a", "|") shouldBe None
    Content.fromShortString("strign|nominal|a", "|") shouldBe None
  }

  "A Nominal Double Content" should "return its string value" in {
    Content(NominalSchema[Double](), 1.0).toString shouldBe "Content(NominalType,DoubleValue(1.0,DoubleCodec))"
    Content(NominalSchema[Double](Set[Double](1, 2, 3)), 1.0).toString shouldBe
      "Content(NominalType,DoubleValue(1.0,DoubleCodec))"
  }

  it should "return its short string value" in {
    Content(NominalSchema[Double](), 1.0).toShortString("|") shouldBe "double|nominal|1.0"
    Content(NominalSchema[Double](Set[Double](1, 2, 3)), 1.0).toShortString("|") shouldBe "double|nominal|1.0"
  }

  it should "parse from string" in {
    Content.fromShortString("double|nominal|1.0", "|") shouldBe Option(Content(NominalSchema[Double](), 1.0))
    Content.fromShortString("double|nominal(set={1.0,2.0,3.0})|1.0", "|") shouldBe
      Option(Content(NominalSchema[Double](), 1.0))
    Content.fromShortString("double|nominal|abc", "|") shouldBe None
    Content.fromShortString("double|nominas|1.0", "|") shouldBe None
    Content.fromShortString("doubel|nominal|1.0", "|") shouldBe None
  }

  "A Nominal Float Content" should "return its string value" in {
    Content(NominalSchema[Float](), 1.0f).toString shouldBe "Content(NominalType,FloatValue(1.0,FloatCodec))"
    Content(NominalSchema[Float](Set[Float](1, 2, 3)), 1.0f).toString shouldBe
      "Content(NominalType,FloatValue(1.0,FloatCodec))"
  }

  it should "return its short string value" in {
    Content(NominalSchema[Float](), 1.0f).toShortString("|") shouldBe "float|nominal|1.0"
    Content(NominalSchema[Float](Set[Float](1, 2, 3)), 1.0f).toShortString("|") shouldBe "float|nominal|1.0"
  }

  it should "parse from string" in {
    Content.fromShortString("float|nominal|1.0", "|") shouldBe Option(Content(NominalSchema[Float](), 1.0f))
    Content.fromShortString("float|nominal(set={1.0,2.0,3.0})|1.0", "|") shouldBe
      Option(Content(NominalSchema[Float](), 1.0f))
    Content.fromShortString("float|nominal|abc", "|") shouldBe None
    Content.fromShortString("float|nominas|1.0", "|") shouldBe None
    Content.fromShortString("flota|nominal|1.0", "|") shouldBe None
  }

  "A Nominal Long Content" should "return its string value" in {
    Content(NominalSchema[Long](), 1L).toString shouldBe "Content(NominalType,LongValue(1,LongCodec))"
    Content(NominalSchema[Long](Set[Long](1, 2, 3)), 1L).toString shouldBe
      "Content(NominalType,LongValue(1,LongCodec))"
  }

  it should "return its short string value" in {
    Content(NominalSchema[Long](), 1L).toShortString("|") shouldBe "long|nominal|1"
    Content(NominalSchema[Long](Set[Long](1, 2, 3)), 1L).toShortString("|") shouldBe "long|nominal|1"
  }

  it should "parse from string" in {
    Content.fromShortString("long|nominal|1", "|") shouldBe Option(Content(NominalSchema[Long](), 1L))
    Content.fromShortString("long|nominal(set={1,2,3})|1", "|") shouldBe Option(Content(NominalSchema[Long](), 1L))
    Content.fromShortString("long|nominal|abc", "|") shouldBe None
    Content.fromShortString("long|nominas|1", "|") shouldBe None
    Content.fromShortString("logn|nominal|1", "|") shouldBe None
  }

  "A Ordinal String Content" should "return its string value" in {
    Content(OrdinalSchema[String](), "a").toString shouldBe "Content(OrdinalType,StringValue(a,StringCodec))"
    Content(OrdinalSchema[String](Set("a", "b", "c")), "a").toString shouldBe
      "Content(OrdinalType,StringValue(a,StringCodec))"
  }

  it should "return its short string value" in {
    Content(OrdinalSchema[String](), "a").toShortString("|") shouldBe "string|ordinal|a"
    Content(OrdinalSchema[String](Set("a", "b", "c")), "a").toShortString("|") shouldBe "string|ordinal|a"
  }

  it should "parse from string" in {
    Content.fromShortString("string|ordinal|a", "|") shouldBe Option(Content(OrdinalSchema[String](), "a"))
    Content.fromShortString("string|ordinal(set={a,b,c})|a", "|") shouldBe Option(Content(OrdinalSchema[String](), "a"))
    Content.fromShortString("string|ordinas|a", "|") shouldBe None
    Content.fromShortString("strign|ordinal|a", "|") shouldBe None
  }

  "A Ordinal Double Content" should "return its string value" in {
    Content(OrdinalSchema[Double](), 1.0).toString shouldBe "Content(OrdinalType,DoubleValue(1.0,DoubleCodec))"
    Content(OrdinalSchema[Double](Set[Double](1, 2, 3)), 1.0).toString shouldBe
      "Content(OrdinalType,DoubleValue(1.0,DoubleCodec))"
  }

  it should "return its short string value" in {
    Content(OrdinalSchema[Double](), 1.0).toShortString("|") shouldBe "double|ordinal|1.0"
    Content(OrdinalSchema[Double](Set[Double](1, 2, 3)), 1.0).toShortString("|") shouldBe "double|ordinal|1.0"
  }

  it should "parse from string" in {
    Content.fromShortString("double|ordinal|1.0", "|") shouldBe Option(Content(OrdinalSchema[Double](), 1.0))
    Content.fromShortString("double|ordinal(set={1.0,2.0,3.0})|1.0", "|") shouldBe
      Option(Content(OrdinalSchema[Double](), 1.0))
    Content.fromShortString("double|ordinal|abc", "|") shouldBe None
    Content.fromShortString("double|ordinas|1.0", "|") shouldBe None
    Content.fromShortString("doubel|ordinal|1.0", "|") shouldBe None
  }

  "A Ordinal Float Content" should "return its string value" in {
    Content(OrdinalSchema[Float](), 1.0f).toString shouldBe "Content(OrdinalType,FloatValue(1.0,FloatCodec))"
    Content(OrdinalSchema[Float](Set[Float](1, 2, 3)), 1.0f).toString shouldBe
      "Content(OrdinalType,FloatValue(1.0,FloatCodec))"
  }

  it should "return its short string value" in {
    Content(OrdinalSchema[Float](), 1.0f).toShortString("|") shouldBe "float|ordinal|1.0"
    Content(OrdinalSchema[Float](Set[Float](1, 2, 3)), 1.0f).toShortString("|") shouldBe "float|ordinal|1.0"
  }

  it should "parse from string" in {
    Content.fromShortString("float|ordinal|1.0", "|") shouldBe Option(Content(OrdinalSchema[Float](), 1.0f))
    Content.fromShortString("float|ordinal(set={1.0,2.0,3.0})|1.0", "|") shouldBe
      Option(Content(OrdinalSchema[Float](), 1.0f))
    Content.fromShortString("float|ordinal|abc", "|") shouldBe None
    Content.fromShortString("float|ordinas|1.0", "|") shouldBe None
    Content.fromShortString("flota|ordinal|1.0", "|") shouldBe None
  }

  "A Ordinal Long Content" should "return its string value" in {
    Content(OrdinalSchema[Long](), 1L).toString shouldBe "Content(OrdinalType,LongValue(1,LongCodec))"
    Content(OrdinalSchema[Long](Set[Long](1, 2, 3)), 1L).toString shouldBe
      "Content(OrdinalType,LongValue(1,LongCodec))"
  }

  it should "return its short string value" in {
    Content(OrdinalSchema[Long](), 1L).toShortString("|") shouldBe "long|ordinal|1"
    Content(OrdinalSchema[Long](Set[Long](1, 2, 3)), 1L).toShortString("|") shouldBe "long|ordinal|1"
  }

  it should "parse from string" in {
    Content.fromShortString("long|ordinal|1", "|") shouldBe Option(Content(OrdinalSchema[Long](), 1L))
    Content.fromShortString("long|ordinal(set={1,2,3})|1", "|") shouldBe Option(Content(OrdinalSchema[Long](), 1L))
    Content.fromShortString("long|ordinal|abc", "|") shouldBe None
    Content.fromShortString("long|ordinas|1", "|") shouldBe None
    Content.fromShortString("logn|ordinal|1", "|") shouldBe None
  }

  val dfmt = new java.text.SimpleDateFormat("yyyy-MM-dd")

  "A Date Date Content" should "return its string value" in {
    Content(
      DateSchema[java.util.Date](),
      DateValue(dfmt.parse("2001-01-01"))
    ).toString shouldBe "Content(DateType,DateValue(" + dfmt.parse("2001-01-01").toString + ",DateCodec(yyyy-MM-dd)))"
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
    ).toString shouldBe "Content(DateType,DateValue(" +
      dtfmt.parse("2001-01-01 01:01:01").toString +
      ",DateCodec(yyyy-MM-dd hh:mm:ss)))"
  }

  it should "return its short string value" in {
    Content(
      DateSchema[java.util.Date](),
      DateValue(dtfmt.parse("2001-01-01 01:01:01"), DateCodec("yyyy-MM-dd hh:mm:ss"))
    ).toShortString("|") shouldBe "date(yyyy-MM-dd hh:mm:ss)|date|2001-01-01 01:01:01"
  }

  it should "parse from string" in {
    Content.fromShortString("date(yyyy-MM-dd)|date|2001-01-01", "|") shouldBe
      Option(Content(DateSchema[java.util.Date](), DateValue(dfmt.parse("2001-01-01"))))
    Content.fromShortString("string|date|2001-01-01", "|") shouldBe None
    Content.fromShortString("date(yyyy-MM-dd)|date|abc", "|") shouldBe None
    Content.fromShortString("date(yyyy-MM-dd)|dats|2001-01-01", "|") shouldBe None
    Content.fromShortString("daet(yyyy-MM-dd)|date|2001-01-01", "|") shouldBe None
  }
}

