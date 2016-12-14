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

import commbank.grimlock.framework._
import commbank.grimlock.framework.content._
import commbank.grimlock.framework.content.metadata._
import commbank.grimlock.framework.encoding._
import commbank.grimlock.framework.position._

import shapeless.nat.{ _1, _2, _3, _4, _5, _6 }

class TestCell extends TestGrimlock {

  "A Cell" should "return its string" in {
    Cell(Position("foo", 123), Content(ContinuousSchema[Double](), 3.14)).toString() shouldBe
      "Cell(Position(StringValue(foo,StringCodec),LongValue(123,LongCodec)),Content(ContinuousSchema[Double](),DoubleValue(3.14,DoubleCodec)))"
    Cell(Position("foo", 123), Content(ContinuousSchema[Double](), 3.14)).toShortString(".", true) shouldBe
      "foo.123.double.continuous.3.14"
    Cell(Position("foo", 123), Content(ContinuousSchema[Double](), 3.14)).toShortString(".", false) shouldBe
      "foo.123.3.14"
  }

  "A Cell" should "relocate" in {
    Cell(Position("foo", 123), Content(ContinuousSchema[Double](), 3.14)).relocate(_.position.append("abc")) shouldBe
      Cell(Position("foo", 123, "abc"), Content(ContinuousSchema[Double](), 3.14))
  }

  "A Cell" should "mutate" in {
    Cell(Position("foo", 123), Content(ContinuousSchema[Double](), 3.14))
      .mutate(_ => Content(DiscreteSchema[Long](), 42)) shouldBe
        Cell(Position("foo", 123), Content(DiscreteSchema[Long](), 42))
  }

  val schema = Content.parser(DoubleCodec, ContinuousSchema[Double]())
  val dictionary = Map("123" -> schema)

  "A Cell" should "parse 1D" in {
    val f1 = Cell.parse1D(":", LongCodec)
    f1("123:double:continuous:3.14") shouldBe List(
      Right(Cell(Position(123), Content(ContinuousSchema[Double](), 3.14)))
    )
    val f2 = Cell.parse1D(":", LongCodec)
    f2("abc:double:continuous:3.14") shouldBe List(Left("Unable to decode: 'abc:double:continuous:3.14'"))
    val f3 = Cell.parse1D(":", LongCodec)
    f3("123:double:continuous:abc") shouldBe List(Left("Unable to decode: '123:double:continuous:abc'"))
    val f4 = Cell.parse1D(":", LongCodec)
    f4("123:double:continuous:3:14") shouldBe List(Left("Unable to decode: '123:double:continuous:3:14'"))
    val f5 = Cell.parse1D(":", LongCodec)
    f5("123:double|continuous:3.14") shouldBe List(Left("Unable to split: '123:double|continuous:3.14'"))
  }

  "A Cell" should "parse 1D with dictionary" in {
    val f1 = Cell.parse1DWithDictionary(dictionary, ":", LongCodec)
    f1("123:3.14") shouldBe List(Right(Cell(Position(123), Content(ContinuousSchema[Double](), 3.14))))
    val f2 = Cell.parse1DWithDictionary(dictionary, ":", LongCodec)
    f2("abc:3.14") shouldBe List(Left("Missing schema for: 'abc:3.14'"))
    val f3 = Cell.parse1DWithDictionary(dictionary, ":", LongCodec)
    f3("123:abc") shouldBe List(Left("Unable to decode: '123:abc'"))
    val f4 = Cell.parse1DWithDictionary(dictionary, ":", LongCodec)
    f4("123:3:14") shouldBe List(Left("Unable to decode: '123:3:14'"))
    val f5 = Cell.parse1DWithDictionary(dictionary, ":", LongCodec)
    f5("123|3.14") shouldBe List(Left("Unable to split: '123|3.14'"))
  }

  "A Cell" should "parse 1D with schema" in {
    val f1 = Cell.parse1DWithSchema(schema, ":", LongCodec)
    f1("123:3.14") shouldBe List(Right(Cell(Position(123), Content(ContinuousSchema[Double](), 3.14))))
    val f2 = Cell.parse1DWithSchema(schema, ":", LongCodec)
    f2("abc:3.14") shouldBe List(Left("Unable to decode: 'abc:3.14'"))
    val f3 = Cell.parse1DWithSchema(schema, ":", LongCodec)
    f3("123:abc") shouldBe List(Left("Unable to decode: '123:abc'"))
    val f4 = Cell.parse1DWithSchema(schema, ":", LongCodec)
    f4("123:3:14") shouldBe List(Left("Unable to decode: '123:3:14'"))
    val f5 = Cell.parse1DWithSchema(schema, ":", LongCodec)
    f5("123|3.14") shouldBe List(Left("Unable to split: '123|3.14'"))
  }

  "A Cell" should "parse 2D" in {
    val f1 = Cell.parse2D(":", LongCodec, StringCodec)
    f1("123:def:double:continuous:3.14") shouldBe List(
      Right(Cell(Position(123, "def"), Content(ContinuousSchema[Double](), 3.14)))
    )
    val f2 = Cell.parse2D(":", LongCodec, StringCodec)
    f2("abc:def:double:continuous:3.14") shouldBe List(Left("Unable to decode: 'abc:def:double:continuous:3.14'"))
    val f3 = Cell.parse2D(":", StringCodec, LongCodec)
    f3("abc:def:double:continuous:3.14") shouldBe List(Left("Unable to decode: 'abc:def:double:continuous:3.14'"))
    val f4 = Cell.parse2D(":", LongCodec, StringCodec)
    f4("123:def:double:continuous:abc") shouldBe List(Left("Unable to decode: '123:def:double:continuous:abc'"))
    val f5 = Cell.parse2D(":", LongCodec, StringCodec)
    f5("123:def:double:continuous:3:14") shouldBe List(Left("Unable to decode: '123:def:double:continuous:3:14'"))
    val f6 = Cell.parse2D(":", LongCodec, StringCodec)
    f6("123:def:double|continuous:3.14") shouldBe List(Left("Unable to split: '123:def:double|continuous:3.14'"))
  }

  "A Cell" should "parse 2D with dictionary" in {
    val f1 = Cell.parse2DWithDictionary(dictionary, _1, ":", LongCodec, StringCodec)
    f1("123:def:3.14") shouldBe List(Right(Cell(Position(123, "def"), Content(ContinuousSchema[Double](), 3.14))))
    val f2 = Cell.parse2DWithDictionary(dictionary, _2, ":", StringCodec, LongCodec)
    f2("def:123:3.14") shouldBe List(Right(Cell(Position("def", 123), Content(ContinuousSchema[Double](), 3.14))))
    val f3 = Cell.parse2DWithDictionary(dictionary, _1, ":", LongCodec, StringCodec)
    f3("abc:def:3.14") shouldBe List(Left("Missing schema for: 'abc:def:3.14'"))
    val f4 = Cell.parse2DWithDictionary(dictionary, _2, ":", LongCodec, StringCodec)
    f4("abc:def:3.14") shouldBe List(Left("Missing schema for: 'abc:def:3.14'"))
    val f5 = Cell.parse2DWithDictionary(dictionary, _1, ":", LongCodec, StringCodec)
    f5("123:def:abc") shouldBe List(Left("Unable to decode: '123:def:abc'"))
    val f6 = Cell.parse2DWithDictionary(dictionary, _1, ":", LongCodec, StringCodec)
    f6("123:def:3:14") shouldBe List(Left("Unable to decode: '123:def:3:14'"))
    val f7 = Cell.parse2DWithDictionary(dictionary, _1, ":", LongCodec, StringCodec)
    f7("123|def:3.14") shouldBe List(Left("Unable to split: '123|def:3.14'"))
  }

  "A Cell" should "parse 2D with schema" in {
    val f1 = Cell.parse2DWithSchema(schema, ":", LongCodec, StringCodec)
    f1("123:def:3.14") shouldBe List(Right(Cell(Position(123, "def"), Content(ContinuousSchema[Double](), 3.14))))
    val f2 = Cell.parse2DWithSchema(schema, ":", LongCodec, StringCodec)
    f2("abc:def:3.14") shouldBe List(Left("Unable to decode: 'abc:def:3.14'"))
    val f3 = Cell.parse2DWithSchema(schema, ":", LongCodec, StringCodec)
    f3("123:def:abc") shouldBe List(Left("Unable to decode: '123:def:abc'"))
    val f4 = Cell.parse2DWithSchema(schema, ":", LongCodec, StringCodec)
    f4("123:def:3:14") shouldBe List(Left("Unable to decode: '123:def:3:14'"))
    val f5 = Cell.parse2DWithSchema(schema, ":", LongCodec, StringCodec)
    f5("123:def|3.14") shouldBe List(Left("Unable to split: '123:def|3.14'"))
  }

  "A Cell" should "parse 3D" in {
    val f1 = Cell.parse3D(":", LongCodec, StringCodec, StringCodec)
    f1("123:def:ghi:double:continuous:3.14") shouldBe List(
      Right(Cell(Position(123, "def", "ghi"), Content(ContinuousSchema[Double](), 3.14)))
    )
    val f2 = Cell.parse3D(":", LongCodec, StringCodec, StringCodec)
    f2("abc:def:ghi:double:continuous:3.14") shouldBe List(
      Left("Unable to decode: 'abc:def:ghi:double:continuous:3.14'")
    )
    val f3 = Cell.parse3D(":", StringCodec, LongCodec, StringCodec)
    f3("def:abc:ghi:double:continuous:3.14") shouldBe List(
      Left("Unable to decode: 'def:abc:ghi:double:continuous:3.14'")
    )
    val f4 = Cell.parse3D(":", StringCodec, StringCodec, LongCodec)
    f4("def:ghi:abc:double:continuous:3.14") shouldBe List(
      Left("Unable to decode: 'def:ghi:abc:double:continuous:3.14'")
    )
    val f5 = Cell.parse3D(":", LongCodec, StringCodec, StringCodec)
    f5("123:def:ghi:double:continuous:abc") shouldBe List(
      Left("Unable to decode: '123:def:ghi:double:continuous:abc'")
    )
    val f6 = Cell.parse3D(":", LongCodec, StringCodec, StringCodec)
    f6("123:def:ghi:double:continuous:3:14") shouldBe List(
      Left("Unable to decode: '123:def:ghi:double:continuous:3:14'")
    )
    val f7 = Cell.parse3D(":", LongCodec, StringCodec, StringCodec)
    f7("123:def:ghi:double|continuous:3.14") shouldBe List(
      Left("Unable to split: '123:def:ghi:double|continuous:3.14'")
    )
  }

  "A Cell" should "parse 3D with dictionary" in {
    val f1 = Cell.parse3DWithDictionary(dictionary, _1, ":", LongCodec, StringCodec, StringCodec)
    f1("123:def:ghi:3.14") shouldBe List(
      Right(Cell(Position(123, "def", "ghi"), Content(ContinuousSchema[Double](), 3.14)))
    )
    val f2 = Cell.parse3DWithDictionary(dictionary, _2, ":", StringCodec, LongCodec, StringCodec)
    f2("def:123:ghi:3.14") shouldBe List(
      Right(Cell(Position("def", 123, "ghi"), Content(ContinuousSchema[Double](), 3.14)))
    )
    val f3 = Cell.parse3DWithDictionary(dictionary, _3, ":", StringCodec, StringCodec, LongCodec)
    f3("def:ghi:123:3.14") shouldBe List(
      Right(Cell(Position("def", "ghi", 123), Content(ContinuousSchema[Double](), 3.14)))
    )
    val f4 = Cell.parse3DWithDictionary(dictionary, _1, ":", LongCodec, StringCodec, StringCodec)
    f4("abc:def:ghi:3.14") shouldBe List(Left("Missing schema for: 'abc:def:ghi:3.14'"))
    val f5 = Cell.parse3DWithDictionary(dictionary, _2, ":", LongCodec, StringCodec, StringCodec)
    f5("abc:def:ghi:3.14") shouldBe List(Left("Missing schema for: 'abc:def:ghi:3.14'"))
    val f6 = Cell.parse3DWithDictionary(dictionary, _3, ":", LongCodec, StringCodec, StringCodec)
    f6("abc:def:ghi:3.14") shouldBe List(Left("Missing schema for: 'abc:def:ghi:3.14'"))
    val f7 = Cell.parse3DWithDictionary(dictionary, _1, ":", LongCodec, StringCodec, StringCodec)
    f7("123:def:ghi:abc") shouldBe List(Left("Unable to decode: '123:def:ghi:abc'"))
    val f8 = Cell.parse3DWithDictionary(dictionary, _1, ":", LongCodec, StringCodec, StringCodec)
    f8("123:def:ghi:3:14") shouldBe List(Left("Unable to decode: '123:def:ghi:3:14'"))
    val f9 = Cell.parse3DWithDictionary(dictionary, _1, ":", LongCodec, StringCodec, StringCodec)
    f9("123|def:ghi:3.14") shouldBe List(Left("Unable to split: '123|def:ghi:3.14'"))
  }

  "A Cell" should "parse 3D with schema" in {
    val f1 = Cell.parse3DWithSchema(schema, ":", LongCodec, StringCodec, StringCodec)
    f1("123:def:ghi:3.14") shouldBe List(
      Right(Cell(Position(123, "def", "ghi"), Content(ContinuousSchema[Double](), 3.14)))
    )
    val f2 = Cell.parse3DWithSchema(schema, ":", LongCodec, StringCodec, StringCodec)
    f2("abc:def:ghi:3.14") shouldBe List(Left("Unable to decode: 'abc:def:ghi:3.14'"))
    val f3 = Cell.parse3DWithSchema(schema, ":", LongCodec, StringCodec, StringCodec)
    f3("123:def:ghi:abc") shouldBe List(Left("Unable to decode: '123:def:ghi:abc'"))
    val f4 = Cell.parse3DWithSchema(schema, ":", LongCodec, StringCodec, StringCodec)
    f4("123:def:ghi:3:14") shouldBe List(Left("Unable to decode: '123:def:ghi:3:14'"))
    val f5 = Cell.parse3DWithSchema(schema, ":", LongCodec, StringCodec, StringCodec)
    f5("123:def|ghi:3.14") shouldBe List(Left("Unable to split: '123:def|ghi:3.14'"))
  }

  "A Cell" should "parse 4D" in {
    val f1 = Cell.parse4D(":", LongCodec, StringCodec, StringCodec, StringCodec)
    f1("123:def:ghi:klm:double:continuous:3.14") shouldBe List(
      Right(Cell(Position(123, "def", "ghi", "klm"), Content(ContinuousSchema[Double](), 3.14)))
    )
    val f2 = Cell.parse4D(":", LongCodec, StringCodec, StringCodec, StringCodec)
    f2("abc:def:ghi:klm:double:continuous:3.14") shouldBe List(
      Left("Unable to decode: 'abc:def:ghi:klm:double:continuous:3.14'")
    )
    val f3 = Cell.parse4D(":", StringCodec, LongCodec, StringCodec, StringCodec)
    f3("def:abc:ghi:klm:double:continuous:3.14") shouldBe List(
      Left("Unable to decode: 'def:abc:ghi:klm:double:continuous:3.14'")
    )
    val f4 = Cell.parse4D(":", StringCodec, StringCodec, LongCodec, StringCodec)
    f4("def:ghi:abc:klm:double:continuous:3.14") shouldBe List(
      Left("Unable to decode: 'def:ghi:abc:klm:double:continuous:3.14'")
    )
    val f5 = Cell.parse4D(":", StringCodec, StringCodec, StringCodec, LongCodec)
    f5("def:ghi:klm:abc:double:continuous:3.14") shouldBe List(
      Left("Unable to decode: 'def:ghi:klm:abc:double:continuous:3.14'")
    )
    val f6 = Cell.parse4D(":", LongCodec, StringCodec, StringCodec, StringCodec)
    f6("123:def:ghi:klm:double:continuous:abc") shouldBe List(
      Left("Unable to decode: '123:def:ghi:klm:double:continuous:abc'")
    )
    val f7 = Cell.parse4D(":", LongCodec, StringCodec, StringCodec, StringCodec)
    f7("123:def:ghi:klm:double:continuous:3:14") shouldBe List(
      Left("Unable to decode: '123:def:ghi:klm:double:continuous:3:14'")
    )
    val f8 = Cell.parse4D(":", LongCodec, StringCodec, StringCodec, StringCodec)
    f8("123:def:ghi:klm:double|continuous:3.14") shouldBe List(
      Left("Unable to split: '123:def:ghi:klm:double|continuous:3.14'")
    )
  }

  "A Cell" should "parse 4D with dictionary" in {
    val f1 = Cell.parse4DWithDictionary(dictionary, _1, ":", LongCodec, StringCodec, StringCodec, StringCodec)
    f1("123:def:ghi:klm:3.14") shouldBe List(
      Right(Cell(Position(123, "def", "ghi", "klm"), Content(ContinuousSchema[Double](), 3.14)))
    )
    val f2 = Cell.parse4DWithDictionary(dictionary, _2, ":", StringCodec, LongCodec, StringCodec, StringCodec)
    f2("def:123:ghi:klm:3.14") shouldBe List(
      Right(Cell(Position("def", 123, "ghi", "klm"), Content(ContinuousSchema[Double](), 3.14)))
    )
    val f3 = Cell.parse4DWithDictionary(dictionary, _3, ":", StringCodec, StringCodec, LongCodec, StringCodec)
    f3("def:ghi:123:klm:3.14") shouldBe List(
      Right(Cell(Position("def", "ghi", 123, "klm"), Content(ContinuousSchema[Double](), 3.14)))
    )
    val f4 = Cell.parse4DWithDictionary(dictionary, _4, ":", StringCodec, StringCodec, StringCodec, LongCodec)
    f4("def:ghi:klm:123:3.14") shouldBe List(
      Right(Cell(Position("def", "ghi", "klm", 123), Content(ContinuousSchema[Double](), 3.14)))
    )
    val f5 = Cell.parse4DWithDictionary(dictionary, _1, ":", LongCodec, StringCodec, StringCodec, StringCodec)
    f5("abc:def:ghi:klm:3.14") shouldBe List(Left("Missing schema for: 'abc:def:ghi:klm:3.14'"))
    val f6 = Cell.parse4DWithDictionary(dictionary, _2, ":", LongCodec, StringCodec, StringCodec, StringCodec)
    f6("abc:def:ghi:klm:3.14") shouldBe List(Left("Missing schema for: 'abc:def:ghi:klm:3.14'"))
    val f7 = Cell.parse4DWithDictionary(dictionary, _3, ":", LongCodec, StringCodec, StringCodec, StringCodec)
    f7("abc:def:ghi:klm:3.14") shouldBe List(Left("Missing schema for: 'abc:def:ghi:klm:3.14'"))
    val f8 = Cell.parse4DWithDictionary(dictionary, _4, ":", LongCodec, StringCodec, StringCodec, StringCodec)
    f8("abc:def:ghi:klm:3.14") shouldBe List(Left("Missing schema for: 'abc:def:ghi:klm:3.14'"))
    val f9 = Cell.parse4DWithDictionary(dictionary, _1, ":", LongCodec, StringCodec, StringCodec, StringCodec)
    f9("123:def:ghi:klm:abc") shouldBe List(Left("Unable to decode: '123:def:ghi:klm:abc'"))
    val f10 = Cell.parse4DWithDictionary(dictionary, _1, ":", LongCodec, StringCodec, StringCodec, StringCodec)
    f10("123:def:ghi:klm:3:14") shouldBe List(Left("Unable to decode: '123:def:ghi:klm:3:14'"))
    val f11 = Cell.parse4DWithDictionary(dictionary, _1, ":", LongCodec, StringCodec, StringCodec, StringCodec)
    f11("123|def:ghi:klm:3.14") shouldBe List(Left("Unable to split: '123|def:ghi:klm:3.14'"))
  }

  "A Cell" should "parse 4D with schema" in {
    val f1 = Cell.parse4DWithSchema(schema, ":", LongCodec, StringCodec, StringCodec, StringCodec)
    f1("123:def:ghi:klm:3.14") shouldBe List(
      Right(Cell(Position(123, "def", "ghi", "klm"), Content(ContinuousSchema[Double](), 3.14)))
    )
    val f2 = Cell.parse4DWithSchema(schema, ":", LongCodec, StringCodec, StringCodec, StringCodec)
    f2("abc:def:ghi:klm:3.14") shouldBe List(Left("Unable to decode: 'abc:def:ghi:klm:3.14'"))
    val f3 = Cell.parse4DWithSchema(schema, ":", LongCodec, StringCodec, StringCodec, StringCodec)
    f3("123:def:ghi:klm:abc") shouldBe List(Left("Unable to decode: '123:def:ghi:klm:abc'"))
    val f4 = Cell.parse4DWithSchema(schema, ":", LongCodec, StringCodec, StringCodec, StringCodec)
    f4("123:def:ghi:klm:3:14") shouldBe List(Left("Unable to decode: '123:def:ghi:klm:3:14'"))
    val f5 = Cell.parse4DWithSchema(schema, ":", LongCodec, StringCodec, StringCodec, StringCodec)
    f5("123:def|ghi:klm:3.14") shouldBe List(Left("Unable to split: '123:def|ghi:klm:3.14'"))
  }

  "A Cell" should "parse 5D" in {
    val f1 = Cell.parse5D(":", LongCodec, StringCodec, StringCodec, StringCodec, StringCodec)
    f1("123:def:ghi:klm:xyz:double:continuous:3.14") shouldBe List(
      Right(Cell(Position(123, "def", "ghi", "klm", "xyz"), Content(ContinuousSchema[Double](), 3.14)))
    )
    val f2 = Cell.parse5D(":", LongCodec, StringCodec, StringCodec, StringCodec, StringCodec)
    f2("abc:def:ghi:klm:xyz:double:continuous:3.14") shouldBe List(
      Left("Unable to decode: 'abc:def:ghi:klm:xyz:double:continuous:3.14'")
    )
    val f3 = Cell.parse5D(":", StringCodec, LongCodec, StringCodec, StringCodec, StringCodec)
    f3("def:abc:ghi:klm:xyz:double:continuous:3.14") shouldBe List(
      Left("Unable to decode: 'def:abc:ghi:klm:xyz:double:continuous:3.14'")
    )
    val f4 = Cell.parse5D(":", StringCodec, StringCodec, LongCodec, StringCodec, StringCodec)
    f4("def:ghi:abc:klm:xyz:double:continuous:3.14") shouldBe List(
      Left("Unable to decode: 'def:ghi:abc:klm:xyz:double:continuous:3.14'")
    )
    val f5 = Cell.parse5D(":", StringCodec, StringCodec, StringCodec, LongCodec, StringCodec)
    f5("def:ghi:klm:abc:xyz:double:continuous:3.14") shouldBe List(
      Left("Unable to decode: 'def:ghi:klm:abc:xyz:double:continuous:3.14'")
    )
    val f6 = Cell.parse5D(":", StringCodec, StringCodec, StringCodec, StringCodec, LongCodec)
    f6("def:ghi:klm:xyz:abc:double:continuous:3.14") shouldBe List(
      Left("Unable to decode: 'def:ghi:klm:xyz:abc:double:continuous:3.14'")
    )
    val f7 = Cell.parse5D(":", LongCodec, StringCodec, StringCodec, StringCodec, StringCodec)
    f7("123:def:ghi:klm:xyz:double:continuous:abc") shouldBe List(
      Left("Unable to decode: '123:def:ghi:klm:xyz:double:continuous:abc'")
    )
    val f8 = Cell.parse5D(":", LongCodec, StringCodec, StringCodec, StringCodec, StringCodec)
    f8("123:def:ghi:klm:xyz:double:continuous:3:14") shouldBe List(
      Left("Unable to decode: '123:def:ghi:klm:xyz:double:continuous:3:14'")
    )
    val f9 = Cell.parse5D(":", LongCodec, StringCodec, StringCodec, StringCodec, StringCodec)
    f9("123:def:ghi:klm:xyz:double|continuous:3.14") shouldBe List(
      Left("Unable to split: '123:def:ghi:klm:xyz:double|continuous:3.14'")
    )
  }

  "A Cell" should "parse 5D with dictionary" in {
    val f1 = Cell
      .parse5DWithDictionary(dictionary, _1, ":", LongCodec, StringCodec, StringCodec, StringCodec, StringCodec)
    f1("123:def:ghi:klm:xyz:3.14") shouldBe List(
      Right(Cell(Position(123, "def", "ghi", "klm", "xyz"), Content(ContinuousSchema[Double](), 3.14)))
    )
    val f2 = Cell
      .parse5DWithDictionary(dictionary, _2, ":", StringCodec, LongCodec, StringCodec, StringCodec, StringCodec)
    f2("def:123:ghi:klm:xyz:3.14") shouldBe List(
      Right(Cell(Position("def", 123, "ghi", "klm", "xyz"), Content(ContinuousSchema[Double](), 3.14)))
    )
    val f3 = Cell
      .parse5DWithDictionary(dictionary, _3, ":", StringCodec, StringCodec, LongCodec, StringCodec, StringCodec)
    f3("def:ghi:123:klm:xyz:3.14") shouldBe List(
      Right(Cell(Position("def", "ghi", 123, "klm", "xyz"), Content(ContinuousSchema[Double](), 3.14)))
    )
    val f4 = Cell
      .parse5DWithDictionary(dictionary, _4, ":", StringCodec, StringCodec, StringCodec, LongCodec, StringCodec)
    f4("def:ghi:klm:123:xyz:3.14") shouldBe List(
      Right(Cell(Position("def", "ghi", "klm", 123, "xyz"), Content(ContinuousSchema[Double](), 3.14)))
    )
    val f5 = Cell
      .parse5DWithDictionary(dictionary, _5, ":", StringCodec, StringCodec, StringCodec, StringCodec, LongCodec)
    f5("def:ghi:klm:xyz:123:3.14") shouldBe List(
      Right(Cell(Position("def", "ghi", "klm", "xyz", 123), Content(ContinuousSchema[Double](), 3.14)))
    )
    val f6 = Cell
      .parse5DWithDictionary(dictionary, _1, ":", LongCodec, StringCodec, StringCodec, StringCodec, StringCodec)
    f6("abc:def:ghi:klm:xyz:3.14") shouldBe List(Left("Missing schema for: 'abc:def:ghi:klm:xyz:3.14'"))
    val f7 = Cell
      .parse5DWithDictionary(dictionary, _2, ":", LongCodec, StringCodec, StringCodec, StringCodec, StringCodec)
    f7("abc:def:ghi:klm:xyz:3.14") shouldBe List(Left("Missing schema for: 'abc:def:ghi:klm:xyz:3.14'"))
    val f8 = Cell
      .parse5DWithDictionary(dictionary, _3, ":", LongCodec, StringCodec, StringCodec, StringCodec, StringCodec)
    f8("abc:def:ghi:klm:xyz:3.14") shouldBe List(Left("Missing schema for: 'abc:def:ghi:klm:xyz:3.14'"))
    val f9 = Cell
      .parse5DWithDictionary(dictionary, _4, ":", LongCodec, StringCodec, StringCodec, StringCodec, StringCodec)
    f9("abc:def:ghi:klm:xyz:3.14") shouldBe List(Left("Missing schema for: 'abc:def:ghi:klm:xyz:3.14'"))
    val f10 = Cell
      .parse5DWithDictionary(dictionary, _5, ":", LongCodec, StringCodec, StringCodec, StringCodec, StringCodec)
    f10("abc:def:ghi:klm:xyz:3.14") shouldBe List(Left("Missing schema for: 'abc:def:ghi:klm:xyz:3.14'"))
    val f11 = Cell
      .parse5DWithDictionary(dictionary, _1, ":", LongCodec, StringCodec, StringCodec, StringCodec, StringCodec)
    f11("123:def:ghi:klm:xyz:abc") shouldBe List(Left("Unable to decode: '123:def:ghi:klm:xyz:abc'"))
    val f12 = Cell
      .parse5DWithDictionary(dictionary, _1, ":", LongCodec, StringCodec, StringCodec, StringCodec, StringCodec)
    f12("123:def:ghi:klm:xyz:3:14") shouldBe List(Left("Unable to decode: '123:def:ghi:klm:xyz:3:14'"))
    val f13 = Cell
      .parse5DWithDictionary(dictionary, _1, ":", LongCodec, StringCodec, StringCodec, StringCodec, StringCodec)
    f13("123|def:ghi:klm:xyz:3.14") shouldBe List(Left("Unable to split: '123|def:ghi:klm:xyz:3.14'"))
  }

  "A Cell" should "parse 5D with schema" in {
    val f1 = Cell.parse5DWithSchema(schema, ":", LongCodec, StringCodec, StringCodec, StringCodec, StringCodec)
    f1("123:def:ghi:klm:xyz:3.14") shouldBe List(
      Right(Cell(Position(123, "def", "ghi", "klm", "xyz"), Content(ContinuousSchema[Double](), 3.14)))
    )
    val f2 = Cell.parse5DWithSchema(schema, ":", LongCodec, StringCodec, StringCodec, StringCodec, StringCodec)
    f2("abc:def:ghi:klm:xyz:3.14") shouldBe List(Left("Unable to decode: 'abc:def:ghi:klm:xyz:3.14'"))
    val f3 = Cell.parse5DWithSchema(schema, ":", LongCodec, StringCodec, StringCodec, StringCodec, StringCodec)
    f3("123:def:ghi:klm:xyz:abc") shouldBe List(Left("Unable to decode: '123:def:ghi:klm:xyz:abc'"))
    val f4 = Cell.parse5DWithSchema(schema, ":", LongCodec, StringCodec, StringCodec, StringCodec, StringCodec)
    f4("123:def:ghi:klm:xyz:3:14") shouldBe List(Left("Unable to decode: '123:def:ghi:klm:xyz:3:14'"))
    val f5 = Cell.parse5DWithSchema(schema, ":", LongCodec, StringCodec, StringCodec, StringCodec, StringCodec)
    f5("123:def|ghi:klm:xyz:3.14") shouldBe List(Left("Unable to split: '123:def|ghi:klm:xyz:3.14'"))
  }

  "A Cell" should "parse 6D" in {
    val f1 = Cell.parse6D(":", LongCodec, StringCodec, StringCodec, StringCodec, StringCodec, StringCodec)
    f1("123:def:ghi:klm:xyz:uvw:double:continuous:3.14") shouldBe List(
      Right(Cell(Position(123, "def", "ghi", "klm", "xyz", "uvw"), Content(ContinuousSchema[Double](), 3.14)))
    )
    val f2 = Cell.parse6D(":", LongCodec, StringCodec, StringCodec, StringCodec, StringCodec, StringCodec)
    f2("abc:def:ghi:klm:xyz:uvw:double:continuous:3.14") shouldBe List(
      Left("Unable to decode: 'abc:def:ghi:klm:xyz:uvw:double:continuous:3.14'")
    )
    val f3 = Cell.parse6D(":", StringCodec, LongCodec, StringCodec, StringCodec, StringCodec, StringCodec)
    f3("def:abc:ghi:klm:xyz:uvw:double:continuous:3.14") shouldBe List(
      Left("Unable to decode: 'def:abc:ghi:klm:xyz:uvw:double:continuous:3.14'")
    )
    val f4 = Cell.parse6D(":", StringCodec, StringCodec, LongCodec, StringCodec, StringCodec, StringCodec)
    f4("def:ghi:abc:klm:xyz:uvw:double:continuous:3.14") shouldBe List(
      Left("Unable to decode: 'def:ghi:abc:klm:xyz:uvw:double:continuous:3.14'")
    )
    val f5 = Cell.parse6D(":", StringCodec, StringCodec, StringCodec, LongCodec, StringCodec, StringCodec)
    f5("def:ghi:klm:abc:xyz:uvw:double:continuous:3.14") shouldBe List(
      Left("Unable to decode: 'def:ghi:klm:abc:xyz:uvw:double:continuous:3.14'")
    )
    val f6 = Cell.parse6D(":", StringCodec, StringCodec, StringCodec, StringCodec, LongCodec, StringCodec)
    f6("def:ghi:klm:xyz:abc:uvw:double:continuous:3.14") shouldBe List(
      Left("Unable to decode: 'def:ghi:klm:xyz:abc:uvw:double:continuous:3.14'")
    )
    val f7 = Cell.parse6D(":", LongCodec, StringCodec, StringCodec, StringCodec, StringCodec, StringCodec)
    f7("123:def:ghi:klm:xyz:uvw:double:continuous:abc") shouldBe List(
      Left("Unable to decode: '123:def:ghi:klm:xyz:uvw:double:continuous:abc'")
    )
    val f8 = Cell.parse6D(":", LongCodec, StringCodec, StringCodec, StringCodec, StringCodec, StringCodec)
    f8("123:def:ghi:klm:xyz:uvw:double:continuous:3:14") shouldBe List(
      Left("Unable to decode: '123:def:ghi:klm:xyz:uvw:double:continuous:3:14'")
    )
    val f9 = Cell.parse6D(":", LongCodec, StringCodec, StringCodec, StringCodec, StringCodec, StringCodec)
    f9("123:def:ghi:klm:xyz:uvw:double|continuous:3.14") shouldBe List(
      Left("Unable to split: '123:def:ghi:klm:xyz:uvw:double|continuous:3.14'")
    )
  }

  "A Cell" should "parse 6D with dictionary" in {
    val f1 = Cell.parse6DWithDictionary(
      dictionary,
      _1,
      ":",
      LongCodec,
      StringCodec,
      StringCodec,
      StringCodec,
      StringCodec,
      StringCodec
    )
    f1("123:def:ghi:klm:xyz:uvw:3.14") shouldBe List(
      Right(Cell(Position(123, "def", "ghi", "klm", "xyz", "uvw"), Content(ContinuousSchema[Double](), 3.14)))
    )
    val f2 = Cell.parse6DWithDictionary(
      dictionary,
      _2,
      ":",
      StringCodec,
      LongCodec,
      StringCodec,
      StringCodec,
      StringCodec,
      StringCodec)
    f2("def:123:ghi:klm:xyz:uvw:3.14") shouldBe List(
      Right(Cell(Position("def", 123, "ghi", "klm", "xyz", "uvw"), Content(ContinuousSchema[Double](), 3.14)))
    )
    val f3 = Cell.parse6DWithDictionary(
      dictionary,
      _3,
      ":",
      StringCodec,
      StringCodec,
      LongCodec,
      StringCodec,
      StringCodec,
      StringCodec
    )
    f3("def:ghi:123:klm:xyz:uvw:3.14") shouldBe List(
      Right(Cell(Position("def", "ghi", 123, "klm", "xyz", "uvw"), Content(ContinuousSchema[Double](), 3.14)))
    )
    val f4 = Cell.parse6DWithDictionary(
      dictionary,
      _4,
      ":",
      StringCodec,
      StringCodec,
      StringCodec,
      LongCodec,
      StringCodec,
      StringCodec
    )
    f4("def:ghi:klm:123:xyz:uvw:3.14") shouldBe List(
      Right(Cell(Position("def", "ghi", "klm", 123, "xyz", "uvw"), Content(ContinuousSchema[Double](), 3.14)))
    )
    val f5 = Cell.parse6DWithDictionary(
      dictionary,
      _5,
      ":",
      StringCodec,
      StringCodec,
      StringCodec,
      StringCodec,
      LongCodec,
      StringCodec
    )
    f5("def:ghi:klm:xyz:123:uvw:3.14") shouldBe List(
      Right(Cell(Position("def", "ghi", "klm", "xyz", 123, "uvw"), Content(ContinuousSchema[Double](), 3.14)))
    )
    val f6 = Cell.parse6DWithDictionary(
      dictionary,
      _6,
      ":",
      StringCodec,
      StringCodec,
      StringCodec,
      StringCodec,
      StringCodec,
      LongCodec
    )
    f6("def:ghi:klm:xyz:uvw:123:3.14") shouldBe List(
      Right(Cell(Position("def", "ghi", "klm", "xyz", "uvw", 123), Content(ContinuousSchema[Double](), 3.14)))
    )
    val f7 = Cell.parse6DWithDictionary(
      dictionary,
      _1,
      ":",
      LongCodec,
      StringCodec,
      StringCodec,
      StringCodec,
      StringCodec,
      StringCodec
    )
    f7("abc:def:ghi:klm:xyz:uvw:3.14") shouldBe List(Left("Missing schema for: 'abc:def:ghi:klm:xyz:uvw:3.14'"))
    val f8 = Cell.parse6DWithDictionary(
      dictionary,
      _2,
      ":",
      LongCodec,
      StringCodec,
      StringCodec,
      StringCodec,
      StringCodec,
      StringCodec
    )
    f8("abc:def:ghi:klm:xyz:uvw:3.14") shouldBe List(Left("Missing schema for: 'abc:def:ghi:klm:xyz:uvw:3.14'"))
    val f9 = Cell.parse6DWithDictionary(
      dictionary,
      _3,
      ":",
      LongCodec,
      StringCodec,
      StringCodec,
      StringCodec,
      StringCodec,
      StringCodec
    )
    f9("abc:def:ghi:klm:xyz:uvw:3.14") shouldBe List(Left("Missing schema for: 'abc:def:ghi:klm:xyz:uvw:3.14'"))
    val f10 = Cell.parse6DWithDictionary(
      dictionary,
      _4,
      ":",
      LongCodec,
      StringCodec,
      StringCodec,
      StringCodec,
      StringCodec,
      StringCodec
    )
    f10("abc:def:ghi:klm:xyz:uvw:3.14") shouldBe List(Left("Missing schema for: 'abc:def:ghi:klm:xyz:uvw:3.14'"))
    val f11 = Cell.parse6DWithDictionary(
      dictionary,
      _5,
      ":",
      LongCodec,
      StringCodec,
      StringCodec,
      StringCodec,
      StringCodec,
      StringCodec
    )
    f11("abc:def:ghi:klm:xyz:uvw:3.14") shouldBe List(Left("Missing schema for: 'abc:def:ghi:klm:xyz:uvw:3.14'"))
    val f12 = Cell.parse6DWithDictionary(
      dictionary,
      _1,
      ":",
      LongCodec,
      StringCodec,
      StringCodec,
      StringCodec,
      StringCodec,
      StringCodec
    )
    f12("123:def:ghi:klm:xyz:abc:uvw") shouldBe List(Left("Unable to decode: '123:def:ghi:klm:xyz:abc:uvw'"))
    val f13 = Cell.parse6DWithDictionary(
      dictionary,
      _1,
      ":",
      LongCodec,
      StringCodec,
      StringCodec,
      StringCodec,
      StringCodec,
      StringCodec
    )
    f13("123:def:ghi:klm:xyz:uvw:3:14") shouldBe List(Left("Unable to decode: '123:def:ghi:klm:xyz:uvw:3:14'"))
    val f14 = Cell.parse6DWithDictionary(
      dictionary,
      _1,
      ":",
      LongCodec,
      StringCodec,
      StringCodec,
      StringCodec,
      StringCodec,
      StringCodec
    )
    f14("123|def:ghi:klm:xyz:uvw:3.14") shouldBe List(Left("Unable to split: '123|def:ghi:klm:xyz:uvw:3.14'"))
  }

  "A Cell" should "parse 6D with schema" in {
    val f1 = Cell.parse6DWithSchema(
      schema,
      ":",
      LongCodec,
      StringCodec,
      StringCodec,
      StringCodec,
      StringCodec,
      StringCodec
    )
    f1("123:def:ghi:klm:xyz:uvw:3.14") shouldBe List(
      Right(Cell(Position(123, "def", "ghi", "klm", "xyz", "uvw"), Content(ContinuousSchema[Double](), 3.14)))
    )
    val f2 = Cell.parse6DWithSchema(
      schema,
      ":",
      LongCodec,
      StringCodec,
      StringCodec,
      StringCodec,
      StringCodec,
      StringCodec
    )
    f2("abc:def:ghi:klm:xyz:uvw:3.14") shouldBe List(Left("Unable to decode: 'abc:def:ghi:klm:xyz:uvw:3.14'"))
    val f3 = Cell.parse6DWithSchema(
      schema,
      ":",
      LongCodec,
      StringCodec,
      StringCodec,
      StringCodec,
      StringCodec,
      StringCodec
    )
    f3("123:def:ghi:klm:xyz:abc:uvw") shouldBe List(Left("Unable to decode: '123:def:ghi:klm:xyz:abc:uvw'"))
    val f4 = Cell.parse6DWithSchema(
      schema,
      ":",
      LongCodec,
      StringCodec,
      StringCodec,
      StringCodec,
      StringCodec,
      StringCodec
    )
    f4("123:def:ghi:klm:xyz:uvw:3:14") shouldBe List(Left("Unable to decode: '123:def:ghi:klm:xyz:uvw:3:14'"))
    val f5 = Cell.parse6DWithSchema(
      schema,
      ":",
      LongCodec,
      StringCodec,
      StringCodec,
      StringCodec,
      StringCodec,
      StringCodec
    )
    f5("123:def|ghi:klm:xyz:uvw:3.14") shouldBe List(Left("Unable to split: '123:def|ghi:klm:xyz:uvw:3.14'"))
  }

  val columns = List(
    ("abc", Content.parser(DoubleCodec, ContinuousSchema[Double]())),
    ("def", Content.parser(DoubleCodec, ContinuousSchema[Double]())),
    ("ghi", Content.parser(DoubleCodec, ContinuousSchema[Double]()))
  )

  "A Cell" should "parse table" in {
    val f1 = Cell.parseTable(columns, 0, ":")
    f1("3.14:6.28:9.42") shouldBe List(
      Right(Cell(Position("3.14", "def"), Content(ContinuousSchema[Double](), 6.28))),
      Right(Cell(Position("3.14", "ghi"), Content(ContinuousSchema[Double](), 9.42)))
    )
    val f2 = Cell.parseTable(columns, 1, ":")
    f2("3.14:6.28:9.42") shouldBe List(
      Right(Cell(Position("6.28", "abc"), Content(ContinuousSchema[Double](), 3.14))),
      Right(Cell(Position("6.28", "ghi"), Content(ContinuousSchema[Double](), 9.42)))
    )
    val f3 = Cell.parseTable(columns, 2, ":")
    f3("3.14:6.28:9.42") shouldBe List(
      Right(Cell(Position("9.42", "abc"), Content(ContinuousSchema[Double](), 3.14))),
      Right(Cell(Position("9.42", "def"), Content(ContinuousSchema[Double](), 6.28)))
    )
    val f4 = Cell.parseTable(columns, 0, ":")
    f4("3.14:foo:bar") shouldBe List(
      Left("Unable to decode: '3.14:foo:bar'"),
      Left("Unable to decode: '3.14:foo:bar'")
    )
    val f5 = Cell.parseTable(columns, 0, ":")
    f5("3.14:foo") shouldBe List(Left("Unable to split: '3.14:foo'"))
  }

  "A Cell" should "parse JSON" in {
    val cell1 = Cell(Position("foo"), Content(ContinuousSchema[Double](), 3.14))
    val cell2 = Cell(Position("foo", 1), Content(ContinuousSchema[Double](), 3.14))
    val cell3 = Cell(Position("foo", 1, true), Content(ContinuousSchema[Double](), 3.14))
    val cell4 = Cell(Position("foo", 1, true, 3.14), Content(ContinuousSchema[Double](), 3.14))
    val cell5 = Cell(Position("foo", 1, true, 3.14, "bar"), Content(ContinuousSchema[Double](), 3.14))
    val cell6 = Cell(Position("foo", 1, true, 3.14, "bar", "baz"), Content(ContinuousSchema[Double](), 3.14))
    val cell7 = Cell(Position("foo", 1, true, 3.14, "bar", "baz", 42), Content(ContinuousSchema[Double](), 3.14))
    val cell8 = Cell(Position("foo", 1, true, 3.14, "bar", "baz", 42, 0), Content(ContinuousSchema[Double](), 3.14))
    val cell9 = Cell(
      Position("foo", 1, true, 3.14, "bar", "baz", 42, 0, false),
      Content(ContinuousSchema[Double](), 3.14)
    )

    val f1 = Cell.parse1DJSON(StringCodec)
    cell1.toJSON(false,false) shouldBe """{"position":["foo"],"content":{"value":"3.14"}}"""
    f1(cell1.toJSON(true, true)) shouldBe List(Right(cell1))

    val f2 = Cell.parse2DJSON(StringCodec, LongCodec)
    cell2.toJSON(true, true) shouldBe """{
  "position" : [ "foo", "1" ],
  "content" : {
    "codec" : "double",
    "schema" : "continuous",
    "value" : "3.14"
  }
}"""
    f2(cell2.toJSON(true, true)) shouldBe List(Right(cell2))

    val f3 = Cell.parse3DJSON(StringCodec, LongCodec, BooleanCodec)
    f3(cell3.toJSON(true, true)) shouldBe List(Right(cell3))

    val f4 = Cell.parse4DJSON(StringCodec, LongCodec, BooleanCodec, DoubleCodec)
    f4(cell4.toJSON(true, true)) shouldBe List(Right(cell4))

    val f5 = Cell.parse5DJSON(StringCodec, LongCodec, BooleanCodec, DoubleCodec, StringCodec)
    f5(cell5.toJSON(true, true)) shouldBe List(Right(cell5))

    val f6 = Cell.parse6DJSON(StringCodec, LongCodec, BooleanCodec, DoubleCodec, StringCodec, StringCodec)
    f6(cell6.toJSON(true, true)) shouldBe List(Right(cell6))

    val f7 = Cell.parse7DJSON(StringCodec, LongCodec, BooleanCodec, DoubleCodec, StringCodec, StringCodec, LongCodec)
    f7(cell7.toJSON(true, true)) shouldBe List(Right(cell7))

    val f8 = Cell.parse8DJSON(
      StringCodec,
      LongCodec,
      BooleanCodec,
      DoubleCodec,
      StringCodec,
      StringCodec,
      LongCodec,
      LongCodec
    )
    f8(cell8.toJSON(true, true)) shouldBe List(Right(cell8))

    val f9 = Cell.parse9DJSON(
      StringCodec,
      LongCodec,
      BooleanCodec,
      DoubleCodec,
      StringCodec,
      StringCodec,
      LongCodec,
      LongCodec,
      BooleanCodec
    )
    f9(cell9.toJSON(true, true)) shouldBe List(Right(cell9))
  }
}

