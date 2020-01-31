// Copyright 2015,2016,2017,2018,2019,2020 Commonwealth Bank of Australia
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

import commbank.grimlock.framework._
import commbank.grimlock.framework.content._
import commbank.grimlock.framework.encoding._
import commbank.grimlock.framework.environment.implicits._
import commbank.grimlock.framework.error._
import commbank.grimlock.framework.metadata._
import commbank.grimlock.framework.position._

import scala.util.{ Failure, Success }

import shapeless.HNil
import shapeless.nat.{ _0, _1, _2 }

class TestCell extends TestGrimlock {
  "A Cell" should "return its string" in {
    Cell(Position("foo", 123L), Content(ContinuousSchema[Double](), 3.14))
      .toString shouldBe "Cell(Position(StringValue(foo,StringCodec) :: LongValue(123,LongCodec) :: HNil),Content(ContinuousType,DoubleValue(3.14,DoubleCodec)))"
    Cell(Position("foo", 123L), Content(ContinuousSchema[Double](), 3.14))
      .toShortString(true, ".") shouldBe "foo.123.double.continuous.3.14"
    Cell(Position("foo", 123L), Content(ContinuousSchema[Double](), 3.14))
      .toShortString(false, ".") shouldBe "foo.123.3.14"
  }

  "A Cell" should "relocate" in {
    Cell(Position("foo", 123L), Content(ContinuousSchema[Double](), 3.14))
      .relocate(_.position.append("abc")) shouldBe Cell(
        Position("foo", 123L, "abc"),
        Content(ContinuousSchema[Double](), 3.14)
      )
  }

  "A Cell" should "mutate" in {
    Cell(Position("foo", 123L), Content(ContinuousSchema[Double](), 3.14))
      .mutate(_ => Content(DiscreteSchema[Long](), 42L)) shouldBe Cell(
        Position("foo", 123L),
        Content(DiscreteSchema[Long](), 42L)
      )
  }

  val schema = Content.decoder(DoubleCodec, ContinuousSchema[Double]())
  val dictionary = Map(123L -> schema)
  val method = (l: Long) => Map(123L -> schema).get(l)
  val codecs1 = LongCodec :: HNil
  val codecs2a = LongCodec :: StringCodec :: HNil
  val codecs2b = StringCodec :: LongCodec :: HNil
  val codecs3a = LongCodec :: StringCodec :: StringCodec :: HNil
  val codecs3b = StringCodec :: LongCodec :: StringCodec :: HNil
  val codecs3c = StringCodec :: StringCodec :: LongCodec :: HNil

  "A Cell" should "parse 1D" in {
    val f1 = Cell.shortStringParser(codecs1, ":")
    f1("123:double:continuous:3.14") shouldBe List(
      Success(Cell(Position(123L), Content(ContinuousSchema[Double](), 3.14)))
    )
    val f2 = Cell.shortStringParser(codecs1, ":")
    f2("abc:double:continuous:3.14") shouldBe List(Failure(UnableToDecodeCell("abc:double:continuous:3.14")))
    val f3 = Cell.shortStringParser(codecs1, ":")
    f3("123:double:continuous:abc") shouldBe List(Failure(UnableToDecodeCell("123:double:continuous:abc")))
    val f4 = Cell.shortStringParser(codecs1, ":")
    f4("123:double:continuous:3:14") shouldBe List(Failure(UnableToDecodeCell("123:double:continuous:3:14")))
    val f5 = Cell.shortStringParser(codecs1, ":")
    f5("123:double|continuous:3.14") shouldBe List(Failure(IncorrectNumberOfFields("123:double|continuous:3.14")))
  }

  "A Cell" should "parse 1D with dictionary" in {
    val f1 = Cell.shortStringParser(codecs1, dictionary, _0, ":")
    f1("123:3.14") shouldBe List(Success(Cell(Position(123L), Content(ContinuousSchema[Double](), 3.14))))
    val f2 = Cell.shortStringParser(codecs1, dictionary, _0, ":")
    f2("abc:3.14") shouldBe List(Failure(UnableToDecodeCell("abc:3.14")))
    val f3 = Cell.shortStringParser(codecs1, dictionary, _0, ":")
    f3("123:abc") shouldBe List(Failure(UnableToDecodeCell("123:abc")))
    val f4 = Cell.shortStringParser(codecs1, dictionary, _0, ":")
    f4("123:3:14") shouldBe List(Failure(UnableToDecodeCell("123:3:14")))
    val f5 = Cell.shortStringParser(codecs1, dictionary, _0, ":")
    f5("123|3.14") shouldBe List(Failure(IncorrectNumberOfFields("123|3.14")))
  }

  "A Cell" should "parse 1D with method" in {
    val f1 = Cell.shortStringParser(codecs1, method, _0, ":")
    f1("123:3.14") shouldBe List(Success(Cell(Position(123L), Content(ContinuousSchema[Double](), 3.14))))
    val f2 = Cell.shortStringParser(codecs1, method, _0, ":")
    f2("abc:3.14") shouldBe List(Failure(UnableToDecodeCell("abc:3.14")))
    val f3 = Cell.shortStringParser(codecs1, method, _0, ":")
    f3("123:abc") shouldBe List(Failure(UnableToDecodeCell("123:abc")))
    val f4 = Cell.shortStringParser(codecs1, method, _0, ":")
    f4("123:3:14") shouldBe List(Failure(UnableToDecodeCell("123:3:14")))
    val f5 = Cell.shortStringParser(codecs1, method, _0, ":")
    f5("123|3.14") shouldBe List(Failure(IncorrectNumberOfFields("123|3.14")))
  }

  "A Cell" should "parse 1D with schema" in {
    val f1 = Cell.shortStringParser(codecs1, schema, ":")
    f1("123:3.14") shouldBe List(Success(Cell(Position(123L), Content(ContinuousSchema[Double](), 3.14))))
    val f2 = Cell.shortStringParser(codecs1, schema, ":")
    f2("abc:3.14") shouldBe List(Failure(UnableToDecodeCell("abc:3.14")))
    val f3 = Cell.shortStringParser(codecs1, schema, ":")
    f3("123:abc") shouldBe List(Failure(UnableToDecodeCell("123:abc")))
    val f4 = Cell.shortStringParser(codecs1, schema, ":")
    f4("123:3:14") shouldBe List(Failure(UnableToDecodeCell("123:3:14")))
    val f5 = Cell.shortStringParser(codecs1, schema, ":")
    f5("123|3.14") shouldBe List(Failure(IncorrectNumberOfFields("123|3.14")))
  }

  "A Cell" should "parse 2D" in {
    val f1 = Cell.shortStringParser(codecs2a, ":")
    f1("123:def:double:continuous:3.14") shouldBe List(
      Success(Cell(Position(123L, "def"), Content(ContinuousSchema[Double](), 3.14)))
    )
    val f2 = Cell.shortStringParser(codecs2a, ":")
    f2("abc:def:double:continuous:3.14") shouldBe List(Failure(UnableToDecodeCell("abc:def:double:continuous:3.14")))
    val f3 = Cell.shortStringParser(codecs2b, ":")
    f3("abc:def:double:continuous:3.14") shouldBe List(Failure(UnableToDecodeCell("abc:def:double:continuous:3.14")))
    val f4 = Cell.shortStringParser(codecs2a, ":")
    f4("123:def:double:continuous:abc") shouldBe List(Failure(UnableToDecodeCell("123:def:double:continuous:abc")))
    val f5 = Cell.shortStringParser(codecs2a, ":")
    f5("123:def:double:continuous:3:14") shouldBe List(Failure(UnableToDecodeCell("123:def:double:continuous:3:14")))
    val f6 = Cell.shortStringParser(codecs2a, ":")
    f6("123:def:double|continuous:3.14") shouldBe List(Failure(UnableToDecodeCell("123:def:double|continuous:3.14")))
  }

  "A Cell" should "parse 2D with dictionary" in {
    val f1 = Cell.shortStringParser(codecs2a, dictionary, _0, ":")
    f1("123:def:3.14") shouldBe List(Success(Cell(Position(123L, "def"), Content(ContinuousSchema[Double](), 3.14))))
    val f2 = Cell.shortStringParser(codecs2b, dictionary, _1, ":")
    f2("def:123:3.14") shouldBe List(Success(Cell(Position("def", 123L), Content(ContinuousSchema[Double](), 3.14))))
    val f3 = Cell.shortStringParser(codecs2a, dictionary, _0, ":")
    f3("abc:def:3.14") shouldBe List(Failure(UnableToDecodeCell("abc:def:3.14")))
    val f4 = Cell.shortStringParser(codecs2b, dictionary, _1, ":")
    f4("abc:def:3.14") shouldBe List(Failure(UnableToDecodeCell("abc:def:3.14")))
    val f5 = Cell.shortStringParser(codecs2a, dictionary, _0, ":")
    f5("123:def:abc") shouldBe List(Failure(UnableToDecodeCell("123:def:abc")))
    val f6 = Cell.shortStringParser(codecs2a, dictionary, _0, ":")
    f6("123:def:3:14") shouldBe List(Failure(UnableToDecodeCell("123:def:3:14")))
    val f7 = Cell.shortStringParser(codecs2a, dictionary, _0, ":")
    f7("123|def:3.14") shouldBe List(Failure(UnableToDecodeCell("123|def:3.14")))
  }

  "A Cell" should "parse 2D with method" in {
    val f1 = Cell.shortStringParser(codecs2a, method, _0, ":")
    f1("123:def:3.14") shouldBe List(Success(Cell(Position(123L, "def"), Content(ContinuousSchema[Double](), 3.14))))
    val f2 = Cell.shortStringParser(codecs2b, method, _1, ":")
    f2("def:123:3.14") shouldBe List(Success(Cell(Position("def", 123L), Content(ContinuousSchema[Double](), 3.14))))
    val f3 = Cell.shortStringParser(codecs2a, method, _0, ":")
    f3("abc:def:3.14") shouldBe List(Failure(UnableToDecodeCell("abc:def:3.14")))
    val f4 = Cell.shortStringParser(codecs2b, method, _1, ":")
    f4("abc:def:3.14") shouldBe List(Failure(UnableToDecodeCell("abc:def:3.14")))
    val f5 = Cell.shortStringParser(codecs2a, method, _0, ":")
    f5("123:def:abc") shouldBe List(Failure(UnableToDecodeCell("123:def:abc")))
    val f6 = Cell.shortStringParser(codecs2a, method, _0, ":")
    f6("123:def:3:14") shouldBe List(Failure(UnableToDecodeCell("123:def:3:14")))
    val f7 = Cell.shortStringParser(codecs2a, method, _0, ":")
    f7("123|def:3.14") shouldBe List(Failure(UnableToDecodeCell("123|def:3.14")))
  }

  "A Cell" should "parse 2D with schema" in {
    val f1 = Cell.shortStringParser(codecs2a, schema, ":")
    f1("123:def:3.14") shouldBe List(Success(Cell(Position(123L, "def"), Content(ContinuousSchema[Double](), 3.14))))
    val f2 = Cell.shortStringParser(codecs2a, schema, ":")
    f2("abc:def:3.14") shouldBe List(Failure(UnableToDecodeCell("abc:def:3.14")))
    val f3 = Cell.shortStringParser(codecs2a, schema, ":")
    f3("123:def:abc") shouldBe List(Failure(UnableToDecodeCell("123:def:abc")))
    val f4 = Cell.shortStringParser(codecs2a, schema, ":")
    f4("123:def:3:14") shouldBe List(Failure(UnableToDecodeCell("123:def:3:14")))
    val f5 = Cell.shortStringParser(codecs2a, schema, ":")
    f5("123:def|3.14") shouldBe List(Failure(UnableToDecodeCell("123:def|3.14")))
  }

  "A Cell" should "parse 3D" in {
    val f1 = Cell.shortStringParser(codecs3a, ":")
    f1("123:def:ghi:double:continuous:3.14") shouldBe List(
      Success(Cell(Position(123L, "def", "ghi"), Content(ContinuousSchema[Double](), 3.14)))
    )
    val f2 = Cell.shortStringParser(codecs3a, ":")
    f2("abc:def:ghi:double:continuous:3.14") shouldBe List(
      Failure(UnableToDecodeCell("abc:def:ghi:double:continuous:3.14"))
    )
    val f3 = Cell.shortStringParser(codecs3b, ":")
    f3("def:abc:ghi:double:continuous:3.14") shouldBe List(
      Failure(UnableToDecodeCell("def:abc:ghi:double:continuous:3.14"))
    )
    val f4 = Cell.shortStringParser(codecs3c, ":")
    f4("def:ghi:abc:double:continuous:3.14") shouldBe List(
      Failure(UnableToDecodeCell("def:ghi:abc:double:continuous:3.14"))
    )
    val f5 = Cell.shortStringParser(codecs3a, ":")
    f5("123:def:ghi:double:continuous:abc") shouldBe List(
      Failure(UnableToDecodeCell("123:def:ghi:double:continuous:abc"))
    )
    val f6 = Cell.shortStringParser(codecs3a, ":")
    f6("123:def:ghi:double:continuous:3:14") shouldBe List(
      Failure(UnableToDecodeCell("123:def:ghi:double:continuous:3:14"))
    )
    val f7 = Cell.shortStringParser(codecs3a, ":")
    f7("123:def:ghi:double|continuous:3.14") shouldBe List(
      Failure(UnableToDecodeCell("123:def:ghi:double|continuous:3.14"))
    )
  }

  "A Cell" should "parse 3D with dictionary" in {
    val f1 = Cell.shortStringParser(codecs3a, dictionary, _0, ":")
    f1("123:def:ghi:3.14") shouldBe List(
      Success(Cell(Position(123L, "def", "ghi"), Content(ContinuousSchema[Double](), 3.14)))
    )
    val f2 = Cell.shortStringParser(codecs3b, dictionary, _1, ":")
    f2("def:123:ghi:3.14") shouldBe List(
      Success(Cell(Position("def", 123L, "ghi"), Content(ContinuousSchema[Double](), 3.14)))
    )
    val f3 = Cell.shortStringParser(codecs3c, dictionary, _2, ":")
    f3("def:ghi:123:3.14") shouldBe List(
      Success(Cell(Position("def", "ghi", 123L), Content(ContinuousSchema[Double](), 3.14)))
    )
    val f4 = Cell.shortStringParser(codecs3a, dictionary, _0, ":")
    f4("abc:def:ghi:3.14") shouldBe List(Failure(UnableToDecodeCell("abc:def:ghi:3.14")))
    val f5 = Cell.shortStringParser(codecs3b, dictionary, _1, ":")
    f5("abc:def:ghi:3.14") shouldBe List(Failure(UnableToDecodeCell("abc:def:ghi:3.14")))
    val f6 = Cell.shortStringParser(codecs3c, dictionary, _2, ":")
    f6("abc:def:ghi:3.14") shouldBe List(Failure(UnableToDecodeCell("abc:def:ghi:3.14")))
    val f7 = Cell.shortStringParser(codecs3a, dictionary, _0, ":")
    f7("123:def:ghi:abc") shouldBe List(Failure(UnableToDecodeCell("123:def:ghi:abc")))
    val f8 = Cell.shortStringParser(codecs3a, dictionary, _0, ":")
    f8("123:def:ghi:3:14") shouldBe List(Failure(UnableToDecodeCell("123:def:ghi:3:14")))
    val f9 = Cell.shortStringParser(codecs3a, dictionary, _0, ":")
    f9("123|def:ghi:3.14") shouldBe List(Failure(UnableToDecodeCell("123|def:ghi:3.14")))
  }

  "A Cell" should "parse 3D with method" in {
    val f1 = Cell.shortStringParser(codecs3a, method, _0, ":")
    f1("123:def:ghi:3.14") shouldBe List(
      Success(Cell(Position(123L, "def", "ghi"), Content(ContinuousSchema[Double](), 3.14)))
    )
    val f2 = Cell.shortStringParser(codecs3b, method, _1, ":")
    f2("def:123:ghi:3.14") shouldBe List(
      Success(Cell(Position("def", 123L, "ghi"), Content(ContinuousSchema[Double](), 3.14)))
    )
    val f3 = Cell.shortStringParser(codecs3c, method, _2, ":")
    f3("def:ghi:123:3.14") shouldBe List(
      Success(Cell(Position("def", "ghi", 123L), Content(ContinuousSchema[Double](), 3.14)))
    )
    val f4 = Cell.shortStringParser(codecs3a, method, _0, ":")
    f4("abc:def:ghi:3.14") shouldBe List(Failure(UnableToDecodeCell("abc:def:ghi:3.14")))
    val f5 = Cell.shortStringParser(codecs3b, method, _1, ":")
    f5("abc:def:ghi:3.14") shouldBe List(Failure(UnableToDecodeCell("abc:def:ghi:3.14")))
    val f6 = Cell.shortStringParser(codecs3c, method, _2, ":")
    f6("abc:def:ghi:3.14") shouldBe List(Failure(UnableToDecodeCell("abc:def:ghi:3.14")))
    val f7 = Cell.shortStringParser(codecs3a, method, _0, ":")
    f7("123:def:ghi:abc") shouldBe List(Failure(UnableToDecodeCell("123:def:ghi:abc")))
    val f8 = Cell.shortStringParser(codecs3a, method, _0, ":")
    f8("123:def:ghi:3:14") shouldBe List(Failure(UnableToDecodeCell("123:def:ghi:3:14")))
    val f9 = Cell.shortStringParser(codecs3a, method, _0, ":")
    f9("123|def:ghi:3.14") shouldBe List(Failure(UnableToDecodeCell("123|def:ghi:3.14")))
  }

  "A Cell" should "parse 3D with schema" in {
    val f1 = Cell.shortStringParser(codecs3a, schema, ":")
    f1("123:def:ghi:3.14") shouldBe List(
      Success(Cell(Position(123L, "def", "ghi"), Content(ContinuousSchema[Double](), 3.14)))
    )
    val f2 = Cell.shortStringParser(codecs3a, schema, ":")
    f2("abc:def:ghi:3.14") shouldBe List(Failure(UnableToDecodeCell("abc:def:ghi:3.14")))
    val f3 = Cell.shortStringParser(codecs3a, schema, ":")
    f3("123:def:ghi:abc") shouldBe List(Failure(UnableToDecodeCell("123:def:ghi:abc")))
    val f4 = Cell.shortStringParser(codecs3a, schema, ":")
    f4("123:def:ghi:3:14") shouldBe List(Failure(UnableToDecodeCell("123:def:ghi:3:14")))
    val f5 = Cell.shortStringParser(codecs3a, schema, ":")
    f5("123:def|ghi:3.14") shouldBe List(Failure(UnableToDecodeCell("123:def|ghi:3.14")))
  }

  "A Cell" should "parse table" in {
    val f1 = Cell.tableParser(
      KeyDecoder(_0, DoubleCodec),
      Set(ColumnDecoder(_1, "def", schema), ColumnDecoder(_2, "ghi", schema)),
      ":"
    )
    f1("3.14:6.28:9.42") shouldBe Set(
      Success(Cell(Position(3.14, "def"), Content(ContinuousSchema[Double](), 6.28))),
      Success(Cell(Position(3.14, "ghi"), Content(ContinuousSchema[Double](), 9.42)))
    )
    val f2 = Cell.tableParser(
      KeyDecoder(_1, DoubleCodec),
      Set(ColumnDecoder(_0, "abc", schema), ColumnDecoder(_2, "ghi", schema)),
      ":"
    )
    f2("3.14:6.28:9.42") shouldBe Set(
      Success(Cell(Position(6.28, "abc"), Content(ContinuousSchema[Double](), 3.14))),
      Success(Cell(Position(6.28, "ghi"), Content(ContinuousSchema[Double](), 9.42)))
    )
    val f3 = Cell.tableParser(
      KeyDecoder(_2, DoubleCodec),
      Set(ColumnDecoder(_0, "abc", schema), ColumnDecoder(_1, "def", schema)),
      ":"
    )
    f3("3.14:6.28:9.42") shouldBe Set(
      Success(Cell(Position(9.42, "abc"), Content(ContinuousSchema[Double](), 3.14))),
      Success(Cell(Position(9.42, "def"), Content(ContinuousSchema[Double](), 6.28)))
    )
    val f4 = Cell.tableParser(
      KeyDecoder(_0, DoubleCodec),
      Set(ColumnDecoder(_1, "def", schema), ColumnDecoder(_2, "ghi", schema)),
      ":"
    )
    f4("3.14:foo:bar") shouldBe Set(
      Failure(UnableToDecodeCell("3.14:foo:bar", UnableToDecodeContent("foo"))),
      Failure(UnableToDecodeCell("3.14:foo:bar", UnableToDecodeContent("bar")))
    )
    val f5 = Cell.tableParser(
      KeyDecoder(_0, DoubleCodec),
      Set(ColumnDecoder(_1, "def", schema), ColumnDecoder(_2, "ghi", schema)),
      ":"
    )
    f5("3.14:foo") shouldBe List(Failure(IncorrectNumberOfFields("3.14:foo")))
  }

  "A Cell" should "parse JSON" in {
    val cell1 = Cell(Position("foo"), Content(ContinuousSchema[Double](), 3.14))
    val cell2 = Cell(Position("foo", 1), Content(ContinuousSchema[Double](), 3.14))
    val cell3 = Cell(Position("foo", 1, true), Content(ContinuousSchema[Double](), 3.14))

    val f1 = Cell.jsonParser(StringCodec :: HNil)
    cell1.toJSON(false,false) shouldBe """{"position":["foo"],"content":{"value":"3.14"}}"""
    f1(cell1.toJSON(true, true)) shouldBe List(Success(cell1))

    val f2 = Cell.jsonParser(StringCodec :: IntCodec :: HNil)
    cell2.toJSON(true, true) shouldBe """{
  "position" : [ "foo", "1" ],
  "content" : {
    "codec" : "double",
    "schema" : "continuous",
    "value" : "3.14"
  }
}"""
    f2(cell2.toJSON(true, true)) shouldBe List(Success(cell2))

    val f3 = Cell.jsonParser(StringCodec :: IntCodec :: BooleanCodec :: HNil)
    f3(cell3.toJSON(true, true)) shouldBe List(Success(cell3))
  }

  "A Cell" should "parse with empty strings" in {
    val f = Cell.shortStringParser(StringCodec :: StringCodec :: HNil, "|")

    f("|def|string|nominal|ghi") shouldBe List(
      Success(Cell(Position("", "def"), Content(NominalSchema[String](), "ghi")))
    )

    f("abc||string|nominal|ghi") shouldBe List(
      Success(Cell(Position("abc", ""), Content(NominalSchema[String](), "ghi")))
    )

    f("abc|def|string|nominal|") shouldBe List(
      Success(Cell(Position("abc", "def"), Content(NominalSchema[String](), "")))
    )
  }
}

