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

import commbank.grimlock.framework._
import commbank.grimlock.framework.content._
import commbank.grimlock.framework.encoding._
import commbank.grimlock.framework.environment.implicits._
import commbank.grimlock.framework.environment.tuner._
import commbank.grimlock.framework.metadata._
import commbank.grimlock.framework.position._

import shapeless.HList
import shapeless.nat.{ _0, _1, _2 }

trait TestMatrixExpand extends TestMatrix {
  val dataA = List(
    Cell(Position("foo", "letter"), Content(NominalSchema[String](), "a")),
    Cell(Position("foo", "number"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("bar", "letter"), Content(NominalSchema[String](), "b")),
    Cell(Position("bar", "number"), Content(DiscreteSchema[Long](), 2L)),
    Cell(Position("baz", "letter"), Content(NominalSchema[String](), "a")),
    Cell(Position("qux", "number"), Content(DiscreteSchema[Long](), 2L))
  )

  val dataB = List(
    Cell(Position("foo", "letter", true), Content(NominalSchema[String](), "a")),
    Cell(Position("foo", "number", true), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("bar", "letter", true), Content(NominalSchema[String](), "b")),
    Cell(Position("bar", "number", true), Content(DiscreteSchema[Long](), 2L)),
    Cell(Position("baz", "letter", true), Content(NominalSchema[String](), "a")),
    Cell(Position("qux", "number", true), Content(DiscreteSchema[Long](), 2L)),
    Cell(Position("foo", "number", false), Content(DiscreteSchema[Long](), 3L)),
    Cell(Position("bar", "letter", false), Content(NominalSchema[String](), "c")),
    Cell(Position("baz", "number", false), Content(DiscreteSchema[Long](), 4L)),
    Cell(Position("qux", "letter", false), Content(NominalSchema[String](), "d"))
  )

  val result1 = List(
    Cell(Position("bar", "letter", "a"), Content(NominalSchema[String](), "b")),
    Cell(Position("bar", "number", "NA"), Content(DiscreteSchema[Long](), 2L)),
    Cell(Position("foo", "letter", "a"), Content(NominalSchema[String](), "a")),
    Cell(Position("foo", "number", "NA"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("qux", "number", "NA"), Content(DiscreteSchema[Long](), 2L))
  )

  val result2 = List(
    Cell(Position("bar", "number", "b"), Content(DiscreteSchema[Long](), 2L)),
    Cell(Position("foo", "number", "a"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("qux", "number", "NA"), Content(DiscreteSchema[Long](), 2L))
  )

  val result3 = List(
    Cell(Position("bar", "letter", false, "d"), Content(NominalSchema[String](), "c")),
    Cell(Position("bar", "letter", true, "NA"), Content(NominalSchema[String](), "b")),
    Cell(Position("bar", "number", true, "2"), Content(DiscreteSchema[Long](), 2L)),
    Cell(Position("baz", "letter", true, "NA"), Content(NominalSchema[String](), "a")),
    Cell(Position("baz", "number", false, "NA"), Content(DiscreteSchema[Long](), 4L)),
    Cell(Position("foo", "letter", true, "NA"), Content(NominalSchema[String](), "a")),
    Cell(Position("foo", "number", false, "NA"), Content(DiscreteSchema[Long](), 3L)),
    Cell(Position("foo", "number", true, "2"), Content(DiscreteSchema[Long](), 1L))
  )

  val result4 = List(
    Cell(Position("bar", "letter", false, "NA"), Content(NominalSchema[String](), "c")),
    Cell(Position("bar", "letter", true, "2"), Content(NominalSchema[String](), "b")),
    Cell(Position("baz", "letter", true, "NA"), Content(NominalSchema[String](), "a")),
    Cell(Position("foo", "letter", true, "1"), Content(NominalSchema[String](), "a")),
    Cell(Position("qux", "letter", false, "NA"), Content(NominalSchema[String](), "d"))
  )

  val result5 = List(
    Cell(Position("bar", "letter", false, "b"), Content(NominalSchema[String](), "c")),
    Cell(Position("baz", "number", false, "NA"), Content(DiscreteSchema[Long](), 4L)),
    Cell(Position("foo", "number", false, "1"), Content(DiscreteSchema[Long](), 3L)),
    Cell(Position("qux", "letter", false, "NA"), Content(NominalSchema[String](), "d"))
  )
}

object TestMatrixExpand {
  def cast[
    P <: HList
  ](implicit
    ev: Position.AppendConstraints[P, Value[String]]
  ) = (cell: Cell[P], value: Option[Value[_]]) => cell
    .position
    .append(value.map(_.toShortString).getOrElse("NA"))
    .toOption
}

class TestScalaMatrixExpand extends TestMatrixExpand with TestScala {
  import commbank.grimlock.scala.environment.implicits._

  "A Matrix.expand" should "reshape its first dimension in 2D" in {
    toU(dataA)
      .expand(_0, "baz", TestMatrixExpand.cast, Default())
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "reshape its second dimension in 2D" in {
    toU(dataA)
      .expand(_1, "letter", TestMatrixExpand.cast, Default())
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "reshape its first dimension in 3D" in {
    toU(dataB)
      .expand(_0, "qux", TestMatrixExpand.cast, Default())
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "reshape its second dimension in 3D" in {
    toU(dataB)
      .expand(_1, "number", TestMatrixExpand.cast, Default())
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "reshape its third dimension in 3D" in {
    toU(dataB)
      .expand(_2, true, TestMatrixExpand.cast, Default())
      .toList.sortBy(_.position) shouldBe result5
  }
}

class TestScaldingMatrixExpand extends TestMatrixExpand with TestScalding {
  import commbank.grimlock.scalding.environment.implicits._

  "A Matrix.expand" should "reshape its first dimension in 2D" in {
    toU(dataA)
      .expand(_0, "baz", TestMatrixExpand.cast, InMemory())
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "reshape its second dimension in 2D" in {
    toU(dataA)
      .expand(_1, "letter", TestMatrixExpand.cast, Default())
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "reshape its first dimension in 3D" in {
    toU(dataB)
      .expand(_0, "qux", TestMatrixExpand.cast, Default(12))
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "reshape its second dimension in 3D" in {
    toU(dataB)
      .expand(_1, "number", TestMatrixExpand.cast, Unbalanced(12))
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "reshape its third dimension in 3D" in {
    toU(dataB)
      .expand(_2, true, TestMatrixExpand.cast, InMemory())
      .toList.sortBy(_.position) shouldBe result5
  }
}

class TestSparkMatrixExpand extends TestMatrixExpand with TestSpark {
  import commbank.grimlock.spark.environment.implicits._

  "A Matrix.expand" should "reshape its first dimension in 2D" in {
    toU(dataA)
      .expand(_0, "baz", TestMatrixExpand.cast, InMemory())
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "reshape its second dimension in 2D" in {
    toU(dataA)
      .expand(_1, "letter", TestMatrixExpand.cast, Default())
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "reshape its first dimension in 3D" in {
    toU(dataB)
      .expand(_0, "qux", TestMatrixExpand.cast, Default(12))
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "reshape its second dimension in 3D" in {
    toU(dataB)
      .expand(_1, "number", TestMatrixExpand.cast, InMemory())
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "reshape its third dimension in 3D" in {
    toU(dataB)
      .expand(_2, true, TestMatrixExpand.cast, Default())
      .toList.sortBy(_.position) shouldBe result5
  }
}

