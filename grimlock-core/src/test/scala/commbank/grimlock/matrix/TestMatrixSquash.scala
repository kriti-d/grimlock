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
import commbank.grimlock.framework.environment.tuner._
import commbank.grimlock.framework.metadata._
import commbank.grimlock.framework.position._
import commbank.grimlock.framework.squash._

import commbank.grimlock.library.squash._

import com.twitter.scalding.typed.ValuePipe

import shapeless.{ HList, Nat }
import shapeless.nat.{ _0, _1, _2 }

trait TestMatrixSquash extends TestMatrix {
  val ext = "ext"

  val result1 = List(
    Cell(Position(1), Content(OrdinalSchema[String](), "12.56")),
    Cell(Position(2), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position(3), Content(NominalSchema[String](), "9.42")),
    Cell(
      Position(4),
      Content(
        DateSchema[java.util.Date](),
        DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
      )
    )
  )

  val result2 = List(
    Cell(Position("bar"), Content(OrdinalSchema[Long](), 19L)),
    Cell(Position("baz"), Content(DiscreteSchema[Long](), 19L)),
    Cell(
      Position("foo"),
      Content(
        DateSchema[java.util.Date](),
        DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
      )
    ),
    Cell(Position("qux"), Content(OrdinalSchema[String](), "12.56"))
  )

  val result3 = List(
    Cell(Position(1, "xyz"), Content(OrdinalSchema[String](), "12.56")),
    Cell(Position(2, "xyz"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position(3, "xyz"), Content(NominalSchema[String](), "9.42")),
    Cell(
      Position(4, "xyz"),
      Content(
        DateSchema[java.util.Date](),
        DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
      )
    )
  )

  val result4 = List(
    Cell(Position("bar", "xyz"), Content(OrdinalSchema[Long](), 19L)),
    Cell(Position("baz", "xyz"), Content(DiscreteSchema[Long](), 19L)),
    Cell(
      Position("foo", "xyz"),
      Content(
        DateSchema[java.util.Date](),
        DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
      )
    ),
    Cell(Position("qux", "xyz"), Content(OrdinalSchema[String](), "12.56"))
  )

  val result5 = List(
    Cell(Position("bar", 1), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("bar", 2), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", 3), Content(OrdinalSchema[Long](), 19L)),
    Cell(Position("baz", 1), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", 2), Content(DiscreteSchema[Long](), 19L)),
    Cell(Position("foo", 1), Content(OrdinalSchema[String](), "3.14")),
    Cell(Position("foo", 2), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("foo", 3), Content(NominalSchema[String](), "9.42")),
    Cell(
      Position("foo", 4),
      Content(
        DateSchema[java.util.Date](),
        DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
      )
    ),
    Cell(Position("qux", 1), Content(OrdinalSchema[String](), "12.56"))
  )

  val result6 = List(
    Cell(Position(1), Content(OrdinalSchema[String](), "12.56")),
    Cell(Position(2), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position(3), Content(NominalSchema[String](), "9.42")),
    Cell(
      Position(4),
      Content(
        DateSchema[java.util.Date](),
        DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
      )
    )
  )

  val result7 = List(
    Cell(Position("bar"), Content(OrdinalSchema[Long](), 19L)),
    Cell(Position("baz"), Content(DiscreteSchema[Long](), 19L)),
    Cell(
      Position("foo"),
      Content(
        DateSchema[java.util.Date](),
        DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
      )
    ),
    Cell(Position("qux"), Content(OrdinalSchema[String](), "12.56"))
  )

  val result8 = List(
    Cell(Position(1, "xyz"), Content(OrdinalSchema[String](), "12.56")),
    Cell(Position(2, "xyz"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position(3, "xyz"), Content(NominalSchema[String](), "9.42")),
    Cell(
      Position(4, "xyz"),
      Content(
        DateSchema[java.util.Date](),
        DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
      )
    )
  )

  val result9 = List(
    Cell(Position("bar", "xyz"), Content(OrdinalSchema[Long](), 19L)),
    Cell(Position("baz", "xyz"), Content(DiscreteSchema[Long](), 19L)),
    Cell(
      Position("foo", "xyz"),
      Content(
        DateSchema[java.util.Date](),
        DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
      )
    ),
    Cell(Position("qux", "xyz"), Content(OrdinalSchema[String](), "12.56"))
  )

  val result10 = List(
    Cell(Position("bar", 1), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("bar", 2), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", 3), Content(OrdinalSchema[Long](), 19L)),
    Cell(Position("baz", 1), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", 2), Content(DiscreteSchema[Long](), 19L)),
    Cell(Position("foo", 1), Content(OrdinalSchema[String](), "3.14")),
    Cell(Position("foo", 2), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("foo", 3), Content(NominalSchema[String](), "9.42")),
    Cell(
      Position("foo", 4),
      Content(
        DateSchema[java.util.Date](),
        DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
      )
    ),
    Cell(Position("qux", 1), Content(OrdinalSchema[String](), "12.56"))
  )
}

object TestMatrixSquash {
  case class PreservingMaxPositionWithValue[P <: HList]() extends SquasherWithValue[P] {
    type V = String
    type T = squasher.T

    val squasher = PreservingMaximumPosition[P]()
    val tTag = squasher.tTag

    def prepareWithValue[
      D <: Nat
    ](
      cell: Cell[P],
      dim: D,
      ext: V
    )(implicit
      ev: Position.IndexConstraints[P, D] { type V <: Value[_] }
    ): Option[T] = squasher.prepare(cell, dim)

    def reduce(lt: T, rt: T): T = squasher.reduce(lt, rt)

    def presentWithValue(t: T, ext: V): Option[Content] = if (ext == "ext") squasher.present(t) else None
  }
}

class TestScalaMatrixSquash extends TestMatrixSquash with TestScala {
  import commbank.grimlock.scala.environment.implicits._

  "A Matrix.squash" should "return its first squashed data in 2D" in {
    toU(data2)
      .squash(_0, PreservingMaximumPosition(), Default())
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its second squashed data in 2D" in {
    toU(data2)
      .squash(_1, PreservingMaximumPosition(), Default())
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its first squashed data in 3D" in {
    toU(data3)
      .squash(_0, PreservingMaximumPosition(), Default())
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its second squashed data in 3D" in {
    toU(data3)
      .squash(_1, PreservingMaximumPosition(), Default())
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its third squashed data in 3D" in {
    toU(data3)
      .squash(_2, PreservingMaximumPosition(), Default())
      .toList.sortBy(_.position) shouldBe result5
  }

  "A Matrix.squashWithValue" should "return its first squashed data in 2D" in {
    toU(data2)
      .squashWithValue(_0, ext, TestMatrixSquash.PreservingMaxPositionWithValue(), Default())
      .toList.sortBy(_.position) shouldBe result6
  }

  it should "return its second squashed data in 2D" in {
    toU(data2)
      .squashWithValue(_1, ext, TestMatrixSquash.PreservingMaxPositionWithValue(), Default())
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its first squashed data in 3D" in {
    toU(data3)
      .squashWithValue(_0, ext, TestMatrixSquash.PreservingMaxPositionWithValue(), Default())
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its second squashed data in 3D" in {
    toU(data3)
      .squashWithValue(_1, ext, TestMatrixSquash.PreservingMaxPositionWithValue(), Default())
      .toList.sortBy(_.position) shouldBe result9
  }

  it should "return its third squashed data in 3D" in {
    toU(data3)
      .squashWithValue(_2, ext, TestMatrixSquash.PreservingMaxPositionWithValue(), Default())
      .toList.sortBy(_.position) shouldBe result10
  }
}

class TestScaldingMatrixSquash extends TestMatrixSquash with TestScalding {
  import commbank.grimlock.scalding.environment.implicits._

  "A Matrix.squash" should "return its first squashed data in 2D" in {
    toU(data2)
      .squash(_0, PreservingMaximumPosition(), Default())
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its second squashed data in 2D" in {
    toU(data2)
      .squash(_1, PreservingMaximumPosition(), Default(12))
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its first squashed data in 3D" in {
    toU(data3)
      .squash(_0, PreservingMaximumPosition(), Default())
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its second squashed data in 3D" in {
    toU(data3)
      .squash(_1, PreservingMaximumPosition(), Default(12))
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its third squashed data in 3D" in {
    toU(data3)
      .squash(_2, PreservingMaximumPosition(), Default())
      .toList.sortBy(_.position) shouldBe result5
  }

  "A Matrix.squashWithValue" should "return its first squashed data in 2D" in {
    toU(data2)
      .squashWithValue(_0, ValuePipe(ext), TestMatrixSquash.PreservingMaxPositionWithValue(), Default(12))
      .toList.sortBy(_.position) shouldBe result6
  }

  it should "return its second squashed data in 2D" in {
    toU(data2)
      .squashWithValue(_1, ValuePipe(ext), TestMatrixSquash.PreservingMaxPositionWithValue(), Default())
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its first squashed data in 3D" in {
    toU(data3)
      .squashWithValue(_0, ValuePipe(ext), TestMatrixSquash.PreservingMaxPositionWithValue(), Default(12))
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its second squashed data in 3D" in {
    toU(data3)
      .squashWithValue(_1, ValuePipe(ext), TestMatrixSquash.PreservingMaxPositionWithValue(), Default())
      .toList.sortBy(_.position) shouldBe result9
  }

  it should "return its third squashed data in 3D" in {
    toU(data3)
      .squashWithValue(_2, ValuePipe(ext), TestMatrixSquash.PreservingMaxPositionWithValue(), Default(12))
      .toList.sortBy(_.position) shouldBe result10
  }
}

class TestSparkMatrixSquash extends TestMatrixSquash with TestSpark {
  import commbank.grimlock.spark.environment.implicits._

  "A Matrix.squash" should "return its first squashed data in 2D" in {
    toU(data2)
      .squash(_0, PreservingMaximumPosition(), Default())
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its second squashed data in 2D" in {
    toU(data2)
      .squash(_1, PreservingMaximumPosition(), Default(12))
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its first squashed data in 3D" in {
    toU(data3)
      .squash(_0, PreservingMaximumPosition(), Default())
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its second squashed data in 3D" in {
    toU(data3)
      .squash(_1, PreservingMaximumPosition(), Default(12))
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its third squashed data in 3D" in {
    toU(data3)
      .squash(_2, PreservingMaximumPosition(), Default())
      .toList.sortBy(_.position) shouldBe result5
  }

  "A Matrix.squashWithValue" should "return its first squashed data in 2D" in {
    toU(data2)
      .squashWithValue(_0, ext, TestMatrixSquash.PreservingMaxPositionWithValue(), Default(12))
      .toList.sortBy(_.position) shouldBe result6
  }

  it should "return its second squashed data in 2D" in {
    toU(data2)
      .squashWithValue(_1, ext, TestMatrixSquash.PreservingMaxPositionWithValue(), Default())
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its first squashed data in 3D" in {
    toU(data3)
      .squashWithValue(_0, ext, TestMatrixSquash.PreservingMaxPositionWithValue(), Default(12))
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its second squashed data in 3D" in {
    toU(data3)
      .squashWithValue(_1, ext, TestMatrixSquash.PreservingMaxPositionWithValue(), Default())
      .toList.sortBy(_.position) shouldBe result9
  }

  it should "return its third squashed data in 3D" in {
    toU(data3)
      .squashWithValue(_2, ext, TestMatrixSquash.PreservingMaxPositionWithValue(), Default(12))
      .toList.sortBy(_.position) shouldBe result10
  }
}

