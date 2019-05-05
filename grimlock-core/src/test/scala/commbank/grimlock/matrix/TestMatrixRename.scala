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
import commbank.grimlock.framework.metadata._
import commbank.grimlock.framework.position._

import com.twitter.scalding.typed.ValuePipe

import shapeless.{ HList, Nat }
import shapeless.nat.{ _0, _1, _2 }

trait TestMatrixRename extends TestMatrix {
  val ext = ".new"

  val result1 = List(
    Cell(Position("bar.new"), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("baz.new"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("foo.new"), Content(OrdinalSchema[String](), "3.14")),
    Cell(Position("qux.new"), Content(OrdinalSchema[String](), "12.56"))
  )

  val result2 = List(
    Cell(Position("bar.new", 1), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("bar.new", 2), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar.new", 3), Content(OrdinalSchema[Long](), 19L)),
    Cell(Position("baz.new", 1), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz.new", 2), Content(DiscreteSchema[Long](), 19L)),
    Cell(Position("foo.new", 1), Content(OrdinalSchema[String](), "3.14")),
    Cell(Position("foo.new", 2), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("foo.new", 3), Content(NominalSchema[String](), "9.42")),
    Cell(
      Position("foo.new", 4),
      Content(
        DateSchema[java.util.Date](),
        DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
      )
    ),
    Cell(Position("qux.new", 1), Content(OrdinalSchema[String](), "12.56"))
  )

  val result3 = List(
    Cell(Position("bar", "1.new"), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("bar", "2.new"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", "3.new"), Content(OrdinalSchema[Long](), 19L)),
    Cell(Position("baz", "1.new"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", "2.new"), Content(DiscreteSchema[Long](), 19L)),
    Cell(Position("foo", "1.new"), Content(OrdinalSchema[String](), "3.14")),
    Cell(Position("foo", "2.new"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("foo", "3.new"), Content(NominalSchema[String](), "9.42")),
    Cell(
      Position("foo", "4.new"),
      Content(
        DateSchema[java.util.Date](),
        DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
      )
    ),
    Cell(Position("qux", "1.new"), Content(OrdinalSchema[String](), "12.56"))
  )

  val result4 = List(
    Cell(Position("bar.new", 1, "xyz"), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("bar.new", 2, "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar.new", 3, "xyz"), Content(OrdinalSchema[Long](), 19L)),
    Cell(Position("baz.new", 1, "xyz"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz.new", 2, "xyz"), Content(DiscreteSchema[Long](), 19L)),
    Cell(Position("foo.new", 1, "xyz"), Content(OrdinalSchema[String](), "3.14")),
    Cell(Position("foo.new", 2, "xyz"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("foo.new", 3, "xyz"), Content(NominalSchema[String](), "9.42")),
    Cell(
      Position("foo.new", 4, "xyz"),
      Content(
        DateSchema[java.util.Date](),
        DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
      )
    ),
    Cell(Position("qux.new", 1, "xyz"), Content(OrdinalSchema[String](), "12.56"))
  )

  val result5 = List(
    Cell(Position("bar", "1.new", "xyz"), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("bar", "2.new", "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", "3.new", "xyz"), Content(OrdinalSchema[Long](), 19L)),
    Cell(Position("baz", "1.new", "xyz"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", "2.new", "xyz"), Content(DiscreteSchema[Long](), 19L)),
    Cell(Position("foo", "1.new", "xyz"), Content(OrdinalSchema[String](), "3.14")),
    Cell(Position("foo", "2.new", "xyz"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("foo", "3.new", "xyz"), Content(NominalSchema[String](), "9.42")),
    Cell(
      Position("foo", "4.new", "xyz"),
      Content(
        DateSchema[java.util.Date](),
        DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
      )
    ),
    Cell(Position("qux", "1.new", "xyz"), Content(OrdinalSchema[String](), "12.56"))
  )

  val result6 = List(
    Cell(Position("bar", 1, "xyz.new"), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("bar", 2, "xyz.new"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", 3, "xyz.new"), Content(OrdinalSchema[Long](), 19L)),
    Cell(Position("baz", 1, "xyz.new"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", 2, "xyz.new"), Content(DiscreteSchema[Long](), 19L)),
    Cell(Position("foo", 1, "xyz.new"), Content(OrdinalSchema[String](), "3.14")),
    Cell(Position("foo", 2, "xyz.new"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("foo", 3, "xyz.new"), Content(NominalSchema[String](), "9.42")),
    Cell(
      Position("foo", 4, "xyz.new"),
      Content(
        DateSchema[java.util.Date](),
        DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
      )
    ),
    Cell(Position("qux", 1, "xyz.new"), Content(OrdinalSchema[String](), "12.56"))
  )

  val result7 = List(
    Cell(Position("bar.new"), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("baz.new"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("foo.new"), Content(OrdinalSchema[String](), "3.14")),
    Cell(Position("qux.new"), Content(OrdinalSchema[String](), "12.56"))
  )

  val result8 = List(
    Cell(Position("bar.new", 1), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("bar.new", 2), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar.new", 3), Content(OrdinalSchema[Long](), 19L)),
    Cell(Position("baz.new", 1), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz.new", 2), Content(DiscreteSchema[Long](), 19L)),
    Cell(Position("foo.new", 1), Content(OrdinalSchema[String](), "3.14")),
    Cell(Position("foo.new", 2), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("foo.new", 3), Content(NominalSchema[String](), "9.42")),
    Cell(
      Position("foo.new", 4),
      Content(
        DateSchema[java.util.Date](),
        DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
      )
    ),
    Cell(Position("qux.new", 1), Content(OrdinalSchema[String](), "12.56"))
  )

  val result9 = List(
    Cell(Position("bar", "1.new"), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("bar", "2.new"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", "3.new"), Content(OrdinalSchema[Long](), 19L)),
    Cell(Position("baz", "1.new"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", "2.new"), Content(DiscreteSchema[Long](), 19L)),
    Cell(Position("foo", "1.new"), Content(OrdinalSchema[String](), "3.14")),
    Cell(Position("foo", "2.new"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("foo", "3.new"), Content(NominalSchema[String](), "9.42")),
    Cell(
      Position("foo", "4.new"),
      Content(
        DateSchema[java.util.Date](),
        DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
      )
    ),
    Cell(Position("qux", "1.new"), Content(OrdinalSchema[String](), "12.56"))
  )

  val result10 = List(
    Cell(Position("bar.new", 1, "xyz"), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("bar.new", 2, "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar.new", 3, "xyz"), Content(OrdinalSchema[Long](), 19L)),
    Cell(Position("baz.new", 1, "xyz"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz.new", 2, "xyz"), Content(DiscreteSchema[Long](), 19L)),
    Cell(Position("foo.new", 1, "xyz"), Content(OrdinalSchema[String](), "3.14")),
    Cell(Position("foo.new", 2, "xyz"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("foo.new", 3, "xyz"), Content(NominalSchema[String](), "9.42")),
    Cell(
      Position("foo.new", 4, "xyz"),
      Content(
        DateSchema[java.util.Date](),
        DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
      )
    ),
    Cell(Position("qux.new", 1, "xyz"), Content(OrdinalSchema[String](), "12.56"))
  )

  val result11 = List(
    Cell(Position("bar", "1.new", "xyz"), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("bar", "2.new", "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", "3.new", "xyz"), Content(OrdinalSchema[Long](), 19L)),
    Cell(Position("baz", "1.new", "xyz"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", "2.new", "xyz"), Content(DiscreteSchema[Long](), 19L)),
    Cell(Position("foo", "1.new", "xyz"), Content(OrdinalSchema[String](), "3.14")),
    Cell(Position("foo", "2.new", "xyz"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("foo", "3.new", "xyz"), Content(NominalSchema[String](), "9.42")),
    Cell(
      Position("foo", "4.new", "xyz"),
      Content(
        DateSchema[java.util.Date](),
        DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
      )
    ),
    Cell(Position("qux", "1.new", "xyz"), Content(OrdinalSchema[String](), "12.56"))
  )

  val result12 = List(
    Cell(Position("bar", 1, "xyz.new"), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("bar", 2, "xyz.new"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", 3, "xyz.new"), Content(OrdinalSchema[Long](), 19L)),
    Cell(Position("baz", 1, "xyz.new"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", 2, "xyz.new"), Content(DiscreteSchema[Long](), 19L)),
    Cell(Position("foo", 1, "xyz.new"), Content(OrdinalSchema[String](), "3.14")),
    Cell(Position("foo", 2, "xyz.new"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("foo", 3, "xyz.new"), Content(NominalSchema[String](), "9.42")),
    Cell(
      Position("foo", 4, "xyz.new"),
      Content(
        DateSchema[java.util.Date](),
        DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
      )
    ),
    Cell(Position("qux", 1, "xyz.new"), Content(OrdinalSchema[String](), "12.56"))
  )
}

object TestMatrixRename {
  def renamer[
    P <: HList,
    D <: Nat
  ](
    dim: D
  )(implicit
    ev1: Position.IndexConstraints[P, D],
    ev2: Position.UpdateConstraints[P, D, Value[String]]
  ) = (cell: Cell[P]) => cell.position.update(dim, cell.position(dim).toShortString + ".new").toOption

  def renamerWithValue[
    P <: HList,
    D <: Nat
  ](
    dim: D
  )(implicit
    ev1: Position.IndexConstraints[P, D],
    ev2: Position.UpdateConstraints[P, D, Value[String]]
  ) = (cell: Cell[P], ext: String) => cell.position.update(dim, cell.position(dim).toShortString + ext).toOption
}

class TestScalaMatrixRename extends TestMatrixRename with TestScala {
  import commbank.grimlock.scala.environment.implicits._

  "A Matrix.relocate" should "return its first renamed data in 1D" in {
    toU(data1)
      .relocate(TestMatrixRename.renamer(_0))
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its first renamed data in 2D" in {
    toU(data2)
      .relocate(TestMatrixRename.renamer(_0))
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its second renamed data in 2D" in {
    toU(data2)
      .relocate(TestMatrixRename.renamer(_1))
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its first renamed data in 3D" in {
    toU(data3)
      .relocate(TestMatrixRename.renamer(_0))
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its second renamed data in 3D" in {
    toU(data3)
      .relocate(TestMatrixRename.renamer(_1))
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its third renamed data in 3D" in {
    toU(data3)
      .relocate(TestMatrixRename.renamer(_2))
      .toList.sortBy(_.position) shouldBe result6
  }

  "A Matrix.renameWithValue" should "return its first renamed data in 1D" in {
    toU(data1)
      .relocateWithValue(ext, TestMatrixRename.renamerWithValue(_0))
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its first renamed data in 2D" in {
    toU(data2)
      .relocateWithValue(ext, TestMatrixRename.renamerWithValue(_0))
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its second renamed data in 2D" in {
    toU(data2)
      .relocateWithValue(ext, TestMatrixRename.renamerWithValue(_1))
      .toList.sortBy(_.position) shouldBe result9
  }

  it should "return its first renamed data in 3D" in {
    toU(data3)
      .relocateWithValue(ext, TestMatrixRename.renamerWithValue(_0))
      .toList.sortBy(_.position) shouldBe result10
  }

  it should "return its second renamed data in 3D" in {
    toU(data3)
      .relocateWithValue(ext, TestMatrixRename.renamerWithValue(_1))
      .toList.sortBy(_.position) shouldBe result11
  }

  it should "return its third renamed data in 3D" in {
    toU(data3)
      .relocateWithValue(ext, TestMatrixRename.renamerWithValue(_2))
      .toList.sortBy(_.position) shouldBe result12
  }
}

class TestScaldingMatrixRename extends TestMatrixRename with TestScalding {
  import commbank.grimlock.scalding.environment.implicits._

  "A Matrix.relocate" should "return its first renamed data in 1D" in {
    toU(data1)
      .relocate(TestMatrixRename.renamer(_0))
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its first renamed data in 2D" in {
    toU(data2)
      .relocate(TestMatrixRename.renamer(_0))
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its second renamed data in 2D" in {
    toU(data2)
      .relocate(TestMatrixRename.renamer(_1))
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its first renamed data in 3D" in {
    toU(data3)
      .relocate(TestMatrixRename.renamer(_0))
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its second renamed data in 3D" in {
    toU(data3)
      .relocate(TestMatrixRename.renamer(_1))
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its third renamed data in 3D" in {
    toU(data3)
      .relocate(TestMatrixRename.renamer(_2))
      .toList.sortBy(_.position) shouldBe result6
  }

  "A Matrix.renameWithValue" should "return its first renamed data in 1D" in {
    toU(data1)
      .relocateWithValue(ValuePipe(ext), TestMatrixRename.renamerWithValue(_0))
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its first renamed data in 2D" in {
    toU(data2)
      .relocateWithValue(ValuePipe(ext), TestMatrixRename.renamerWithValue(_0))
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its second renamed data in 2D" in {
    toU(data2)
      .relocateWithValue(ValuePipe(ext), TestMatrixRename.renamerWithValue(_1))
      .toList.sortBy(_.position) shouldBe result9
  }

  it should "return its first renamed data in 3D" in {
    toU(data3)
      .relocateWithValue(ValuePipe(ext), TestMatrixRename.renamerWithValue(_0))
      .toList.sortBy(_.position) shouldBe result10
  }

  it should "return its second renamed data in 3D" in {
    toU(data3)
      .relocateWithValue(ValuePipe(ext), TestMatrixRename.renamerWithValue(_1))
      .toList.sortBy(_.position) shouldBe result11
  }

  it should "return its third renamed data in 3D" in {
    toU(data3)
      .relocateWithValue(ValuePipe(ext), TestMatrixRename.renamerWithValue(_2))
      .toList.sortBy(_.position) shouldBe result12
  }
}

class TestSparkMatrixRename extends TestMatrixRename with TestSpark {
  import commbank.grimlock.spark.environment.implicits._

  "A Matrix.rename" should "return its first renamed data in 1D" in {
    toU(data1)
      .relocate(TestMatrixRename.renamer(_0))
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its first renamed data in 2D" in {
    toU(data2)
      .relocate(TestMatrixRename.renamer(_0))
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its second renamed data in 2D" in {
    toU(data2)
      .relocate(TestMatrixRename.renamer(_1))
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its first renamed data in 3D" in {
    toU(data3)
      .relocate(TestMatrixRename.renamer(_0))
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its second renamed data in 3D" in {
    toU(data3)
      .relocate(TestMatrixRename.renamer(_1))
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its third renamed data in 3D" in {
    toU(data3)
      .relocate(TestMatrixRename.renamer(_2))
      .toList.sortBy(_.position) shouldBe result6
  }

  "A Matrix.renameWithValue" should "return its first renamed data in 1D" in {
    toU(data1)
      .relocateWithValue(ext, TestMatrixRename.renamerWithValue(_0))
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its first renamed data in 2D" in {
    toU(data2)
      .relocateWithValue(ext, TestMatrixRename.renamerWithValue(_0))
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its second renamed data in 2D" in {
    toU(data2)
      .relocateWithValue(ext, TestMatrixRename.renamerWithValue(_1))
      .toList.sortBy(_.position) shouldBe result9
  }

  it should "return its first renamed data in 3D" in {
    toU(data3)
      .relocateWithValue(ext, TestMatrixRename.renamerWithValue(_0))
      .toList.sortBy(_.position) shouldBe result10
  }

  it should "return its second renamed data in 3D" in {
    toU(data3)
      .relocateWithValue(ext, TestMatrixRename.renamerWithValue(_1))
      .toList.sortBy(_.position) shouldBe result11
  }

  it should "return its third renamed data in 3D" in {
    toU(data3)
      .relocateWithValue(ext, TestMatrixRename.renamerWithValue(_2))
      .toList.sortBy(_.position) shouldBe result12
  }
}
