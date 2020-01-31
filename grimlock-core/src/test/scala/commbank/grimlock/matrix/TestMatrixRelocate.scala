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
import commbank.grimlock.framework.metadata._
import commbank.grimlock.framework.position._

import com.twitter.scalding.typed.ValuePipe

import shapeless.HList

trait TestMatrixRelocate extends TestMatrix {
  val ext = "abc"

  val result1 = List(
    Cell(Position("bar", "abc"), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("baz", "abc"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("foo", "abc"), Content(OrdinalSchema[String](), "3.14")),
    Cell(Position("qux", "abc"), Content(OrdinalSchema[String](), "12.56"))
  )

  val result2 = List(
    Cell(Position("bar", "abc", "def"), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("baz", "abc", "def"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("foo", "abc", "def"), Content(OrdinalSchema[String](), "3.14")),
    Cell(Position("qux", "abc", "def"), Content(OrdinalSchema[String](), "12.56"))
  )

  val result3 = List(
    Cell(Position("bar", "abc", "def", "ghi"), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("baz", "abc", "def", "ghi"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("foo", "abc", "def", "ghi"), Content(OrdinalSchema[String](), "3.14")),
    Cell(Position("qux", "abc", "def", "ghi"), Content(OrdinalSchema[String](), "12.56"))
  )

  val result4 = List(
    Cell(Position("bar", "abc", "def", "ghi", "jkl"), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("baz", "abc", "def", "ghi", "jkl"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("foo", "abc", "def", "ghi", "jkl"), Content(OrdinalSchema[String](), "3.14")),
    Cell(Position("qux", "abc", "def", "ghi", "jkl"), Content(OrdinalSchema[String](), "12.56"))
  )

  val result5 = List(
    Cell(Position("bar", 1, "abc"), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("bar", 2, "abc"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", 3, "abc"), Content(OrdinalSchema[Long](), 19L)),
    Cell(Position("baz", 1, "abc"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", 2, "abc"), Content(DiscreteSchema[Long](), 19L)),
    Cell(Position("foo", 1, "abc"), Content(OrdinalSchema[String](), "3.14")),
    Cell(Position("foo", 2, "abc"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("foo", 3, "abc"), Content(NominalSchema[String](), "9.42")),
    Cell(
      Position("foo", 4, "abc"),
      Content(
        DateSchema[java.util.Date](),
        DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
      )
    ),
    Cell(Position("qux", 1, "abc"), Content(OrdinalSchema[String](), "12.56"))
  )

  val result6 = List(
    Cell(Position("bar", 1, "abc", "def"), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("bar", 2, "abc", "def"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", 3, "abc", "def"), Content(OrdinalSchema[Long](), 19L)),
    Cell(Position("baz", 1, "abc", "def"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", 2, "abc", "def"), Content(DiscreteSchema[Long](), 19L)),
    Cell(Position("foo", 1, "abc", "def"), Content(OrdinalSchema[String](), "3.14")),
    Cell(Position("foo", 2, "abc", "def"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("foo", 3, "abc", "def"), Content(NominalSchema[String](), "9.42")),
    Cell(
      Position("foo", 4, "abc", "def"),
      Content(
        DateSchema[java.util.Date](),
        DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
      )
    ),
    Cell(Position("qux", 1, "abc", "def"), Content(OrdinalSchema[String](), "12.56"))
  )

  val result7 = List(
    Cell(Position("bar", 1, "abc", "def", "ghi"), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("bar", 2, "abc", "def", "ghi"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", 3, "abc", "def", "ghi"), Content(OrdinalSchema[Long](), 19L)),
    Cell(Position("baz", 1, "abc", "def", "ghi"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", 2, "abc", "def", "ghi"), Content(DiscreteSchema[Long](), 19L)),
    Cell(Position("foo", 1, "abc", "def", "ghi"), Content(OrdinalSchema[String](), "3.14")),
    Cell(Position("foo", 2, "abc", "def", "ghi"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("foo", 3, "abc", "def", "ghi"), Content(NominalSchema[String](), "9.42")),
    Cell(
      Position("foo", 4, "abc", "def", "ghi"),
      Content(
        DateSchema[java.util.Date](),
        DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
      )
    ),
    Cell(Position("qux", 1, "abc", "def", "ghi"), Content(OrdinalSchema[String](), "12.56"))
  )

  val result8 = List(
    Cell(Position("bar", 1, "xyz", "abc"), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("bar", 2, "xyz", "abc"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", 3, "xyz", "abc"), Content(OrdinalSchema[Long](), 19L)),
    Cell(Position("baz", 1, "xyz", "abc"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", 2, "xyz", "abc"), Content(DiscreteSchema[Long](), 19L)),
    Cell(Position("foo", 1, "xyz", "abc"), Content(OrdinalSchema[String](), "3.14")),
    Cell(Position("foo", 2, "xyz", "abc"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("foo", 3, "xyz", "abc"), Content(NominalSchema[String](), "9.42")),
    Cell(
      Position("foo", 4, "xyz", "abc"),
      Content(
        DateSchema[java.util.Date](),
        DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
      )
    ),
    Cell(Position("qux", 1, "xyz", "abc"), Content(OrdinalSchema[String](), "12.56"))
  )

  val result9 = List(
    Cell(Position("bar", 1, "xyz", "abc", "def"), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("bar", 2, "xyz", "abc", "def"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", 3, "xyz", "abc", "def"), Content(OrdinalSchema[Long](), 19L)),
    Cell(Position("baz", 1, "xyz", "abc", "def"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", 2, "xyz", "abc", "def"), Content(DiscreteSchema[Long](), 19L)),
    Cell(Position("foo", 1, "xyz", "abc", "def"), Content(OrdinalSchema[String](), "3.14")),
    Cell(Position("foo", 2, "xyz", "abc", "def"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("foo", 3, "xyz", "abc", "def"), Content(NominalSchema[String](), "9.42")),
    Cell(
      Position("foo", 4, "xyz", "abc", "def"),
      Content(
        DateSchema[java.util.Date](),
        DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
      )
    ),
    Cell(Position("qux", 1, "xyz", "abc", "def"), Content(OrdinalSchema[String](), "12.56"))
  )

  val result10 = List(
    Cell(Position("bar", "abc"), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("baz", "abc"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("foo", "abc"), Content(OrdinalSchema[String](), "3.14")),
    Cell(Position("qux", "abc"), Content(OrdinalSchema[String](), "12.56"))
  )

  val result11 = List(
    Cell(Position("bar", "abc", "def"), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("baz", "abc", "def"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("foo", "abc", "def"), Content(OrdinalSchema[String](), "3.14")),
    Cell(Position("qux", "abc", "def"), Content(OrdinalSchema[String](), "12.56"))
  )

  val result12 = List(
    Cell(Position("bar", "abc", "def", "ghi"), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("baz", "abc", "def", "ghi"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("foo", "abc", "def", "ghi"), Content(OrdinalSchema[String](), "3.14")),
    Cell(Position("qux", "abc", "def", "ghi"), Content(OrdinalSchema[String](), "12.56"))
  )

  val result13 = List(
    Cell(Position("bar", "abc", "def", "ghi", "jkl"), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("baz", "abc", "def", "ghi", "jkl"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("foo", "abc", "def", "ghi", "jkl"), Content(OrdinalSchema[String](), "3.14")),
    Cell(Position("qux", "abc", "def", "ghi", "jkl"), Content(OrdinalSchema[String](), "12.56"))
  )

  val result14 = List(
    Cell(Position("bar", 1, "abc"), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("bar", 2, "abc"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", 3, "abc"), Content(OrdinalSchema[Long](), 19L)),
    Cell(Position("baz", 1, "abc"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", 2, "abc"), Content(DiscreteSchema[Long](), 19L)),
    Cell(Position("foo", 1, "abc"), Content(OrdinalSchema[String](), "3.14")),
    Cell(Position("foo", 2, "abc"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("foo", 3, "abc"), Content(NominalSchema[String](), "9.42")),
    Cell(
      Position("foo", 4, "abc"),
      Content(
        DateSchema[java.util.Date](),
        DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
      )
    ),
    Cell(Position("qux", 1, "abc"), Content(OrdinalSchema[String](), "12.56"))
  )

  val result15 = List(
    Cell(Position("bar", 1, "abc", "def"), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("bar", 2, "abc", "def"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", 3, "abc", "def"), Content(OrdinalSchema[Long](), 19L)),
    Cell(Position("baz", 1, "abc", "def"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", 2, "abc", "def"), Content(DiscreteSchema[Long](), 19L)),
    Cell(Position("foo", 1, "abc", "def"), Content(OrdinalSchema[String](), "3.14")),
    Cell(Position("foo", 2, "abc", "def"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("foo", 3, "abc", "def"), Content(NominalSchema[String](), "9.42")),
    Cell(
      Position("foo", 4, "abc", "def"),
      Content(
        DateSchema[java.util.Date](),
        DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
      )
    ),
    Cell(Position("qux", 1, "abc", "def"), Content(OrdinalSchema[String](), "12.56"))
  )

  val result16 = List(
    Cell(Position("bar", 1, "abc", "def", "ghi"), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("bar", 2, "abc", "def", "ghi"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", 3, "abc", "def", "ghi"), Content(OrdinalSchema[Long](), 19L)),
    Cell(Position("baz", 1, "abc", "def", "ghi"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", 2, "abc", "def", "ghi"), Content(DiscreteSchema[Long](), 19L)),
    Cell(Position("foo", 1, "abc", "def", "ghi"), Content(OrdinalSchema[String](), "3.14")),
    Cell(Position("foo", 2, "abc", "def", "ghi"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("foo", 3, "abc", "def", "ghi"), Content(NominalSchema[String](), "9.42")),
    Cell(
      Position("foo", 4, "abc", "def", "ghi"),
      Content(
        DateSchema[java.util.Date](),
        DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
      )
    ),
    Cell(Position("qux", 1, "abc", "def", "ghi"), Content(OrdinalSchema[String](), "12.56"))
  )

  val result17 = List(
    Cell(Position("bar", 1, "xyz", "abc"), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("bar", 2, "xyz", "abc"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", 3, "xyz", "abc"), Content(OrdinalSchema[Long](), 19L)),
    Cell(Position("baz", 1, "xyz", "abc"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", 2, "xyz", "abc"), Content(DiscreteSchema[Long](), 19L)),
    Cell(Position("foo", 1, "xyz", "abc"), Content(OrdinalSchema[String](), "3.14")),
    Cell(Position("foo", 2, "xyz", "abc"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("foo", 3, "xyz", "abc"), Content(NominalSchema[String](), "9.42")),
    Cell(
      Position("foo", 4, "xyz", "abc"),
      Content(
        DateSchema[java.util.Date](),
        DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
      )
    ),
    Cell(Position("qux", 1, "xyz", "abc"), Content(OrdinalSchema[String](), "12.56"))
  )

  val result18 = List(
    Cell(Position("bar", 1, "xyz", "abc", "def"), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("bar", 2, "xyz", "abc", "def"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", 3, "xyz", "abc", "def"), Content(OrdinalSchema[Long](), 19L)),
    Cell(Position("baz", 1, "xyz", "abc", "def"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", 2, "xyz", "abc", "def"), Content(DiscreteSchema[Long](), 19L)),
    Cell(Position("foo", 1, "xyz", "abc", "def"), Content(OrdinalSchema[String](), "3.14")),
    Cell(Position("foo", 2, "xyz", "abc", "def"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("foo", 3, "xyz", "abc", "def"), Content(NominalSchema[String](), "9.42")),
    Cell(
      Position("foo", 4, "xyz", "abc", "def"),
      Content(
        DateSchema[java.util.Date](),
        DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
      )
    ),
    Cell(Position("qux", 1, "xyz", "abc", "def"), Content(OrdinalSchema[String](), "12.56"))
  )
}

object TestMatrixRelocate {
  def expand1D[
    P <: HList
  ](implicit
    ev1: Position.AppendConstraints[P, Value[String]]
  ) = (cell: Cell[P]) => cell.position.append("abc").toOption

  def expand2D[
    P <: HList,
    Q1 <: HList
  ](implicit
    ev1: Position.AppendConstraints.Aux[P, Value[String], Q1],
    ev2: Position.AppendConstraints[Q1, Value[String]]
  ) = (cell: Cell[P]) => cell.position.append("abc").append("def") .toOption

  def expand3D[
    P <: HList,
    Q1 <: HList,
    Q2 <: HList
  ](implicit
    ev1: Position.AppendConstraints.Aux[P, Value[String], Q1],
    ev2: Position.AppendConstraints.Aux[Q1, Value[String], Q2],
    ev3: Position.AppendConstraints[Q2, Value[String]]
  ) = (cell: Cell[P]) => cell.position.append("abc").append("def").append("ghi") .toOption

  def expand4D[
    P <: HList,
    Q1 <: HList,
    Q2 <: HList,
    Q3 <: HList
  ](implicit
    ev1: Position.AppendConstraints.Aux[P, Value[String], Q1],
    ev2: Position.AppendConstraints.Aux[Q1, Value[String], Q2],
    ev3: Position.AppendConstraints.Aux[Q2, Value[String], Q3],
    ev4: Position.AppendConstraints[Q3, Value[String]]
  ) = (cell: Cell[P]) => cell.position.append("abc").append("def").append("ghi").append("jkl").toOption

  def expand1DWithValue[
    P <: HList
  ](implicit
    ev1: Position.AppendConstraints[P, Value[String]]
  ) = (cell: Cell[P], ext: String) => cell.position.append(ext).toOption

  def expand2DWithValue[
    P <: HList,
    Q1 <: HList
  ](implicit
    ev1: Position.AppendConstraints.Aux[P, Value[String], Q1],
    ev2: Position.AppendConstraints[Q1, Value[String]]
  ) = (cell: Cell[P], ext: String) => cell.position.append(ext).append("def").toOption

  def expand3DWithValue[
    P <: HList,
    Q1 <: HList,
    Q2 <: HList
  ](implicit
    ev1: Position.AppendConstraints.Aux[P, Value[String], Q1],
    ev2: Position.AppendConstraints.Aux[Q1, Value[String], Q2],
    ev3: Position.AppendConstraints[Q2, Value[String]]
  ) = (cell: Cell[P], ext: String) => cell.position.append(ext).append("def").append("ghi") .toOption

  def expand4DWithValue[
    P <: HList,
    Q1 <: HList,
    Q2 <: HList,
    Q3 <: HList
  ](implicit
    ev1: Position.AppendConstraints.Aux[P, Value[String], Q1],
    ev2: Position.AppendConstraints.Aux[Q1, Value[String], Q2],
    ev3: Position.AppendConstraints.Aux[Q2, Value[String], Q3],
    ev4: Position.AppendConstraints[Q3, Value[String]]
  ) = (cell: Cell[P], ext: String) => cell.position.append(ext).append("def").append("ghi").append("jkl").toOption
}

class TestScalaMatrixRelocate extends TestMatrixRelocate with TestScala {
  import commbank.grimlock.scala.environment.implicits._

  "A Matrix.expand" should "return its 1D expanded data in 1D" in {
    toU(data1)
      .relocate(TestMatrixRelocate.expand1D)
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its 2D expanded data in 1D" in {
    toU(data1)
      .relocate(TestMatrixRelocate.expand2D)
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its 3D expanded data in 1D" in {
    toU(data1)
      .relocate(TestMatrixRelocate.expand3D)
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its 4D expanded data in 1D" in {
    toU(data1)
      .relocate(TestMatrixRelocate.expand4D)
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its 1D expanded data in 2D" in {
    toU(data2)
      .relocate(TestMatrixRelocate.expand1D)
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its 2D expanded data in 2D" in {
    toU(data2)
      .relocate(TestMatrixRelocate.expand2D)
      .toList.sortBy(_.position) shouldBe result6
  }

  it should "return its 3D expanded data in 2D" in {
    toU(data2)
      .relocate(TestMatrixRelocate.expand3D)
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its 1D expanded data in 3D" in {
    toU(data3)
      .relocate(TestMatrixRelocate.expand1D)
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its 2D expanded data in 3D" in {
    toU(data3)
      .relocate(TestMatrixRelocate.expand2D)
      .toList.sortBy(_.position) shouldBe result9
  }

  "A Matrix.expandWithValue" should "return its 1D expanded data in 1D" in {
    toU(data1)
      .relocateWithValue(ext, TestMatrixRelocate.expand1DWithValue)
      .toList.sortBy(_.position) shouldBe result10
  }

  it should "return its 2D expanded data in 1D" in {
    toU(data1)
      .relocateWithValue(ext, TestMatrixRelocate.expand2DWithValue)
      .toList.sortBy(_.position) shouldBe result11
  }

  it should "return its 3D expanded data in 1D" in {
    toU(data1)
      .relocateWithValue(ext, TestMatrixRelocate.expand3DWithValue)
      .toList.sortBy(_.position) shouldBe result12
  }

  it should "return its 4D expanded data in 1D" in {
    toU(data1)
      .relocateWithValue(ext, TestMatrixRelocate.expand4DWithValue)
      .toList.sortBy(_.position) shouldBe result13
  }

  it should "return its 1D expanded data in 2D" in {
    toU(data2)
      .relocateWithValue(ext, TestMatrixRelocate.expand1DWithValue)
      .toList.sortBy(_.position) shouldBe result14
  }

  it should "return its 2D expanded data in 2D" in {
    toU(data2)
      .relocateWithValue(ext, TestMatrixRelocate.expand2DWithValue)
      .toList.sortBy(_.position) shouldBe result15
  }

  it should "return its 3D expanded data in 2D" in {
    toU(data2)
      .relocateWithValue(ext, TestMatrixRelocate.expand3DWithValue)
      .toList.sortBy(_.position) shouldBe result16
  }

  it should "return its 1D expanded data in 3D" in {
    toU(data3)
      .relocateWithValue(ext, TestMatrixRelocate.expand1DWithValue)
      .toList.sortBy(_.position) shouldBe result17
  }

  it should "return its 2D expanded data in 3D" in {
    toU(data3)
      .relocateWithValue(ext, TestMatrixRelocate.expand2DWithValue)
      .toList.sortBy(_.position) shouldBe result18
  }
}

class TestScaldingMatrixRelocate extends TestMatrixRelocate with TestScalding {
  import commbank.grimlock.scalding.environment.implicits._

  "A Matrix.expand" should "return its 1D expanded data in 1D" in {
    toU(data1)
      .relocate(TestMatrixRelocate.expand1D)
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its 2D expanded data in 1D" in {
    toU(data1)
      .relocate(TestMatrixRelocate.expand2D)
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its 3D expanded data in 1D" in {
    toU(data1)
      .relocate(TestMatrixRelocate.expand3D)
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its 4D expanded data in 1D" in {
    toU(data1)
      .relocate(TestMatrixRelocate.expand4D)
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its 1D expanded data in 2D" in {
    toU(data2)
      .relocate(TestMatrixRelocate.expand1D)
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its 2D expanded data in 2D" in {
    toU(data2)
      .relocate(TestMatrixRelocate.expand2D)
      .toList.sortBy(_.position) shouldBe result6
  }

  it should "return its 3D expanded data in 2D" in {
    toU(data2)
      .relocate(TestMatrixRelocate.expand3D)
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its 1D expanded data in 3D" in {
    toU(data3)
      .relocate(TestMatrixRelocate.expand1D)
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its 2D expanded data in 3D" in {
    toU(data3)
      .relocate(TestMatrixRelocate.expand2D)
      .toList.sortBy(_.position) shouldBe result9
  }

  "A Matrix.expandWithValue" should "return its 1D expanded data in 1D" in {
    toU(data1)
      .relocateWithValue(ValuePipe(ext), TestMatrixRelocate.expand1DWithValue)
      .toList.sortBy(_.position) shouldBe result10
  }

  it should "return its 2D expanded data in 1D" in {
    toU(data1)
      .relocateWithValue(ValuePipe(ext), TestMatrixRelocate.expand2DWithValue)
      .toList.sortBy(_.position) shouldBe result11
  }

  it should "return its 3D expanded data in 1D" in {
    toU(data1)
      .relocateWithValue(ValuePipe(ext), TestMatrixRelocate.expand3DWithValue)
      .toList.sortBy(_.position) shouldBe result12
  }

  it should "return its 4D expanded data in 1D" in {
    toU(data1)
      .relocateWithValue(ValuePipe(ext), TestMatrixRelocate.expand4DWithValue)
      .toList.sortBy(_.position) shouldBe result13
  }

  it should "return its 1D expanded data in 2D" in {
    toU(data2)
      .relocateWithValue(ValuePipe(ext), TestMatrixRelocate.expand1DWithValue)
      .toList.sortBy(_.position) shouldBe result14
  }

  it should "return its 2D expanded data in 2D" in {
    toU(data2)
      .relocateWithValue(ValuePipe(ext), TestMatrixRelocate.expand2DWithValue)
      .toList.sortBy(_.position) shouldBe result15
  }

  it should "return its 3D expanded data in 2D" in {
    toU(data2)
      .relocateWithValue(ValuePipe(ext), TestMatrixRelocate.expand3DWithValue)
      .toList.sortBy(_.position) shouldBe result16
  }

  it should "return its 1D expanded data in 3D" in {
    toU(data3)
      .relocateWithValue(ValuePipe(ext), TestMatrixRelocate.expand1DWithValue)
      .toList.sortBy(_.position) shouldBe result17
  }

  it should "return its 2D expanded data in 3D" in {
    toU(data3)
      .relocateWithValue(ValuePipe(ext), TestMatrixRelocate.expand2DWithValue)
      .toList.sortBy(_.position) shouldBe result18
  }
}

class TestSparkMatrixRelocate extends TestMatrixRelocate with TestSpark {
  import commbank.grimlock.spark.environment.implicits._

  "A Matrix.expand" should "return its 1D expanded data in 1D" in {
    toU(data1)
      .relocate(TestMatrixRelocate.expand1D)
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its 2D expanded data in 1D" in {
    toU(data1)
      .relocate(TestMatrixRelocate.expand2D)
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its 3D expanded data in 1D" in {
    toU(data1)
      .relocate(TestMatrixRelocate.expand3D)
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its 4D expanded data in 1D" in {
    toU(data1)
      .relocate(TestMatrixRelocate.expand4D)
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its expanded 1D data in 2D" in {
    toU(data2)
      .relocate(TestMatrixRelocate.expand1D)
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its expanded 2D data in 2D" in {
    toU(data2)
      .relocate(TestMatrixRelocate.expand2D)
      .toList.sortBy(_.position) shouldBe result6
  }

  it should "return its expanded 3D data in 2D" in {
    toU(data2)
      .relocate(TestMatrixRelocate.expand3D)
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its expanded 1D data in 3D" in {
    toU(data3)
      .relocate(TestMatrixRelocate.expand1D)
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its expanded 2D data in 3D" in {
    toU(data3)
      .relocate(TestMatrixRelocate.expand2D)
      .toList.sortBy(_.position) shouldBe result9
  }

  "A Matrix.expandWithValue" should "return its 1D expanded data in 1D" in {
    toU(data1)
      .relocateWithValue(ext, TestMatrixRelocate.expand1DWithValue)
      .toList.sortBy(_.position) shouldBe result10
  }

  it should "return its 2D expanded data in 1D" in {
    toU(data1)
      .relocateWithValue(ext, TestMatrixRelocate.expand2DWithValue)
      .toList.sortBy(_.position) shouldBe result11
  }

  it should "return its 3D expanded data in 1D" in {
    toU(data1)
      .relocateWithValue(ext, TestMatrixRelocate.expand3DWithValue)
      .toList.sortBy(_.position) shouldBe result12
  }

  it should "return its 4D expanded data in 1D" in {
    toU(data1)
      .relocateWithValue(ext, TestMatrixRelocate.expand4DWithValue)
      .toList.sortBy(_.position) shouldBe result13
  }

  it should "return its 1D expanded data in 2D" in {
    toU(data2)
      .relocateWithValue(ext, TestMatrixRelocate.expand1DWithValue)
      .toList.sortBy(_.position) shouldBe result14
  }

  it should "return its 2D expanded data in 2D" in {
    toU(data2)
      .relocateWithValue(ext, TestMatrixRelocate.expand2DWithValue)
      .toList.sortBy(_.position) shouldBe result15
  }

  it should "return its 3D expanded data in 2D" in {
    toU(data2)
      .relocateWithValue(ext, TestMatrixRelocate.expand3DWithValue)
      .toList.sortBy(_.position) shouldBe result16
  }

  it should "return its 1D expanded data in 3D" in {
    toU(data3)
      .relocateWithValue(ext, TestMatrixRelocate.expand1DWithValue)
      .toList.sortBy(_.position) shouldBe result17
  }

  it should "return its 2D expanded data in 3D" in {
    toU(data3)
      .relocateWithValue(ext, TestMatrixRelocate.expand2DWithValue)
      .toList.sortBy(_.position) shouldBe result18
  }
}

