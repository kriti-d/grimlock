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

import shapeless.nat.{ _0, _1, _2 }

trait TestMatrixGather extends TestMatrix {
  val result1 = data1.map { case c => c.position -> c.content }.toMap

  val result2 = Map(
    Position("foo") -> Map(
      Position(1) -> Content(OrdinalSchema[String](), "3.14"),
      Position(2) -> Content(ContinuousSchema[Double](), 6.28),
      Position(3) -> Content(NominalSchema[String](), "9.42"),
      Position(4) -> Content(
        DateSchema[java.util.Date](),
        DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
      )
    ),
    Position("bar") -> Map(
      Position(1) -> Content(OrdinalSchema[String](), "6.28"),
      Position(2) -> Content(ContinuousSchema[Double](), 12.56),
      Position(3) -> Content(OrdinalSchema[Long](), 19L)
    ),
    Position("baz") -> Map(
      Position(1) -> Content(OrdinalSchema[String](), "9.42"),
      Position(2) -> Content(DiscreteSchema[Long](), 19L)
    ),
    Position("qux") -> Map(Position(1) -> Content(OrdinalSchema[String](), "12.56"))
  )

  val result3 = Map(
    Position(1) -> Map(
      Position("foo") -> Content(OrdinalSchema[String](), "3.14"),
      Position("bar") -> Content(OrdinalSchema[String](), "6.28"),
      Position("baz") -> Content(OrdinalSchema[String](), "9.42"),
      Position("qux") -> Content(OrdinalSchema[String](), "12.56")
    ),
    Position(2) -> Map(
      Position("foo") -> Content(ContinuousSchema[Double](), 6.28),
      Position("bar") -> Content(ContinuousSchema[Double](), 12.56),
      Position("baz") -> Content(DiscreteSchema[Long](), 19L)
    ),
    Position(3) -> Map(
      Position("foo") -> Content(NominalSchema[String](), "9.42"),
      Position("bar") -> Content(OrdinalSchema[Long](), 19L)
    ),
    Position(4) -> Map(
      Position("foo") -> Content(
        DateSchema[java.util.Date](),
        DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
      )
    )
  )

  val result4 = Map(
    Position(1) -> Map(
      Position("foo") -> Content(OrdinalSchema[String](), "3.14"),
      Position("bar") -> Content(OrdinalSchema[String](), "6.28"),
      Position("baz") -> Content(OrdinalSchema[String](), "9.42"),
      Position("qux") -> Content(OrdinalSchema[String](), "12.56")
    ),
    Position(2) -> Map(
      Position("foo") -> Content(ContinuousSchema[Double](), 6.28),
      Position("bar") -> Content(ContinuousSchema[Double](), 12.56),
      Position("baz") -> Content(DiscreteSchema[Long](), 19L)
    ),
    Position(3) -> Map(
      Position("foo") -> Content(NominalSchema[String](), "9.42"),
      Position("bar") -> Content(OrdinalSchema[Long](), 19L)
    ),
    Position(4) -> Map(
      Position("foo") -> Content(
        DateSchema[java.util.Date](),
        DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
      )
    )
  )

  val result5 = Map(
    Position("foo") -> Map(
      Position(1) -> Content(OrdinalSchema[String](), "3.14"),
      Position(2) -> Content(ContinuousSchema[Double](), 6.28),
      Position(3) -> Content(NominalSchema[String](), "9.42"),
      Position(4) -> Content(
        DateSchema[java.util.Date](),
        DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
      )
    ),
    Position("bar") -> Map(
      Position(1) -> Content(OrdinalSchema[String](), "6.28"),
      Position(2) -> Content(ContinuousSchema[Double](), 12.56),
      Position(3) -> Content(OrdinalSchema[Long](), 19L)
    ),
    Position("baz") -> Map(
      Position(1) -> Content(OrdinalSchema[String](), "9.42"),
      Position(2) -> Content(DiscreteSchema[Long](), 19L)
    ),
    Position("qux") -> Map(Position(1) -> Content(OrdinalSchema[String](), "12.56"))
  )

  val result6 = Map(
    Position("foo") -> Map(
      Position(1, "xyz") -> Content(OrdinalSchema[String](), "3.14"),
      Position(2, "xyz") -> Content(ContinuousSchema[Double](), 6.28),
      Position(3, "xyz") -> Content(NominalSchema[String](), "9.42"),
      Position(4, "xyz") -> Content(
        DateSchema[java.util.Date](),
        DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
      )
    ),
    Position("bar") -> Map(
      Position(1, "xyz") -> Content(OrdinalSchema[String](), "6.28"),
      Position(2, "xyz") -> Content(ContinuousSchema[Double](), 12.56),
      Position(3, "xyz") -> Content(OrdinalSchema[Long](), 19L)
    ),
    Position("baz") -> Map(
      Position(1, "xyz") -> Content(OrdinalSchema[String](), "9.42"),
      Position(2, "xyz") -> Content(DiscreteSchema[Long](), 19L)
    ),
    Position("qux") -> Map(Position(1, "xyz") -> Content(OrdinalSchema[String](), "12.56"))
  )

  val result7 = Map(
    Position(1, "xyz") -> Map(
      Position("foo") -> Content(OrdinalSchema[String](), "3.14"),
      Position("bar") -> Content(OrdinalSchema[String](), "6.28"),
      Position("baz") -> Content(OrdinalSchema[String](), "9.42"),
      Position("qux") -> Content(OrdinalSchema[String](), "12.56")
    ),
    Position(2, "xyz") -> Map(
      Position("foo") -> Content(ContinuousSchema[Double](), 6.28),
      Position("bar") -> Content(ContinuousSchema[Double](), 12.56),
      Position("baz") -> Content(DiscreteSchema[Long](), 19L)
    ),
    Position(3, "xyz") -> Map(
      Position("foo") -> Content(NominalSchema[String](), "9.42"),
      Position("bar") -> Content(OrdinalSchema[Long](), 19L)
    ),
    Position(4, "xyz") -> Map(
      Position("foo") -> Content(
        DateSchema[java.util.Date](),
        DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
      )
    )
  )

  val result8 = Map(
    Position(1) -> Map(
      Position("foo", "xyz") -> Content(OrdinalSchema[String](), "3.14"),
      Position("bar", "xyz") -> Content(OrdinalSchema[String](), "6.28"),
      Position("baz", "xyz") -> Content(OrdinalSchema[String](), "9.42"),
      Position("qux", "xyz") -> Content(OrdinalSchema[String](), "12.56")
    ),
    Position(2) -> Map(
      Position("foo", "xyz") -> Content(ContinuousSchema[Double](), 6.28),
      Position("bar", "xyz") -> Content(ContinuousSchema[Double](), 12.56),
      Position("baz", "xyz") -> Content(DiscreteSchema[Long](), 19L)
    ),
    Position(3) -> Map(
      Position("foo", "xyz") -> Content(NominalSchema[String](), "9.42"),
      Position("bar", "xyz") -> Content(OrdinalSchema[Long](), 19L)
    ),
    Position(4) -> Map(
      Position("foo", "xyz") -> Content(
        DateSchema[java.util.Date](),
        DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
      )
    )
  )

  val result9 = Map(
    Position("foo", "xyz") -> Map(
      Position(1) -> Content(OrdinalSchema[String](), "3.14"),
      Position(2) -> Content(ContinuousSchema[Double](), 6.28),
      Position(3) -> Content(NominalSchema[String](), "9.42"),
      Position(4) -> Content(
        DateSchema[java.util.Date](),
        DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
      )
    ),
    Position("bar", "xyz") -> Map(
      Position(1) -> Content(OrdinalSchema[String](), "6.28"),
      Position(2) -> Content(ContinuousSchema[Double](), 12.56),
      Position(3) -> Content(OrdinalSchema[Long](), 19L)
    ),
    Position("baz", "xyz") -> Map(
      Position(1) -> Content(OrdinalSchema[String](), "9.42"),
      Position(2) -> Content(DiscreteSchema[Long](), 19L)
    ),
    Position("qux", "xyz") -> Map(Position(1) -> Content(OrdinalSchema[String](), "12.56"))
  )

  val result10 = Map(
    Position("xyz") -> Map(
      Position("foo", 1) -> Content(OrdinalSchema[String](), "3.14"),
      Position("bar", 1) -> Content(OrdinalSchema[String](), "6.28"),
      Position("baz", 1) -> Content(OrdinalSchema[String](), "9.42"),
      Position("qux", 1) -> Content(OrdinalSchema[String](), "12.56"),
      Position("foo", 2) -> Content(ContinuousSchema[Double](), 6.28),
      Position("bar", 2) -> Content(ContinuousSchema[Double](), 12.56),
      Position("baz", 2) -> Content(DiscreteSchema[Long](), 19L),
      Position("foo", 3) -> Content(NominalSchema[String](), "9.42"),
      Position("bar", 3) -> Content(OrdinalSchema[Long](), 19L),
      Position("foo", 4) -> Content(
        DateSchema[java.util.Date](),
        DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
      )
    )
  )

  val result11 = Map(
    Position("foo", 1) -> Map(Position("xyz") -> Content(OrdinalSchema[String](), "3.14")),
    Position("foo", 2) -> Map(Position("xyz") -> Content(ContinuousSchema[Double](), 6.28)),
    Position("foo", 3) -> Map(Position("xyz") -> Content(NominalSchema[String](), "9.42")),
    Position("foo", 4) -> Map(
      Position("xyz") -> Content(
        DateSchema[java.util.Date](),
        DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
      )
    ),
    Position("bar", 1) -> Map(Position("xyz") -> Content(OrdinalSchema[String](), "6.28")),
    Position("bar", 2) -> Map(Position("xyz") -> Content(ContinuousSchema[Double](), 12.56)),
    Position("bar", 3) -> Map(Position("xyz") -> Content(OrdinalSchema[Long](), 19L)),
    Position("baz", 1) -> Map(Position("xyz") -> Content(OrdinalSchema[String](), "9.42")),
    Position("baz", 2) -> Map(Position("xyz") -> Content(DiscreteSchema[Long](), 19L)),
    Position("qux", 1) -> Map(Position("xyz") -> Content(OrdinalSchema[String](), "12.56"))
  )
}

class TestScalaMatrixGather extends TestMatrixGather with TestScala {
  import commbank.grimlock.scala.environment.implicits._

  "A Matrix.gather" should "return its first over map in 1D" in {
    toU(data1)
      .gatherByPosition(Over(_0), Default()) shouldBe result1
  }

  it should "return its first over map in 2D" in {
    toU(data2)
      .gatherByPosition(Over(_0), Default()) shouldBe result2
  }

  it should "return its first along map in 2D" in {
    toU(data2)
      .gatherByPosition(Along(_0), Default()) shouldBe result3
  }

  it should "return its second over map in 2D" in {
    toU(data2)
      .gatherByPosition(Over(_1), Default()) shouldBe result4
  }

  it should "return its second along map in 2D" in {
    toU(data2)
      .gatherByPosition(Along(_1), Default()) shouldBe result5
  }

  it should "return its first over map in 3D" in {
    toU(data3)
      .gatherByPosition(Over(_0), Default()) shouldBe result6
  }

  it should "return its first along map in 3D" in {
    toU(data3)
      .gatherByPosition(Along(_0), Default()) shouldBe result7
  }

  it should "return its second over map in 3D" in {
    toU(data3)
      .gatherByPosition(Over(_1), Default()) shouldBe result8
  }

  it should "return its second along map in 3D" in {
    toU(data3)
      .gatherByPosition(Along(_1), Default()) shouldBe result9
  }

  it should "return its third over map in 3D" in {
    toU(data3)
      .gatherByPosition(Over(_2), Default()) shouldBe result10
  }

  it should "return its third along map in 3D" in {
    toU(data3)
      .gatherByPosition(Along(_2), Default()) shouldBe result11
  }

  it should "return its empty map" in {
    toU(List[Cell[P3]]())
      .gatherByPosition(Along(_2), Default()) shouldBe Map()
  }

  it should "return its compacted 1D" in {
    toU(data1)
      .gather() shouldBe result1
  }

  it should "return its empty compacted" in {
    toU(List[Cell[P2]]())
      .gather() shouldBe Map()
  }
}

class TestScaldingMatrixGather extends TestMatrixGather with TestScalding {
  import commbank.grimlock.scalding.environment.implicits._

  "A Matrix.gather" should "return its first over map in 1D" in {
    toU(data1)
      .gatherByPosition(Over(_0), Default()).toTypedPipe
      .toList shouldBe List(result1)
  }

  it should "return its first over map in 2D" in {
    toU(data2)
      .gatherByPosition(Over(_0), Default(12)).toTypedPipe
      .toList shouldBe List(result2)
  }

  it should "return its first along map in 2D" in {
    toU(data2)
      .gatherByPosition(Along(_0), Default()).toTypedPipe
      .toList shouldBe List(result3)
  }

  it should "return its second over map in 2D" in {
    toU(data2)
      .gatherByPosition(Over(_1), Default(12)).toTypedPipe
      .toList shouldBe List(result4)
  }

  it should "return its second along map in 2D" in {
    toU(data2)
      .gatherByPosition(Along(_1), Default()).toTypedPipe
      .toList shouldBe List(result5)
  }

  it should "return its first over map in 3D" in {
    toU(data3)
      .gatherByPosition(Over(_0), Default(12)).toTypedPipe
      .toList shouldBe List(result6)
  }

  it should "return its first along map in 3D" in {
    toU(data3)
      .gatherByPosition(Along(_0), Default()).toTypedPipe
      .toList shouldBe List(result7)
  }

  it should "return its second over map in 3D" in {
    toU(data3)
      .gatherByPosition(Over(_1), Default(12)).toTypedPipe
      .toList shouldBe List(result8)
  }

  it should "return its second along map in 3D" in {
    toU(data3)
      .gatherByPosition(Along(_1), Default()).toTypedPipe
      .toList shouldBe List(result9)
  }

  it should "return its third over map in 3D" in {
    toU(data3)
      .gatherByPosition(Over(_2), Default(12)).toTypedPipe
      .toList shouldBe List(result10)
  }

  it should "return its third along map in 3D" in {
    toU(data3)
      .gatherByPosition(Along(_2), Default()).toTypedPipe
      .toList shouldBe List(result11)
  }

  it should "return its empty map" in {
    toU(List[Cell[P3]]())
      .gatherByPosition(Along(_2), Default()).toTypedPipe
      .toList shouldBe List(Map())
  }

  it should "return its compacted 1D" in {
    toU(data1)
      .gather().toTypedPipe
      .toList shouldBe List(result1)
  }

  it should "return its empty compacted" in {
    toU(List[Cell[P2]]())
      .gather().toTypedPipe
      .toList shouldBe List(Map())
  }
}

class TestSparkMatrixGather extends TestMatrixGather with TestSpark {
  import commbank.grimlock.spark.environment.implicits._

  "A Matrix.gather" should "return its first over map in 1D" in {
    toU(data1)
      .gatherByPosition(Over(_0), Default()) shouldBe result1
  }

  it should "return its first over map in 2D" in {
    toU(data2)
      .gatherByPosition(Over(_0), Default(12)) shouldBe result2
  }

  it should "return its first along map in 2D" in {
    toU(data2)
      .gatherByPosition(Along(_0), Default()) shouldBe result3
  }

  it should "return its second over map in 2D" in {
    toU(data2)
      .gatherByPosition(Over(_1), Default(12)) shouldBe result4
  }

  it should "return its second along map in 2D" in {
    toU(data2)
      .gatherByPosition(Along(_1), Default()) shouldBe result5
  }

  it should "return its first over map in 3D" in {
    toU(data3)
      .gatherByPosition(Over(_0), Default(12)) shouldBe result6
  }

  it should "return its first along map in 3D" in {
    toU(data3)
      .gatherByPosition(Along(_0), Default()) shouldBe result7
  }

  it should "return its second over map in 3D" in {
    toU(data3)
      .gatherByPosition(Over(_1), Default(12)) shouldBe result8
  }

  it should "return its second along map in 3D" in {
    toU(data3)
      .gatherByPosition(Along(_1), Default()) shouldBe result9
  }

  it should "return its third over map in 3D" in {
    toU(data3)
      .gatherByPosition(Over(_2), Default(12)) shouldBe result10
  }

  it should "return its third along map in 3D" in {
    toU(data3)
      .gatherByPosition(Along(_2), Default()) shouldBe result11
  }

  it should "return its empty map" in {
    toU(List[Cell[P3]]())
      .gatherByPosition(Along(_2), Default()) shouldBe Map()
  }

  it should "return its compacted 1D" in {
    toU(data1)
      .gather() shouldBe result1
  }

  it should "return its empty compacted" in {
    toU(List[Cell[P2]]())
      .gather() shouldBe Map()
  }
}

