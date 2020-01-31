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

trait TestMatrixSet extends TestMatrix {
  val dataA = List(
    Cell(Position("foo"), Content(ContinuousSchema[Double](), 1.0)),
    Cell(Position("quxx"), Content(ContinuousSchema[Double](), 2.0))
  )

  val dataB = List(
    Cell(Position("foo", 2), Content(ContinuousSchema[Double](), 1.0)),
    Cell(Position("quxx", 5), Content(ContinuousSchema[Double](), 2.0))
  )

  val dataC = List(
    Cell(Position("foo", 2, "xyz"), Content(ContinuousSchema[Double](), 1.0)),
    Cell(Position("quxx", 5, "abc"), Content(ContinuousSchema[Double](), 2.0))
  )

  val result1 = List(
    Cell(Position("bar"), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("baz"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("foo"), Content(ContinuousSchema[Double](), 1.0)),
    Cell(Position("qux"), Content(OrdinalSchema[String](), "12.56"))
  )

  val result2 = List(
    Cell(Position("bar"), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("baz"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("foo"), Content(ContinuousSchema[Double](), 1.0)),
    Cell(Position("qux"), Content(OrdinalSchema[String](), "12.56")),
    Cell(Position("quxx"), Content(ContinuousSchema[Double](), 1.0))
  )

  val result3 = List(
    Cell(Position("bar"), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("baz"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("foo"), Content(ContinuousSchema[Double](), 1.0)),
    Cell(Position("qux"), Content(OrdinalSchema[String](), "12.56")),
    Cell(Position("quxx"), Content(ContinuousSchema[Double](), 2.0))
  )

  val result4 = List(
    Cell(Position("bar", 1), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("bar", 2), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", 3), Content(OrdinalSchema[Long](), 19L)),
    Cell(Position("baz", 1), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", 2), Content(DiscreteSchema[Long](), 19L)),
    Cell(Position("foo", 1), Content(OrdinalSchema[String](), "3.14")),
    Cell(Position("foo", 2), Content(ContinuousSchema[Double](), 1.0)),
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

  val result5 = List(
    Cell(Position("bar", 1), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("bar", 2), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", 3), Content(OrdinalSchema[Long](), 19L)),
    Cell(Position("baz", 1), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", 2), Content(DiscreteSchema[Long](), 19L)),
    Cell(Position("foo", 1), Content(OrdinalSchema[String](), "3.14")),
    Cell(Position("foo", 2), Content(ContinuousSchema[Double](), 1.0)),
    Cell(Position("foo", 3), Content(NominalSchema[String](), "9.42")),
    Cell(
      Position("foo", 4),
      Content(
        DateSchema[java.util.Date](),
        DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
      )
    ),
    Cell(Position("qux", 1), Content(OrdinalSchema[String](), "12.56")),
    Cell(Position("quxx", 5), Content(ContinuousSchema[Double](), 1.0))
  )

  val result6 = List(
    Cell(Position("bar", 1), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("bar", 2), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", 3), Content(OrdinalSchema[Long](), 19L)),
    Cell(Position("baz", 1), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", 2), Content(DiscreteSchema[Long](), 19L)),
    Cell(Position("foo", 1), Content(OrdinalSchema[String](), "3.14")),
    Cell(Position("foo", 2), Content(ContinuousSchema[Double](), 1.0)),
    Cell(Position("foo", 3), Content(NominalSchema[String](), "9.42")),
    Cell(
      Position("foo", 4),
      Content(
        DateSchema[java.util.Date](),
        DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
      )
    ),
    Cell(Position("qux", 1), Content(OrdinalSchema[String](), "12.56")),
    Cell(Position("quxx", 5), Content(ContinuousSchema[Double](), 2.0))
  )

  val result7 = List(
    Cell(Position("bar", 1, "xyz"), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("bar", 2, "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", 3, "xyz"), Content(OrdinalSchema[Long](), 19L)),
    Cell(Position("baz", 1, "xyz"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", 2, "xyz"), Content(DiscreteSchema[Long](), 19L)),
    Cell(Position("foo", 1, "xyz"), Content(OrdinalSchema[String](), "3.14")),
    Cell(Position("foo", 2, "xyz"), Content(ContinuousSchema[Double](), 1.0)),
    Cell(Position("foo", 3, "xyz"), Content(NominalSchema[String](), "9.42")),
    Cell(
      Position("foo", 4, "xyz"),
      Content(
        DateSchema[java.util.Date](),
        DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
      )
    ),
    Cell(Position("qux", 1, "xyz"), Content(OrdinalSchema[String](), "12.56"))
  )

  val result8 = List(
    Cell(Position("bar", 1, "xyz"), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("bar", 2, "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", 3, "xyz"), Content(OrdinalSchema[Long](), 19L)),
    Cell(Position("baz", 1, "xyz"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", 2, "xyz"), Content(DiscreteSchema[Long](), 19L)),
    Cell(Position("foo", 1, "xyz"), Content(OrdinalSchema[String](), "3.14")),
    Cell(Position("foo", 2, "xyz"), Content(ContinuousSchema[Double](), 1.0)),
    Cell(Position("foo", 3, "xyz"), Content(NominalSchema[String](), "9.42")),
    Cell(
      Position("foo", 4, "xyz"),
      Content(
        DateSchema[java.util.Date](),
        DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
      )
    ),
    Cell(Position("qux", 1, "xyz"), Content(OrdinalSchema[String](), "12.56")),
    Cell(Position("quxx", 5, "abc"), Content(ContinuousSchema[Double](), 1.0))
  )

  val result9 = List(
    Cell(Position("bar", 1, "xyz"), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("bar", 2, "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", 3, "xyz"), Content(OrdinalSchema[Long](), 19L)),
    Cell(Position("baz", 1, "xyz"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", 2, "xyz"), Content(DiscreteSchema[Long](), 19L)),
    Cell(Position("foo", 1, "xyz"), Content(OrdinalSchema[String](), "3.14")),
    Cell(Position("foo", 2, "xyz"), Content(ContinuousSchema[Double](), 1.0)),
    Cell(Position("foo", 3, "xyz"), Content(NominalSchema[String](), "9.42")),
    Cell(
      Position("foo", 4, "xyz"),
      Content(
        DateSchema[java.util.Date](),
        DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
      )
    ),
    Cell(Position("qux", 1, "xyz"), Content(OrdinalSchema[String](), "12.56")),
    Cell(Position("quxx", 5, "abc"), Content(ContinuousSchema[Double](), 2.0))
  )
}

class TestScalaMatrixSet extends TestMatrixSet with TestScala {
  import commbank.grimlock.scala.environment.implicits._

  "A Matrix.set" should "return its updated data in 1D" in {
    toU(data1)
      .set(Cell(Position("foo"), Content(ContinuousSchema[Double](), 1.0)), Default())
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its updated and added data in 1D" in {
    toU(data1)
      .set(List("foo", "quxx").map(pos => Cell(Position(pos), Content(ContinuousSchema[Double](), 1.0))), Default())
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its matrix updated data in 1D" in {
    toU(data1)
      .set(toU(dataA), Default())
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its updated data in 2D" in {
    toU(data2)
      .set(Cell(Position("foo", 2), Content(ContinuousSchema[Double](), 1.0)), Default())
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its updated and added data in 2D" in {
    toU(data2)
      .set(
        List(Position("foo", 2), Position("quxx", 5)).map(pos => Cell(pos, Content(ContinuousSchema[Double](), 1.0))),
        Default()
      )
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its matrix updated data in 2D" in {
    toU(data2)
      .set(toU(dataB), Default())
      .toList.sortBy(_.position) shouldBe result6
  }

  it should "return its updated data in 3D" in {
    toU(data3)
      .set(Cell(Position("foo", 2, "xyz"), Content(ContinuousSchema[Double](), 1.0)), Default())
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its updated and added data in 3D" in {
    toU(data3)
      .set(
        List(Position("foo", 2, "xyz"), Position("quxx", 5, "abc"))
          .map(pos => Cell(pos, Content(ContinuousSchema[Double](), 1.0))),
        Default()
      )
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its matrix updated data in 3D" in {
    toU(data3)
      .set(toU(dataC), Default())
      .toList.sortBy(_.position) shouldBe result9
  }
}

class TestScaldingMatrixSet extends TestMatrixSet with TestScalding {
  import commbank.grimlock.scalding.environment.implicits._

  "A Matrix.set" should "return its updated data in 1D" in {
    toU(data1)
      .set(Cell(Position("foo"), Content(ContinuousSchema[Double](), 1.0)), Default())
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its updated and added data in 1D" in {
    toU(data1)
      .set(List("foo", "quxx").map(pos => Cell(Position(pos), Content(ContinuousSchema[Double](), 1.0))), Default(12))
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its matrix updated data in 1D" in {
    toU(data1)
      .set(toU(dataA), Default())
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its updated data in 2D" in {
    toU(data2)
      .set(Cell(Position("foo", 2), Content(ContinuousSchema[Double](), 1.0)), Default(12))
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its updated and added data in 2D" in {
    toU(data2)
      .set(
        List(Position("foo", 2), Position("quxx", 5)).map(pos => Cell(pos, Content(ContinuousSchema[Double](), 1.0))),
        Default()
      )
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its matrix updated data in 2D" in {
    toU(data2)
      .set(toU(dataB), Default(12))
      .toList.sortBy(_.position) shouldBe result6
  }

  it should "return its updated data in 3D" in {
    toU(data3)
      .set(Cell(Position("foo", 2, "xyz"), Content(ContinuousSchema[Double](), 1.0)), Default())
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its updated and added data in 3D" in {
    toU(data3)
      .set(
        List(Position("foo", 2, "xyz"), Position("quxx", 5, "abc"))
          .map(pos => Cell(pos, Content(ContinuousSchema[Double](), 1.0))),
        Default(12)
      )
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its matrix updated data in 3D" in {
    toU(data3)
      .set(toU(dataC), Default())
      .toList.sortBy(_.position) shouldBe result9
  }
}

class TestSparkMatrixSet extends TestMatrixSet with TestSpark {
  import commbank.grimlock.spark.environment.implicits._

  "A Matrix.set" should "return its updated data in 1D" in {
    toU(data1)
      .set(Cell(Position("foo"), Content(ContinuousSchema[Double](), 1.0)), Default())
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its updated and added data in 1D" in {
    toU(data1)
      .set(List("foo", "quxx").map(pos => Cell(Position(pos), Content(ContinuousSchema[Double](), 1.0))), Default(12))
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its matrix updated data in 1D" in {
    toU(data1)
      .set(toU(dataA), Default())
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its updated data in 2D" in {
    toU(data2)
      .set(Cell(Position("foo", 2), Content(ContinuousSchema[Double](), 1.0)), Default(12))
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its updated and added data in 2D" in {
    toU(data2)
      .set(
        List(Position("foo", 2), Position("quxx", 5)).map(pos => Cell(pos, Content(ContinuousSchema[Double](), 1.0))),
        Default()
      )
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its matrix updated data in 2D" in {
    toU(data2)
      .set(toU(dataB), Default(12))
      .toList.sortBy(_.position) shouldBe result6
  }

  it should "return its updated data in 3D" in {
    toU(data3)
      .set(Cell(Position("foo", 2, "xyz"), Content(ContinuousSchema[Double](), 1.0)), Default())
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its updated and added data in 3D" in {
    toU(data3)
      .set(
        List(Position("foo", 2, "xyz"), Position("quxx", 5, "abc"))
          .map(pos => Cell(pos, Content(ContinuousSchema[Double](), 1.0))),
        Default(12)
      )
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its matrix updated data in 3D" in {
    toU(data3)
      .set(toU(dataC), Default())
      .toList.sortBy(_.position) shouldBe result9
  }
}

