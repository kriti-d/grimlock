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

trait TestMatrixSelect extends TestMatrix {
  val result1 = List(
    Cell(Position("baz"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("foo"), Content(OrdinalSchema[String](), "3.14"))
  )

  val result2 = List(
    Cell(Position("bar"), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("qux"), Content(OrdinalSchema[String](), "12.56"))
  )

  val result3 = List(
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
    )
  )

  val result4 = List(
    Cell(Position("bar", 1), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("bar", 2), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", 3), Content(OrdinalSchema[Long](), 19L)),
    Cell(Position("qux", 1), Content(OrdinalSchema[String](), "12.56"))
  )

  val result5 = List(
    Cell(Position("bar", 2), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("baz", 2), Content(DiscreteSchema[Long](), 19L)),
    Cell(Position("foo", 2), Content(ContinuousSchema[Double](), 6.28)),
    Cell(
      Position("foo", 4),
      Content(
        DateSchema[java.util.Date](),
        DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
      )
    )
  )

  val result6 = List(
    Cell(Position("bar", 1), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("bar", 3), Content(OrdinalSchema[Long](), 19L)),
    Cell(Position("baz", 1), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("foo", 1), Content(OrdinalSchema[String](), "3.14")),
    Cell(Position("foo", 3), Content(NominalSchema[String](), "9.42")),
    Cell(Position("qux", 1), Content(OrdinalSchema[String](), "12.56"))
  )

  val result7 = List(
    Cell(Position("bar", 2), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("baz", 2), Content(DiscreteSchema[Long](), 19L)),
    Cell(Position("foo", 2), Content(ContinuousSchema[Double](), 6.28)),
    Cell(
      Position("foo", 4),
      Content(
        DateSchema[java.util.Date](),
        DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
      )
    )
  )

  val result8 = List(
    Cell(Position("bar", 1), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("bar", 3), Content(OrdinalSchema[Long](), 19L)),
    Cell(Position("baz", 1), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("foo", 1), Content(OrdinalSchema[String](), "3.14")),
    Cell(Position("foo", 3), Content(NominalSchema[String](), "9.42")),
    Cell(Position("qux", 1), Content(OrdinalSchema[String](), "12.56"))
  )

  val result9 = List(
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
    )
  )

  val result10 = List(
    Cell(Position("bar", 1), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("bar", 2), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", 3), Content(OrdinalSchema[Long](), 19L)),
    Cell(Position("qux", 1), Content(OrdinalSchema[String](), "12.56"))
  )

  val result11 = List(
    Cell(Position("baz", 1, "xyz"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", 2, "xyz"), Content(DiscreteSchema[Long](), 19L)),
    Cell(Position("foo", 1, "xyz"), Content(OrdinalSchema[String](), "3.14")),
    Cell(Position("foo", 2, "xyz"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("foo", 3, "xyz"), Content(NominalSchema[String](), "9.42")),
    Cell(
      Position("foo", 4, "xyz"),
      Content(
        DateSchema[java.util.Date](),
        DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
      )
    )
  )

  val result12 = List(
    Cell(Position("bar", 1, "xyz"), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("bar", 2, "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", 3, "xyz"), Content(OrdinalSchema[Long](), 19L)),
    Cell(Position("qux", 1, "xyz"), Content(OrdinalSchema[String](), "12.56"))
  )

  val result13 = List(
    Cell(Position("bar", 2, "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("baz", 2, "xyz"), Content(DiscreteSchema[Long](), 19L)),
    Cell(Position("foo", 2, "xyz"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(
      Position("foo", 4, "xyz"),
      Content(
        DateSchema[java.util.Date](),
        DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
      )
    )
  )

  val result14 = List(
    Cell(Position("bar", 1, "xyz"), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("bar", 3, "xyz"), Content(OrdinalSchema[Long](), 19L)),
    Cell(Position("baz", 1, "xyz"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("foo", 1, "xyz"), Content(OrdinalSchema[String](), "3.14")),
    Cell(Position("foo", 3, "xyz"), Content(NominalSchema[String](), "9.42")),
    Cell(Position("qux", 1, "xyz"), Content(OrdinalSchema[String](), "12.56"))
  )

  val result15 = List(
    Cell(Position("bar", 2, "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("baz", 2, "xyz"), Content(DiscreteSchema[Long](), 19L)),
    Cell(Position("foo", 2, "xyz"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(
      Position("foo", 4, "xyz"),
      Content(
        DateSchema[java.util.Date](),
        DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
      )
    )
  )

  val result16 = List(
    Cell(Position("bar", 1, "xyz"), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("bar", 3, "xyz"), Content(OrdinalSchema[Long](), 19L)),
    Cell(Position("baz", 1, "xyz"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("foo", 1, "xyz"), Content(OrdinalSchema[String](), "3.14")),
    Cell(Position("foo", 3, "xyz"), Content(NominalSchema[String](), "9.42")),
    Cell(Position("qux", 1, "xyz"), Content(OrdinalSchema[String](), "12.56"))
  )

  val result17 = List(
    Cell(Position("baz", 1, "xyz"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", 2, "xyz"), Content(DiscreteSchema[Long](), 19L)),
    Cell(Position("foo", 1, "xyz"), Content(OrdinalSchema[String](), "3.14")),
    Cell(Position("foo", 2, "xyz"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("foo", 3, "xyz"), Content(NominalSchema[String](), "9.42")),
    Cell(
      Position("foo", 4, "xyz"),
      Content(
        DateSchema[java.util.Date](),
        DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
      )
    )
  )

  val result18 = List(
    Cell(Position("bar", 1, "xyz"), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("bar", 2, "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", 3, "xyz"), Content(OrdinalSchema[Long](), 19L)),
    Cell(Position("qux", 1, "xyz"), Content(OrdinalSchema[String](), "12.56"))
  )

  val result19 = List()

  val result20 = data3.sortBy(_.position)

  val result21 = List(
    Cell(Position("bar", 1, "xyz"), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("bar", 2, "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", 3, "xyz"), Content(OrdinalSchema[Long](), 19L)),
    Cell(Position("baz", 2, "xyz"), Content(DiscreteSchema[Long](), 19L)),
    Cell(Position("foo", 1, "xyz"), Content(OrdinalSchema[String](), "3.14")),
    Cell(Position("foo", 2, "xyz"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(
      Position("foo", 4, "xyz"),
      Content(
        DateSchema[java.util.Date](),
        DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
      )
    ),
    Cell(Position("qux", 1, "xyz"), Content(OrdinalSchema[String](), "12.56"))
  )

  val result22 = List(
    Cell(Position("baz", 1, "xyz"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("foo", 3, "xyz"), Content(NominalSchema[String](), "9.42"))
  )
}

class TestScalaMatrixSelect extends TestMatrixSelect with TestScala {
  import commbank.grimlock.scala.environment.implicits._

  "A Matrix.select" should "return its first over slice in 1D" in {
    toU(data1)
      .select(Over(_0), Default())(false, List("bar", "qux"))
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its first over inverse slice in 1D" in {
    toU(data1)
      .select(Over(_0), Default())(true, List("bar", "qux"))
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its first over slice in 2D" in {
    toU(data2)
      .select(Over(_0), Default())(false, List("bar", "qux"))
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its first over inverse slice in 2D" in {
    toU(data2)
      .select(Over(_0), Default())(true, List("bar", "qux"))
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its first along slice in 2D" in {
    toU(data2)
      .select(Along(_0), Default())(false, List(1, 3))
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its first along inverse slice in 2D" in {
    toU(data2)
      .select(Along(_0), Default())(true, List(1, 3))
      .toList.sortBy(_.position) shouldBe result6
  }

  it should "return its second over slice in 2D" in {
    toU(data2)
      .select(Over(_1), Default())(false, List(1, 3))
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its second over inverse slice in 2D" in {
    toU(data2)
      .select(Over(_1), Default())(true, List(1, 3))
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its second along slice in 2D" in {
    toU(data2)
      .select(Along(_1), Default())(false, List("bar", "qux"))
      .toList.sortBy(_.position) shouldBe result9
  }

  it should "return its second along inverse slice in 2D" in {
    toU(data2)
      .select(Along(_1), Default())(true, List("bar", "qux"))
      .toList.sortBy(_.position) shouldBe result10
  }

  it should "return its first over slice in 3D" in {
    toU(data3)
      .select(Over(_0), Default())(false, List("bar", "qux"))
      .toList.sortBy(_.position) shouldBe result11
  }

  it should "return its first over inverse slice in 3D" in {
    toU(data3)
      .select(Over(_0), Default())(true, List("bar", "qux"))
      .toList.sortBy(_.position) shouldBe result12
  }

  it should "return its first along slice in 3D" in {
    toU(data3)
      .select(Along(_0), Default())(false, List(Position(1, "xyz"), Position(3, "xyz")))
      .toList.sortBy(_.position) shouldBe result13
  }

  it should "return its first along inverse slice in 3D" in {
    toU(data3)
      .select(Along(_0), Default())(true, List(Position(1, "xyz"), Position(3, "xyz")))
      .toList.sortBy(_.position) shouldBe result14
  }

  it should "return its second over slice in 3D" in {
    toU(data3)
      .select(Over(_1), Default())(false, List(1, 3))
      .toList.sortBy(_.position) shouldBe result15
  }

  it should "return its second over inverse slice in 3D" in {
    toU(data3)
      .select(Over(_1), Default())(true, List(1, 3))
      .toList.sortBy(_.position) shouldBe result16
  }

  it should "return its second along slice in 3D" in {
    toU(data3)
      .select(Along(_1), Default())(false, List(Position("bar", "xyz"), Position("qux", "xyz")))
      .toList.sortBy(_.position) shouldBe result17
  }

  it should "return its second along inverse slice in 3D" in {
    toU(data3)
      .select(Along(_1), Default())(true, List(Position("bar", "xyz"), Position("qux", "xyz")))
      .toList.sortBy(_.position) shouldBe result18
  }

  it should "return its third over slice in 3D" in {
    toU(data3)
      .select(Over(_2), Default())(false, "xyz")
      .toList.sortBy(_.position) shouldBe result19
  }

  it should "return its third over inverse slice in 3D" in {
    toU(data3)
      .select(Over(_2), Default())(true, "xyz")
      .toList.sortBy(_.position) shouldBe result20
  }

  it should "return its third along slice in 3D" in {
    toU(data3)
      .select(Along(_2), Default())(false, List(Position("foo", 3), Position("baz", 1)))
      .toList.sortBy(_.position) shouldBe result21
  }

  it should "return its third along inverse slice in 3D" in {
    toU(data3)
      .select(Along(_2), Default())(true, List(Position("foo", 3), Position("baz", 1)))
      .toList.sortBy(_.position) shouldBe result22
  }

  it should "return empty data - Default" in {
    toU(data3)
      .select(Along(_2), Default())(true, List())
      .toList.sortBy(_.position) shouldBe List()
  }

  it should "return all data - Default" in {
    toU(data3)
      .select(Along(_2), Default())(false, List())
      .toList.sortBy(_.position) shouldBe data3.sortBy(_.position)
  }
}

class TestScaldingMatrixSelect extends TestMatrixSelect with TestScalding {
  import commbank.grimlock.scalding.environment.implicits._

  "A Matrix.select" should "return its first over slice in 1D" in {
    toU(data1)
      .select(Over(_0), InMemory())(false, List("bar", "qux"))
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its first over inverse slice in 1D" in {
    toU(data1)
      .select(Over(_0), Default())(true, List("bar", "qux"))
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its first over slice in 2D" in {
    toU(data2)
      .select(Over(_0), Default(12))(false, List("bar", "qux"))
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its first over inverse slice in 2D" in {
    toU(data2)
      .select(Over(_0), Unbalanced(12))(true, List("bar", "qux"))
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its first along slice in 2D" in {
    toU(data2)
      .select(Along(_0), InMemory())(false, List(1, 3))
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its first along inverse slice in 2D" in {
    toU(data2)
      .select(Along(_0), Default())(true, List(1, 3))
      .toList.sortBy(_.position) shouldBe result6
  }

  it should "return its second over slice in 2D" in {
    toU(data2)
      .select(Over(_1), Default(12))(false, List(1, 3))
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its second over inverse slice in 2D" in {
    toU(data2)
      .select(Over(_1), Unbalanced(12))(true, List(1, 3))
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its second along slice in 2D" in {
    toU(data2)
      .select(Along(_1), InMemory())(false, List("bar", "qux"))
      .toList.sortBy(_.position) shouldBe result9
  }

  it should "return its second along inverse slice in 2D" in {
    toU(data2)
      .select(Along(_1), Default())(true, List("bar", "qux"))
      .toList.sortBy(_.position) shouldBe result10
  }

  it should "return its first over slice in 3D" in {
    toU(data3)
      .select(Over(_0), Default(12))(false, List("bar", "qux"))
      .toList.sortBy(_.position) shouldBe result11
  }

  it should "return its first over inverse slice in 3D" in {
    toU(data3)
      .select(Over(_0), Unbalanced(12))(true, List("bar", "qux"))
      .toList.sortBy(_.position) shouldBe result12
  }

  it should "return its first along slice in 3D" in {
    toU(data3)
      .select(Along(_0), InMemory())(false, List(Position(1, "xyz"), Position(3, "xyz")))
      .toList.sortBy(_.position) shouldBe result13
  }

  it should "return its first along inverse slice in 3D" in {
    toU(data3)
      .select(Along(_0), Default())(true, List(Position(1, "xyz"), Position(3, "xyz")))
      .toList.sortBy(_.position) shouldBe result14
  }

  it should "return its second over slice in 3D" in {
    toU(data3)
      .select(Over(_1), Default(12))(false, List(1, 3))
      .toList.sortBy(_.position) shouldBe result15
  }

  it should "return its second over inverse slice in 3D" in {
    toU(data3)
      .select(Over(_1), Unbalanced(12))(true, List(1, 3))
      .toList.sortBy(_.position) shouldBe result16
  }

  it should "return its second along slice in 3D" in {
    toU(data3)
      .select(Along(_1), InMemory())(false, List(Position("bar", "xyz"), Position("qux", "xyz")))
      .toList.sortBy(_.position) shouldBe result17
  }

  it should "return its second along inverse slice in 3D" in {
    toU(data3)
      .select(Along(_1), Default())(true, List(Position("bar", "xyz"), Position("qux", "xyz")))
      .toList.sortBy(_.position) shouldBe result18
  }

  it should "return its third over slice in 3D" in {
    toU(data3)
      .select(Over(_2), Default(12))(false, "xyz")
      .toList.sortBy(_.position) shouldBe result19
  }

  it should "return its third over inverse slice in 3D" in {
    toU(data3)
      .select(Over(_2), Unbalanced(12))(true, "xyz")
      .toList.sortBy(_.position) shouldBe result20
  }

  it should "return its third along slice in 3D" in {
    toU(data3)
      .select(Along(_2), InMemory())(false, List(Position("foo", 3), Position("baz", 1)))
      .toList.sortBy(_.position) shouldBe result21
  }

  it should "return its third along inverse slice in 3D" in {
    toU(data3)
      .select(Along(_2), Default())(true, List(Position("foo", 3), Position("baz", 1)))
      .toList.sortBy(_.position) shouldBe result22
  }

  it should "return empty data - InMemory" in {
    toU(data3)
      .select(Along(_2), InMemory())(true, List())
      .toList.sortBy(_.position) shouldBe List()
  }

  it should "return all data - InMemory" in {
    toU(data3)
      .select(Along(_2), InMemory())(false, List())
      .toList.sortBy(_.position) shouldBe data3.sortBy(_.position)
  }

  it should "return empty data - Default" in {
    toU(data3)
      .select(Along(_2), Default())(true, List())
      .toList.sortBy(_.position) shouldBe List()
  }

  it should "return all data - Default" in {
    toU(data3)
      .select(Along(_2), Default())(false, List())
      .toList.sortBy(_.position) shouldBe data3.sortBy(_.position)
  }
}

class TestSparkMatrixSelect extends TestMatrixSelect with TestSpark {
  import commbank.grimlock.spark.environment.implicits._

  "A Matrix.select" should "return its first over slice in 1D" in {
    toU(data1)
      .select(Over(_0), InMemory())(false, List("bar", "qux"))
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its first over inverse slice in 1D" in {
    toU(data1)
      .select(Over(_0), Default())(true, List("bar", "qux"))
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its first over slice in 2D" in {
    toU(data2)
      .select(Over(_0), Default(12))(false, List("bar", "qux"))
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its first over inverse slice in 2D" in {
    toU(data2)
      .select(Over(_0), InMemory())(true, List("bar", "qux"))
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its first along slice in 2D" in {
    toU(data2)
      .select(Along(_0), Default())(false, List(1, 3))
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its first along inverse slice in 2D" in {
    toU(data2)
      .select(Along(_0), Default(12))(true, List(1, 3))
      .toList.sortBy(_.position) shouldBe result6
  }

  it should "return its second over slice in 2D" in {
    toU(data2)
      .select(Over(_1), InMemory())(false, List(1, 3))
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its second over inverse slice in 2D" in {
    toU(data2)
      .select(Over(_1), Default())(true, List(1, 3))
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its second along slice in 2D" in {
    toU(data2)
      .select(Along(_1), Default(12))(false, List("bar", "qux"))
      .toList.sortBy(_.position) shouldBe result9
  }

  it should "return its second along inverse slice in 2D" in {
    toU(data2)
      .select(Along(_1), InMemory())(true, List("bar", "qux"))
      .toList.sortBy(_.position) shouldBe result10
  }

  it should "return its first over slice in 3D" in {
    toU(data3)
      .select(Over(_0), Default())(false, List("bar", "qux"))
      .toList.sortBy(_.position) shouldBe result11
  }

  it should "return its first over inverse slice in 3D" in {
    toU(data3)
      .select(Over(_0), Default(12))(true, List("bar", "qux"))
      .toList.sortBy(_.position) shouldBe result12
  }

  it should "return its first along slice in 3D" in {
    toU(data3)
      .select(Along(_0), InMemory())(false, List(Position(1, "xyz"), Position(3, "xyz")))
      .toList.sortBy(_.position) shouldBe result13
  }

  it should "return its first along inverse slice in 3D" in {
    toU(data3)
      .select(Along(_0), Default())(true, List(Position(1, "xyz"), Position(3, "xyz")))
      .toList.sortBy(_.position) shouldBe result14
  }

  it should "return its second over slice in 3D" in {
    toU(data3)
      .select(Over(_1), Default(12))(false, List(1, 3))
      .toList.sortBy(_.position) shouldBe result15
  }

  it should "return its second over inverse slice in 3D" in {
    toU(data3)
      .select(Over(_1), InMemory())(true, List(1, 3))
      .toList.sortBy(_.position) shouldBe result16
  }

  it should "return its second along slice in 3D" in {
    toU(data3)
      .select(Along(_1), Default())(false, List(Position("bar", "xyz"), Position("qux", "xyz")))
      .toList.sortBy(_.position) shouldBe result17
  }

  it should "return its second along inverse slice in 3D" in {
    toU(data3)
      .select(Along(_1), Default(12))(true, List(Position("bar", "xyz"), Position("qux", "xyz")))
      .toList.sortBy(_.position) shouldBe result18
  }

  it should "return its third over slice in 3D" in {
    toU(data3)
      .select(Over(_2), InMemory())(false, "xyz")
      .toList.sortBy(_.position) shouldBe result19
  }

  it should "return its third over inverse slice in 3D" in {
    toU(data3)
      .select(Over(_2), Default())(true, "xyz")
      .toList.sortBy(_.position) shouldBe result20
  }

  it should "return its third along slice in 3D" in {
    toU(data3)
      .select(Along(_2), Default(12))(false, List(Position("foo", 3), Position("baz", 1)))
      .toList.sortBy(_.position) shouldBe result21
  }

  it should "return its third along inverse slice in 3D" in {
    toU(data3)
      .select(Along(_2), InMemory())(true, List(Position("foo", 3), Position("baz", 1)))
      .toList.sortBy(_.position) shouldBe result22
  }

  it should "return empty data - Default" in {
    toU(data3)
      .select(Along(_2), Default())(true, List())
      .toList.sortBy(_.position) shouldBe List()
  }

  it should "return all data - Default" in {
    toU(data3)
      .select(Along(_2), Default())(false, List())
      .toList.sortBy(_.position) shouldBe data3.sortBy(_.position)
  }
}

