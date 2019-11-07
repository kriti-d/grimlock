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

import com.twitter.scalding.typed.TypedPipe

import shapeless.nat.{ _0, _1, _2 }

trait TestMatrixJoin extends TestMatrix {
  val dataA = List(
    Cell(Position("bar", 5), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("baz", 5), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("qux", 5), Content(OrdinalSchema[String](), "12.56")),
    Cell(Position("bar", 6), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("baz", 6), Content(DiscreteSchema[Long](), 19L)),
    Cell(Position("bar", 7), Content(OrdinalSchema[Long](), 19L))
  )

  val dataB = List(
    Cell(Position("foo.2", 2), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("bar.2", 2), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("baz.2", 2), Content(DiscreteSchema[Long](), 19L)),
    Cell(Position("foo.2", 3), Content(NominalSchema[String](), "9.42")),
    Cell(Position("bar.2", 3), Content(OrdinalSchema[Long](), 19L))
  )

  val dataC = List(
    Cell(Position("foo.2", 2), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("bar.2", 2), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("baz.2", 2), Content(DiscreteSchema[Long](), 19L)),
    Cell(Position("foo.2", 3), Content(NominalSchema[String](), "9.42")),
    Cell(Position("bar.2", 3), Content(OrdinalSchema[Long](), 19L))
  )

  val dataD = List(
    Cell(Position("bar", 5), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("baz", 5), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("qux", 5), Content(OrdinalSchema[String](), "12.56")),
    Cell(Position("bar", 6), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("baz", 6), Content(DiscreteSchema[Long](), 19L)),
    Cell(Position("bar", 7), Content(OrdinalSchema[Long](), 19L))
  )

  val dataE = List(
    Cell(Position("bar", 5, "xyz"), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("baz", 5, "xyz"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("qux", 5, "xyz"), Content(OrdinalSchema[String](), "12.56")),
    Cell(Position("bar", 6, "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("baz", 6, "xyz"), Content(DiscreteSchema[Long](), 19L)),
    Cell(Position("bar", 7, "xyz"), Content(OrdinalSchema[Long](), 19L))
  )

  val dataF = List(
    Cell(Position("foo.2", 2, "xyz"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("bar.2", 2, "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("baz.2", 2, "xyz"), Content(DiscreteSchema[Long](), 19L)),
    Cell(Position("foo.2", 3, "xyz"), Content(NominalSchema[String](), "9.42")),
    Cell(Position("bar.2", 3, "xyz"), Content(OrdinalSchema[Long](), 19L))
  )

  val dataG = List(
    Cell(Position("foo.2", 2, "xyz"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("bar.2", 2, "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("baz.2", 2, "xyz"), Content(DiscreteSchema[Long](), 19L)),
    Cell(Position("foo.2", 3, "xyz"), Content(NominalSchema[String](), "9.42")),
    Cell(Position("bar.2", 3, "xyz"), Content(OrdinalSchema[Long](), 19L))
  )

  val dataH = List(
    Cell(Position("bar", 5, "xyz"), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("baz", 5, "xyz"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("qux", 5, "xyz"), Content(OrdinalSchema[String](), "12.56")),
    Cell(Position("bar", 6, "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("baz", 6, "xyz"), Content(DiscreteSchema[Long](), 19L)),
    Cell(Position("bar", 7, "xyz"), Content(OrdinalSchema[Long](), 19L))
  )

  val dataI = List(
    Cell(Position("foo.2", 2, "xyz"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("bar.2", 2, "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("baz.2", 2, "xyz"), Content(DiscreteSchema[Long](), 19L)),
    Cell(Position("foo.2", 3, "xyz"), Content(NominalSchema[String](), "9.42")),
    Cell(Position("bar.2", 3, "xyz"), Content(OrdinalSchema[Long](), 19L))
  )

  val dataJ = List(
    Cell(Position("bar", 1, "xyz.2"), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("baz", 1, "xyz.2"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("qux", 1, "xyz.2"), Content(OrdinalSchema[String](), "12.56")),
    Cell(Position("bar", 2, "xyz.2"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("baz", 2, "xyz.2"), Content(DiscreteSchema[Long](), 19L)),
    Cell(Position("bar", 3, "xyz.2"), Content(OrdinalSchema[Long](), 19L))
  )

  val result1 = List(
    Cell(Position("bar", 1), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("bar", 2), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", 3), Content(OrdinalSchema[Long](), 19L)),
    Cell(Position("bar", 5), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("bar", 6), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", 7), Content(OrdinalSchema[Long](), 19L)),
    Cell(Position("baz", 1), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", 2), Content(DiscreteSchema[Long](), 19L)),
    Cell(Position("baz", 5), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", 6), Content(DiscreteSchema[Long](), 19L)),
    Cell(Position("qux", 1), Content(OrdinalSchema[String](), "12.56")),
    Cell(Position("qux", 5), Content(OrdinalSchema[String](), "12.56"))
  )

  val result2 = List(
    Cell(Position("bar", 2), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", 3), Content(OrdinalSchema[Long](), 19L)),
    Cell(Position("bar.2", 2), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar.2", 3), Content(OrdinalSchema[Long](), 19L)),
    Cell(Position("baz", 2), Content(DiscreteSchema[Long](), 19L)),
    Cell(Position("baz.2", 2), Content(DiscreteSchema[Long](), 19L)),
    Cell(Position("foo", 2), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("foo", 3), Content(NominalSchema[String](), "9.42")),
    Cell(Position("foo.2", 2), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("foo.2", 3), Content(NominalSchema[String](), "9.42"))
  )

  val result3 = List(
    Cell(Position("bar", 2), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", 3), Content(OrdinalSchema[Long](), 19L)),
    Cell(Position("bar.2", 2), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar.2", 3), Content(OrdinalSchema[Long](), 19L)),
    Cell(Position("baz", 2), Content(DiscreteSchema[Long](), 19L)),
    Cell(Position("baz.2", 2), Content(DiscreteSchema[Long](), 19L)),
    Cell(Position("foo", 2), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("foo", 3), Content(NominalSchema[String](), "9.42")),
    Cell(Position("foo.2", 2), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("foo.2", 3), Content(NominalSchema[String](), "9.42"))
  )

  val result4 = List(
    Cell(Position("bar", 1), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("bar", 2), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", 3), Content(OrdinalSchema[Long](), 19L)),
    Cell(Position("bar", 5), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("bar", 6), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", 7), Content(OrdinalSchema[Long](), 19L)),
    Cell(Position("baz", 1), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", 2), Content(DiscreteSchema[Long](), 19L)),
    Cell(Position("baz", 5), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", 6), Content(DiscreteSchema[Long](), 19L)),
    Cell(Position("qux", 1), Content(OrdinalSchema[String](), "12.56")),
    Cell(Position("qux", 5), Content(OrdinalSchema[String](), "12.56"))
  )

  val result5 = List(
    Cell(Position("bar", 1, "xyz"), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("bar", 2, "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", 3, "xyz"), Content(OrdinalSchema[Long](), 19L)),
    Cell(Position("bar", 5, "xyz"), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("bar", 6, "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", 7, "xyz"), Content(OrdinalSchema[Long](), 19L)),
    Cell(Position("baz", 1, "xyz"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", 2, "xyz"), Content(DiscreteSchema[Long](), 19L)),
    Cell(Position("baz", 5, "xyz"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", 6, "xyz"), Content(DiscreteSchema[Long](), 19L)),
    Cell(Position("qux", 1, "xyz"), Content(OrdinalSchema[String](), "12.56")),
    Cell(Position("qux", 5, "xyz"), Content(OrdinalSchema[String](), "12.56"))
  )

  val result6 = List(
    Cell(Position("bar", 2, "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", 3, "xyz"), Content(OrdinalSchema[Long](), 19L)),
    Cell(Position("bar.2", 2, "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar.2", 3, "xyz"), Content(OrdinalSchema[Long](), 19L)),
    Cell(Position("baz", 2, "xyz"), Content(DiscreteSchema[Long](), 19L)),
    Cell(Position("baz.2", 2, "xyz"), Content(DiscreteSchema[Long](), 19L)),
    Cell(Position("foo", 2, "xyz"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("foo", 3, "xyz"), Content(NominalSchema[String](), "9.42")),
    Cell(Position("foo.2", 2, "xyz"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("foo.2", 3, "xyz"), Content(NominalSchema[String](), "9.42"))
  )

  val result7 = List(
    Cell(Position("bar", 2, "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", 3, "xyz"), Content(OrdinalSchema[Long](), 19L)),
    Cell(Position("bar.2", 2, "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar.2", 3, "xyz"), Content(OrdinalSchema[Long](), 19L)),
    Cell(Position("baz", 2, "xyz"), Content(DiscreteSchema[Long](), 19L)),
    Cell(Position("baz.2", 2, "xyz"), Content(DiscreteSchema[Long](), 19L)),
    Cell(Position("foo", 2, "xyz"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("foo", 3, "xyz"), Content(NominalSchema[String](), "9.42")),
    Cell(Position("foo.2", 2, "xyz"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("foo.2", 3, "xyz"), Content(NominalSchema[String](), "9.42"))
  )

  val result8 = List(
    Cell(Position("bar", 1, "xyz"), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("bar", 2, "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", 3, "xyz"), Content(OrdinalSchema[Long](), 19L)),
    Cell(Position("bar", 5, "xyz"), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("bar", 6, "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", 7, "xyz"), Content(OrdinalSchema[Long](), 19L)),
    Cell(Position("baz", 1, "xyz"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", 2, "xyz"), Content(DiscreteSchema[Long](), 19L)),
    Cell(Position("baz", 5, "xyz"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", 6, "xyz"), Content(DiscreteSchema[Long](), 19L)),
    Cell(Position("qux", 1, "xyz"), Content(OrdinalSchema[String](), "12.56")),
    Cell(Position("qux", 5, "xyz"), Content(OrdinalSchema[String](), "12.56"))
  )

  val result9 = List(
    Cell(Position("bar", 1, "xyz"), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("bar", 2, "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", 3, "xyz"), Content(OrdinalSchema[Long](), 19L)),
    Cell(Position("bar.2", 2, "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar.2", 3, "xyz"), Content(OrdinalSchema[Long](), 19L)),
    Cell(Position("baz", 1, "xyz"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", 2, "xyz"), Content(DiscreteSchema[Long](), 19L)),
    Cell(Position("baz.2", 2, "xyz"), Content(DiscreteSchema[Long](), 19L)),
    Cell(Position("foo", 1, "xyz"), Content(OrdinalSchema[String](), "3.14")),
    Cell(Position("foo", 2, "xyz"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("foo", 3, "xyz"), Content(NominalSchema[String](), "9.42")),
    Cell(
      Position("foo", 4, "xyz"),
      Content(
        DateSchema[java.util.Date](),
        DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
      )
    ),
    Cell(Position("foo.2", 2, "xyz"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("foo.2", 3, "xyz"), Content(NominalSchema[String](), "9.42")),
    Cell(Position("qux", 1, "xyz"), Content(OrdinalSchema[String](), "12.56"))
  )

  val result10 = List(
    Cell(Position("bar", 1, "xyz"), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("bar", 1, "xyz.2"), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("bar", 2, "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", 2, "xyz.2"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", 3, "xyz"), Content(OrdinalSchema[Long](), 19L)),
    Cell(Position("bar", 3, "xyz.2"), Content(OrdinalSchema[Long](), 19L)),
    Cell(Position("baz", 1, "xyz"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", 1, "xyz.2"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", 2, "xyz"), Content(DiscreteSchema[Long](), 19L)),
    Cell(Position("baz", 2, "xyz.2"), Content(DiscreteSchema[Long](), 19L)),
    Cell(Position("qux", 1, "xyz"), Content(OrdinalSchema[String](), "12.56")),
    Cell(Position("qux", 1, "xyz.2"), Content(OrdinalSchema[String](), "12.56"))
  )
}

class TestScalaMatrixJoin extends TestMatrixJoin with TestScala {
  import commbank.grimlock.scala.environment.implicits._

  "A Matrix.join" should "return its first over join in 2D" in {
    toU(data2)
      .join(Over(_0), Default())(toU(dataA))
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its first along join in 2D" in {
    toU(dataB)
      .join(Along(_0), Default())(toU(data2))
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its second over join in 2D" in {
    toU(dataC)
      .join(Over(_1), Default())(toU(data2))
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its second along join in 2D" in {
    toU(data2)
      .join(Along(_1), Default())(toU(dataD))
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its first over join in 3D" in {
    toU(data3)
      .join(Over(_0), Default())(toU(dataE))
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its first along join in 3D" in {
    toU(dataF)
      .join(Along(_0), Default())(toU(data3))
      .toList.sortBy(_.position) shouldBe result6
  }

  it should "return its second over join in 3D" in {
    toU(dataG)
      .join(Over(_1), Default())(toU(data3))
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its second along join in 3D" in {
    toU(data3)
      .join(Along(_1), Default())(toU(dataH))
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its third over join in 3D" in {
    toU(dataI)
      .join(Over(_2), Default())(toU(data3))
      .toList.sortBy(_.position) shouldBe result9
  }

  it should "return its third along join in 3D" in {
    toU(data3)
      .join(Along(_2), Default())(toU(dataJ))
      .toList.sortBy(_.position) shouldBe result10
  }

  it should "return empty data - Default" in {
    toU(data3)
      .join(Along(_2), Default())(List.empty)
      .toList.sortBy(_.position) shouldBe List()
  }
}

class TestScaldingMatrixJoin extends TestMatrixJoin with TestScalding {
  import commbank.grimlock.scalding.environment.implicits._

  "A Matrix.join" should "return its first over join in 2D" in {
    toU(data2)
      .join(Over(_0), InMemory())(toU(dataA))
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its first along join in 2D" in {
    toU(dataB)
      .join(Along(_0), Default())(toU(data2))
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its second over join in 2D" in {
    toU(dataC)
      .join(Over(_1), Binary(InMemory(), Default()))(toU(data2))
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its second along join in 2D" in {
    toU(data2)
      .join(Along(_1), Binary(InMemory(), Default(12)))(toU(dataD))
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its first over join in 3D" in {
    toU(data3)
      .join(Over(_0), Binary(InMemory(), Unbalanced(12)))(toU(dataE))
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its first along join in 3D" in {
    toU(dataF)
      .join(Along(_0), Binary(InMemory(12), Default(12)))(toU(data3))
      .toList.sortBy(_.position) shouldBe result6
  }

  it should "return its second over join in 3D" in {
    toU(dataG)
      .join(Over(_1), Binary(InMemory(12), Unbalanced(12)))(toU(data3))
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its second along join in 3D" in {
    toU(data3)
      .join(Along(_1), Binary(Default(), Default(12)))(toU(dataH))
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its third over join in 3D" in {
    toU(dataI)
      .join(Over(_2), Binary(Default(), Unbalanced(12)))(toU(data3))
      .toList.sortBy(_.position) shouldBe result9
  }

  it should "return its third along join in 3D" in {
    toU(data3)
      .join(Along(_2), Binary(Default(12), Default(12)))(toU(dataJ))
      .toList.sortBy(_.position) shouldBe result10
  }

  it should "return empty data - InMemory" in {
    toU(data3)
      .join(Along(_2), InMemory())(TypedPipe.empty)
      .toList.sortBy(_.position) shouldBe List()
  }

  it should "return empty data - Default" in {
    toU(data3)
      .join(Along(_2), Default())(TypedPipe.empty)
      .toList.sortBy(_.position) shouldBe List()
  }
}

class TestSparkMatrixJoin extends TestMatrixJoin with TestSpark {
  import commbank.grimlock.spark.environment.implicits._

  "A Matrix.join" should "return its first over join in 2D" in {
    toU(data2)
      .join(Over(_0), InMemory())(toU(dataA))
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its first along join in 2D" in {
    toU(dataB)
      .join(Along(_0), Default())(toU(data2))
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its second over join in 2D" in {
    toU(dataC)
      .join(Over(_1), Binary(InMemory(), Default()))(toU(data2))
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its second along join in 2D" in {
    toU(data2)
      .join(Along(_1), Binary(InMemory(), Default(12)))(toU(dataD))
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its first over join in 3D" in {
    toU(data3)
      .join(Over(_0), Binary(InMemory(12), Default(12)))(toU(dataE))
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its first along join in 3D" in {
    toU(dataF)
      .join(Along(_0), Binary(Default(), Default(12)))(toU(data3))
      .toList.sortBy(_.position) shouldBe result6
  }

  it should "return its second over join in 3D" in {
    toU(dataG)
      .join(Over(_1), Binary(Default(12), Default(12)))(toU(data3))
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its second along join in 3D" in {
    toU(data3)
      .join(Along(_1), InMemory())(toU(dataH))
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its third over join in 3D" in {
    toU(dataI)
      .join(Over(_2), Default())(toU(data3))
      .toList.sortBy(_.position) shouldBe result9
  }

  it should "return its third along join in 3D" in {
    toU(data3)
      .join(Along(_2), Binary(InMemory(), Default()))(toU(dataJ))
      .toList.sortBy(_.position) shouldBe result10
  }

  it should "return empty data - Default" in {
    toU(data3)
      .join(Along(_2), Default())(toU(List()))
      .toList.sortBy(_.position) shouldBe List()
  }
}

