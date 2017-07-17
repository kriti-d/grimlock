// Copyright 2015,2016,2017 Commonwealth Bank of Australia
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
import commbank.grimlock.framework.aggregate._
import commbank.grimlock.framework.content._
import commbank.grimlock.framework.encoding._
import commbank.grimlock.framework.environment.tuner._
import commbank.grimlock.framework.extract._
import commbank.grimlock.framework.metadata._
import commbank.grimlock.framework.pairwise._
import commbank.grimlock.framework.partition._
import commbank.grimlock.framework.position._
import commbank.grimlock.framework.sample._
import commbank.grimlock.framework.squash._
import commbank.grimlock.framework.window._

import commbank.grimlock.library.aggregate._
import commbank.grimlock.library.pairwise._
import commbank.grimlock.library.squash._
import commbank.grimlock.library.transform._

import commbank.grimlock.scalding.environment._

import commbank.grimlock.spark.environment._

import com.twitter.scalding.typed.{ TypedPipe, ValuePipe }

import shapeless.{ Nat, Succ }
import shapeless.nat.{ _0, _1, _2, _3, _4, _5 }
import shapeless.ops.nat.{ Diff, LTEq, ToInt }

trait TestMatrix extends TestGrimlock {

  val data1 = List(
    Cell(Position("foo"), Content(OrdinalSchema[String](), "3.14")),
    Cell(Position("bar"), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("baz"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("qux"), Content(OrdinalSchema[String](), "12.56"))
  )

  val data2 = List(
    Cell(Position("foo", 1), Content(OrdinalSchema[String](), "3.14")),
    Cell(Position("bar", 1), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("baz", 1), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("qux", 1), Content(OrdinalSchema[String](), "12.56")),
    Cell(Position("foo", 2), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("bar", 2), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("baz", 2), Content(DiscreteSchema[Long](), 19)),
    Cell(Position("foo", 3), Content(NominalSchema[String](), "9.42")),
    Cell(Position("bar", 3), Content(OrdinalSchema[Long](), 19)),
    Cell(
      Position("foo", 4),
      Content(
        DateSchema[java.util.Date](),
        DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
      )
    )
  )

  val data3 = List(
    Cell(Position("foo", 1, "xyz"), Content(OrdinalSchema[String](), "3.14")),
    Cell(Position("bar", 1, "xyz"), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("baz", 1, "xyz"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("qux", 1, "xyz"), Content(OrdinalSchema[String](), "12.56")),
    Cell(Position("foo", 2, "xyz"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("bar", 2, "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("baz", 2, "xyz"), Content(DiscreteSchema[Long](), 19)),
    Cell(Position("foo", 3, "xyz"), Content(NominalSchema[String](), "9.42")),
    Cell(Position("bar", 3, "xyz"), Content(OrdinalSchema[Long](), 19)),
    Cell(
      Position("foo", 4, "xyz"),
      Content(
        DateSchema[java.util.Date](),
        DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
      )
    )
  )

  val num1 = List(
    Cell(Position("foo"), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position("bar"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("baz"), Content(ContinuousSchema[Double](), 9.42)),
    Cell(Position("qux"), Content(ContinuousSchema[Double](), 12.56))
  )

  val num2 = List(
    Cell(Position("foo", 1), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position("bar", 1), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("baz", 1), Content(ContinuousSchema[Double](), 9.42)),
    Cell(Position("qux", 1), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("foo", 2), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("bar", 2), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("baz", 2), Content(ContinuousSchema[Double](), 18.84)),
    Cell(Position("foo", 3), Content(ContinuousSchema[Double](), 9.42)),
    Cell(Position("bar", 3), Content(ContinuousSchema[Double](), 18.84)),
    Cell(Position("foo", 4), Content(ContinuousSchema[Double](), 12.56))
  )

  val num3 = List(
    Cell(Position("foo", 1, "xyz"), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position("bar", 1, "xyz"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("baz", 1, "xyz"), Content(ContinuousSchema[Double](), 9.42)),
    Cell(Position("qux", 1, "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("foo", 2, "xyz"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("bar", 2, "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("baz", 2, "xyz"), Content(ContinuousSchema[Double](), 18.84)),
    Cell(Position("foo", 3, "xyz"), Content(ContinuousSchema[Double](), 9.42)),
    Cell(Position("bar", 3, "xyz"), Content(ContinuousSchema[Double](), 18.84)),
    Cell(Position("foo", 4, "xyz"), Content(ContinuousSchema[Double](), 12.56))
  )
}

class TestScaldingMatrixNames extends TestMatrix {

  "A Matrix.names" should "return its first over names in 1D" in {
    toPipe(data1)
      .names(Over(_1), Default())
      .toList.sorted shouldBe List(Position("bar"), Position("baz"), Position("foo"), Position("qux"))
  }

  it should "return its first over names in 2D" in {
    toPipe(data2)
      .names(Over(_1), Default(12))
      .toList.sorted shouldBe List(Position("bar"), Position("baz"), Position("foo"), Position("qux"))
  }

  it should "return its first along names in 2D" in {
    toPipe(data2)
      .names(Along(_1), Default())
      .toList.sorted shouldBe List(Position(1), Position(2), Position(3), Position(4))
  }

  it should "return its second over names in 2D" in {
    toPipe(data2)
      .names(Over(_2), Default(12))
      .toList.sorted shouldBe List(Position(1), Position(2), Position(3), Position(4))
  }

  it should "return its second along names in 2D" in {
    toPipe(data2)
      .names(Along(_2), Default())
      .toList.sorted shouldBe List(Position("bar"), Position("baz"), Position("foo"), Position("qux"))
  }

  it should "return its first over names in 3D" in {
    toPipe(data3)
      .names(Over(_1), Default(12))
      .toList.sorted shouldBe List(Position("bar"), Position("baz"), Position("foo"), Position("qux"))
  }

  it should "return its first along names in 3D" in {
    toPipe(data3)
      .names(Along(_1), Default())
      .toList.sorted shouldBe List(Position(1, "xyz"), Position(2, "xyz"), Position(3, "xyz"), Position(4, "xyz"))
  }

  it should "return its second over names in 3D" in {
    toPipe(data3)
      .names(Over(_2), Default(12))
      .toList.sorted shouldBe List(Position(1), Position(2), Position(3), Position(4))
  }

  it should "return its second along names in 3D" in {
    toPipe(data3)
      .names(Along(_2), Default())
      .toList.sorted shouldBe List(
        Position("bar", "xyz"),
        Position("baz", "xyz"),
        Position("foo", "xyz"),
        Position("qux", "xyz")
      )
  }

  it should "return its third over names in 3D" in {
    toPipe(data3)
      .names(Over(_3), Default(12))
      .toList.sorted shouldBe List(Position("xyz"))
  }

  it should "return its third along names in 3D" in {
    toPipe(data3)
      .names(Along(_3), Default())
      .toList.sorted shouldBe List(
        Position("bar", 1),
        Position("bar", 2),
        Position("bar", 3),
        Position("baz", 1),
        Position("baz", 2),
        Position("foo", 1),
        Position("foo", 2),
        Position("foo", 3),
        Position("foo", 4),
        Position("qux", 1)
      )
  }
}

class TestSparkMatrixNames extends TestMatrix {

  "A Matrix.names" should "return its first over names in 1D" in {
    toRDD(data1)
      .names(Over(_1), Default())
      .toList.sorted shouldBe List(Position("bar"), Position("baz"), Position("foo"), Position("qux"))
  }

  it should "return its first over names in 2D" in {
    toRDD(data2)
      .names(Over(_1), Default(12))
      .toList.sorted shouldBe List(Position("bar"), Position("baz"), Position("foo"), Position("qux"))
  }

  it should "return its first along names in 2D" in {
    toRDD(data2)
      .names(Along(_1), Default())
      .toList.sorted shouldBe List(Position(1), Position(2), Position(3), Position(4))
  }

  it should "return its second over names in 2D" in {
    toRDD(data2)
      .names(Over(_2), Default(12))
      .toList.sorted shouldBe List(Position(1), Position(2), Position(3), Position(4))
  }

  it should "return its second along names in 2D" in {
    toRDD(data2)
      .names(Along(_2), Default())
      .toList.sorted shouldBe List(Position("bar"), Position("baz"), Position("foo"), Position("qux"))
  }

  it should "return its first over names in 3D" in {
    toRDD(data3)
      .names(Over(_1), Default(12))
      .toList.sorted shouldBe List(Position("bar"), Position("baz"), Position("foo"), Position("qux"))
  }

  it should "return its first along names in 3D" in {
    toRDD(data3)
      .names(Along(_1), Default())
      .toList.sorted shouldBe List(Position(1, "xyz"), Position(2, "xyz"), Position(3, "xyz"), Position(4, "xyz"))
  }

  it should "return its second over names in 3D" in {
    toRDD(data3)
      .names(Over(_2), Default(12))
      .toList.sorted shouldBe List(Position(1), Position(2), Position(3), Position(4))
  }

  it should "return its second along names in 3D" in {
    toRDD(data3)
      .names(Along(_2), Default())
      .toList.sorted shouldBe List(
        Position("bar", "xyz"),
        Position("baz", "xyz"),
        Position("foo", "xyz"),
        Position("qux", "xyz")
      )
  }

  it should "return its third over names in 3D" in {
    toRDD(data3)
      .names(Over(_3), Default(12))
      .toList.sorted shouldBe List(Position("xyz"))
  }

  it should "return its third along names in 3D" in {
    toRDD(data3)
      .names(Along(_3), Default())
      .toList.sorted shouldBe List(
        Position("bar", 1),
        Position("bar", 2),
        Position("bar", 3),
        Position("baz", 1),
        Position("baz", 2),
        Position("foo", 1),
        Position("foo", 2),
        Position("foo", 3),
        Position("foo", 4),
        Position("qux", 1)
      )
  }
}

trait TestMatrixTypes extends TestMatrix {

  val result1 = List(
    Cell(Position("bar"), Content(NominalSchema[Type](), CategoricalType)),
    Cell(Position("baz"), Content(NominalSchema[Type](), CategoricalType)),
    Cell(Position("foo"), Content(NominalSchema[Type](), CategoricalType)),
    Cell(Position("qux"), Content(NominalSchema[Type](), CategoricalType))
  )

  val result2 = List(
    Cell(Position("bar"), Content(NominalSchema[Type](), OrdinalType)),
    Cell(Position("baz"), Content(NominalSchema[Type](), OrdinalType)),
    Cell(Position("foo"), Content(NominalSchema[Type](), OrdinalType)),
    Cell(Position("qux"), Content(NominalSchema[Type](), OrdinalType))
  )

  val result3 = List(
    Cell(Position("bar"), Content(NominalSchema[Type](), MixedType)),
    Cell(Position("baz"), Content(NominalSchema[Type](), MixedType)),
    Cell(Position("foo"), Content(NominalSchema[Type](), MixedType)),
    Cell(Position("qux"), Content(NominalSchema[Type](), CategoricalType))
  )

  val result4 = List(
    Cell(Position("bar"), Content(NominalSchema[Type](), MixedType)),
    Cell(Position("baz"), Content(NominalSchema[Type](), MixedType)),
    Cell(Position("foo"), Content(NominalSchema[Type](), MixedType)),
    Cell(Position("qux"), Content(NominalSchema[Type](), OrdinalType))
  )

  val result5 = List(
    Cell(Position(1), Content(NominalSchema[Type](), CategoricalType)),
    Cell(Position(2), Content(NominalSchema[Type](), NumericType)),
    Cell(Position(3), Content(NominalSchema[Type](), CategoricalType)),
    Cell(Position(4), Content(NominalSchema[Type](), DateType))
  )

  val result6 = List(
    Cell(Position(1), Content(NominalSchema[Type](), OrdinalType)),
    Cell(Position(2), Content(NominalSchema[Type](), NumericType)),
    Cell(Position(3), Content(NominalSchema[Type](), CategoricalType)),
    Cell(Position(4), Content(NominalSchema[Type](), DateType))
  )

  val result7 = List(
    Cell(Position(1), Content(NominalSchema[Type](), CategoricalType)),
    Cell(Position(2), Content(NominalSchema[Type](), NumericType)),
    Cell(Position(3), Content(NominalSchema[Type](), CategoricalType)),
    Cell(Position(4), Content(NominalSchema[Type](), DateType))
  )

  val result8 = List(
    Cell(Position(1), Content(NominalSchema[Type](), OrdinalType)),
    Cell(Position(2), Content(NominalSchema[Type](), NumericType)),
    Cell(Position(3), Content(NominalSchema[Type](), CategoricalType)),
    Cell(Position(4), Content(NominalSchema[Type](), DateType))
  )

  val result9 = List(
    Cell(Position("bar"), Content(NominalSchema[Type](), MixedType)),
    Cell(Position("baz"), Content(NominalSchema[Type](), MixedType)),
    Cell(Position("foo"), Content(NominalSchema[Type](), MixedType)),
    Cell(Position("qux"), Content(NominalSchema[Type](), CategoricalType))
  )

  val result10 = List(
    Cell(Position("bar"), Content(NominalSchema[Type](), MixedType)),
    Cell(Position("baz"), Content(NominalSchema[Type](), MixedType)),
    Cell(Position("foo"), Content(NominalSchema[Type](), MixedType)),
    Cell(Position("qux"), Content(NominalSchema[Type](), OrdinalType))
  )

  val result11 = List(
    Cell(Position("bar"), Content(NominalSchema[Type](), MixedType)),
    Cell(Position("baz"), Content(NominalSchema[Type](), MixedType)),
    Cell(Position("foo"), Content(NominalSchema[Type](), MixedType)),
    Cell(Position("qux"), Content(NominalSchema[Type](), CategoricalType))
  )

  val result12 = List(
    Cell(Position("bar"), Content(NominalSchema[Type](), MixedType)),
    Cell(Position("baz"), Content(NominalSchema[Type](), MixedType)),
    Cell(Position("foo"), Content(NominalSchema[Type](), MixedType)),
    Cell(Position("qux"), Content(NominalSchema[Type](), OrdinalType))
  )

  val result13 = List(
    Cell(Position(1, "xyz"), Content(NominalSchema[Type](), CategoricalType)),
    Cell(Position(2, "xyz"), Content(NominalSchema[Type](), NumericType)),
    Cell(Position(3, "xyz"), Content(NominalSchema[Type](), CategoricalType)),
    Cell(Position(4, "xyz"), Content(NominalSchema[Type](), DateType))
  )

  val result14 = List(
    Cell(Position(1, "xyz"), Content(NominalSchema[Type](), OrdinalType)),
    Cell(Position(2, "xyz"), Content(NominalSchema[Type](), NumericType)),
    Cell(Position(3, "xyz"), Content(NominalSchema[Type](), CategoricalType)),
    Cell(Position(4, "xyz"), Content(NominalSchema[Type](), DateType))
  )

  val result15 = List(
    Cell(Position(1), Content(NominalSchema[Type](), CategoricalType)),
    Cell(Position(2), Content(NominalSchema[Type](), NumericType)),
    Cell(Position(3), Content(NominalSchema[Type](), CategoricalType)),
    Cell(Position(4), Content(NominalSchema[Type](), DateType))
  )

  val result16 = List(
    Cell(Position(1), Content(NominalSchema[Type](), OrdinalType)),
    Cell(Position(2), Content(NominalSchema[Type](), NumericType)),
    Cell(Position(3), Content(NominalSchema[Type](), CategoricalType)),
    Cell(Position(4), Content(NominalSchema[Type](), DateType))
  )

  val result17 = List(
    Cell(Position("bar", "xyz"), Content(NominalSchema[Type](), MixedType)),
    Cell(Position("baz", "xyz"), Content(NominalSchema[Type](), MixedType)),
    Cell(Position("foo", "xyz"), Content(NominalSchema[Type](), MixedType)),
    Cell(Position("qux", "xyz"), Content(NominalSchema[Type](), CategoricalType))
  )

  val result18 = List(
    Cell(Position("bar", "xyz"), Content(NominalSchema[Type](), MixedType)),
    Cell(Position("baz", "xyz"), Content(NominalSchema[Type](), MixedType)),
    Cell(Position("foo", "xyz"), Content(NominalSchema[Type](), MixedType)),
    Cell(Position("qux", "xyz"), Content(NominalSchema[Type](), OrdinalType))
  )

  val result19 = List(Cell(Position("xyz"), Content(NominalSchema[Type](), MixedType)))

  val result20 = List(Cell(Position("xyz"), Content(NominalSchema[Type](), MixedType)))

  val result21 = List(
    Cell(Position("bar", 1), Content(NominalSchema[Type](), CategoricalType)),
    Cell(Position("bar", 2), Content(NominalSchema[Type](), NumericType)),
    Cell(Position("bar", 3), Content(NominalSchema[Type](), CategoricalType)),
    Cell(Position("baz", 1), Content(NominalSchema[Type](), CategoricalType)),
    Cell(Position("baz", 2), Content(NominalSchema[Type](), NumericType)),
    Cell(Position("foo", 1), Content(NominalSchema[Type](), CategoricalType)),
    Cell(Position("foo", 2), Content(NominalSchema[Type](), NumericType)),
    Cell(Position("foo", 3), Content(NominalSchema[Type](), CategoricalType)),
    Cell(Position("foo", 4), Content(NominalSchema[Type](), DateType)),
    Cell(Position("qux", 1), Content(NominalSchema[Type](), CategoricalType))
  )

  val result22 = List(
    Cell(Position("bar", 1), Content(NominalSchema[Type](), OrdinalType)),
    Cell(Position("bar", 2), Content(NominalSchema[Type](), ContinuousType)),
    Cell(Position("bar", 3), Content(NominalSchema[Type](), OrdinalType)),
    Cell(Position("baz", 1), Content(NominalSchema[Type](), OrdinalType)),
    Cell(Position("baz", 2), Content(NominalSchema[Type](), DiscreteType)),
    Cell(Position("foo", 1), Content(NominalSchema[Type](), OrdinalType)),
    Cell(Position("foo", 2), Content(NominalSchema[Type](), ContinuousType)),
    Cell(Position("foo", 3), Content(NominalSchema[Type](), NominalType)),
    Cell(Position("foo", 4), Content(NominalSchema[Type](), DateType)),
    Cell(Position("qux", 1), Content(NominalSchema[Type](), OrdinalType))
  )
}

class TestScaldingMatrixTypes extends TestMatrixTypes {

  "A Matrix.types" should "return its first over types in 1D" in {
    toPipe(data1)
      .types(Over(_1), Default())(false)
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its first over specific types in 1D" in {
    toPipe(data1)
      .types(Over(_1), Default(12))(true)
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its first over types in 2D" in {
    toPipe(data2)
      .types(Over(_1), Default())(false)
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its first over specific types in 2D" in {
    toPipe(data2)
      .types(Over(_1), Default(12))(true)
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its first along types in 2D" in {
    toPipe(data2)
      .types(Along(_1), Default())(false)
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its first along specific types in 2D" in {
    toPipe(data2)
      .types(Along(_1), Default(12))(true)
      .toList.sortBy(_.position) shouldBe result6
  }

  it should "return its second over types in 2D" in {
    toPipe(data2)
      .types(Over(_2), Default())(false)
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its second over specific types in 2D" in {
    toPipe(data2)
      .types(Over(_2), Default(12))(true)
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its second along types in 2D" in {
    toPipe(data2)
      .types(Along(_2), Default())(false)
      .toList.sortBy(_.position) shouldBe result9
  }

  it should "return its second along specific types in 2D" in {
    toPipe(data2)
      .types(Along(_2), Default(12))(true)
      .toList.sortBy(_.position) shouldBe result10
  }

  it should "return its first over types in 3D" in {
    toPipe(data3)
      .types(Over(_1), Default())(false)
      .toList.sortBy(_.position) shouldBe result11
  }

  it should "return its first over specific types in 3D" in {
    toPipe(data3)
      .types(Over(_1), Default(12))(true)
      .toList.sortBy(_.position) shouldBe result12
  }

  it should "return its first along types in 3D" in {
    toPipe(data3)
      .types(Along(_1), Default())(false)
      .toList.sortBy(_.position) shouldBe result13
  }

  it should "return its first along specific types in 3D" in {
    toPipe(data3)
      .types(Along(_1), Default(12))(true)
      .toList.sortBy(_.position) shouldBe result14
  }

  it should "return its second over types in 3D" in {
    toPipe(data3)
      .types(Over(_2), Default())(false)
      .toList.sortBy(_.position) shouldBe result15
  }

  it should "return its second over specific types in 3D" in {
    toPipe(data3)
      .types(Over(_2), Default(12))(true)
      .toList.sortBy(_.position) shouldBe result16
  }

  it should "return its second along types in 3D" in {
    toPipe(data3)
      .types(Along(_2), Default())(false)
      .toList.sortBy(_.position) shouldBe result17
  }

  it should "return its second along specific types in 3D" in {
    toPipe(data3)
      .types(Along(_2), Default(12))(true)
      .toList.sortBy(_.position) shouldBe result18
  }

  it should "return its third over types in 3D" in {
    toPipe(data3)
      .types(Over(_3), Default())(false)
      .toList.sortBy(_.position) shouldBe result19
  }

  it should "return its third over specific types in 3D" in {
    toPipe(data3)
      .types(Over(_3), Default(12))(true)
      .toList.sortBy(_.position) shouldBe result20
  }

  it should "return its third along types in 3D" in {
    toPipe(data3)
      .types(Along(_3), Default())(false)
      .toList.sortBy(_.position) shouldBe result21
  }

  it should "return its third along specific types in 3D" in {
    toPipe(data3)
      .types(Along(_3), Default(12))(true)
      .toList.sortBy(_.position) shouldBe result22
  }
}

class TestSparkMatrixTypes extends TestMatrixTypes {

  "A Matrix.types" should "return its first over types in 1D" in {
    toRDD(data1)
      .types(Over(_1), Default())(false)
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its first over specific types in 1D" in {
    toRDD(data1)
      .types(Over(_1), Default(12))(true)
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its first over types in 2D" in {
    toRDD(data2)
      .types(Over(_1), Default())(false)
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its first over specific types in 2D" in {
    toRDD(data2)
      .types(Over(_1), Default(12))(true)
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its first along types in 2D" in {
    toRDD(data2)
      .types(Along(_1), Default())(false)
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its first along specific types in 2D" in {
    toRDD(data2)
      .types(Along(_1), Default(12))(true)
      .toList.sortBy(_.position) shouldBe result6
  }

  it should "return its second over types in 2D" in {
    toRDD(data2)
      .types(Over(_2), Default())(false)
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its second over specific types in 2D" in {
    toRDD(data2)
      .types(Over(_2), Default(12))(true)
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its second along types in 2D" in {
    toRDD(data2)
      .types(Along(_2), Default())(false)
      .toList.sortBy(_.position) shouldBe result9
  }

  it should "return its second along specific types in 2D" in {
    toRDD(data2)
      .types(Along(_2), Default(12))(true)
      .toList.sortBy(_.position) shouldBe result10
  }

  it should "return its first over types in 3D" in {
    toRDD(data3)
      .types(Over(_1), Default())(false)
      .toList.sortBy(_.position) shouldBe result11
  }

  it should "return its first over specific types in 3D" in {
    toRDD(data3)
      .types(Over(_1), Default(12))(true)
      .toList.sortBy(_.position) shouldBe result12
  }

  it should "return its first along types in 3D" in {
    toRDD(data3)
      .types(Along(_1), Default())(false)
      .toList.sortBy(_.position) shouldBe result13
  }

  it should "return its first along specific types in 3D" in {
    toRDD(data3)
      .types(Along(_1), Default(12))(true)
      .toList.sortBy(_.position) shouldBe result14
  }

  it should "return its second over types in 3D" in {
    toRDD(data3)
      .types(Over(_2), Default())(false)
      .toList.sortBy(_.position) shouldBe result15
  }

  it should "return its second over specific types in 3D" in {
    toRDD(data3)
      .types(Over(_2), Default(12))(true)
      .toList.sortBy(_.position) shouldBe result16
  }

  it should "return its second along types in 3D" in {
    toRDD(data3)
      .types(Along(_2), Default())(false)
      .toList.sortBy(_.position) shouldBe result17
  }

  it should "return its second along specific types in 3D" in {
    toRDD(data3)
      .types(Along(_2), Default(12))(true)
      .toList.sortBy(_.position) shouldBe result18
  }

  it should "return its third over types in 3D" in {
    toRDD(data3)
      .types(Over(_3), Default())(false)
      .toList.sortBy(_.position) shouldBe result19
  }

  it should "return its third over specific types in 3D" in {
    toRDD(data3)
      .types(Over(_3), Default(12))(true)
      .toList.sortBy(_.position) shouldBe result20
  }

  it should "return its third along types in 3D" in {
    toRDD(data3)
      .types(Along(_3), Default())(false)
      .toList.sortBy(_.position) shouldBe result21
  }

  it should "return its third along specific types in 3D" in {
    toRDD(data3)
      .types(Along(_3), Default(12))(true)
      .toList.sortBy(_.position) shouldBe result22
  }
}

trait TestMatrixSize extends TestMatrix {

  val dataA = List(
    Cell(Position(1, 1), Content(OrdinalSchema[String](), "a")),
    Cell(Position(2, 2), Content(OrdinalSchema[String](), "b")),
    Cell(Position(3, 3), Content(OrdinalSchema[String](), "c"))
  )

  val result1 = List(Cell(Position(1), Content(DiscreteSchema[Long](), 4)))

  val result2 = List(Cell(Position(1), Content(DiscreteSchema[Long](), 4)))

  val result3 = List(Cell(Position(1), Content(DiscreteSchema[Long](), 4)))

  val result4 = List(Cell(Position(1), Content(DiscreteSchema[Long](), data2.length)))

  val result5 = List(Cell(Position(2), Content(DiscreteSchema[Long](), 4)))

  val result6 = List(Cell(Position(2), Content(DiscreteSchema[Long](), data2.length)))

  val result7 = List(Cell(Position(1), Content(DiscreteSchema[Long](), 4)))

  val result8 = List(Cell(Position(1), Content(DiscreteSchema[Long](), data3.length)))

  val result9 = List(Cell(Position(2), Content(DiscreteSchema[Long](), 4)))

  val result10 = List(Cell(Position(2), Content(DiscreteSchema[Long](), data3.length)))

  val result11 = List(Cell(Position(3), Content(DiscreteSchema[Long](), 1)))

  val result12 = List(Cell(Position(3), Content(DiscreteSchema[Long](), data3.length)))

  val result13 = List(Cell(Position(2), Content(DiscreteSchema[Long](), 3)))
}

class TestScaldingMatrixSize extends TestMatrixSize {

  "A Matrix.size" should "return its first size in 1D" in {
    toPipe(data1)
      .size(_1, false, Default())
      .toList shouldBe result1
  }

  it should "return its first distinct size in 1D" in {
    toPipe(data1)
      .size(_1, true, Default(12))
      .toList shouldBe result2
  }

  it should "return its first size in 2D" in {
    toPipe(data2)
      .size(_1, false, Default())
      .toList shouldBe result3
  }

  it should "return its first distinct size in 2D" in {
    toPipe(data2)
      .size(_1, true, Default(12))
      .toList shouldBe result4
  }

  it should "return its second size in 2D" in {
    toPipe(data2)
      .size(_2, false, Default())
      .toList shouldBe result5
  }

  it should "return its second distinct size in 2D" in {
    toPipe(data2)
      .size(_2, true, Default(12))
      .toList shouldBe result6
  }

  it should "return its first size in 3D" in {
    toPipe(data3)
      .size(_1, false, Default())
      .toList shouldBe result7
  }

  it should "return its first distinct size in 3D" in {
    toPipe(data3)
      .size(_1, true, Default(12))
      .toList shouldBe result8
  }

  it should "return its second size in 3D" in {
    toPipe(data3)
      .size(_2, false, Default())
      .toList shouldBe result9
  }

  it should "return its second distinct size in 3D" in {
    toPipe(data3)
      .size(_2, true, Default(12))
      .toList shouldBe result10
  }

  it should "return its third size in 3D" in {
    toPipe(data3)
      .size(_3, false, Default())
      .toList shouldBe result11
  }

  it should "return its third distinct size in 3D" in {
    toPipe(data3)
      .size(_3, true, Default(12))
      .toList shouldBe result12
  }

  it should "return its distinct size" in {
    toPipe(dataA)
      .size(_2, true, Default())
      .toList shouldBe result13
  }
}

class TestSparkMatrixSize extends TestMatrixSize {

  "A Matrix.size" should "return its first size in 1D" in {
    toRDD(data1)
      .size(_1, false, Default())
      .toList shouldBe result1
  }

  it should "return its first distinct size in 1D" in {
    toRDD(data1)
      .size(_1, true, Default(12))
      .toList shouldBe result2
  }

  it should "return its first size in 2D" in {
    toRDD(data2)
      .size(_1, false, Default())
      .toList shouldBe result3
  }

  it should "return its first distinct size in 2D" in {
    toRDD(data2)
      .size(_1, true, Default(12))
      .toList shouldBe result4
  }

  it should "return its second size in 2D" in {
    toRDD(data2)
      .size(_2, false, Default())
      .toList shouldBe result5
  }

  it should "return its second distinct size in 2D" in {
    toRDD(data2)
      .size(_2, true, Default(12))
      .toList shouldBe result6
  }

  it should "return its first size in 3D" in {
    toRDD(data3)
      .size(_1, false, Default())
      .toList shouldBe result7
  }

  it should "return its first distinct size in 3D" in {
    toRDD(data3)
      .size(_1, true, Default(12))
      .toList shouldBe result8
  }

  it should "return its second size in 3D" in {
    toRDD(data3)
      .size(_2, false, Default())
      .toList shouldBe result9
  }

  it should "return its second distinct size in 3D" in {
    toRDD(data3)
      .size(_2, true, Default(12))
      .toList shouldBe result10
  }

  it should "return its third size in 3D" in {
    toRDD(data3)
      .size(_3, false, Default())
      .toList shouldBe result11
  }

  it should "return its third distinct size in 3D" in {
    toRDD(data3)
      .size(_3, true, Default(12))
      .toList shouldBe result12
  }

  it should "return its distinct size" in {
    toRDD(dataA)
      .size(_2, true, Default())
      .toList shouldBe result13
  }
}

trait TestMatrixShape extends TestMatrix {

  val result1 = List(Cell(Position(1), Content(DiscreteSchema[Long](), 4)))

  val result2 = List(
    Cell(Position(1), Content(DiscreteSchema[Long](), 4)),
    Cell(Position(2), Content(DiscreteSchema[Long](), 4))
  )

  val result3 = List(
    Cell(Position(1), Content(DiscreteSchema[Long](), 4)),
    Cell(Position(2), Content(DiscreteSchema[Long](), 4)),
    Cell(Position(3), Content(DiscreteSchema[Long](), 1))
  )
}

class TestScaldingMatrixShape extends TestMatrixShape {

  "A Matrix.shape" should "return its shape in 1D" in {
    toPipe(data1)
      .shape(Default())
      .toList shouldBe result1
  }

  it should "return its shape in 2D" in {
    toPipe(data2)
      .shape(Default(12))
      .toList shouldBe result2
  }

  it should "return its shape in 3D" in {
    toPipe(data3)
      .shape(Default())
      .toList shouldBe result3
  }
}

class TestSparkMatrixShape extends TestMatrixShape {

  "A Matrix.shape" should "return its shape in 1D" in {
    toRDD(data1)
      .shape(Default())
      .toList shouldBe result1
  }

  it should "return its shape in 2D" in {
    toRDD(data2)
      .shape(Default(12))
      .toList shouldBe result2
  }

  it should "return its shape in 3D" in {
    toRDD(data3)
      .shape(Default())
      .toList shouldBe result3
  }
}

trait TestMatrixSlice extends TestMatrix {

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
    Cell(Position("baz", 2), Content(DiscreteSchema[Long](), 19)),
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
    Cell(Position("bar", 3), Content(OrdinalSchema[Long](), 19)),
    Cell(Position("qux", 1), Content(OrdinalSchema[String](), "12.56"))
  )

  val result5 = List(
    Cell(Position("bar", 2), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("baz", 2), Content(DiscreteSchema[Long](), 19)),
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
    Cell(Position("bar", 3), Content(OrdinalSchema[Long](), 19)),
    Cell(Position("baz", 1), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("foo", 1), Content(OrdinalSchema[String](), "3.14")),
    Cell(Position("foo", 3), Content(NominalSchema[String](), "9.42")),
    Cell(Position("qux", 1), Content(OrdinalSchema[String](), "12.56"))
  )

  val result7 = List(
    Cell(Position("bar", 2), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("baz", 2), Content(DiscreteSchema[Long](), 19)),
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
    Cell(Position("bar", 3), Content(OrdinalSchema[Long](), 19)),
    Cell(Position("baz", 1), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("foo", 1), Content(OrdinalSchema[String](), "3.14")),
    Cell(Position("foo", 3), Content(NominalSchema[String](), "9.42")),
    Cell(Position("qux", 1), Content(OrdinalSchema[String](), "12.56"))
  )

  val result9 = List(
    Cell(Position("baz", 1), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", 2), Content(DiscreteSchema[Long](), 19)),
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
    Cell(Position("bar", 3), Content(OrdinalSchema[Long](), 19)),
    Cell(Position("qux", 1), Content(OrdinalSchema[String](), "12.56"))
  )

  val result11 = List(
    Cell(Position("baz", 1, "xyz"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", 2, "xyz"), Content(DiscreteSchema[Long](), 19)),
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
    Cell(Position("bar", 3, "xyz"), Content(OrdinalSchema[Long](), 19)),
    Cell(Position("qux", 1, "xyz"), Content(OrdinalSchema[String](), "12.56"))
  )

  val result13 = List(
    Cell(Position("bar", 2, "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("baz", 2, "xyz"), Content(DiscreteSchema[Long](), 19)),
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
    Cell(Position("bar", 3, "xyz"), Content(OrdinalSchema[Long](), 19)),
    Cell(Position("baz", 1, "xyz"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("foo", 1, "xyz"), Content(OrdinalSchema[String](), "3.14")),
    Cell(Position("foo", 3, "xyz"), Content(NominalSchema[String](), "9.42")),
    Cell(Position("qux", 1, "xyz"), Content(OrdinalSchema[String](), "12.56"))
  )

  val result15 = List(
    Cell(Position("bar", 2, "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("baz", 2, "xyz"), Content(DiscreteSchema[Long](), 19)),
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
    Cell(Position("bar", 3, "xyz"), Content(OrdinalSchema[Long](), 19)),
    Cell(Position("baz", 1, "xyz"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("foo", 1, "xyz"), Content(OrdinalSchema[String](), "3.14")),
    Cell(Position("foo", 3, "xyz"), Content(NominalSchema[String](), "9.42")),
    Cell(Position("qux", 1, "xyz"), Content(OrdinalSchema[String](), "12.56"))
  )

  val result17 = List(
    Cell(Position("baz", 1, "xyz"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", 2, "xyz"), Content(DiscreteSchema[Long](), 19)),
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
    Cell(Position("bar", 3, "xyz"), Content(OrdinalSchema[Long](), 19)),
    Cell(Position("qux", 1, "xyz"), Content(OrdinalSchema[String](), "12.56"))
  )

  val result19 = List()

  val result20 = data3.sortBy(_.position)

  val result21 = List(
    Cell(Position("bar", 1, "xyz"), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("bar", 2, "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", 3, "xyz"), Content(OrdinalSchema[Long](), 19)),
    Cell(Position("baz", 2, "xyz"), Content(DiscreteSchema[Long](), 19)),
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

class TestScaldingMatrixSlice extends TestMatrixSlice {

  "A Matrix.slice" should "return its first over slice in 1D" in {
    toPipe(data1)
      .slice(Over(_1), InMemory())(false, List("bar", "qux"))
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its first over inverse slice in 1D" in {
    toPipe(data1)
      .slice(Over(_1), Default())(true, List("bar", "qux"))
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its first over slice in 2D" in {
    toPipe(data2)
      .slice(Over(_1), Default(12))(false, List("bar", "qux"))
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its first over inverse slice in 2D" in {
    toPipe(data2)
      .slice(Over(_1), Unbalanced(12))(true, List("bar", "qux"))
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its first along slice in 2D" in {
    toPipe(data2)
      .slice(Along(_1), InMemory())(false, List(1, 3))
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its first along inverse slice in 2D" in {
    toPipe(data2)
      .slice(Along(_1), Default())(true, List(1, 3))
      .toList.sortBy(_.position) shouldBe result6
  }

  it should "return its second over slice in 2D" in {
    toPipe(data2)
      .slice(Over(_2), Default(12))(false, List(1, 3))
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its second over inverse slice in 2D" in {
    toPipe(data2)
      .slice(Over(_2), Unbalanced(12))(true, List(1, 3))
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its second along slice in 2D" in {
    toPipe(data2)
      .slice(Along(_2), InMemory())(false, List("bar", "qux"))
      .toList.sortBy(_.position) shouldBe result9
  }

  it should "return its second along inverse slice in 2D" in {
    toPipe(data2)
      .slice(Along(_2), Default())(true, List("bar", "qux"))
      .toList.sortBy(_.position) shouldBe result10
  }

  it should "return its first over slice in 3D" in {
    toPipe(data3)
      .slice(Over(_1), Default(12))(false, List("bar", "qux"))
      .toList.sortBy(_.position) shouldBe result11
  }

  it should "return its first over inverse slice in 3D" in {
    toPipe(data3)
      .slice(Over(_1), Unbalanced(12))(true, List("bar", "qux"))
      .toList.sortBy(_.position) shouldBe result12
  }

  it should "return its first along slice in 3D" in {
    toPipe(data3)
      .slice(Along(_1), InMemory())(false, List(Position(1, "xyz"), Position(3, "xyz")))
      .toList.sortBy(_.position) shouldBe result13
  }

  it should "return its first along inverse slice in 3D" in {
    toPipe(data3)
      .slice(Along(_1), Default())(true, List(Position(1, "xyz"), Position(3, "xyz")))
      .toList.sortBy(_.position) shouldBe result14
  }

  it should "return its second over slice in 3D" in {
    toPipe(data3)
      .slice(Over(_2), Default(12))(false, List(1, 3))
      .toList.sortBy(_.position) shouldBe result15
  }

  it should "return its second over inverse slice in 3D" in {
    toPipe(data3)
      .slice(Over(_2), Unbalanced(12))(true, List(1, 3))
      .toList.sortBy(_.position) shouldBe result16
  }

  it should "return its second along slice in 3D" in {
    toPipe(data3)
      .slice(Along(_2), InMemory())(false, List(Position("bar", "xyz"), Position("qux", "xyz")))
      .toList.sortBy(_.position) shouldBe result17
  }

  it should "return its second along inverse slice in 3D" in {
    toPipe(data3)
      .slice(Along(_2), Default())(true, List(Position("bar", "xyz"), Position("qux", "xyz")))
      .toList.sortBy(_.position) shouldBe result18
  }

  it should "return its third over slice in 3D" in {
    toPipe(data3)
      .slice(Over(_3), Default(12))(false, "xyz")
      .toList.sortBy(_.position) shouldBe result19
  }

  it should "return its third over inverse slice in 3D" in {
    toPipe(data3)
      .slice(Over(_3), Unbalanced(12))(true, "xyz")
      .toList.sortBy(_.position) shouldBe result20
  }

  it should "return its third along slice in 3D" in {
    toPipe(data3)
      .slice(Along(_3), InMemory())(false, List(Position("foo", 3), Position("baz", 1)))
      .toList.sortBy(_.position) shouldBe result21
  }

  it should "return its third along inverse slice in 3D" in {
    toPipe(data3)
      .slice(Along(_3), Default())(true, List(Position("foo", 3), Position("baz", 1)))
      .toList.sortBy(_.position) shouldBe result22
  }

  it should "return empty data - InMemory" in {
    toPipe(data3)
      .slice(Along(_3), InMemory())(true, List.empty[Position[_2]])
      .toList.sortBy(_.position) shouldBe List()
  }

  it should "return all data - InMemory" in {
    toPipe(data3)
      .slice(Along(_3), InMemory())(false, List.empty[Position[_2]])
      .toList.sortBy(_.position) shouldBe data3.sortBy(_.position)
  }

  it should "return empty data - Default" in {
    toPipe(data3)
      .slice(Along(_3), Default())(true, List.empty[Position[_2]])
      .toList.sortBy(_.position) shouldBe List()
  }

  it should "return all data - Default" in {
    toPipe(data3)
      .slice(Along(_3), Default())(false, List.empty[Position[_2]])
      .toList.sortBy(_.position) shouldBe data3.sortBy(_.position)
  }
}

class TestSparkMatrixSlice extends TestMatrixSlice {

  "A Matrix.slice" should "return its first over slice in 1D" in {
    toRDD(data1)
      .slice(Over(_1), InMemory())(false, List("bar", "qux"))
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its first over inverse slice in 1D" in {
    toRDD(data1)
      .slice(Over(_1), Default())(true, List("bar", "qux"))
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its first over slice in 2D" in {
    toRDD(data2)
      .slice(Over(_1), Default(12))(false, List("bar", "qux"))
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its first over inverse slice in 2D" in {
    toRDD(data2)
      .slice(Over(_1), InMemory())(true, List("bar", "qux"))
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its first along slice in 2D" in {
    toRDD(data2)
      .slice(Along(_1), Default())(false, List(1, 3))
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its first along inverse slice in 2D" in {
    toRDD(data2)
      .slice(Along(_1), Default(12))(true, List(1, 3))
      .toList.sortBy(_.position) shouldBe result6
  }

  it should "return its second over slice in 2D" in {
    toRDD(data2)
      .slice(Over(_2), InMemory())(false, List(1, 3))
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its second over inverse slice in 2D" in {
    toRDD(data2)
      .slice(Over(_2), Default())(true, List(1, 3))
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its second along slice in 2D" in {
    toRDD(data2)
      .slice(Along(_2), Default(12))(false, List("bar", "qux"))
      .toList.sortBy(_.position) shouldBe result9
  }

  it should "return its second along inverse slice in 2D" in {
    toRDD(data2)
      .slice(Along(_2), InMemory())(true, List("bar", "qux"))
      .toList.sortBy(_.position) shouldBe result10
  }

  it should "return its first over slice in 3D" in {
    toRDD(data3)
      .slice(Over(_1), Default())(false, List("bar", "qux"))
      .toList.sortBy(_.position) shouldBe result11
  }

  it should "return its first over inverse slice in 3D" in {
    toRDD(data3)
      .slice(Over(_1), Default(12))(true, List("bar", "qux"))
      .toList.sortBy(_.position) shouldBe result12
  }

  it should "return its first along slice in 3D" in {
    toRDD(data3)
      .slice(Along(_1), InMemory())(false, List(Position(1, "xyz"), Position(3, "xyz")))
      .toList.sortBy(_.position) shouldBe result13
  }

  it should "return its first along inverse slice in 3D" in {
    toRDD(data3)
      .slice(Along(_1), Default())(true, List(Position(1, "xyz"), Position(3, "xyz")))
      .toList.sortBy(_.position) shouldBe result14
  }

  it should "return its second over slice in 3D" in {
    toRDD(data3)
      .slice(Over(_2), Default(12))(false, List(1, 3))
      .toList.sortBy(_.position) shouldBe result15
  }

  it should "return its second over inverse slice in 3D" in {
    toRDD(data3)
      .slice(Over(_2), InMemory())(true, List(1, 3))
      .toList.sortBy(_.position) shouldBe result16
  }

  it should "return its second along slice in 3D" in {
    toRDD(data3)
      .slice(Along(_2), Default())(false, List(Position("bar", "xyz"), Position("qux", "xyz")))
      .toList.sortBy(_.position) shouldBe result17
  }

  it should "return its second along inverse slice in 3D" in {
    toRDD(data3)
      .slice(Along(_2), Default(12))(true, List(Position("bar", "xyz"), Position("qux", "xyz")))
      .toList.sortBy(_.position) shouldBe result18
  }

  it should "return its third over slice in 3D" in {
    toRDD(data3)
      .slice(Over(_3), InMemory())(false, "xyz")
      .toList.sortBy(_.position) shouldBe result19
  }

  it should "return its third over inverse slice in 3D" in {
    toRDD(data3)
      .slice(Over(_3), Default())(true, "xyz")
      .toList.sortBy(_.position) shouldBe result20
  }

  it should "return its third along slice in 3D" in {
    toRDD(data3)
      .slice(Along(_3), Default(12))(false, List(Position("foo", 3), Position("baz", 1)))
      .toList.sortBy(_.position) shouldBe result21
  }

  it should "return its third along inverse slice in 3D" in {
    toRDD(data3)
      .slice(Along(_3), InMemory())(true, List(Position("foo", 3), Position("baz", 1)))
      .toList.sortBy(_.position) shouldBe result22
  }

  it should "return empty data - Default" in {
    toRDD(data3)
      .slice(Along(_3), Default())(true, List.empty[Position[_2]])
      .toList.sortBy(_.position) shouldBe List()
  }

  it should "return all data - Default" in {
    toRDD(data3)
      .slice(Along(_3), Default())(false, List.empty[Position[_2]])
      .toList.sortBy(_.position) shouldBe data3.sortBy(_.position)
  }
}

trait TestMatrixWhich extends TestMatrix {

  val result1 = List(Position("qux"))

  val result2 = List(Position("qux"))

  val result3 = List(Position("foo"), Position("qux"))

  val result4 = List(Position("foo", 3), Position("foo", 4), Position("qux", 1))

  val result5 = List(Position("qux", 1))

  val result6 = List(Position("foo", 4))

  val result7 = List(Position("foo", 4))

  val result8 = List(Position("qux", 1))

  val result9 = List(Position("foo", 1), Position("foo", 2), Position("qux", 1))

  val result10 = List(Position("bar", 2), Position("baz", 2), Position("foo", 2), Position("foo", 4))

  val result11 = List(Position("bar", 2), Position("baz", 2), Position("foo", 2), Position("foo", 4))

  val result12 = List(Position("foo", 1), Position("foo", 2), Position("qux", 1))

  val result13 = List(Position("foo", 3, "xyz"), Position("foo", 4, "xyz"), Position("qux", 1, "xyz"))

  val result14 = List(Position("qux", 1, "xyz"))

  val result15 = List(Position("foo", 4, "xyz"))

  val result16 = List(Position("foo", 4, "xyz"))

  val result17 = List(Position("qux", 1, "xyz"))

  val result18 = List(Position("foo", 3, "xyz"), Position("foo", 4, "xyz"), Position("qux", 1, "xyz"))

  val result19 = List(Position("qux", 1, "xyz"))

  val result20 = List(Position("foo", 1, "xyz"), Position("foo", 2, "xyz"), Position("qux", 1, "xyz"))

  val result21 = List(
    Position("bar", 2, "xyz"),
    Position("baz", 2, "xyz"),
    Position("foo", 2, "xyz"),
    Position("foo", 4, "xyz")
  )

  val result22 = List(
    Position("bar", 2, "xyz"),
    Position("baz", 2, "xyz"),
    Position("foo", 2, "xyz"),
    Position("foo", 4, "xyz")
  )

  val result23 = List(Position("foo", 1, "xyz"), Position("foo", 2, "xyz"), Position("qux", 1, "xyz"))

  val result24 = data3.map(_.position).sorted

  val result25 = List(Position("foo", 2, "xyz"), Position("qux", 1, "xyz"))
}

object TestMatrixWhich {

  def predicate[P <: Nat](cell: Cell[P]): Boolean =
    (cell.content.schema == NominalSchema[String]()) ||
    (cell.content.value.codec.isInstanceOf[DateCodec]) ||
    (cell.content.value equ "12.56")
}

class TestScaldingMatrixWhich extends TestMatrixWhich {

  "A Matrix.which" should "return its coordinates in 1D" in {
    toPipe(data1)
      .which(TestMatrixWhich.predicate)
      .toList.sorted shouldBe result1
  }

  it should "return its first over coordinates in 1D" in {
    toPipe(data1)
      .whichByPosition(Over(_1), InMemory())((List("bar", "qux"), (c: Cell[_1]) => TestMatrixWhich.predicate(c)))
      .toList.sorted shouldBe result2
  }

  it should "return its first over multiple coordinates in 1D" in {
    toPipe(data1)
      .whichByPosition(Over(_1), Default())(
        List(
          (List("bar", "qux"), (c: Cell[_1]) => TestMatrixWhich.predicate(c)),
          (List("foo"), (c: Cell[_1]) => !TestMatrixWhich.predicate(c))
        )
      )
      .toList.sorted shouldBe result3
  }

  it should "return its coordinates in 2D" in {
    toPipe(data2)
      .which(TestMatrixWhich.predicate)
      .toList.sorted shouldBe result4
  }

  it should "return its first over coordinates in 2D" in {
    toPipe(data2)
      .whichByPosition(Over(_1), Default(12))((List("bar", "qux"), (c: Cell[_2]) => TestMatrixWhich.predicate(c)))
      .toList.sorted shouldBe result5
  }

  it should "return its first along coordinates in 2D" in {
    toPipe(data2)
      .whichByPosition(Along(_1), Unbalanced(12))((List(2, 4), (c: Cell[_2]) => TestMatrixWhich.predicate(c)))
      .toList.sorted shouldBe result6
  }

  it should "return its second over coordinates in 2D" in {
    toPipe(data2)
      .whichByPosition(Over(_2), InMemory())((List(2, 4), (c: Cell[_2]) => TestMatrixWhich.predicate(c)))
      .toList.sorted shouldBe result7
  }

  it should "return its second along coordinates in 2D" in {
    toPipe(data2)
      .whichByPosition(Along(_2), Default())((List("bar", "qux"), (c: Cell[_2]) => TestMatrixWhich.predicate(c)))
      .toList.sorted shouldBe result8
  }

  it should "return its first over multiple coordinates in 2D" in {
    toPipe(data2)
      .whichByPosition(Over(_1), Default(12))(
        List(
          (List("bar", "qux"), (c: Cell[_2]) => TestMatrixWhich.predicate(c)),
          (List("foo"), (c: Cell[_2]) => !TestMatrixWhich.predicate(c))
        )
      )
      .toList.sorted shouldBe result9
  }

  it should "return its first along multiple coordinates in 2D" in {
    toPipe(data2)
      .whichByPosition(Along(_1), Unbalanced(12))(
        List(
          (List(2, 4), (c: Cell[_2]) => TestMatrixWhich.predicate(c)),
          (List(2), (c: Cell[_2]) => !TestMatrixWhich.predicate(c))
        )
      )
      .toList.sorted shouldBe result10
  }

  it should "return its second over multiple coordinates in 2D" in {
    toPipe(data2)
      .whichByPosition(Over(_2), InMemory())(
        List(
          (List(2, 4), (c: Cell[_2]) => TestMatrixWhich.predicate(c)),
          (List(2), (c: Cell[_2]) => !TestMatrixWhich.predicate(c))
        )
      )
      .toList.sorted shouldBe result11
  }

  it should "return its second along multiple coordinates in 2D" in {
    toPipe(data2)
      .whichByPosition(Along(_2), Default())(
        List(
          (List("bar", "qux"), (c: Cell[_2]) => TestMatrixWhich.predicate(c)),
          (List("foo"), (c: Cell[_2]) => !TestMatrixWhich.predicate(c))
        )
      )
      .toList.sorted shouldBe result12
  }

  it should "return its coordinates in 3D" in {
    toPipe(data3)
      .which(TestMatrixWhich.predicate)
      .toList.sorted shouldBe result13
  }

  it should "return its first over coordinates in 3D" in {
    toPipe(data3)
      .whichByPosition(Over(_1), Default(12))((List("bar", "qux"), (c: Cell[_3]) => TestMatrixWhich.predicate(c)))
      .toList.sorted shouldBe result14
  }

  it should "return its first along coordinates in 3D" in {
    toPipe(data3)
      .whichByPosition(Along(_1), Unbalanced(12))(
        (List(Position(2, "xyz"), Position(4, "xyz")), (c: Cell[_3]) => TestMatrixWhich.predicate(c))
      )
      .toList.sorted shouldBe result15
  }

  it should "return its second over coordinates in 3D" in {
    toPipe(data3)
      .whichByPosition(Over(_2), InMemory())((List(2, 4), (c: Cell[_3]) => TestMatrixWhich.predicate(c)))
      .toList.sorted shouldBe result16
  }

  it should "return its second along coordinates in 3D" in {
    toPipe(data3)
      .whichByPosition(Along(_2), Default())(
        (List(Position("bar", "xyz"), Position("qux", "xyz")), (c: Cell[_3]) => TestMatrixWhich.predicate(c))
      )
      .toList.sorted shouldBe result17
  }

  it should "return its third over coordinates in 3D" in {
    toPipe(data3)
      .whichByPosition(Over(_3), Default(12))(("xyz", (c: Cell[_3]) => TestMatrixWhich.predicate(c)))
      .toList.sorted shouldBe result18
  }

  it should "return its third along coordinates in 3D" in {
    toPipe(data3)
      .whichByPosition(Along(_3), Unbalanced(12))(
        (List(Position("bar", 2), Position("qux", 1)), (c: Cell[_3]) => TestMatrixWhich.predicate(c))
      )
      .toList.sorted shouldBe result19
  }

  it should "return its first over multiple coordinates in 3D" in {
    toPipe(data3)
      .whichByPosition(Over(_1), InMemory())(
        List(
          (List("bar", "qux"), (c: Cell[_3]) => TestMatrixWhich.predicate(c)),
          (List("foo"), (c: Cell[_3]) => !TestMatrixWhich.predicate(c))
        )
      )
      .toList.sorted shouldBe result20
  }

  it should "return its first along multiple coordinates in 3D" in {
    toPipe(data3)
      .whichByPosition(Along(_1), Default())(
        List(
          (List(Position(2, "xyz"), Position(4, "xyz")), (c: Cell[_3]) => TestMatrixWhich.predicate(c)),
          (List(Position(2, "xyz")), (c: Cell[_3]) => !TestMatrixWhich.predicate(c))
        )
      )
      .toList.sorted shouldBe result21
  }

  it should "return its second over multiple coordinates in 3D" in {
    toPipe(data3)
      .whichByPosition(Over(_2), Default(12))(
        List(
          (List(2, 4), (c: Cell[_3]) => TestMatrixWhich.predicate(c)),
          (List(2), (c: Cell[_3]) => !TestMatrixWhich.predicate(c))
        )
      )
      .toList.sorted shouldBe result22
  }

  it should "return its second along multiple coordinates in 3D" in {
    toPipe(data3)
      .whichByPosition(Along(_2), Unbalanced(12))(
        List(
          (List(Position("bar", "xyz"), Position("qux", "xyz")), (c: Cell[_3]) => TestMatrixWhich.predicate(c)),
          (List(Position("foo", "xyz")), (c: Cell[_3]) => !TestMatrixWhich.predicate(c))
        )
      )
      .toList.sorted shouldBe result23
  }

  it should "return its third over multiple coordinates in 3D" in {
    toPipe(data3)
      .whichByPosition(Over(_3), InMemory())(
        List(
          ("xyz", (c: Cell[_3]) => TestMatrixWhich.predicate(c)),
          ("xyz", (c: Cell[_3]) => !TestMatrixWhich.predicate(c))
        )
      )
      .toList.sorted shouldBe result24
  }

  it should "return its third along multiple coordinates in 3D" in {
    toPipe(data3)
      .whichByPosition(Along(_3), Default())(
        List(
          (List(Position("foo", 1), Position("qux", 1)), (c: Cell[_3]) => TestMatrixWhich.predicate(c)),
          (List(Position("foo", 2)), (c: Cell[_3]) => !TestMatrixWhich.predicate(c))
        )
      )
      .toList.sorted shouldBe result25
  }

  it should "return empty data - InMemory" in {
    toPipe(data3)
      .whichByPosition(Along(_3), InMemory())(
        List((List.empty[Position[_2]], (c: Cell[_3]) => !TestMatrixWhich.predicate(c)))
      )
      .toList.sorted shouldBe List()
  }

  it should "return empty data - Default" in {
    toPipe(data3)
      .whichByPosition(Along(_3), Default())(
        List((List.empty[Position[_2]], (c: Cell[_3]) => !TestMatrixWhich.predicate(c)))
      )
      .toList.sorted shouldBe List()
  }
}

class TestSparkMatrixWhich extends TestMatrixWhich {

  "A Matrix.which" should "return its coordinates in 1D" in {
    toRDD(data1)
      .which(TestMatrixWhich.predicate)
      .toList.sorted shouldBe result1
  }

  it should "return its first over coordinates in 1D" in {
    toRDD(data1)
      .whichByPosition(Over(_1), InMemory())((List("bar", "qux"), (c: Cell[_1]) => TestMatrixWhich.predicate(c)))
      .toList.sorted shouldBe result2
  }

  it should "return its first over multiple coordinates in 1D" in {
    toRDD(data1)
      .whichByPosition(Over(_1), Default())(
        List(
          (List("bar", "qux"), (c: Cell[_1]) => TestMatrixWhich.predicate(c)),
          (List("foo"), (c: Cell[_1]) => !TestMatrixWhich.predicate(c))
        )
      )
      .toList.sorted shouldBe result3
  }

  it should "return its coordinates in 2D" in {
    toRDD(data2)
      .which(TestMatrixWhich.predicate)
      .toList.sorted shouldBe result4
  }

  it should "return its first over coordinates in 2D" in {
    toRDD(data2)
      .whichByPosition(Over(_1), Default(12))((List("bar", "qux"), (c: Cell[_2]) => TestMatrixWhich.predicate(c)))
      .toList.sorted shouldBe result5
  }

  it should "return its first along coordinates in 2D" in {
    toRDD(data2)
      .whichByPosition(Along(_1), InMemory())((List(2, 4), (c: Cell[_2]) => TestMatrixWhich.predicate(c)))
      .toList.sorted shouldBe result6
  }

  it should "return its second over coordinates in 2D" in {
    toRDD(data2)
      .whichByPosition(Over(_2), Default())((List(2, 4), (c: Cell[_2]) => TestMatrixWhich.predicate(c)))
      .toList.sorted shouldBe result7
  }

  it should "return its second along coordinates in 2D" in {
    toRDD(data2)
      .whichByPosition(Along(_2), Default(12))((List("bar", "qux"), (c: Cell[_2]) => TestMatrixWhich.predicate(c)))
      .toList.sorted shouldBe result8
  }

  it should "return its first over multiple coordinates in 2D" in {
    toRDD(data2)
      .whichByPosition(Over(_1), InMemory())(
        List(
          (List("bar", "qux"), (c: Cell[_2]) => TestMatrixWhich.predicate(c)),
          (List("foo"), (c: Cell[_2]) => !TestMatrixWhich.predicate(c))
        )
      )
      .toList.sorted shouldBe result9
  }

  it should "return its first along multiple coordinates in 2D" in {
    toRDD(data2)
      .whichByPosition(Along(_1), Default())(
        List(
          (List(2, 4), (c: Cell[_2]) => TestMatrixWhich.predicate(c)),
          (List(2), (c: Cell[_2]) => !TestMatrixWhich.predicate(c))
        )
      )
      .toList.sorted shouldBe result10
  }

  it should "return its second over multiple coordinates in 2D" in {
    toRDD(data2)
      .whichByPosition(Over(_2), Default(12))(
        List(
          (List(2, 4), (c: Cell[_2]) => TestMatrixWhich.predicate(c)),
          (List(2), (c: Cell[_2]) => !TestMatrixWhich.predicate(c))
        )
      )
      .toList.sorted shouldBe result11
  }

  it should "return its second along multiple coordinates in 2D" in {
    toRDD(data2)
      .whichByPosition(Along(_2), InMemory())(
        List(
          (List("bar", "qux"), (c: Cell[_2]) => TestMatrixWhich.predicate(c)),
          (List("foo"), (c: Cell[_2]) => !TestMatrixWhich.predicate(c))
        )
      )
      .toList.sorted shouldBe result12
  }

  it should "return its coordinates in 3D" in {
    toRDD(data3)
      .which(TestMatrixWhich.predicate)
      .toList.sorted shouldBe result13
  }

  it should "return its first over coordinates in 3D" in {
    toRDD(data3)
      .whichByPosition(Over(_1), Default())((List("bar", "qux"), (c: Cell[_3]) => TestMatrixWhich.predicate(c)))
      .toList.sorted shouldBe result14
  }

  it should "return its first along coordinates in 3D" in {
    toRDD(data3)
      .whichByPosition(Along(_1), Default(12))(
        (List(Position(2, "xyz"), Position(4, "xyz")), (c: Cell[_3]) => TestMatrixWhich.predicate(c))
      )
      .toList.sorted shouldBe result15
  }

  it should "return its second over coordinates in 3D" in {
    toRDD(data3)
      .whichByPosition(Over(_2), InMemory())((List(2, 4), (c: Cell[_3]) => TestMatrixWhich.predicate(c)))
      .toList.sorted shouldBe result16
  }

  it should "return its second along coordinates in 3D" in {
    toRDD(data3)
      .whichByPosition(Along(_2), Default())(
        (List(Position("bar", "xyz"), Position("qux", "xyz")), (c: Cell[_3]) => TestMatrixWhich.predicate(c))
      )
      .toList.sorted shouldBe result17
  }

  it should "return its third over coordinates in 3D" in {
    toRDD(data3)
      .whichByPosition(Over(_3), Default(12))(("xyz", (c: Cell[_3]) => TestMatrixWhich.predicate(c)))
      .toList.sorted shouldBe result18
  }

  it should "return its third along coordinates in 3D" in {
    toRDD(data3)
      .whichByPosition(Along(_3), InMemory())(
        (List(Position("bar", 2), Position("qux", 1)), (c: Cell[_3]) => TestMatrixWhich.predicate(c))
      )
      .toList.sorted shouldBe result19
  }

  it should "return its first over multiple coordinates in 3D" in {
    toRDD(data3)
      .whichByPosition(Over(_1), Default())(
        List(
          (List("bar", "qux"), (c: Cell[_3]) => TestMatrixWhich.predicate(c)),
          (List("foo"), (c: Cell[_3]) => !TestMatrixWhich.predicate(c))
        )
      )
      .toList.sorted shouldBe result20
  }

  it should "return its first along multiple coordinates in 3D" in {
    toRDD(data3)
      .whichByPosition(Along(_1), Default(12))(
        List(
          (List(Position(2, "xyz"), Position(4, "xyz")), (c: Cell[_3]) => TestMatrixWhich.predicate(c)),
          (List(Position(2, "xyz")), (c: Cell[_3]) => !TestMatrixWhich.predicate(c))
        )
      )
      .toList.sorted shouldBe result21
  }

  it should "return its second over multiple coordinates in 3D" in {
    toRDD(data3)
      .whichByPosition(Over(_2), InMemory())(
        List(
          (List(2, 4), (c: Cell[_3]) => TestMatrixWhich.predicate(c)),
          (List(2), (c: Cell[_3]) => !TestMatrixWhich.predicate(c))
        )
      )
      .toList.sorted shouldBe result22
  }

  it should "return its second along multiple coordinates in 3D" in {
    toRDD(data3)
      .whichByPosition(Along(_2), Default())(
        List(
          (List(Position("bar", "xyz"), Position("qux", "xyz")), (c: Cell[_3]) => TestMatrixWhich.predicate(c)),
          (List(Position("foo", "xyz")), (c: Cell[_3]) => !TestMatrixWhich.predicate(c))
        )
      )
      .toList.sorted shouldBe result23
  }

  it should "return its third over multiple coordinates in 3D" in {
    toRDD(data3)
      .whichByPosition(Over(_3), Default(12))(
        List(
          ("xyz", (c: Cell[_3]) => TestMatrixWhich.predicate(c)),
          ("xyz", (c: Cell[_3]) => !TestMatrixWhich.predicate(c))
        )
      )
      .toList.sorted shouldBe result24
  }

  it should "return its third along multiple coordinates in 3D" in {
    toRDD(data3)
      .whichByPosition(Along(_3), InMemory())(
        List(
          (List(Position("foo", 1), Position("qux", 1)), (c: Cell[_3]) => TestMatrixWhich.predicate(c)),
          (List(Position("foo", 2)), (c: Cell[_3]) => !TestMatrixWhich.predicate(c))
        )
      )
      .toList.sorted shouldBe result25
  }

  it should "return empty data - Default" in {
    toRDD(data3)
      .whichByPosition(Along(_3), Default())(
        List((List.empty[Position[_2]], (c: Cell[_3]) => !TestMatrixWhich.predicate(c)))
      )
      .toList.sorted shouldBe List()
  }
}

trait TestMatrixGet extends TestMatrix {

  val result1 = List(Cell(Position("qux"), Content(OrdinalSchema[String](), "12.56")))

  val result2 = List(
    Cell(Position("foo", 3), Content(NominalSchema[String](), "9.42")),
    Cell(Position("qux", 1), Content(OrdinalSchema[String](), "12.56"))
  )

  val result3 = List(
    Cell(Position("foo", 3, "xyz"), Content(NominalSchema[String](), "9.42")),
    Cell(Position("qux", 1, "xyz"), Content(OrdinalSchema[String](), "12.56"))
  )
}

class TestScaldingMatrixGet extends TestMatrixGet {

  "A Matrix.get" should "return its cells in 1D" in {
    toPipe(data1)
      .get("qux", InMemory())
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its cells in 2D" in {
    toPipe(data2)
      .get(List(Position("foo", 3), Position("qux", 1), Position("baz", 4)), Default())
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its cells in 3D" in {
    toPipe(data3)
      .get(List(Position("foo", 3, "xyz"), Position("qux", 1, "xyz"), Position("baz", 4, "xyz")), Unbalanced(12))
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return empty data - InMemory" in {
    toPipe(data3)
      .get(List.empty[Position[_3]], InMemory())
      .toList.sortBy(_.position) shouldBe List()
  }

  it should "return empty data - Default" in {
    toPipe(data3)
      .get(List.empty[Position[_3]], Default(12))
      .toList.sortBy(_.position) shouldBe List()
  }
}

class TestSparkMatrixGet extends TestMatrixGet {

  "A Matrix.get" should "return its cells in 1D" in {
    toRDD(data1)
      .get("qux", Default())
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its cells in 2D" in {
    toRDD(data2)
      .get(List(Position("foo", 3), Position("qux", 1), Position("baz", 4)), Default(12))
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its cells in 3D" in {
    toRDD(data3)
      .get(List(Position("foo", 3, "xyz"), Position("qux", 1, "xyz"), Position("baz", 4, "xyz")), Default())
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return empty data - Default" in {
    toRDD(data3)
      .get(List.empty[Position[_3]], Default())
      .toList.sortBy(_.position) shouldBe List()
  }
}

trait TestMatrixCompact extends TestMatrix {
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
      Position(3) -> Content(OrdinalSchema[Long](), 19)
    ),
    Position("baz") -> Map(
      Position(1) -> Content(OrdinalSchema[String](), "9.42"),
      Position(2) -> Content(DiscreteSchema[Long](), 19)
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
      Position("baz") -> Content(DiscreteSchema[Long](), 19)
    ),
    Position(3) -> Map(
      Position("foo") -> Content(NominalSchema[String](), "9.42"),
      Position("bar") -> Content(OrdinalSchema[Long](), 19)
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
      Position("baz") -> Content(DiscreteSchema[Long](), 19)
    ),
    Position(3) -> Map(
      Position("foo") -> Content(NominalSchema[String](), "9.42"),
      Position("bar") -> Content(OrdinalSchema[Long](), 19)
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
      Position(3) -> Content(OrdinalSchema[Long](), 19)
    ),
    Position("baz") -> Map(
      Position(1) -> Content(OrdinalSchema[String](), "9.42"),
      Position(2) -> Content(DiscreteSchema[Long](), 19)
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
      Position(3, "xyz") -> Content(OrdinalSchema[Long](), 19)
    ),
    Position("baz") -> Map(
      Position(1, "xyz") -> Content(OrdinalSchema[String](), "9.42"),
      Position(2, "xyz") -> Content(DiscreteSchema[Long](), 19)
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
      Position("baz") -> Content(DiscreteSchema[Long](), 19)
    ),
    Position(3, "xyz") -> Map(
      Position("foo") -> Content(NominalSchema[String](), "9.42"),
      Position("bar") -> Content(OrdinalSchema[Long](), 19)
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
      Position("baz", "xyz") -> Content(DiscreteSchema[Long](), 19)
    ),
    Position(3) -> Map(
      Position("foo", "xyz") -> Content(NominalSchema[String](), "9.42"),
      Position("bar", "xyz") -> Content(OrdinalSchema[Long](), 19)
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
      Position(3) -> Content(OrdinalSchema[Long](), 19)
    ),
    Position("baz", "xyz") -> Map(
      Position(1) -> Content(OrdinalSchema[String](), "9.42"),
      Position(2) -> Content(DiscreteSchema[Long](), 19)
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
      Position("baz", 2) -> Content(DiscreteSchema[Long](), 19),
      Position("foo", 3) -> Content(NominalSchema[String](), "9.42"),
      Position("bar", 3) -> Content(OrdinalSchema[Long](), 19),
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
    Position("bar", 3) -> Map(Position("xyz") -> Content(OrdinalSchema[Long](), 19)),
    Position("baz", 1) -> Map(Position("xyz") -> Content(OrdinalSchema[String](), "9.42")),
    Position("baz", 2) -> Map(Position("xyz") -> Content(DiscreteSchema[Long](), 19)),
    Position("qux", 1) -> Map(Position("xyz") -> Content(OrdinalSchema[String](), "12.56"))
  )
}

class TestScaldingMatrixCompact extends TestMatrixCompact {

  "A Matrix.compact" should "return its first over map in 1D" in {
    toPipe(data1)
      .compact(Over(_1), Default()).toTypedPipe
      .toList shouldBe List(result1)
  }

  it should "return its first over map in 2D" in {
    toPipe(data2)
      .compact(Over(_1), Default(12)).toTypedPipe
      .toList shouldBe List(result2)
  }

  it should "return its first along map in 2D" in {
    toPipe(data2)
      .compact(Along(_1), Default()).toTypedPipe
      .toList shouldBe List(result3)
  }

  it should "return its second over map in 2D" in {
    toPipe(data2)
      .compact(Over(_2), Default(12)).toTypedPipe
      .toList shouldBe List(result4)
  }

  it should "return its second along map in 2D" in {
    toPipe(data2)
      .compact(Along(_2), Default()).toTypedPipe
      .toList shouldBe List(result5)
  }

  it should "return its first over map in 3D" in {
    toPipe(data3)
      .compact(Over(_1), Default(12)).toTypedPipe
      .toList shouldBe List(result6)
  }

  it should "return its first along map in 3D" in {
    toPipe(data3)
      .compact(Along(_1), Default()).toTypedPipe
      .toList shouldBe List(result7)
  }

  it should "return its second over map in 3D" in {
    toPipe(data3)
      .compact(Over(_2), Default(12)).toTypedPipe
      .toList shouldBe List(result8)
  }

  it should "return its second along map in 3D" in {
    toPipe(data3)
      .compact(Along(_2), Default()).toTypedPipe
      .toList shouldBe List(result9)
  }

  it should "return its third over map in 3D" in {
    toPipe(data3)
      .compact(Over(_3), Default(12)).toTypedPipe
      .toList shouldBe List(result10)
  }

  it should "return its third along map in 3D" in {
    toPipe(data3)
      .compact(Along(_3), Default()).toTypedPipe
      .toList shouldBe List(result11)
  }

  it should "return its empty map" in {
    toPipe(List[Cell[_3]]())
      .compact(Along(_3), Default()).toTypedPipe
      .toList shouldBe List(Map())
  }

  it should "return its compacted 1D" in {
    toPipe(data1)
      .compact().toTypedPipe
      .toList shouldBe List(result1)
  }

  it should "return its empty compacted" in {
    toPipe(List[Cell[_2]]())
      .compact().toTypedPipe
      .toList shouldBe List(Map())
  }
}

class TestSparkMatrixCompact extends TestMatrixCompact {

  "A Matrix.compact" should "return its first over map in 1D" in {
    toRDD(data1)
      .compact(Over(_1), Default()) shouldBe result1
  }

  it should "return its first over map in 2D" in {
    toRDD(data2)
      .compact(Over(_1), Default(12)) shouldBe result2
  }

  it should "return its first along map in 2D" in {
    toRDD(data2)
      .compact(Along(_1), Default()) shouldBe result3
  }

  it should "return its second over map in 2D" in {
    toRDD(data2)
      .compact(Over(_2), Default(12)) shouldBe result4
  }

  it should "return its second along map in 2D" in {
    toRDD(data2)
      .compact(Along(_2), Default()) shouldBe result5
  }

  it should "return its first over map in 3D" in {
    toRDD(data3)
      .compact(Over(_1), Default(12)) shouldBe result6
  }

  it should "return its first along map in 3D" in {
    toRDD(data3)
      .compact(Along(_1), Default()) shouldBe result7
  }

  it should "return its second over map in 3D" in {
    toRDD(data3)
      .compact(Over(_2), Default(12)) shouldBe result8
  }

  it should "return its second along map in 3D" in {
    toRDD(data3)
      .compact(Along(_2), Default()) shouldBe result9
  }

  it should "return its third over map in 3D" in {
    toRDD(data3)
      .compact(Over(_3), Default(12)) shouldBe result10
  }

  it should "return its third along map in 3D" in {
    toRDD(data3)
      .compact(Along(_3), Default()) shouldBe result11
  }

  it should "return its empty map" in {
    toRDD(List[Cell[_3]]())
      .compact(Along(_3), Default()) shouldBe Map()
  }

  it should "return its compacted 1D" in {
    toRDD(data1)
      .compact() shouldBe result1
  }

  it should "return its empty compacted" in {
    toRDD(List[Cell[_2]]())
      .compact() shouldBe Map()
  }
}

trait TestMatrixSummarise extends TestMatrix {

  val ext = Map(
    Position("foo") -> 1.0 / 1,
    Position("bar") -> 1.0 / 2,
    Position("baz") -> 1.0 / 3,
    Position("qux") -> 1.0 / 4,
    Position("foo.2") -> 1.0,
    Position("bar.2") -> 1.0,
    Position("baz.2") -> 1.0,
    Position("qux.2") -> 1.0,
    Position(1) -> 1.0 / 2,
    Position(2) -> 1.0 / 4,
    Position(3) -> 1.0 / 6,
    Position(4) -> 1.0 / 8,
    Position("1.2") -> 1.0,
    Position("2.2") -> 1.0,
    Position("3.2") -> 1.0,
    Position("4.2") -> 1.0,
    Position("xyz") -> 1 / 3.14,
    Position("xyz.2") -> 1 / 6.28
  )

  type W = Map[Position[_1], Double]

  val result1 = List(
    Cell(Position("bar"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("baz"), Content(ContinuousSchema[Double](), 9.42)),
    Cell(Position("foo"), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position("qux"), Content(ContinuousSchema[Double](), 12.56))
  )

  val result2 = List(
    Cell(Position(1), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position(2), Content(ContinuousSchema[Double](), 18.84)),
    Cell(Position(3), Content(ContinuousSchema[Double](), 18.84)),
    Cell(Position(4), Content(ContinuousSchema[Double](), 12.56))
  )

  val result3 = List(
    Cell(Position(1), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position(2), Content(ContinuousSchema[Double](), 18.84)),
    Cell(Position(3), Content(ContinuousSchema[Double](), 18.84)),
    Cell(Position(4), Content(ContinuousSchema[Double](), 12.56))
  )

  val result4 = List(
    Cell(Position("bar"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("baz"), Content(ContinuousSchema[Double](), 9.42)),
    Cell(Position("foo"), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position("qux"), Content(ContinuousSchema[Double](), 12.56))
  )

  val result5 = List(
    Cell(Position("bar"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("baz"), Content(ContinuousSchema[Double](), 9.42)),
    Cell(Position("foo"), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position("qux"), Content(ContinuousSchema[Double](), 12.56))
  )

  val result6 = List(
    Cell(Position(1, "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position(2, "xyz"), Content(ContinuousSchema[Double](), 18.84)),
    Cell(Position(3, "xyz"), Content(ContinuousSchema[Double](), 18.84)),
    Cell(Position(4, "xyz"), Content(ContinuousSchema[Double](), 12.56))
  )

  val result7 = List(
    Cell(Position(1), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position(2), Content(ContinuousSchema[Double](), 18.84)),
    Cell(Position(3), Content(ContinuousSchema[Double](), 18.84)),
    Cell(Position(4), Content(ContinuousSchema[Double](), 12.56))
  )

  val result8 = List(
    Cell(Position("bar", "xyz"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("baz", "xyz"), Content(ContinuousSchema[Double](), 9.42)),
    Cell(Position("foo", "xyz"), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position("qux", "xyz"), Content(ContinuousSchema[Double](), 12.56))
  )

  val result9 = List(Cell(Position("xyz"), Content(ContinuousSchema[Double](), 18.84)))

  val result10 = List(
    Cell(Position("bar", 1), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("bar", 2), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", 3), Content(ContinuousSchema[Double](), 18.84)),
    Cell(Position("baz", 1), Content(ContinuousSchema[Double](), 9.42)),
    Cell(Position("baz", 2), Content(ContinuousSchema[Double](), 18.84)),
    Cell(Position("foo", 1), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position("foo", 2), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("foo", 3), Content(ContinuousSchema[Double](), 9.42)),
    Cell(Position("foo", 4), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("qux", 1), Content(ContinuousSchema[Double](), 12.56))
  )

  val result11 = List(
    Cell(Position("bar"), Content(ContinuousSchema[Double](), (6.28 + 12.56 + 18.84) * (1.0 / 2))),
    Cell(Position("baz"), Content(ContinuousSchema[Double](), (9.42 + 18.84) * (1.0 / 3))),
    Cell(Position("foo"), Content(ContinuousSchema[Double](), (3.14 + 6.28 + 9.42 + 12.56) * (1.0 / 1))),
    Cell(Position("qux"), Content(ContinuousSchema[Double](), 12.56 * (1.0 / 4)))
  )

  val result12 = List(
    Cell(Position(1), Content(ContinuousSchema[Double](), (3.14 + 6.28 + 9.42 + 12.56) * (1.0 / 2))),
    Cell(Position(2), Content(ContinuousSchema[Double](), (6.28 + 12.56 + 18.84) * (1.0 / 4))),
    Cell(Position(3), Content(ContinuousSchema[Double](), (9.42 + 18.84) * (1.0 / 6))),
    Cell(Position(4), Content(ContinuousSchema[Double](), 12.56 * (1.0 / 8)))
  )

  val result13 = List(
    Cell(Position(1), Content(ContinuousSchema[Double](), (3.14 + 6.28 + 9.42 + 12.56) * (1.0 / 2))),
    Cell(Position(2), Content(ContinuousSchema[Double](), (6.28 + 12.56 + 18.84) * (1.0 / 4))),
    Cell(Position(3), Content(ContinuousSchema[Double](), (9.42 + 18.84) * (1.0 / 6))),
    Cell(Position(4), Content(ContinuousSchema[Double](), 12.56 * (1.0 / 8)))
  )

  val result14 = List(
    Cell(Position("bar"), Content(ContinuousSchema[Double](), (6.28 + 12.56 + 18.84) * (1.0 / 2))),
    Cell(Position("baz"), Content(ContinuousSchema[Double](), (9.42 + 18.84) * (1.0 / 3))),
    Cell(Position("foo"), Content(ContinuousSchema[Double](), (3.14 + 6.28 + 9.42 + 12.56) * (1.0 / 1))),
    Cell(Position("qux"), Content(ContinuousSchema[Double](), 12.56 * (1.0 / 4)))
  )

  val result15 = List(
    Cell(Position("bar"), Content(ContinuousSchema[Double](), (6.28 + 12.56 + 18.84) * (1.0 / 2))),
    Cell(Position("baz"), Content(ContinuousSchema[Double](), (9.42 + 18.84) * (1.0 / 3))),
    Cell(Position("foo"), Content(ContinuousSchema[Double](), (3.14 + 6.28 + 9.42 + 12.56) * (1.0 / 1))),
    Cell(Position("qux"), Content(ContinuousSchema[Double](), 12.56 * (1.0 / 4)))
  )

  val result16 = List(
    Cell(Position(1, "xyz"), Content(ContinuousSchema[Double](), (3.14 + 6.28 + 9.42 + 12.56) * (1.0 / 2))),
    Cell(Position(2, "xyz"), Content(ContinuousSchema[Double](), (6.28 + 12.56 + 18.84) * (1.0 / 4))),
    Cell(Position(3, "xyz"), Content(ContinuousSchema[Double](), (9.42 + 18.84) * (1.0 / 6))),
    Cell(Position(4, "xyz"), Content(ContinuousSchema[Double](), 12.56 * (1.0 / 8)))
  )

  val result17 = List(
    Cell(Position(1), Content(ContinuousSchema[Double](), (3.14 + 6.28 + 9.42 + 12.56) * (1.0 / 2))),
    Cell(Position(2), Content(ContinuousSchema[Double](), (6.28 + 12.56 + 18.84) * (1.0 / 4))),
    Cell(Position(3), Content(ContinuousSchema[Double](), (9.42 + 18.84) * (1.0 / 6))),
    Cell(Position(4), Content(ContinuousSchema[Double](), 12.56 * (1.0 / 8)))
  )

  val result18 = List(
    Cell(Position("bar", "xyz"), Content(ContinuousSchema[Double](), (6.28 + 12.56 + 18.84) * (1.0 / 2))),
    Cell(Position("baz", "xyz"), Content(ContinuousSchema[Double](), (9.42 + 18.84) * (1.0 / 3))),
    Cell(Position("foo", "xyz"), Content(ContinuousSchema[Double](), (3.14 + 6.28 + 9.42 + 12.56) * (1.0 / 1))),
    Cell(Position("qux", "xyz"), Content(ContinuousSchema[Double](), 12.56 * (1.0 / 4)))
  )

  val result19 = List(
    Cell(
      Position("xyz"),
      Content(ContinuousSchema[Double](), (3.14 + 2 * 6.28 + 2 * 9.42 + 3 * 12.56 + 2 * 18.84) / 3.14)
    )
  )

  val result20 = List(
    Cell(Position("bar", 1), Content(ContinuousSchema[Double](), 6.28 / 3.14)),
    Cell(Position("bar", 2), Content(ContinuousSchema[Double](), 12.56 / 3.14)),
    Cell(Position("bar", 3), Content(ContinuousSchema[Double](), 18.84 / 3.14)),
    Cell(Position("baz", 1), Content(ContinuousSchema[Double](), 9.42 / 3.14)),
    Cell(Position("baz", 2), Content(ContinuousSchema[Double](), 18.84 / 3.14)),
    Cell(Position("foo", 1), Content(ContinuousSchema[Double](), 3.14 / 3.14)),
    Cell(Position("foo", 2), Content(ContinuousSchema[Double](), 6.28 / 3.14)),
    Cell(Position("foo", 3), Content(ContinuousSchema[Double](), 9.42 / 3.14)),
    Cell(Position("foo", 4), Content(ContinuousSchema[Double](), 12.56 / 3.14)),
    Cell(Position("qux", 1), Content(ContinuousSchema[Double](), 12.56 / 3.14))
  )

  val result21 = List(
    Cell(Position("bar", "min"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("baz", "min"), Content(ContinuousSchema[Double](), 9.42)),
    Cell(Position("foo", "min"), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position("qux", "min"), Content(ContinuousSchema[Double](), 12.56))
  )

  val result22 = List(
    Cell(Position("max"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("min"), Content(ContinuousSchema[Double](), 3.14))
  )

  val result23 = List(
    Cell(Position("bar", "min"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("baz", "min"), Content(ContinuousSchema[Double](), 9.42)),
    Cell(Position("foo", "min"), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position("qux", "min"), Content(ContinuousSchema[Double](), 12.56))
  )

  val result24 = List(
    Cell(Position(1, "max"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position(1, "min"), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position(2, "max"), Content(ContinuousSchema[Double](), 18.84)),
    Cell(Position(2, "min"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position(3, "max"), Content(ContinuousSchema[Double](), 18.84)),
    Cell(Position(3, "min"), Content(ContinuousSchema[Double](), 9.42)),
    Cell(Position(4, "max"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position(4, "min"), Content(ContinuousSchema[Double](), 12.56))
  )

  val result25 = List(
    Cell(Position(1, "max"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position(1, "min"), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position(2, "max"), Content(ContinuousSchema[Double](), 18.84)),
    Cell(Position(2, "min"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position(3, "max"), Content(ContinuousSchema[Double](), 18.84)),
    Cell(Position(3, "min"), Content(ContinuousSchema[Double](), 9.42)),
    Cell(Position(4, "max"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position(4, "min"), Content(ContinuousSchema[Double](), 12.56))
  )

  val result26 = List(
    Cell(Position("bar", "min"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("baz", "min"), Content(ContinuousSchema[Double](), 9.42)),
    Cell(Position("foo", "min"), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position("qux", "min"), Content(ContinuousSchema[Double](), 12.56))
  )

  val result27 = List(
    Cell(Position("bar", "min"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("baz", "min"), Content(ContinuousSchema[Double](), 9.42)),
    Cell(Position("foo", "min"), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position("qux", "min"), Content(ContinuousSchema[Double](), 12.56))
  )

  val result28 = List(
    Cell(Position(1, "xyz", "max"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position(1, "xyz", "min"), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position(2, "xyz", "max"), Content(ContinuousSchema[Double](), 18.84)),
    Cell(Position(2, "xyz", "min"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position(3, "xyz", "max"), Content(ContinuousSchema[Double](), 18.84)),
    Cell(Position(3, "xyz", "min"), Content(ContinuousSchema[Double](), 9.42)),
    Cell(Position(4, "xyz", "max"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position(4, "xyz", "min"), Content(ContinuousSchema[Double](), 12.56))
  )

  val result29 = List(
    Cell(Position(1, "max"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position(1, "min"), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position(2, "max"), Content(ContinuousSchema[Double](), 18.84)),
    Cell(Position(2, "min"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position(3, "max"), Content(ContinuousSchema[Double](), 18.84)),
    Cell(Position(3, "min"), Content(ContinuousSchema[Double](), 9.42)),
    Cell(Position(4, "max"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position(4, "min"), Content(ContinuousSchema[Double](), 12.56))
  )

  val result30 = List(
    Cell(Position("bar", "xyz", "min"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("baz", "xyz", "min"), Content(ContinuousSchema[Double](), 9.42)),
    Cell(Position("foo", "xyz", "min"), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position("qux", "xyz", "min"), Content(ContinuousSchema[Double](), 12.56))
  )

  val result31 = List(
    Cell(Position("xyz", "max"), Content(ContinuousSchema[Double](), 18.84)),
    Cell(Position("xyz", "min"), Content(ContinuousSchema[Double](), 3.14))
  )

  val result32 = List(
    Cell(Position("bar", 1, "min"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("bar", 2, "min"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", 3, "min"), Content(ContinuousSchema[Double](), 18.84)),
    Cell(Position("baz", 1, "min"), Content(ContinuousSchema[Double](), 9.42)),
    Cell(Position("baz", 2, "min"), Content(ContinuousSchema[Double](), 18.84)),
    Cell(Position("foo", 1, "min"), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position("foo", 2, "min"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("foo", 3, "min"), Content(ContinuousSchema[Double](), 9.42)),
    Cell(Position("foo", 4, "min"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("qux", 1, "min"), Content(ContinuousSchema[Double](), 12.56))
  )

  val result33 = List(
    Cell(Position("bar", "sum"), Content(ContinuousSchema[Double](), 6.28 / 2)),
    Cell(Position("baz", "sum"), Content(ContinuousSchema[Double](), 9.42 * (1.0 / 3))),
    Cell(Position("foo", "sum"), Content(ContinuousSchema[Double](), 3.14 / 1)),
    Cell(Position("qux", "sum"), Content(ContinuousSchema[Double](), 12.56 / 4))
  )

  val result34 = List(
    Cell(Position("sum.1"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("sum.2"), Content(ContinuousSchema[Double](), 31.40))
  )

  val result35 = List(
    Cell(Position("bar", "sum"), Content(ContinuousSchema[Double](), (6.28 + 12.56 + 18.84) * (1.0 / 2))),
    Cell(Position("baz", "sum"), Content(ContinuousSchema[Double](), (9.42 + 18.84) * (1.0 / 3))),
    Cell(Position("foo", "sum"), Content(ContinuousSchema[Double](), (3.14 + 6.28 + 9.42 + 12.56) * (1.0 / 1))),
    Cell(Position("qux", "sum"), Content(ContinuousSchema[Double](), 12.56 * (1.0 / 4)))
  )

  val result36 = List(
    Cell(Position(1, "sum.1"), Content(ContinuousSchema[Double](), (3.14 + 6.28 + 9.42 + 12.56) * (1.0 / 2))),
    Cell(Position(1, "sum.2"), Content(ContinuousSchema[Double](), 3.14 + 6.28 + 9.42 + 12.56)),
    Cell(Position(2, "sum.1"), Content(ContinuousSchema[Double](), (6.28 + 12.56 + 18.84) * (1.0 / 4))),
    Cell(Position(2, "sum.2"), Content(ContinuousSchema[Double](), 6.28 + 12.56 + 18.84)),
    Cell(Position(3, "sum.1"), Content(ContinuousSchema[Double](), (9.42 + 18.84) * (1.0 / 6))),
    Cell(Position(3, "sum.2"), Content(ContinuousSchema[Double](), 9.42 + 18.84)),
    Cell(Position(4, "sum.1"), Content(ContinuousSchema[Double](), 12.56 * (1.0 / 8))),
    Cell(Position(4, "sum.2"), Content(ContinuousSchema[Double](), 12.56))
  )

  val result37 = List(
    Cell(Position(1, "sum.1"), Content(ContinuousSchema[Double](), (3.14 + 6.28 + 9.42 + 12.56) * (1.0 / 2))),
    Cell(Position(1, "sum.2"), Content(ContinuousSchema[Double](), 3.14 + 6.28 + 9.42 + 12.56)),
    Cell(Position(2, "sum.1"), Content(ContinuousSchema[Double](), (6.28 + 12.56 + 18.84) * (1.0 / 4))),
    Cell(Position(2, "sum.2"), Content(ContinuousSchema[Double](), 6.28 + 12.56 + 18.84)),
    Cell(Position(3, "sum.1"), Content(ContinuousSchema[Double](), (9.42 + 18.84) * (1.0 / 6))),
    Cell(Position(3, "sum.2"), Content(ContinuousSchema[Double](), 9.42 + 18.84)),
    Cell(Position(4, "sum.1"), Content(ContinuousSchema[Double](), 12.56 * (1.0 / 8))),
    Cell(Position(4, "sum.2"), Content(ContinuousSchema[Double](), 12.56))
  )

  val result38 = List(
    Cell(Position("bar", "sum"), Content(ContinuousSchema[Double](), (6.28 + 12.56 + 18.84) * (1.0 / 2))),
    Cell(Position("baz", "sum"), Content(ContinuousSchema[Double](), (9.42 + 18.84) * (1.0 / 3))),
    Cell(Position("foo", "sum"), Content(ContinuousSchema[Double](), (3.14 + 6.28 + 9.42 + 12.56) * (1.0 / 1))),
    Cell(Position("qux", "sum"), Content(ContinuousSchema[Double](), 12.56 * (1.0 / 4)))
  )

  val result39 = List(
    Cell(Position("bar", "sum"), Content(ContinuousSchema[Double](), (6.28 + 12.56 + 18.84) * (1.0 / 2))),
    Cell(Position("baz", "sum"), Content(ContinuousSchema[Double](), (9.42 + 18.84) * (1.0 / 3))),
    Cell(Position("foo", "sum"), Content(ContinuousSchema[Double](), (3.14 + 6.28 + 9.42 + 12.56) * (1.0 / 1))),
    Cell(Position("qux", "sum"), Content(ContinuousSchema[Double](), 12.56 * (1.0 / 4)))
  )

  val result40 = List(
    Cell(Position(1, "xyz", "sum.1"), Content(ContinuousSchema[Double](), (3.14 + 6.28 + 9.42 + 12.56) * (1.0 / 2))),
    Cell(Position(1, "xyz", "sum.2"), Content(ContinuousSchema[Double](), 3.14 + 6.28 + 9.42 + 12.56)),
    Cell(Position(2, "xyz", "sum.1"), Content(ContinuousSchema[Double](), (6.28 + 12.56 + 18.84) * (1.0 / 4))),
    Cell(Position(2, "xyz", "sum.2"), Content(ContinuousSchema[Double](), 6.28 + 12.56 + 18.84)),
    Cell(Position(3, "xyz", "sum.1"), Content(ContinuousSchema[Double](), (9.42 + 18.84) * (1.0 / 6))),
    Cell(Position(3, "xyz", "sum.2"), Content(ContinuousSchema[Double](), 9.42 + 18.84)),
    Cell(Position(4, "xyz", "sum.1"), Content(ContinuousSchema[Double](), 12.56 * (1.0 / 8))),
    Cell(Position(4, "xyz", "sum.2"), Content(ContinuousSchema[Double](), 12.56))
  )

  val result41 = List(
    Cell(Position(1, "sum.1"), Content(ContinuousSchema[Double](), (3.14 + 6.28 + 9.42 + 12.56) * (1.0 / 2))),
    Cell(Position(1, "sum.2"), Content(ContinuousSchema[Double](), 3.14 + 6.28 + 9.42 + 12.56)),
    Cell(Position(2, "sum.1"), Content(ContinuousSchema[Double](), (6.28 + 12.56 + 18.84) * (1.0 / 4))),
    Cell(Position(2, "sum.2"), Content(ContinuousSchema[Double](), 6.28 + 12.56 + 18.84)),
    Cell(Position(3, "sum.1"), Content(ContinuousSchema[Double](), (9.42 + 18.84) * (1.0 / 6))),
    Cell(Position(3, "sum.2"), Content(ContinuousSchema[Double](), 9.42 + 18.84)),
    Cell(Position(4, "sum.1"), Content(ContinuousSchema[Double](), 12.56 * (1.0 / 8))),
    Cell(Position(4, "sum.2"), Content(ContinuousSchema[Double](), 12.56))
  )

  val result42 = List(
    Cell(Position("bar", "xyz", "sum"), Content(ContinuousSchema[Double](), (6.28 + 12.56 + 18.84) * (1.0 / 2))),
    Cell(Position("baz", "xyz", "sum"), Content(ContinuousSchema[Double](), (9.42 + 18.84) * (1.0 / 3))),
    Cell(Position("foo", "xyz", "sum"), Content(ContinuousSchema[Double](), (3.14 + 6.28 + 9.42 + 12.56) * (1.0 / 1))),
    Cell(Position("qux", "xyz", "sum"), Content(ContinuousSchema[Double](), 12.56 * (1.0 / 4)))
  )

  val result43 = List(
    Cell(
      Position("xyz", "sum.1"),
      Content(ContinuousSchema[Double](), (3.14 + 2 * 6.28 + 2 * 9.42 + 3 * 12.56 + 2 * 18.84) / 3.14)
    ),
    Cell(
      Position("xyz", "sum.2"),
      Content(ContinuousSchema[Double](), (3.14 + 2 * 6.28 + 2 * 9.42 + 3 * 12.56 + 2 * 18.84) / 6.28)
    )
  )

  val result44 = List(
    Cell(Position("bar", 1, "sum"), Content(ContinuousSchema[Double](), 6.28 / 3.14)),
    Cell(Position("bar", 2, "sum"), Content(ContinuousSchema[Double](), 12.56 / 3.14)),
    Cell(Position("bar", 3, "sum"), Content(ContinuousSchema[Double](), 18.84 / 3.14)),
    Cell(Position("baz", 1, "sum"), Content(ContinuousSchema[Double](), 9.42 / 3.14)),
    Cell(Position("baz", 2, "sum"), Content(ContinuousSchema[Double](), 18.84 / 3.14)),
    Cell(Position("foo", 1, "sum"), Content(ContinuousSchema[Double](), 3.14 / 3.14)),
    Cell(Position("foo", 2, "sum"), Content(ContinuousSchema[Double](), 6.28 / 3.14)),
    Cell(Position("foo", 3, "sum"), Content(ContinuousSchema[Double](), 9.42 / 3.14)),
    Cell(Position("foo", 4, "sum"), Content(ContinuousSchema[Double](), 12.56 / 3.14)),
    Cell(Position("qux", 1, "sum"), Content(ContinuousSchema[Double](), 12.56 / 3.14))
  )
}

object TestMatrixSummarise {
  case class ExtractWithName[D <: Nat : ToInt, P <: Nat](dim: D, name: String)(implicit ev: LTEq[D, P])
    extends Extract[P, Map[Position[_1], Double], Double] {
    def extract(cell: Cell[P], ext: Map[Position[_1], Double]): Option[Double] = ext
      .get(Position(name.format(cell.position(dim).toShortString)))
  }

  case class BadCount[P <: Nat, S <: Nat]() extends Aggregator[P, S, S] {
    type T = Long
    type O[A] = Multiple[A]

    val tTag = scala.reflect.classTag[T]
    val oTag = scala.reflect.classTag[O[_]]

    def prepare(cell: Cell[P]): Option[T] = Option(1)
    def reduce(lt: T, rt: T): T = lt + rt
    def present(pos: Position[S], t: T): O[Cell[S]] = Multiple(List(Cell(pos, Content(DiscreteSchema[Long](), t))))
  }
}

class TestScaldingMatrixSummarise extends TestMatrixSummarise {

  "A Matrix.summarise" should "return its first over aggregates in 2D" in {
    toPipe(num2)
      .summarise(Over(_1), Default())(Minimum())
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its first along aggregates in 2D" in {
    toPipe(num2)
      .summarise(Along(_1), Default(12))(Maximum())
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its second over aggregates in 2D" in {
    toPipe(num2)
      .summarise(Over(_2), Default())(Maximum())
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its second along aggregates in 2D" in {
    toPipe(num2)
      .summarise(Along(_2), Default(12))(Minimum())
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its first over aggregates in 3D" in {
    toPipe(num3)
      .summarise(Over(_1), Default())(Minimum())
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its first along aggregates in 3D" in {
    toPipe(num3)
      .summarise(Along(_1), Default(12))(Maximum())
      .toList.sortBy(_.position) shouldBe result6
  }

  it should "return its second over aggregates in 3D" in {
    toPipe(num3)
      .summarise(Over(_2), Default())(Maximum())
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its second along aggregates in 3D" in {
    toPipe(num3)
      .summarise(Along(_2), Default(12))(Minimum())
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its third over aggregates in 3D" in {
    toPipe(num3)
      .summarise(Over(_3), Default())(Maximum())
      .toList.sortBy(_.position) shouldBe result9
  }

  it should "return its third along aggregates in 3D" in {
    toPipe(num3)
      .summarise(Along(_3), Default(12))(Minimum())
      .toList.sortBy(_.position) shouldBe result10
  }

  it should "throw an exception for too many single" in {
    a [Exception] shouldBe thrownBy { toPipe(num2).summarise(Over(_1))(Minimum(), Maximum()) }
  }

  it should "throw an exception for a multiple" in {
    a [Exception] shouldBe thrownBy { toPipe(num2).summarise(Over(_1))(TestMatrixSummarise.BadCount()) }
  }

  "A Matrix.summariseWithValue" should "return its first over aggregates in 2D" in {
    toPipe(num2)
      .summariseWithValue(Over(_1), Default())(ValuePipe(ext), WeightedSums(ExtractWithDimension(_1)))
      .toList.sortBy(_.position) shouldBe result11
  }

  it should "return its first along aggregates in 2D" in {
    toPipe(num2)
      .summariseWithValue(Along(_1), Default(12))(ValuePipe(ext), WeightedSums(ExtractWithDimension(_2)))
      .toList.sortBy(_.position) shouldBe result12
  }

  it should "return its second over aggregates in 2D" in {
    toPipe(num2)
      .summariseWithValue(Over(_2), Default())(ValuePipe(ext), WeightedSums(ExtractWithDimension(_2)))
      .toList.sortBy(_.position) shouldBe result13
  }

  it should "return its second along aggregates in 2D" in {
    toPipe(num2)
      .summariseWithValue(Along(_2), Default(12))(ValuePipe(ext), WeightedSums(ExtractWithDimension(_1)))
      .toList.sortBy(_.position) shouldBe result14
  }

  it should "return its first over aggregates in 3D" in {
    toPipe(num3)
      .summariseWithValue(Over(_1), Default())(ValuePipe(ext), WeightedSums(ExtractWithDimension(_1)))
      .toList.sortBy(_.position) shouldBe result15
  }

  it should "return its first along aggregates in 3D" in {
    toPipe(num3)
      .summariseWithValue(Along(_1), Default(12))(ValuePipe(ext), WeightedSums(ExtractWithDimension(_2)))
      .toList.sortBy(_.position) shouldBe result16
  }

  it should "return its second over aggregates in 3D" in {
    toPipe(num3)
      .summariseWithValue(Over(_2), Default())(ValuePipe(ext), WeightedSums(ExtractWithDimension(_2)))
      .toList.sortBy(_.position) shouldBe result17
  }

  it should "return its second along aggregates in 3D" in {
    toPipe(num3)
      .summariseWithValue(Along(_2), Default(12))(ValuePipe(ext), WeightedSums(ExtractWithDimension(_1)))
      .toList.sortBy(_.position) shouldBe result18
  }

  it should "return its third over aggregates in 3D" in {
    toPipe(num3)
      .summariseWithValue(Over(_3), Default())(ValuePipe(ext), WeightedSums(ExtractWithDimension(_3)))
      .toList.sortBy(_.position) shouldBe result19
  }

  it should "return its third along aggregates in 3D" in {
    toPipe(num3)
      .summariseWithValue(Along(_3), Default(12))(ValuePipe(ext), WeightedSums(ExtractWithDimension(_3)))
      .toList.sortBy(_.position) shouldBe result20
  }

  it should "throw an exception for too many single" in {
    a [Exception] shouldBe thrownBy { toPipe(num2).summariseWithValue(Over(_1))(ValuePipe(ext), Minimum(), Maximum()) }
  }

  it should "throw an exception for a multiple" in {
    a [Exception] shouldBe thrownBy {
      toPipe(num2).summariseWithValue(Over(_1))(ValuePipe(ext), TestMatrixSummarise.BadCount())
    }
  }

  "A Matrix.summariseAndExpand" should "return its first over aggregates in 1D" in {
    toPipe(num1)
      .summarise(Over(_1), Default())(Minimum().andThenRelocate(_.position.append("min").toOption))
      .toList.sortBy(_.position) shouldBe result21
  }

  it should "return its first along aggregates in 1D" in {
    toPipe(num1)
      .summarise(Along(_1), Default(12))(
        List(
          Minimum[_1, _0]().andThenRelocate(_.position.append("min").toOption),
          Maximum[_1, _0]().andThenRelocate(_.position.append("max").toOption)
        )
      )
      .toList.sortBy(_.position) shouldBe result22
  }

  it should "return its first over aggregates in 2D" in {
    toPipe(num2)
      .summarise(Over(_1), Default())(Minimum().andThenRelocate(_.position.append("min").toOption))
      .toList.sortBy(_.position) shouldBe result23
  }

  it should "return its first along aggregates in 2D" in {
    toPipe(num2)
      .summarise(Along(_1), Default(12))(
        Minimum[_2, _1]().andThenRelocate(_.position.append("min").toOption),
        Maximum[_2, _1]().andThenRelocate(_.position.append("max").toOption)
      )
      .toList.sortBy(_.position) shouldBe result24
  }

  it should "return its second over aggregates in 2D" in {
    toPipe(num2)
      .summarise(Over(_2), Default())(
        List(
          Minimum[_2, _1]().andThenRelocate(_.position.append("min").toOption),
          Maximum[_2, _1]().andThenRelocate(_.position.append("max").toOption)
        )
      )
      .toList.sortBy(_.position) shouldBe result25
  }

  it should "return its second along aggregates in 2D" in {
    toPipe(num2)
      .summarise(Along(_2), Default(12))(Minimum().andThenRelocate(_.position.append("min").toOption))
      .toList.sortBy(_.position) shouldBe result26
  }

  it should "return its first over aggregates in 3D" in {
    toPipe(num3)
      .summarise(Over(_1), Default())(Minimum().andThenRelocate(_.position.append("min").toOption))
      .toList.sortBy(_.position) shouldBe result27
  }

  it should "return its first along aggregates in 3D" in {
    toPipe(num3)
      .summarise(Along(_1), Default(12))(
        Minimum[_3, _2]().andThenRelocate(_.position.append("min").toOption),
        Maximum[_3, _2]().andThenRelocate(_.position.append("max").toOption)
      )
      .toList.sortBy(_.position) shouldBe result28
  }

  it should "return its second over aggregates in 3D" in {
    toPipe(num3)
      .summarise(Over(_2), Default())(
        List(
          Minimum[_3, _1]().andThenRelocate(_.position.append("min").toOption),
          Maximum[_3, _1]().andThenRelocate(_.position.append("max").toOption)
        )
      )
      .toList.sortBy(_.position) shouldBe result29
  }

  it should "return its second along aggregates in 3D" in {
    toPipe(num3)
      .summarise(Along(_2), Default(12))(Minimum().andThenRelocate(_.position.append("min").toOption))
      .toList.sortBy(_.position) shouldBe result30
  }

  it should "return its third over aggregates in 3D" in {
    toPipe(num3)
      .summarise(Over(_3), Default())(
        Minimum[_3, _1]().andThenRelocate(_.position.append("min").toOption),
        Maximum[_3, _1]().andThenRelocate(_.position.append("max").toOption)
      )
      .toList.sortBy(_.position) shouldBe result31
  }

  it should "return its third along aggregates in 3D" in {
    toPipe(num3)
      .summarise(Along(_3), Default(12))(Minimum().andThenRelocate(_.position.append("min").toOption))
      .toList.sortBy(_.position) shouldBe result32
  }

  "A Matrix.summariseAndExpandWithValue" should "return its first over aggregates in 1D" in {
    toPipe(num1)
      .summariseWithValue(Over(_1), Default())(
        ValuePipe(ext),
        WeightedSums(
          ExtractWithDimension[_1, Double](_1)
        ).andThenRelocateWithValue((c: Cell[_1], e: W) => c.position.append("sum").toOption)
      )
      .toList.sortBy(_.position) shouldBe result33
  }

  it should "return its first along aggregates in 1D" in {
    toPipe(num1)
      .summariseWithValue(Along(_1), Default(12))(
        ValuePipe(ext),
        List(
          WeightedSums[_1, _0, W](
            ExtractWithDimension(_1)
          ).andThenRelocateWithValue((c: Cell[_0], e: W) => c.position.append("sum.1").toOption),
          WeightedSums[_1, _0, W](
            TestMatrixSummarise.ExtractWithName(_1, "%1$s.2")
          ).andThenRelocateWithValue((c: Cell[_0], e: W) => c.position.append("sum.2").toOption)
        )
      )
      .toList.sortBy(_.position) shouldBe result34
  }

  it should "return its first over aggregates in 2D" in {
    toPipe(num2)
      .summariseWithValue(Over(_1), Default())(
        ValuePipe(ext),
        WeightedSums(
          ExtractWithDimension[_2, Double](_1)
        ).andThenRelocateWithValue((c: Cell[_1], e: W) => c.position.append("sum").toOption)
      )
      .toList.sortBy(_.position) shouldBe result35
  }

  it should "return its first along aggregates in 2D" in {
    toPipe(num2)
      .summariseWithValue(Along(_1), Default(12))(
        ValuePipe(ext),
        WeightedSums[_2, _1, W](
          ExtractWithDimension(_2)
        ).andThenRelocateWithValue((c: Cell[_1], e: W) => c.position.append("sum.1").toOption),
        WeightedSums[_2, _1, W](
          TestMatrixSummarise.ExtractWithName(_1, "%1$s.2")
        ).andThenRelocateWithValue((c: Cell[_1], e: W) => c.position.append("sum.2").toOption)
      )
      .toList.sortBy(_.position) shouldBe result36
  }

  it should "return its second over aggregates in 2D" in {
    toPipe(num2)
      .summariseWithValue(Over(_2), Default())(
        ValuePipe(ext),
        List(
          WeightedSums[_2, _1, W](
            ExtractWithDimension(_2)
          ).andThenRelocateWithValue((c: Cell[_1], e: W) => c.position.append("sum.1").toOption),
          WeightedSums[_2, _1, W](
            TestMatrixSummarise.ExtractWithName(_2, "%1$s.2")
          ).andThenRelocateWithValue((c: Cell[_1], e: W) => c.position.append("sum.2").toOption)
        )
      )
      .toList.sortBy(_.position) shouldBe result37
  }

  it should "return its second along aggregates in 2D" in {
    toPipe(num2)
      .summariseWithValue(Along(_2), Default(12))(
        ValuePipe(ext),
        WeightedSums(
          ExtractWithDimension[_2, Double](_1)
        ).andThenRelocateWithValue((c: Cell[_1], e: W) => c.position.append("sum").toOption)
      )
      .toList.sortBy(_.position) shouldBe result38
  }

  it should "return its first over aggregates in 3D" in {
    toPipe(num3)
      .summariseWithValue(Over(_1), Default())(
        ValuePipe(ext),
        WeightedSums(
          ExtractWithDimension[_3, Double](_1)
        ).andThenRelocateWithValue((c: Cell[_1], e: W) => c.position.append("sum").toOption)
      )
      .toList.sortBy(_.position) shouldBe result39
  }

  it should "return its first along aggregates in 3D" in {
    toPipe(num3)
      .summariseWithValue(Along(_1), Default(12))(
        ValuePipe(ext),
        WeightedSums[_3, _2, W](
          ExtractWithDimension(_2)
        ).andThenRelocateWithValue((c: Cell[_2], e: W) => c.position.append("sum.1").toOption),
        WeightedSums[_3, _2, W](
          TestMatrixSummarise.ExtractWithName(_2, "%1$s.2")
        ).andThenRelocateWithValue((c: Cell[_2], e: W) => c.position.append("sum.2").toOption)
      )
      .toList.sortBy(_.position) shouldBe result40
  }

  it should "return its second over aggregates in 3D" in {
    toPipe(num3)
      .summariseWithValue(Over(_2), Default())(
        ValuePipe(ext),
        List(
          WeightedSums[_3, _1, W](
            ExtractWithDimension(_2)
          ).andThenRelocateWithValue((c: Cell[_1], e: W) => c.position.append("sum.1").toOption),
          WeightedSums[_3, _1, W](
            TestMatrixSummarise.ExtractWithName(_2, "%1$s.2")
          ).andThenRelocateWithValue((c: Cell[_1], e: W) => c.position.append("sum.2").toOption)
        )
      )
      .toList.sortBy(_.position) shouldBe result41
  }

  it should "return its second along aggregates in 3D" in {
    toPipe(num3)
      .summariseWithValue(Along(_2), Default(12))(
        ValuePipe(ext),
        WeightedSums[_3, _2, W](
          ExtractWithDimension(_1)
        ).andThenRelocateWithValue((c: Cell[_2], e: W) => c.position.append("sum").toOption)
      )
      .toList.sortBy(_.position) shouldBe result42
  }

  it should "return its third over aggregates in 3D" in {
    toPipe(num3)
      .summariseWithValue(Over(_3), Default())(
        ValuePipe(ext),
        WeightedSums[_3, _1, W](
          ExtractWithDimension(_3)
        ).andThenRelocateWithValue((c: Cell[_1], e: W) => c.position.append("sum.1").toOption),
        WeightedSums[_3, _1, W](
          TestMatrixSummarise.ExtractWithName(_3, "%1$s.2")
        ).andThenRelocateWithValue((c: Cell[_1], e: W) => c.position.append("sum.2").toOption)
      )
      .toList.sortBy(_.position) shouldBe result43
  }

  it should "return its third along aggregates in 3D" in {
    toPipe(num3)
      .summariseWithValue(Along(_3), Default(12))(
        ValuePipe(ext),
        WeightedSums[_3, _2, W](
          ExtractWithDimension(_3)
        ).andThenRelocateWithValue((c: Cell[_2], e: W) => c.position.append("sum").toOption)
      )
      .toList.sortBy(_.position) shouldBe result44
  }
}

class TestSparkMatrixSummarise extends TestMatrixSummarise {

  "A Matrix.summarise" should "return its first over aggregates in 2D" in {
    toRDD(num2)
      .summarise(Over(_1), Default())(Minimum())
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its first along aggregates in 2D" in {
    toRDD(num2)
      .summarise(Along(_1), Default(12))(Maximum())
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its second over aggregates in 2D" in {
    toRDD(num2)
      .summarise(Over(_2), Default())(Maximum())
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its second along aggregates in 2D" in {
    toRDD(num2)
      .summarise(Along(_2), Default(12))(Minimum())
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its first over aggregates in 3D" in {
    toRDD(num3)
      .summarise(Over(_1), Default())(Minimum())
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its first along aggregates in 3D" in {
    toRDD(num3)
      .summarise(Along(_1), Default(12))(Maximum())
      .toList.sortBy(_.position) shouldBe result6
  }

  it should "return its second over aggregates in 3D" in {
    toRDD(num3)
      .summarise(Over(_2), Default())(Maximum())
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its second along aggregates in 3D" in {
    toRDD(num3)
      .summarise(Along(_2), Default(12))(Minimum())
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its third over aggregates in 3D" in {
    toRDD(num3)
      .summarise(Over(_3), Default())(Maximum())
      .toList.sortBy(_.position) shouldBe result9
  }

  it should "return its third along aggregates in 3D" in {
    toRDD(num3)
      .summarise(Along(_3), Default(12))(Minimum())
      .toList.sortBy(_.position) shouldBe result10
  }

  it should "throw an exception for too many single" in {
    a [Exception] shouldBe thrownBy { toRDD(num2).summarise(Over(_1))(Minimum(), Maximum()) }
  }

  it should "throw an exception for a multiple" in {
    a [Exception] shouldBe thrownBy { toRDD(num2).summarise(Over(_1))(TestMatrixSummarise.BadCount()) }
  }

  "A Matrix.summariseWithValue" should "return its first over aggregates in 2D" in {
    toRDD(num2)
      .summariseWithValue(Over(_1), Default())(ext, WeightedSums(ExtractWithDimension(_1)))
      .toList.sortBy(_.position) shouldBe result11
  }

  it should "return its first along aggregates in 2D" in {
    toRDD(num2)
      .summariseWithValue(Along(_1), Default(12))(ext, WeightedSums(ExtractWithDimension(_2)))
      .toList.sortBy(_.position) shouldBe result12
  }

  it should "return its second over aggregates in 2D" in {
    toRDD(num2)
      .summariseWithValue(Over(_2), Default())(ext, WeightedSums(ExtractWithDimension(_2)))
      .toList.sortBy(_.position) shouldBe result13
  }

  it should "return its second along aggregates in 2D" in {
    toRDD(num2)
      .summariseWithValue(Along(_2), Default(12))(ext, WeightedSums(ExtractWithDimension(_1)))
      .toList.sortBy(_.position) shouldBe result14
  }

  it should "return its first over aggregates in 3D" in {
    toRDD(num3)
      .summariseWithValue(Over(_1), Default())(ext, WeightedSums(ExtractWithDimension(_1)))
      .toList.sortBy(_.position) shouldBe result15
  }

  it should "return its first along aggregates in 3D" in {
    toRDD(num3)
      .summariseWithValue(Along(_1), Default(12))(ext, WeightedSums(ExtractWithDimension(_2)))
      .toList.sortBy(_.position) shouldBe result16
  }

  it should "return its second over aggregates in 3D" in {
    toRDD(num3)
      .summariseWithValue(Over(_2), Default())(ext, WeightedSums(ExtractWithDimension(_2)))
      .toList.sortBy(_.position) shouldBe result17
  }

  it should "return its second along aggregates in 3D" in {
    toRDD(num3)
      .summariseWithValue(Along(_2), Default(12))(ext, WeightedSums(ExtractWithDimension(_1)))
      .toList.sortBy(_.position) shouldBe result18
  }

  it should "return its third over aggregates in 3D" in {
    toRDD(num3)
      .summariseWithValue(Over(_3), Default())(ext, WeightedSums(ExtractWithDimension(_3)))
      .toList.sortBy(_.position) shouldBe result19
  }

  it should "return its third along aggregates in 3D" in {
    toRDD(num3)
      .summariseWithValue(Along(_3), Default(12))(ext, WeightedSums(ExtractWithDimension(_3)))
      .toList.sortBy(_.position) shouldBe result20
  }

  it should "throw an exception for too many single" in {
    a [Exception] shouldBe thrownBy { toRDD(num2).summariseWithValue(Over(_1))(ext, Minimum(), Maximum()) }
  }

  it should "throw an exception for a multiple" in {
    a [Exception] shouldBe thrownBy { toRDD(num2).summariseWithValue(Over(_1))(ext, TestMatrixSummarise.BadCount()) }
  }

  "A Matrix.summariseAndExpand" should "return its first over aggregates in 1D" in {
    toRDD(num1)
      .summarise(Over(_1), Default())(Minimum().andThenRelocate(_.position.append("min").toOption))
      .toList.sortBy(_.position) shouldBe result21
  }

  it should "return its first along aggregates in 1D" in {
    toRDD(num1)
      .summarise(Along(_1), Default(12))(
        List(
          Minimum[_1, _0]().andThenRelocate(_.position.append("min").toOption),
          Maximum[_1, _0]().andThenRelocate(_.position.append("max").toOption)
        )
      )
      .toList.sortBy(_.position) shouldBe result22
  }

  it should "return its first over aggregates in 2D" in {
    toRDD(num2)
      .summarise(Over(_1), Default())(Minimum().andThenRelocate(_.position.append("min").toOption))
      .toList.sortBy(_.position) shouldBe result23
  }

  it should "return its first along aggregates in 2D" in {
    toRDD(num2)
      .summarise(Along(_1), Default(12))(
        Minimum[_2, _1]().andThenRelocate(_.position.append("min").toOption),
        Maximum[_2, _1]().andThenRelocate(_.position.append("max").toOption)
      )
      .toList.sortBy(_.position) shouldBe result24
  }

  it should "return its second over aggregates in 2D" in {
    toRDD(num2)
      .summarise(Over(_2), Default())(
        List(
          Minimum[_2, _1]().andThenRelocate(_.position.append("min").toOption),
          Maximum[_2, _1]().andThenRelocate(_.position.append("max").toOption)
        )
      )
      .toList.sortBy(_.position) shouldBe result25
  }

  it should "return its second along aggregates in 2D" in {
    toRDD(num2)
      .summarise(Along(_2), Default(12))(Minimum().andThenRelocate(_.position.append("min").toOption))
      .toList.sortBy(_.position) shouldBe result26
  }

  it should "return its first over aggregates in 3D" in {
    toRDD(num3)
      .summarise(Over(_1), Default())(Minimum().andThenRelocate(_.position.append("min").toOption))
      .toList.sortBy(_.position) shouldBe result27
  }

  it should "return its first along aggregates in 3D" in {
    toRDD(num3)
      .summarise(Along(_1), Default(12))(
        Minimum[_3, _2]().andThenRelocate(_.position.append("min").toOption),
        Maximum[_3, _2]().andThenRelocate(_.position.append("max").toOption)
      )
      .toList.sortBy(_.position) shouldBe result28
  }

  it should "return its second over aggregates in 3D" in {
    toRDD(num3)
      .summarise(Over(_2), Default())(
        List(
          Minimum[_3, _1]().andThenRelocate(_.position.append("min").toOption),
          Maximum[_3, _1]().andThenRelocate(_.position.append("max").toOption)
        )
      )
      .toList.sortBy(_.position) shouldBe result29
  }

  it should "return its second along aggregates in 3D" in {
    toRDD(num3)
      .summarise(Along(_2), Default(12))(Minimum().andThenRelocate(_.position.append("min").toOption))
      .toList.sortBy(_.position) shouldBe result30
  }

  it should "return its third over aggregates in 3D" in {
    toRDD(num3)
      .summarise(Over(_3), Default())(
        Minimum[_3, _1]().andThenRelocate(_.position.append("min").toOption),
        Maximum[_3, _1]().andThenRelocate(_.position.append("max").toOption)
      )
      .toList.sortBy(_.position) shouldBe result31
  }

  it should "return its third along aggregates in 3D" in {
    toRDD(num3)
      .summarise(Along(_3), Default(12))(Minimum().andThenRelocate(_.position.append("min").toOption))
      .toList.sortBy(_.position) shouldBe result32
  }

  "A Matrix.summariseAndExpandWithValue" should "return its first over aggregates in 1D" in {
    toRDD(num1)
      .summariseWithValue(Over(_1), Default())(
        ext,
        WeightedSums(
          ExtractWithDimension[_1, Double](_1)
        ).andThenRelocateWithValue((c: Cell[_1], e: W) => c.position.append("sum").toOption)
      )
      .toList.sortBy(_.position) shouldBe result33
  }

  it should "return its first along aggregates in 1D" in {
    toRDD(num1)
      .summariseWithValue(Along(_1), Default(12))(
        ext,
        List(
          WeightedSums[_1, _0, W](
            ExtractWithDimension(_1)
          ).andThenRelocateWithValue((c: Cell[_0], e: W) => c.position.append("sum.1").toOption),
          WeightedSums[_1, _0, W](
            TestMatrixSummarise.ExtractWithName(_1, "%1$s.2")
          ).andThenRelocateWithValue((c: Cell[_0], e: W) => c.position.append("sum.2").toOption)
        )
      )
      .toList.sortBy(_.position) shouldBe result34
  }

  it should "return its first over aggregates in 2D" in {
    toRDD(num2)
      .summariseWithValue(Over(_1), Default())(
        ext,
        WeightedSums(
          ExtractWithDimension[_2, Double](_1)
        ).andThenRelocateWithValue((c: Cell[_1], e: W) => c.position.append("sum").toOption)
      )
      .toList.sortBy(_.position) shouldBe result35
  }

  it should "return its first along aggregates in 2D" in {
    toRDD(num2)
      .summariseWithValue(Along(_1), Default(12))(
        ext,
        WeightedSums[_2, _1, W](
          ExtractWithDimension(_2)
        ).andThenRelocateWithValue((c: Cell[_1], e: W) => c.position.append("sum.1").toOption),
        WeightedSums[_2, _1, W](
          TestMatrixSummarise.ExtractWithName(_1, "%1$s.2")
        ).andThenRelocateWithValue((c: Cell[_1], e: W) => c.position.append("sum.2").toOption)
      )
      .toList.sortBy(_.position) shouldBe result36
  }

  it should "return its second over aggregates in 2D" in {
    toRDD(num2)
      .summariseWithValue(Over(_2), Default())(
        ext,
        List(
          WeightedSums[_2, _1, W](
            ExtractWithDimension(_2)
          ).andThenRelocateWithValue((c: Cell[_1], e: W) => c.position.append("sum.1").toOption),
          WeightedSums[_2, _1, W](
            TestMatrixSummarise.ExtractWithName(_2, "%1$s.2")
          ).andThenRelocateWithValue((c: Cell[_1], e: W) => c.position.append("sum.2").toOption)
        )
      )
      .toList.sortBy(_.position) shouldBe result37
  }

  it should "return its second along aggregates in 2D" in {
    toRDD(num2)
      .summariseWithValue(Along(_2), Default(12))(
        ext,
        WeightedSums(
          ExtractWithDimension[_2, Double](_1)
        ).andThenRelocateWithValue((c: Cell[_1], e: W) => c.position.append("sum").toOption)
      )
      .toList.sortBy(_.position) shouldBe result38
  }

  it should "return its first over aggregates in 3D" in {
    toRDD(num3)
      .summariseWithValue(Over(_1), Default())(
        ext,
        WeightedSums(
          ExtractWithDimension[_3, Double](_1)
        ).andThenRelocateWithValue((c: Cell[_1], e: W) => c.position.append("sum").toOption)
      )
      .toList.sortBy(_.position) shouldBe result39
  }

  it should "return its first along aggregates in 3D" in {
    toRDD(num3)
      .summariseWithValue(Along(_1), Default(12))(
        ext,
        WeightedSums[_3, _2, W](
          ExtractWithDimension(_2)
        ).andThenRelocateWithValue((c: Cell[_2], e: W) => c.position.append("sum.1").toOption),
        WeightedSums[_3, _2, W](
          TestMatrixSummarise.ExtractWithName(_2, "%1$s.2")
        ).andThenRelocateWithValue((c: Cell[_2], e: W) => c.position.append("sum.2").toOption)
      )
      .toList.sortBy(_.position) shouldBe result40
  }

  it should "return its second over aggregates in 3D" in {
    toRDD(num3)
      .summariseWithValue(Over(_2), Default())(
        ext,
        List(
          WeightedSums[_3, _1, W](
            ExtractWithDimension(_2)
          ).andThenRelocateWithValue((c: Cell[_1], e: W) => c.position.append("sum.1").toOption),
          WeightedSums[_3, _1, W](
            TestMatrixSummarise.ExtractWithName(_2, "%1$s.2")
          ).andThenRelocateWithValue((c: Cell[_1], e: W) => c.position.append("sum.2").toOption)
        )
      )
      .toList.sortBy(_.position) shouldBe result41
  }

  it should "return its second along aggregates in 3D" in {
    toRDD(num3)
      .summariseWithValue(Along(_2), Default(12))(
        ext,
        WeightedSums[_3, _2, W](
          ExtractWithDimension(_1)
        ).andThenRelocateWithValue((c: Cell[_2], e: W) => c.position.append("sum").toOption)
      )
      .toList.sortBy(_.position) shouldBe result42
  }

  it should "return its third over aggregates in 3D" in {
    toRDD(num3)
      .summariseWithValue(Over(_3), Default())(
        ext,
        WeightedSums[_3, _1, W](
          ExtractWithDimension(_3)
        ).andThenRelocateWithValue((c: Cell[_1], e: W) => c.position.append("sum.1").toOption),
        WeightedSums[_3, _1, W](
          TestMatrixSummarise.ExtractWithName(_3, "%1$s.2")
        ).andThenRelocateWithValue((c: Cell[_1], e: W) => c.position.append("sum.2").toOption)
      )
      .toList.sortBy(_.position) shouldBe result43
  }

  it should "return its third along aggregates in 3D" in {
    toRDD(num3)
      .summariseWithValue(Along(_3), Default(12))(
        ext,
        WeightedSums[_3, _2, W](
          ExtractWithDimension(_3)
        ).andThenRelocateWithValue((c: Cell[_2], e: W) => c.position.append("sum").toOption)
      )
      .toList.sortBy(_.position) shouldBe result44
  }
}

trait TestMatrixSplit extends TestMatrix {

  implicit val TO1 = TestMatrixSplit.TupleOrdering[_1]
  implicit val TO2 = TestMatrixSplit.TupleOrdering[_2]
  implicit val TO3 = TestMatrixSplit.TupleOrdering[_3]

  val result1 = data1.map(c => (c.position(_1).toShortString, c)).sorted

  val result2 = data2.map(c => (c.position(_1).toShortString, c)).sorted

  val result3 = data2.map(c => (c.position(_2).toShortString, c)).sorted

  val result4 = data3.map(c => (c.position(_1).toShortString, c)).sorted

  val result5 = data3.map(c => (c.position(_2).toShortString, c)).sorted

  val result6 = data3.map(c => (c.position(_3).toShortString, c)).sorted

  val result7 = data1.map(c => (c.position(_1).toShortString, c)).sorted

  val result8 = data2.map(c => (c.position(_1).toShortString, c)).sorted

  val result9 = data2.map(c => (c.position(_2).toShortString, c)).sorted

  val result10 = data3.map(c => (c.position(_1).toShortString, c)).sorted

  val result11 = data3.map(c => (c.position(_2).toShortString, c)).sorted

  val result12 = data3.map(c => (c.position(_3).toShortString, c)).sorted
}

object TestMatrixSplit {

  case class TestPartitioner[
    D <: Nat : ToInt,
    P <: Nat
  ](
    dim: D
  )(implicit
    ev: LTEq[D, P]
  ) extends Partitioner[P, String] {
    def assign(cell: Cell[P]): TraversableOnce[String] = List(cell.position(dim).toShortString)
  }

  case class TestPartitionerWithValue[
    D <: Nat : ToInt,
    P <: Nat
  ](
  )(implicit
    ev: LTEq[D, P]
  ) extends PartitionerWithValue[P, String] {
    type V = D
    def assignWithValue(cell: Cell[P], ext: V): TraversableOnce[String] = List(cell.position(ext).toShortString)
  }

  def TupleOrdering[P <: Nat](): Ordering[(String, Cell[P])] = new Ordering[(String, Cell[P])] {
    def compare(x: (String, Cell[P]), y: (String, Cell[P])): Int = x._1.compare(y._1) match {
      case cmp if (cmp == 0) => x._2.position.compare(y._2.position)
      case cmp => cmp
    }
  }
}

class TestScaldingMatrixSplit extends TestMatrixSplit {

  "A Matrix.split" should "return its first partitions in 1D" in {
    toPipe(data1)
      .split(TestMatrixSplit.TestPartitioner(_1))
      .toList.sorted shouldBe result1
  }

  it should "return its first partitions in 2D" in {
    toPipe(data2)
      .split(TestMatrixSplit.TestPartitioner(_1))
      .toList.sorted shouldBe result2
  }

  it should "return its second partitions in 2D" in {
    toPipe(data2)
      .split(TestMatrixSplit.TestPartitioner(_2))
      .toList.sorted shouldBe result3
  }

  it should "return its first partitions in 3D" in {
    toPipe(data3)
      .split(TestMatrixSplit.TestPartitioner(_1))
      .toList.sorted shouldBe result4
  }

  it should "return its second partitions in 3D" in {
    toPipe(data3)
      .split(TestMatrixSplit.TestPartitioner(_2))
      .toList.sorted shouldBe result5
  }

  it should "return its third partitions in 3D" in {
    toPipe(data3)
      .split(TestMatrixSplit.TestPartitioner(_3))
      .toList.sorted shouldBe result6
  }

  "A Matrix.splitWithValue" should "return its first partitions in 1D" in {
    toPipe(data1)
      .splitWithValue(ValuePipe(_1), TestMatrixSplit.TestPartitionerWithValue[_1, _1]())
      .toList.sorted shouldBe result7
  }

  it should "return its first partitions in 2D" in {
    toPipe(data2)
      .splitWithValue(ValuePipe(_1), TestMatrixSplit.TestPartitionerWithValue[_1, _2]())
      .toList.sorted shouldBe result8
  }

  it should "return its second partitions in 2D" in {
    toPipe(data2)
      .splitWithValue(ValuePipe(_2), TestMatrixSplit.TestPartitionerWithValue[_2, _2]())
      .toList.sorted shouldBe result9
  }

  it should "return its first partitions in 3D" in {
    toPipe(data3)
      .splitWithValue(ValuePipe(_1), TestMatrixSplit.TestPartitionerWithValue[_1, _3]())
      .toList.sorted shouldBe result10
  }

  it should "return its second partitions in 3D" in {
    toPipe(data3)
      .splitWithValue(ValuePipe(_2), TestMatrixSplit.TestPartitionerWithValue[_2, _3]())
      .toList.sorted shouldBe result11
  }

  it should "return its third partitions in 3D" in {
    toPipe(data3)
      .splitWithValue(ValuePipe(_3), TestMatrixSplit.TestPartitionerWithValue[_3, _3]())
      .toList.sorted shouldBe result12
  }
}

class TestSparkMatrixSplit extends TestMatrixSplit {

  "A Matrix.split" should "return its first partitions in 1D" in {
    toRDD(data1)
      .split(TestMatrixSplit.TestPartitioner(_1))
      .toList.sorted shouldBe result1
  }

  it should "return its first partitions in 2D" in {
    toRDD(data2)
      .split(TestMatrixSplit.TestPartitioner(_1))
      .toList.sorted shouldBe result2
  }

  it should "return its second partitions in 2D" in {
    toRDD(data2)
      .split(TestMatrixSplit.TestPartitioner(_2))
      .toList.sorted shouldBe result3
  }

  it should "return its first partitions in 3D" in {
    toRDD(data3)
      .split(TestMatrixSplit.TestPartitioner(_1))
      .toList.sorted shouldBe result4
  }

  it should "return its second partitions in 3D" in {
    toRDD(data3)
      .split(TestMatrixSplit.TestPartitioner(_2))
      .toList.sorted shouldBe result5
  }

  it should "return its third partitions in 3D" in {
    toRDD(data3)
      .split(TestMatrixSplit.TestPartitioner(_3))
      .toList.sorted shouldBe result6
  }

  "A Matrix.splitWithValue" should "return its first partitions in 1D" in {
    toRDD(data1)
      .splitWithValue(_1, TestMatrixSplit.TestPartitionerWithValue[_1, _1]())
      .toList.sorted shouldBe result7
  }

  it should "return its first partitions in 2D" in {
    toRDD(data2)
      .splitWithValue(_1, TestMatrixSplit.TestPartitionerWithValue[_1, _2]())
      .toList.sorted shouldBe result8
  }

  it should "return its second partitions in 2D" in {
    toRDD(data2)
      .splitWithValue(_2, TestMatrixSplit.TestPartitionerWithValue[_2, _2]())
      .toList.sorted shouldBe result9
  }

  it should "return its first partitions in 3D" in {
    toRDD(data3)
      .splitWithValue(_1, TestMatrixSplit.TestPartitionerWithValue[_1, _3]())
      .toList.sorted shouldBe result10
  }

  it should "return its second partitions in 3D" in {
    toRDD(data3)
      .splitWithValue(_2, TestMatrixSplit.TestPartitionerWithValue[_2, _3]())
      .toList.sorted shouldBe result11
  }

  it should "return its third partitions in 3D" in {
    toRDD(data3)
      .splitWithValue(_3, TestMatrixSplit.TestPartitionerWithValue[_3, _3]())
      .toList.sorted shouldBe result12
  }
}

trait TestMatrixSubset extends TestMatrix {

  val ext = "foo"

  val result1 = List(Cell(Position("foo"), Content(OrdinalSchema[String](), "3.14")))

  val result2 = List(
    Cell(Position("bar", 2), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("baz", 2), Content(DiscreteSchema[Long](), 19)),
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

  val result3 = List(
    Cell(Position("bar", 2, "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("baz", 2, "xyz"), Content(DiscreteSchema[Long](), 19)),
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

  val result4 = List(Cell(Position("foo"), Content(OrdinalSchema[String](), "3.14")))

  val result5 = List(
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

  val result6 = List(
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
}

object TestMatrixSubset {

  case class TestSampler[P <: Nat]() extends Sampler[P] {
    def select(cell: Cell[P]): Boolean =
      cell.position.coordinates.contains(StringValue("foo")) || cell.position.coordinates.contains(LongValue(2))
  }

  case class TestSamplerWithValue[P <: Nat]() extends SamplerWithValue[P] {
    type V = String
    def selectWithValue(cell: Cell[P], ext: V): Boolean = cell.position.coordinates.contains(StringValue(ext))
  }
}

class TestScaldingMatrixSubset extends TestMatrixSubset {

  "A Matrix.subset" should "return its sampled data in 1D" in {
    toPipe(data1)
      .subset(TestMatrixSubset.TestSampler())
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its sampled data in 2D" in {
    toPipe(data2)
      .subset(TestMatrixSubset.TestSampler())
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its sampled data in 3D" in {
    toPipe(data3)
      .subset(TestMatrixSubset.TestSampler())
      .toList.sortBy(_.position) shouldBe result3
  }

  "A Matrix.subsetWithValue" should "return its sampled data in 1D" in {
    toPipe(data1)
      .subsetWithValue(ValuePipe(ext), TestMatrixSubset.TestSamplerWithValue())
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its sampled data in 2D" in {
    toPipe(data2)
      .subsetWithValue(ValuePipe(ext), TestMatrixSubset.TestSamplerWithValue())
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its sampled data in 3D" in {
    toPipe(data3)
      .subsetWithValue(ValuePipe(ext), TestMatrixSubset.TestSamplerWithValue())
      .toList.sortBy(_.position) shouldBe result6
  }
}

class TestSparkMatrixSubset extends TestMatrixSubset {

  "A Matrix.subset" should "return its sampled data in 1D" in {
    toRDD(data1)
      .subset(TestMatrixSubset.TestSampler())
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its sampled data in 2D" in {
    toRDD(data2)
      .subset(TestMatrixSubset.TestSampler())
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its sampled data in 3D" in {
    toRDD(data3)
      .subset(TestMatrixSubset.TestSampler())
      .toList.sortBy(_.position) shouldBe result3
  }

  "A Matrix.subsetWithValue" should "return its sampled data in 1D" in {
    toRDD(data1)
      .subsetWithValue(ext, TestMatrixSubset.TestSamplerWithValue())
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its sampled data in 2D" in {
    toRDD(data2)
      .subsetWithValue(ext, TestMatrixSubset.TestSamplerWithValue())
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its sampled data in 3D" in {
    toRDD(data3)
      .subsetWithValue(ext, TestMatrixSubset.TestSamplerWithValue())
      .toList.sortBy(_.position) shouldBe result6
  }
}

class TestMatrixDomain extends TestMatrix {

  val dataA = List(
    Cell(Position(1), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position(2), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position(3), Content(ContinuousSchema[Double](), 9.42))
  )

  val dataB = List(
    Cell(Position(1, 3), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position(2, 2), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position(3, 1), Content(ContinuousSchema[Double](), 9.42))
  )

  val dataC = List(
    Cell(Position(1, 1, 1), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position(2, 2, 2), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position(3, 3, 3), Content(ContinuousSchema[Double](), 9.42)),
    Cell(Position(1, 2, 3), Content(ContinuousSchema[Double](), 0.0))
  )

  val dataD = List(
    Cell(Position(1, 4, 2, 3), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position(2, 3, 1, 4), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position(3, 2, 4, 1), Content(ContinuousSchema[Double](), 9.42)),
    Cell(Position(4, 1, 3, 2), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position(1, 2, 3, 4), Content(ContinuousSchema[Double](), 0.0))
  )

  val dataE = List(
    Cell(Position(1, 5, 4, 3, 2), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position(2, 1, 5, 4, 3), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position(3, 2, 1, 5, 4), Content(ContinuousSchema[Double](), 9.42)),
    Cell(Position(4, 3, 2, 1, 5), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position(5, 4, 3, 2, 1), Content(ContinuousSchema[Double](), 18.84)),
    Cell(Position(1, 2, 3, 4, 5), Content(ContinuousSchema[Double](), 0.0))
  )

  val result1 = List(Position(1), Position(2), Position(3))

  val result2 = List(
    Position(1, 1),
    Position(1, 2),
    Position(1, 3),
    Position(2, 1),
    Position(2, 2),
    Position(2, 3),
    Position(3, 1),
    Position(3, 2),
    Position(3, 3)
  )

  private val l3 = List(1, 2, 3)
  private val i3 = for (a <- l3; b <- l3; c <- l3) yield Iterable(Position(a, b, c))
  val result3 = i3.toList.flatten.sorted

  private val l4 = List(1, 2, 3, 4)
  private val i4 = for (a <- l4; b <- l4; c <- l4; d <- l4) yield Iterable(Position(a, b, c, d))
  val result4 = i4.toList.flatten.sorted

  private val l5 = List(1, 2, 3, 4, 5)
  private val i5 = for (a <- l5; b <- l5; c <- l5; d <- l5; e <- l5) yield Iterable(Position(a, b, c, d, e))
  val result5 = i5.toList.flatten.sorted
}

class TestScaldingMatrixDomain extends TestMatrixDomain {

  "A Matrix.domain" should "return its domain in 1D" in {
    toPipe(dataA)
      .domain(InMemory())
      .toList.sorted shouldBe result1
  }

  it should "return its domain in 2D" in {
    toPipe(dataB)
      .domain(InMemory(12))
      .toList.sorted shouldBe result2
  }

  it should "return its domain in 3D" in {
    toPipe(dataC)
      .domain(Default())
      .toList.sorted shouldBe result3
  }

  it should "return its domain in 4D" in {
    toPipe(dataD)
      .domain(Default(12))
      .toList.sorted shouldBe result4
  }

  it should "return its domain in 5D" in {
    toPipe(dataE)
      .domain(InMemory())
      .toList.sorted shouldBe result5
  }
}

class TestSparkMatrixDomain extends TestMatrixDomain {

  "A Matrix.domain" should "return its domain in 1D" in {
    toRDD(dataA)
      .domain(Default())
      .toList.sorted shouldBe result1
  }

  it should "return its domain in 2D" in {
    toRDD(dataB)
      .domain(Default(12))
      .toList.sorted shouldBe result2
  }

  it should "return its domain in 3D" in {
    toRDD(dataC)
      .domain(Default())
      .toList.sorted shouldBe result3
  }

  it should "return its domain in 4D" in {
    toRDD(dataD)
      .domain(Default(12))
      .toList.sorted shouldBe result4
  }

  it should "return its domain in 5D" in {
    toRDD(dataE)
      .domain(Default())
      .toList.sorted shouldBe result5
  }
}

trait TestMatrixJoin extends TestMatrix {

  val dataA = List(
    Cell(Position("bar", 5), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("baz", 5), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("qux", 5), Content(OrdinalSchema[String](), "12.56")),
    Cell(Position("bar", 6), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("baz", 6), Content(DiscreteSchema[Long](), 19)),
    Cell(Position("bar", 7), Content(OrdinalSchema[Long](), 19))
  )

  val dataB = List(
    Cell(Position("foo.2", 2), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("bar.2", 2), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("baz.2", 2), Content(DiscreteSchema[Long](), 19)),
    Cell(Position("foo.2", 3), Content(NominalSchema[String](), "9.42")),
    Cell(Position("bar.2", 3), Content(OrdinalSchema[Long](), 19))
  )

  val dataC = List(
    Cell(Position("foo.2", 2), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("bar.2", 2), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("baz.2", 2), Content(DiscreteSchema[Long](), 19)),
    Cell(Position("foo.2", 3), Content(NominalSchema[String](), "9.42")),
    Cell(Position("bar.2", 3), Content(OrdinalSchema[Long](), 19))
  )

  val dataD = List(
    Cell(Position("bar", 5), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("baz", 5), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("qux", 5), Content(OrdinalSchema[String](), "12.56")),
    Cell(Position("bar", 6), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("baz", 6), Content(DiscreteSchema[Long](), 19)),
    Cell(Position("bar", 7), Content(OrdinalSchema[Long](), 19))
  )

  val dataE = List(
    Cell(Position("bar", 5, "xyz"), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("baz", 5, "xyz"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("qux", 5, "xyz"), Content(OrdinalSchema[String](), "12.56")),
    Cell(Position("bar", 6, "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("baz", 6, "xyz"), Content(DiscreteSchema[Long](), 19)),
    Cell(Position("bar", 7, "xyz"), Content(OrdinalSchema[Long](), 19))
  )

  val dataF = List(
    Cell(Position("foo.2", 2, "xyz"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("bar.2", 2, "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("baz.2", 2, "xyz"), Content(DiscreteSchema[Long](), 19)),
    Cell(Position("foo.2", 3, "xyz"), Content(NominalSchema[String](), "9.42")),
    Cell(Position("bar.2", 3, "xyz"), Content(OrdinalSchema[Long](), 19))
  )

  val dataG = List(
    Cell(Position("foo.2", 2, "xyz"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("bar.2", 2, "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("baz.2", 2, "xyz"), Content(DiscreteSchema[Long](), 19)),
    Cell(Position("foo.2", 3, "xyz"), Content(NominalSchema[String](), "9.42")),
    Cell(Position("bar.2", 3, "xyz"), Content(OrdinalSchema[Long](), 19))
  )

  val dataH = List(
    Cell(Position("bar", 5, "xyz"), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("baz", 5, "xyz"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("qux", 5, "xyz"), Content(OrdinalSchema[String](), "12.56")),
    Cell(Position("bar", 6, "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("baz", 6, "xyz"), Content(DiscreteSchema[Long](), 19)),
    Cell(Position("bar", 7, "xyz"), Content(OrdinalSchema[Long](), 19))
  )

  val dataI = List(
    Cell(Position("foo.2", 2, "xyz"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("bar.2", 2, "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("baz.2", 2, "xyz"), Content(DiscreteSchema[Long](), 19)),
    Cell(Position("foo.2", 3, "xyz"), Content(NominalSchema[String](), "9.42")),
    Cell(Position("bar.2", 3, "xyz"), Content(OrdinalSchema[Long](), 19))
  )

  val dataJ = List(
    Cell(Position("bar", 1, "xyz.2"), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("baz", 1, "xyz.2"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("qux", 1, "xyz.2"), Content(OrdinalSchema[String](), "12.56")),
    Cell(Position("bar", 2, "xyz.2"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("baz", 2, "xyz.2"), Content(DiscreteSchema[Long](), 19)),
    Cell(Position("bar", 3, "xyz.2"), Content(OrdinalSchema[Long](), 19))
  )

  val result1 = List(
    Cell(Position("bar", 1), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("bar", 2), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", 3), Content(OrdinalSchema[Long](), 19)),
    Cell(Position("bar", 5), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("bar", 6), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", 7), Content(OrdinalSchema[Long](), 19)),
    Cell(Position("baz", 1), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", 2), Content(DiscreteSchema[Long](), 19)),
    Cell(Position("baz", 5), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", 6), Content(DiscreteSchema[Long](), 19)),
    Cell(Position("qux", 1), Content(OrdinalSchema[String](), "12.56")),
    Cell(Position("qux", 5), Content(OrdinalSchema[String](), "12.56"))
  )

  val result2 = List(
    Cell(Position("bar", 2), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", 3), Content(OrdinalSchema[Long](), 19)),
    Cell(Position("bar.2", 2), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar.2", 3), Content(OrdinalSchema[Long](), 19)),
    Cell(Position("baz", 2), Content(DiscreteSchema[Long](), 19)),
    Cell(Position("baz.2", 2), Content(DiscreteSchema[Long](), 19)),
    Cell(Position("foo", 2), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("foo", 3), Content(NominalSchema[String](), "9.42")),
    Cell(Position("foo.2", 2), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("foo.2", 3), Content(NominalSchema[String](), "9.42"))
  )

  val result3 = List(
    Cell(Position("bar", 2), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", 3), Content(OrdinalSchema[Long](), 19)),
    Cell(Position("bar.2", 2), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar.2", 3), Content(OrdinalSchema[Long](), 19)),
    Cell(Position("baz", 2), Content(DiscreteSchema[Long](), 19)),
    Cell(Position("baz.2", 2), Content(DiscreteSchema[Long](), 19)),
    Cell(Position("foo", 2), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("foo", 3), Content(NominalSchema[String](), "9.42")),
    Cell(Position("foo.2", 2), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("foo.2", 3), Content(NominalSchema[String](), "9.42"))
  )

  val result4 = List(
    Cell(Position("bar", 1), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("bar", 2), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", 3), Content(OrdinalSchema[Long](), 19)),
    Cell(Position("bar", 5), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("bar", 6), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", 7), Content(OrdinalSchema[Long](), 19)),
    Cell(Position("baz", 1), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", 2), Content(DiscreteSchema[Long](), 19)),
    Cell(Position("baz", 5), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", 6), Content(DiscreteSchema[Long](), 19)),
    Cell(Position("qux", 1), Content(OrdinalSchema[String](), "12.56")),
    Cell(Position("qux", 5), Content(OrdinalSchema[String](), "12.56"))
  )

  val result5 = List(
    Cell(Position("bar", 1, "xyz"), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("bar", 2, "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", 3, "xyz"), Content(OrdinalSchema[Long](), 19)),
    Cell(Position("bar", 5, "xyz"), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("bar", 6, "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", 7, "xyz"), Content(OrdinalSchema[Long](), 19)),
    Cell(Position("baz", 1, "xyz"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", 2, "xyz"), Content(DiscreteSchema[Long](), 19)),
    Cell(Position("baz", 5, "xyz"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", 6, "xyz"), Content(DiscreteSchema[Long](), 19)),
    Cell(Position("qux", 1, "xyz"), Content(OrdinalSchema[String](), "12.56")),
    Cell(Position("qux", 5, "xyz"), Content(OrdinalSchema[String](), "12.56"))
  )

  val result6 = List(
    Cell(Position("bar", 2, "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", 3, "xyz"), Content(OrdinalSchema[Long](), 19)),
    Cell(Position("bar.2", 2, "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar.2", 3, "xyz"), Content(OrdinalSchema[Long](), 19)),
    Cell(Position("baz", 2, "xyz"), Content(DiscreteSchema[Long](), 19)),
    Cell(Position("baz.2", 2, "xyz"), Content(DiscreteSchema[Long](), 19)),
    Cell(Position("foo", 2, "xyz"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("foo", 3, "xyz"), Content(NominalSchema[String](), "9.42")),
    Cell(Position("foo.2", 2, "xyz"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("foo.2", 3, "xyz"), Content(NominalSchema[String](), "9.42"))
  )

  val result7 = List(
    Cell(Position("bar", 2, "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", 3, "xyz"), Content(OrdinalSchema[Long](), 19)),
    Cell(Position("bar.2", 2, "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar.2", 3, "xyz"), Content(OrdinalSchema[Long](), 19)),
    Cell(Position("baz", 2, "xyz"), Content(DiscreteSchema[Long](), 19)),
    Cell(Position("baz.2", 2, "xyz"), Content(DiscreteSchema[Long](), 19)),
    Cell(Position("foo", 2, "xyz"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("foo", 3, "xyz"), Content(NominalSchema[String](), "9.42")),
    Cell(Position("foo.2", 2, "xyz"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("foo.2", 3, "xyz"), Content(NominalSchema[String](), "9.42"))
  )

  val result8 = List(
    Cell(Position("bar", 1, "xyz"), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("bar", 2, "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", 3, "xyz"), Content(OrdinalSchema[Long](), 19)),
    Cell(Position("bar", 5, "xyz"), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("bar", 6, "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", 7, "xyz"), Content(OrdinalSchema[Long](), 19)),
    Cell(Position("baz", 1, "xyz"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", 2, "xyz"), Content(DiscreteSchema[Long](), 19)),
    Cell(Position("baz", 5, "xyz"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", 6, "xyz"), Content(DiscreteSchema[Long](), 19)),
    Cell(Position("qux", 1, "xyz"), Content(OrdinalSchema[String](), "12.56")),
    Cell(Position("qux", 5, "xyz"), Content(OrdinalSchema[String](), "12.56"))
  )

  val result9 = List(
    Cell(Position("bar", 1, "xyz"), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("bar", 2, "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", 3, "xyz"), Content(OrdinalSchema[Long](), 19)),
    Cell(Position("bar.2", 2, "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar.2", 3, "xyz"), Content(OrdinalSchema[Long](), 19)),
    Cell(Position("baz", 1, "xyz"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", 2, "xyz"), Content(DiscreteSchema[Long](), 19)),
    Cell(Position("baz.2", 2, "xyz"), Content(DiscreteSchema[Long](), 19)),
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
    Cell(Position("bar", 3, "xyz"), Content(OrdinalSchema[Long](), 19)),
    Cell(Position("bar", 3, "xyz.2"), Content(OrdinalSchema[Long](), 19)),
    Cell(Position("baz", 1, "xyz"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", 1, "xyz.2"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", 2, "xyz"), Content(DiscreteSchema[Long](), 19)),
    Cell(Position("baz", 2, "xyz.2"), Content(DiscreteSchema[Long](), 19)),
    Cell(Position("qux", 1, "xyz"), Content(OrdinalSchema[String](), "12.56")),
    Cell(Position("qux", 1, "xyz.2"), Content(OrdinalSchema[String](), "12.56"))
  )
}

class TestScaldingMatrixJoin extends TestMatrixJoin {

  "A Matrix.join" should "return its first over join in 2D" in {
    toPipe(data2)
      .join(Over(_1), InMemory())(toPipe(dataA))
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its first along join in 2D" in {
    toPipe(dataB)
      .join(Along(_1), Default())(toPipe(data2))
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its second over join in 2D" in {
    toPipe(dataC)
      .join(Over(_2), Binary(InMemory(), Default()))(toPipe(data2))
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its second along join in 2D" in {
    toPipe(data2)
      .join(Along(_2), Binary(InMemory(), Default(12)))(toPipe(dataD))
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its first over join in 3D" in {
    toPipe(data3)
      .join(Over(_1), Binary(InMemory(), Unbalanced(12)))(toPipe(dataE))
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its first along join in 3D" in {
    toPipe(dataF)
      .join(Along(_1), Binary(InMemory(12), Default(12)))(toPipe(data3))
      .toList.sortBy(_.position) shouldBe result6
  }

  it should "return its second over join in 3D" in {
    toPipe(dataG)
      .join(Over(_2), Binary(InMemory(12), Unbalanced(12)))(toPipe(data3))
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its second along join in 3D" in {
    toPipe(data3)
      .join(Along(_2), Binary(Default(), Default(12)))(toPipe(dataH))
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its third over join in 3D" in {
    toPipe(dataI)
      .join(Over(_3), Binary(Default(), Unbalanced(12)))(toPipe(data3))
      .toList.sortBy(_.position) shouldBe result9
  }

  it should "return its third along join in 3D" in {
    toPipe(data3)
      .join(Along(_3), Binary(Default(12), Default(12)))(toPipe(dataJ))
      .toList.sortBy(_.position) shouldBe result10
  }

  it should "return empty data - InMemory" in {
    toPipe(data3)
      .join(Along(_3), InMemory())(TypedPipe.empty)
      .toList.sortBy(_.position) shouldBe List()
  }

  it should "return empty data - Default" in {
    toPipe(data3)
      .join(Along(_3), Default())(TypedPipe.empty)
      .toList.sortBy(_.position) shouldBe List()
  }
}

class TestSparkMatrixJoin extends TestMatrixJoin {

  "A Matrix.join" should "return its first over join in 2D" in {
    toRDD(data2)
      .join(Over(_1), InMemory())(toRDD(dataA))
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its first along join in 2D" in {
    toRDD(dataB)
      .join(Along(_1), Default())(toRDD(data2))
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its second over join in 2D" in {
    toRDD(dataC)
      .join(Over(_2), Binary(InMemory(), Default()))(toRDD(data2))
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its second along join in 2D" in {
    toRDD(data2)
      .join(Along(_2), Binary(InMemory(), Default(12)))(toRDD(dataD))
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its first over join in 3D" in {
    toRDD(data3)
      .join(Over(_1), Binary(InMemory(12), Default(12)))(toRDD(dataE))
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its first along join in 3D" in {
    toRDD(dataF)
      .join(Along(_1), Binary(Default(), Default(12)))(toRDD(data3))
      .toList.sortBy(_.position) shouldBe result6
  }

  it should "return its second over join in 3D" in {
    toRDD(dataG)
      .join(Over(_2), Binary(Default(12), Default(12)))(toRDD(data3))
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its second along join in 3D" in {
    toRDD(data3)
      .join(Along(_2), InMemory())(toRDD(dataH))
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its third over join in 3D" in {
    toRDD(dataI)
      .join(Over(_3), Default())(toRDD(data3))
      .toList.sortBy(_.position) shouldBe result9
  }

  it should "return its third along join in 3D" in {
    toRDD(data3)
      .join(Along(_3), Binary(InMemory(), Default()))(toRDD(dataJ))
      .toList.sortBy(_.position) shouldBe result10
  }

  it should "return empty data - Default" in {
    toRDD(data3)
      .join(Along(_3), Default())(toRDD(List.empty[Cell[_3]]))
      .toList.sortBy(_.position) shouldBe List()
  }
}

trait TestMatrixUnique extends TestMatrix {

  val result1 = List(
    Content(OrdinalSchema[String](), "12.56"),
    Content(OrdinalSchema[String](), "3.14"),
    Content(OrdinalSchema[String](), "6.28"),
    Content(OrdinalSchema[String](), "9.42")
  )

  val result2 = List(
    (Position("bar"), Content(OrdinalSchema[String](), "6.28")),
    (Position("baz"), Content(OrdinalSchema[String](), "9.42")),
    (Position("foo"), Content(OrdinalSchema[String](), "3.14")),
    (Position("qux"), Content(OrdinalSchema[String](), "12.56"))
  )

  val result3 = List(
    Content(ContinuousSchema[Double](), 12.56),
    Content(ContinuousSchema[Double](), 6.28),
    Content(
      DateSchema[java.util.Date](),
      DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
    ),
    Content(DiscreteSchema[Long](), 19),
    Content(NominalSchema[String](), "9.42"),
    Content(OrdinalSchema[Long](), 19),
    Content(OrdinalSchema[String](), "12.56"),
    Content(OrdinalSchema[String](), "3.14"),
    Content(OrdinalSchema[String](), "6.28"),
    Content(OrdinalSchema[String](), "9.42")
  )

  val result4 = List(
    (Position("bar"), Content(ContinuousSchema[Double](), 12.56)),
    (Position("bar"), Content(OrdinalSchema[Long](), 19)),
    (Position("bar"), Content(OrdinalSchema[String](), "6.28")),
    (Position("baz"), Content(DiscreteSchema[Long](), 19)),
    (Position("baz"), Content(OrdinalSchema[String](), "9.42")),
    (Position("foo"), Content(ContinuousSchema[Double](), 6.28)),
    (
      Position("foo"),
      Content(
        DateSchema[java.util.Date](),
        DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
      )
    ),
    (Position("foo"), Content(NominalSchema[String](), "9.42")),
    (Position("foo"), Content(OrdinalSchema[String](), "3.14")),
    (Position("qux"), Content(OrdinalSchema[String](), "12.56")))

  val result5 = List(
    (Position(1), Content(OrdinalSchema[String](), "12.56")),
    (Position(1), Content(OrdinalSchema[String](), "3.14")),
    (Position(1), Content(OrdinalSchema[String](), "6.28")),
    (Position(1), Content(OrdinalSchema[String](), "9.42")),
    (Position(2), Content(ContinuousSchema[Double](), 12.56)),
    (Position(2), Content(ContinuousSchema[Double](), 6.28)),
    (Position(2), Content(DiscreteSchema[Long](), 19)),
    (Position(3), Content(NominalSchema[String](), "9.42")),
    (Position(3), Content(OrdinalSchema[Long](), 19)),
    (
      Position(4),
      Content(
        DateSchema[java.util.Date](),
        DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
      )
    )
  )

  val result6 = List(
    (Position(1), Content(OrdinalSchema[String](), "12.56")),
    (Position(1), Content(OrdinalSchema[String](), "3.14")),
    (Position(1), Content(OrdinalSchema[String](), "6.28")),
    (Position(1), Content(OrdinalSchema[String](), "9.42")),
    (Position(2), Content(ContinuousSchema[Double](), 12.56)),
    (Position(2), Content(ContinuousSchema[Double](), 6.28)),
    (Position(2), Content(DiscreteSchema[Long](), 19)),
    (Position(3), Content(NominalSchema[String](), "9.42")),
    (Position(3), Content(OrdinalSchema[Long](), 19)),
    (
      Position(4),
      Content(
        DateSchema[java.util.Date](),
        DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
      )
    )
  )

  val result7 = List(
    (Position("bar"), Content(ContinuousSchema[Double](), 12.56)),
    (Position("bar"), Content(OrdinalSchema[Long](), 19)),
    (Position("bar"), Content(OrdinalSchema[String](), "6.28")),
    (Position("baz"), Content(DiscreteSchema[Long](), 19)),
    (Position("baz"), Content(OrdinalSchema[String](), "9.42")),
    (Position("foo"), Content(ContinuousSchema[Double](), 6.28)),
    (
      Position("foo"),
      Content(
        DateSchema[java.util.Date](),
        DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
      )
    ),
    (Position("foo"), Content(NominalSchema[String](), "9.42")),
    (Position("foo"), Content(OrdinalSchema[String](), "3.14")),
    (Position("qux"), Content(OrdinalSchema[String](), "12.56"))
  )

  val result8 = List(
    Content(ContinuousSchema[Double](), 12.56),
    Content(ContinuousSchema[Double](), 6.28),
    Content(
      DateSchema[java.util.Date](),
      DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
    ),
    Content(DiscreteSchema[Long](), 19),
    Content(NominalSchema[String](), "9.42"),
    Content(OrdinalSchema[Long](), 19),
    Content(OrdinalSchema[String](), "12.56"),
    Content(OrdinalSchema[String](), "3.14"),
    Content(OrdinalSchema[String](), "6.28"),
    Content(OrdinalSchema[String](), "9.42")
  )

  val result9 = List(
    (Position("bar"), Content(ContinuousSchema[Double](), 12.56)),
    (Position("bar"), Content(OrdinalSchema[Long](), 19)),
    (Position("bar"), Content(OrdinalSchema[String](), "6.28")),
    (Position("baz"), Content(DiscreteSchema[Long](), 19)),
    (Position("baz"), Content(OrdinalSchema[String](), "9.42")),
    (Position("foo"), Content(ContinuousSchema[Double](), 6.28)),
    (
      Position("foo"),
      Content(
        DateSchema[java.util.Date](),
        DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
      )
    ),
    (Position("foo"), Content(NominalSchema[String](), "9.42")),
    (Position("foo"), Content(OrdinalSchema[String](), "3.14")),
    (Position("qux"), Content(OrdinalSchema[String](), "12.56"))
  )

  val result10 = List(
    (Position(1, "xyz"), Content(OrdinalSchema[String](), "12.56")),
    (Position(1, "xyz"), Content(OrdinalSchema[String](), "3.14")),
    (Position(1, "xyz"), Content(OrdinalSchema[String](), "6.28")),
    (Position(1, "xyz"), Content(OrdinalSchema[String](), "9.42")),
    (Position(2, "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    (Position(2, "xyz"), Content(ContinuousSchema[Double](), 6.28)),
    (Position(2, "xyz"), Content(DiscreteSchema[Long](), 19)),
    (Position(3, "xyz"), Content(NominalSchema[String](), "9.42")),
    (Position(3, "xyz"), Content(OrdinalSchema[Long](), 19)),
    (
      Position(4, "xyz"),
      Content(
        DateSchema[java.util.Date](),
        DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
      )
    )
  )

  val result11 = List(
    (Position(1), Content(OrdinalSchema[String](), "12.56")),
    (Position(1), Content(OrdinalSchema[String](), "3.14")),
    (Position(1), Content(OrdinalSchema[String](), "6.28")),
    (Position(1), Content(OrdinalSchema[String](), "9.42")),
    (Position(2), Content(ContinuousSchema[Double](), 12.56)),
    (Position(2), Content(ContinuousSchema[Double](), 6.28)),
    (Position(2), Content(DiscreteSchema[Long](), 19)),
    (Position(3), Content(NominalSchema[String](), "9.42")),
    (Position(3), Content(OrdinalSchema[Long](), 19)),
    (
      Position(4),
      Content(
        DateSchema[java.util.Date](),
        DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
      )
    )
  )

  val result12 = List(
    (Position("bar", "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    (Position("bar", "xyz"), Content(OrdinalSchema[Long](), 19)),
    (Position("bar", "xyz"), Content(OrdinalSchema[String](), "6.28")),
    (Position("baz", "xyz"), Content(DiscreteSchema[Long](), 19)),
    (Position("baz", "xyz"), Content(OrdinalSchema[String](), "9.42")),
    (Position("foo", "xyz"), Content(ContinuousSchema[Double](), 6.28)),
    (
      Position("foo", "xyz"),
      Content(
        DateSchema[java.util.Date](),
        DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
      )
    ),
    (Position("foo", "xyz"), Content(NominalSchema[String](), "9.42")),
    (Position("foo", "xyz"), Content(OrdinalSchema[String](), "3.14")),
    (Position("qux", "xyz"), Content(OrdinalSchema[String](), "12.56"))
  )

  val result13 = List(
    (Position("xyz"), Content(ContinuousSchema[Double](), 12.56)),
    (Position("xyz"), Content(ContinuousSchema[Double](), 6.28)),
    (
      Position("xyz"),
      Content(
        DateSchema[java.util.Date](),
        DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
      )
    ),
    (Position("xyz"), Content(DiscreteSchema[Long](), 19)),
    (Position("xyz"), Content(NominalSchema[String](), "9.42")),
    (Position("xyz"), Content(OrdinalSchema[Long](), 19)),
    (Position("xyz"), Content(OrdinalSchema[String](), "12.56")),
    (Position("xyz"), Content(OrdinalSchema[String](), "3.14")),
    (Position("xyz"), Content(OrdinalSchema[String](), "6.28")),
    (Position("xyz"), Content(OrdinalSchema[String](), "9.42"))
  )

  val result14 = List(
    (Position("bar", 1), Content(OrdinalSchema[String](), "6.28")),
    (Position("bar", 2), Content(ContinuousSchema[Double](), 12.56)),
    (Position("bar", 3), Content(OrdinalSchema[Long](), 19)),
    (Position("baz", 1), Content(OrdinalSchema[String](), "9.42")),
    (Position("baz", 2), Content(DiscreteSchema[Long](), 19)),
    (Position("foo", 1), Content(OrdinalSchema[String](), "3.14")),
    (Position("foo", 2), Content(ContinuousSchema[Double](), 6.28)),
    (Position("foo", 3), Content(NominalSchema[String](), "9.42")),
    (
      Position("foo", 4),
      Content(
        DateSchema[java.util.Date](),
        DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
      )
    ),
    (Position("qux", 1), Content(OrdinalSchema[String](), "12.56"))
  )
}

class TestScaldingMatrixUnique extends TestMatrixUnique {

  "A Matrix.unique" should "return its content in 1D" in {
    toPipe(data1)
      .unique(Default())
      .toList.sortBy(_.toString) shouldBe result1
  }

  it should "return its first over content in 1D" in {
    toPipe(data1)
      .uniqueByPosition(Over(_1), Default(12))
      .toList.sortBy(_.toString) shouldBe result2
  }

  it should "return its content in 2D" in {
    toPipe(data2)
      .unique(Default())
      .toList.sortBy(_.toString) shouldBe result3
  }

  it should "return its first over content in 2D" in {
    toPipe(data2)
      .uniqueByPosition(Over(_1), Default(12))
      .toList.sortBy(_.toString) shouldBe result4
  }

  it should "return its first along content in 2D" in {
    toPipe(data2)
      .uniqueByPosition(Along(_1), Default())
      .toList.sortBy(_.toString) shouldBe result5
  }

  it should "return its second over content in 2D" in {
    toPipe(data2)
      .uniqueByPosition(Over(_2), Default(12))
      .toList.sortBy(_.toString) shouldBe result6
  }

  it should "return its second along content in 2D" in {
    toPipe(data2)
      .uniqueByPosition(Along(_2), Default())
      .toList.sortBy(_.toString) shouldBe result7
  }

  it should "return its content in 3D" in {
    toPipe(data3)
      .unique(Default(12))
      .toList.sortBy(_.toString) shouldBe result8
  }

  it should "return its first over content in 3D" in {
    toPipe(data3)
      .uniqueByPosition(Over(_1), Default())
      .toList.sortBy(_.toString) shouldBe result9
  }

  it should "return its first along content in 3D" in {
    toPipe(data3)
      .uniqueByPosition(Along(_1), Default(12))
      .toList.sortBy(_.toString) shouldBe result10
  }

  it should "return its second over content in 3D" in {
    toPipe(data3)
      .uniqueByPosition(Over(_2), Default())
      .toList.sortBy(_.toString) shouldBe result11
  }

  it should "return its second along content in 3D" in {
    toPipe(data3)
      .uniqueByPosition(Along(_2), Default(12))
      .toList.sortBy(_.toString) shouldBe result12
  }

  it should "return its third over content in 3D" in {
    toPipe(data3)
      .uniqueByPosition(Over(_3), Default())
      .toList.sortBy(_.toString) shouldBe result13
  }

  it should "return its third along content in 3D" in {
    toPipe(data3)
      .uniqueByPosition(Along(_3), Default(12))
      .toList.sortBy(_.toString) shouldBe result14
  }
}

class TestSparkMatrixUnique extends TestMatrixUnique {

  "A Matrix.unique" should "return its content in 1D" in {
    toRDD(data1)
      .unique(Default())
      .toList.sortBy(_.toString) shouldBe result1
  }

  it should "return its first over content in 1D" in {
    toRDD(data1)
      .uniqueByPosition(Over(_1), Default(12))
      .toList.sortBy(_.toString) shouldBe result2
  }

  it should "return its content in 2D" in {
    toRDD(data2)
      .unique(Default())
      .toList.sortBy(_.toString) shouldBe result3
  }

  it should "return its first over content in 2D" in {
    toRDD(data2)
      .uniqueByPosition(Over(_1), Default(12))
      .toList.sortBy(_.toString) shouldBe result4
  }

  it should "return its first along content in 2D" in {
    toRDD(data2)
      .uniqueByPosition(Along(_1), Default())
      .toList.sortBy(_.toString) shouldBe result5
  }

  it should "return its second over content in 2D" in {
    toRDD(data2)
      .uniqueByPosition(Over(_2), Default(12))
      .toList.sortBy(_.toString) shouldBe result6
  }

  it should "return its second along content in 2D" in {
    toRDD(data2)
      .uniqueByPosition(Along(_2), Default())
      .toList.sortBy(_.toString) shouldBe result7
  }

  it should "return its content in 3D" in {
    toRDD(data3)
      .unique(Default(12))
      .toList.sortBy(_.toString) shouldBe result8
  }

  it should "return its first over content in 3D" in {
    toRDD(data3)
      .uniqueByPosition(Over(_1), Default())
      .toList.sortBy(_.toString) shouldBe result9
  }

  it should "return its first along content in 3D" in {
    toRDD(data3)
      .uniqueByPosition(Along(_1), Default(12))
      .toList.sortBy(_.toString) shouldBe result10
  }

  it should "return its second over content in 3D" in {
    toRDD(data3)
      .uniqueByPosition(Over(_2), Default())
      .toList.sortBy(_.toString) shouldBe result11
  }

  it should "return its second along content in 3D" in {
    toRDD(data3)
      .uniqueByPosition(Along(_2), Default(12))
      .toList.sortBy(_.toString) shouldBe result12
  }

  it should "return its third over content in 3D" in {
    toRDD(data3)
      .uniqueByPosition(Over(_3), Default())
      .toList.sortBy(_.toString) shouldBe result13
  }

  it should "return its third along content in 3D" in {
    toRDD(data3)
      .uniqueByPosition(Along(_3), Default(12))
      .toList.sortBy(_.toString) shouldBe result14
  }
}

trait TestMatrixPairwise extends TestMatrix {

  val ext = 1.0

  val dataA = List(
    Cell(Position("bar"), Content(ContinuousSchema[Double](), 1.0)),
    Cell(Position("baz"), Content(ContinuousSchema[Double](), 2.0))
  )

  val dataB = List(
    Cell(Position("bar", 1), Content(ContinuousSchema[Double](), 1.0)),
    Cell(Position("bar", 2), Content(ContinuousSchema[Double](), 2.0)),
    Cell(Position("bar", 3), Content(ContinuousSchema[Double](), 3.0)),
    Cell(Position("baz", 1), Content(ContinuousSchema[Double](), 4.0)),
    Cell(Position("baz", 2), Content(ContinuousSchema[Double](), 5.0))
  )

  val dataC = List(
    Cell(Position("bar", 2), Content(ContinuousSchema[Double](), 1.0)),
    Cell(Position("baz", 2), Content(ContinuousSchema[Double](), 2.0)),
    Cell(Position("foo", 2), Content(ContinuousSchema[Double](), 3.0)),
    Cell(Position("foo", 4), Content(ContinuousSchema[Double](), 4.0))
  )

  val dataD = List(
    Cell(Position("bar", 2), Content(ContinuousSchema[Double](), 1.0)),
    Cell(Position("baz", 2), Content(ContinuousSchema[Double](), 2.0)),
    Cell(Position("foo", 2), Content(ContinuousSchema[Double](), 3.0)),
    Cell(Position("foo", 4), Content(ContinuousSchema[Double](), 4.0))
  )

  val dataE = List(
    Cell(Position("bar", 1), Content(ContinuousSchema[Double](), 1.0)),
    Cell(Position("bar", 2), Content(ContinuousSchema[Double](), 2.0)),
    Cell(Position("bar", 3), Content(ContinuousSchema[Double](), 3.0)),
    Cell(Position("baz", 1), Content(ContinuousSchema[Double](), 4.0)),
    Cell(Position("baz", 2), Content(ContinuousSchema[Double](), 5.0))
 )

  val dataF = List(
    Cell(Position("bar", 1, "xyz"), Content(ContinuousSchema[Double](), 1.0)),
    Cell(Position("bar", 2, "xyz"), Content(ContinuousSchema[Double](), 2.0)),
    Cell(Position("bar", 3, "xyz"), Content(ContinuousSchema[Double](), 3.0)),
    Cell(Position("baz", 1, "xyz"), Content(ContinuousSchema[Double](), 4.0)),
    Cell(Position("baz", 2, "xyz"), Content(ContinuousSchema[Double](), 5.0))
  )

  val dataG = List(
    Cell(Position("bar", 2, "xyz"), Content(ContinuousSchema[Double](), 1.0)),
    Cell(Position("baz", 2, "xyz"), Content(ContinuousSchema[Double](), 2.0)),
    Cell(Position("foo", 2, "xyz"), Content(ContinuousSchema[Double](), 3.0)),
    Cell(Position("foo", 4, "xyz"), Content(ContinuousSchema[Double](), 4.0))
  )

  val dataH = List(
    Cell(Position("bar", 2, "xyz"), Content(ContinuousSchema[Double](), 1.0)),
    Cell(Position("baz", 2, "xyz"), Content(ContinuousSchema[Double](), 2.0)),
    Cell(Position("foo", 2, "xyz"), Content(ContinuousSchema[Double](), 3.0)),
    Cell(Position("foo", 4, "xyz"), Content(ContinuousSchema[Double](), 4.0))
  )

  val dataI = List(
    Cell(Position("bar", 1, "xyz"), Content(ContinuousSchema[Double](), 1.0)),
    Cell(Position("bar", 2, "xyz"), Content(ContinuousSchema[Double](), 2.0)),
    Cell(Position("bar", 3, "xyz"), Content(ContinuousSchema[Double](), 3.0)),
    Cell(Position("baz", 1, "xyz"), Content(ContinuousSchema[Double](), 4.0)),
    Cell(Position("baz", 2, "xyz"), Content(ContinuousSchema[Double](), 5.0))
  )

  val dataJ = List(
    Cell(Position("bar", 2, "xyz"), Content(ContinuousSchema[Double](), 1.0)),
    Cell(Position("baz", 2, "xyz"), Content(ContinuousSchema[Double](), 2.0)),
    Cell(Position("foo", 2, "xyz"), Content(ContinuousSchema[Double](), 3.0)),
    Cell(Position("foo", 4, "xyz"), Content(ContinuousSchema[Double](), 4.0))
  )

  val dataK = List(
    Cell(Position("bar", 1, "xyz"), Content(ContinuousSchema[Double](), 1.0)),
    Cell(Position("bar", 2, "xyz"), Content(ContinuousSchema[Double](), 2.0)),
    Cell(Position("bar", 3, "xyz"), Content(ContinuousSchema[Double](), 3.0)),
    Cell(Position("baz", 1, "xyz"), Content(ContinuousSchema[Double](), 4.0)),
    Cell(Position("baz", 2, "xyz"), Content(ContinuousSchema[Double](), 5.0))
  )

  val dataL = List(
    Cell(Position("bar"), Content(ContinuousSchema[Double](), 1.0)),
    Cell(Position("baz"), Content(ContinuousSchema[Double](), 2.0))
  )

  val dataM = List(
    Cell(Position("bar", 1), Content(ContinuousSchema[Double](), 1.0)),
    Cell(Position("bar", 2), Content(ContinuousSchema[Double](), 2.0)),
    Cell(Position("bar", 3), Content(ContinuousSchema[Double](), 3.0)),
    Cell(Position("baz", 1), Content(ContinuousSchema[Double](), 4.0)),
    Cell(Position("baz", 2), Content(ContinuousSchema[Double](), 5.0))
  )

  val dataN = List(
    Cell(Position("bar", 2), Content(ContinuousSchema[Double](), 1.0)),
    Cell(Position("baz", 2), Content(ContinuousSchema[Double](), 2.0)),
    Cell(Position("foo", 2), Content(ContinuousSchema[Double](), 3.0)),
    Cell(Position("foo", 4), Content(ContinuousSchema[Double](), 4.0))
  )

  val dataO = List(
    Cell(Position("bar", 2), Content(ContinuousSchema[Double](), 1.0)),
    Cell(Position("baz", 2), Content(ContinuousSchema[Double](), 2.0)),
    Cell(Position("foo", 2), Content(ContinuousSchema[Double](), 3.0)),
    Cell(Position("foo", 4), Content(ContinuousSchema[Double](), 4.0))
  )

  val dataP = List(
    Cell(Position("bar", 1), Content(ContinuousSchema[Double](), 1.0)),
    Cell(Position("bar", 2), Content(ContinuousSchema[Double](), 2.0)),
    Cell(Position("bar", 3), Content(ContinuousSchema[Double](), 3.0)),
    Cell(Position("baz", 1), Content(ContinuousSchema[Double](), 4.0)),
    Cell(Position("baz", 2), Content(ContinuousSchema[Double](), 5.0))
  )

  val dataQ = List(
    Cell(Position("bar", 1, "xyz"), Content(ContinuousSchema[Double](), 1.0)),
    Cell(Position("bar", 2, "xyz"), Content(ContinuousSchema[Double](), 2.0)),
    Cell(Position("bar", 3, "xyz"), Content(ContinuousSchema[Double](), 3.0)),
    Cell(Position("baz", 1, "xyz"), Content(ContinuousSchema[Double](), 4.0)),
    Cell(Position("baz", 2, "xyz"), Content(ContinuousSchema[Double](), 5.0))
  )

  val dataR = List(
    Cell(Position("bar", 2, "xyz"), Content(ContinuousSchema[Double](), 1.0)),
    Cell(Position("baz", 2, "xyz"), Content(ContinuousSchema[Double](), 2.0)),
    Cell(Position("foo", 2, "xyz"), Content(ContinuousSchema[Double](), 3.0)),
    Cell(Position("foo", 4, "xyz"), Content(ContinuousSchema[Double](), 4.0))
  )

  val dataS = List(
    Cell(Position("bar", 2, "xyz"), Content(ContinuousSchema[Double](), 1.0)),
    Cell(Position("baz", 2, "xyz"), Content(ContinuousSchema[Double](), 2.0)),
    Cell(Position("foo", 2, "xyz"), Content(ContinuousSchema[Double](), 3.0)),
    Cell(Position("foo", 4, "xyz"), Content(ContinuousSchema[Double](), 4.0))
  )

  val dataT = List(
    Cell(Position("bar", 1, "xyz"), Content(ContinuousSchema[Double](), 1.0)),
    Cell(Position("bar", 2, "xyz"), Content(ContinuousSchema[Double](), 2.0)),
    Cell(Position("bar", 3, "xyz"), Content(ContinuousSchema[Double](), 3.0)),
    Cell(Position("baz", 1, "xyz"), Content(ContinuousSchema[Double](), 4.0)),
    Cell(Position("baz", 2, "xyz"), Content(ContinuousSchema[Double](), 5.0))
  )

  val dataU = List(
    Cell(Position("bar", 2, "xyz"), Content(ContinuousSchema[Double](), 1.0)),
    Cell(Position("baz", 2, "xyz"), Content(ContinuousSchema[Double](), 2.0)),
    Cell(Position("foo", 2, "xyz"), Content(ContinuousSchema[Double](), 3.0)),
    Cell(Position("foo", 4, "xyz"), Content(ContinuousSchema[Double](), 4.0))
  )

  val dataV = List(
    Cell(Position("bar", 1, "xyz"), Content(ContinuousSchema[Double](), 1.0)),
    Cell(Position("bar", 2, "xyz"), Content(ContinuousSchema[Double](), 2.0)),
    Cell(Position("bar", 3, "xyz"), Content(ContinuousSchema[Double](), 3.0)),
    Cell(Position("baz", 1, "xyz"), Content(ContinuousSchema[Double](), 4.0)),
    Cell(Position("baz", 2, "xyz"), Content(ContinuousSchema[Double](), 5.0))
  )

  val result1 = List(
    Cell(Position("(baz+bar)"), Content(ContinuousSchema[Double](), 9.42 + 6.28)),
    Cell(Position("(foo+bar)"), Content(ContinuousSchema[Double](), 3.14 + 6.28)),
    Cell(Position("(foo+baz)"), Content(ContinuousSchema[Double](), 3.14 + 9.42)),
    Cell(Position("(qux+bar)"), Content(ContinuousSchema[Double](), 12.56 + 6.28)),
    Cell(Position("(qux+baz)"), Content(ContinuousSchema[Double](), 12.56 + 9.42)),
    Cell(Position("(qux+foo)"), Content(ContinuousSchema[Double](), 12.56 + 3.14))
  )

  val result2 = List(
    Cell(Position("(baz+bar)", 1), Content(ContinuousSchema[Double](), 9.42 + 6.28)),
    Cell(Position("(baz+bar)", 2), Content(ContinuousSchema[Double](), 18.84 + 12.56)),
    Cell(Position("(foo+bar)", 1), Content(ContinuousSchema[Double](), 3.14 + 6.28)),
    Cell(Position("(foo+bar)", 2), Content(ContinuousSchema[Double](), 6.28 + 12.56)),
    Cell(Position("(foo+bar)", 3), Content(ContinuousSchema[Double](), 9.42 + 18.84)),
    Cell(Position("(foo+baz)", 1), Content(ContinuousSchema[Double](), 3.14 + 9.42)),
    Cell(Position("(foo+baz)", 2), Content(ContinuousSchema[Double](), 6.28 + 18.84)),
    Cell(Position("(qux+bar)", 1), Content(ContinuousSchema[Double](), 12.56 + 6.28)),
    Cell(Position("(qux+baz)", 1), Content(ContinuousSchema[Double](), 12.56 + 9.42)),
    Cell(Position("(qux+foo)", 1), Content(ContinuousSchema[Double](), 12.56 + 3.14))
  )

  val result3 = List(
    Cell(Position("(2+1)", "bar"), Content(ContinuousSchema[Double](), 12.56 + 6.28)),
    Cell(Position("(2+1)", "baz"), Content(ContinuousSchema[Double](), 18.84 + 9.42)),
    Cell(Position("(2+1)", "foo"), Content(ContinuousSchema[Double](), 6.28 + 3.14)),
    Cell(Position("(2-1)", "bar"), Content(ContinuousSchema[Double](), 12.56 - 6.28)),
    Cell(Position("(2-1)", "baz"), Content(ContinuousSchema[Double](), 18.84 - 9.42)),
    Cell(Position("(2-1)", "foo"), Content(ContinuousSchema[Double](), 6.28 - 3.14)),
    Cell(Position("(3+1)", "bar"), Content(ContinuousSchema[Double](), 18.84 + 6.28)),
    Cell(Position("(3+1)", "foo"), Content(ContinuousSchema[Double](), 9.42 + 3.14)),
    Cell(Position("(3+2)", "bar"), Content(ContinuousSchema[Double](), 18.84 + 12.56)),
    Cell(Position("(3+2)", "foo"), Content(ContinuousSchema[Double](), 9.42 + 6.28)),
    Cell(Position("(3-1)", "bar"), Content(ContinuousSchema[Double](), 18.84 - 6.28)),
    Cell(Position("(3-1)", "foo"), Content(ContinuousSchema[Double](), 9.42 - 3.14)),
    Cell(Position("(3-2)", "bar"), Content(ContinuousSchema[Double](), 18.84 - 12.56)),
    Cell(Position("(3-2)", "foo"), Content(ContinuousSchema[Double](), 9.42 - 6.28)),
    Cell(Position("(4+1)", "foo"), Content(ContinuousSchema[Double](), 12.56 + 3.14)),
    Cell(Position("(4+2)", "foo"), Content(ContinuousSchema[Double](), 12.56 + 6.28)),
    Cell(Position("(4+3)", "foo"), Content(ContinuousSchema[Double](), 12.56 + 9.42)),
    Cell(Position("(4-1)", "foo"), Content(ContinuousSchema[Double](), 12.56 - 3.14)),
    Cell(Position("(4-2)", "foo"), Content(ContinuousSchema[Double](), 12.56 - 6.28)),
    Cell(Position("(4-3)", "foo"), Content(ContinuousSchema[Double](), 12.56 - 9.42))
  )

  val result4 = List(
    Cell(Position("(2+1)", "bar"), Content(ContinuousSchema[Double](), 12.56 + 6.28)),
    Cell(Position("(2+1)", "baz"), Content(ContinuousSchema[Double](), 18.84 + 9.42)),
    Cell(Position("(2+1)", "foo"), Content(ContinuousSchema[Double](), 6.28 + 3.14)),
    Cell(Position("(2-1)", "bar"), Content(ContinuousSchema[Double](), 12.56 - 6.28)),
    Cell(Position("(2-1)", "baz"), Content(ContinuousSchema[Double](), 18.84 - 9.42)),
    Cell(Position("(2-1)", "foo"), Content(ContinuousSchema[Double](), 6.28 - 3.14)),
    Cell(Position("(3+1)", "bar"), Content(ContinuousSchema[Double](), 18.84 + 6.28)),
    Cell(Position("(3+1)", "foo"), Content(ContinuousSchema[Double](), 9.42 + 3.14)),
    Cell(Position("(3+2)", "bar"), Content(ContinuousSchema[Double](), 18.84 + 12.56)),
    Cell(Position("(3+2)", "foo"), Content(ContinuousSchema[Double](), 9.42 + 6.28)),
    Cell(Position("(3-1)", "bar"), Content(ContinuousSchema[Double](), 18.84 - 6.28)),
    Cell(Position("(3-1)", "foo"), Content(ContinuousSchema[Double](), 9.42 - 3.14)),
    Cell(Position("(3-2)", "bar"), Content(ContinuousSchema[Double](), 18.84 - 12.56)),
    Cell(Position("(3-2)", "foo"), Content(ContinuousSchema[Double](), 9.42 - 6.28)),
    Cell(Position("(4+1)", "foo"), Content(ContinuousSchema[Double](), 12.56 + 3.14)),
    Cell(Position("(4+2)", "foo"), Content(ContinuousSchema[Double](), 12.56 + 6.28)),
    Cell(Position("(4+3)", "foo"), Content(ContinuousSchema[Double](), 12.56 + 9.42)),
    Cell(Position("(4-1)", "foo"), Content(ContinuousSchema[Double](), 12.56 - 3.14)),
    Cell(Position("(4-2)", "foo"), Content(ContinuousSchema[Double](), 12.56 - 6.28)),
    Cell(Position("(4-3)", "foo"), Content(ContinuousSchema[Double](), 12.56 - 9.42))
  )

  val result5 = List(
    Cell(Position("(baz+bar)", 1), Content(ContinuousSchema[Double](), 9.42 + 6.28)),
    Cell(Position("(baz+bar)", 2), Content(ContinuousSchema[Double](), 18.84 + 12.56)),
    Cell(Position("(foo+bar)", 1), Content(ContinuousSchema[Double](), 3.14 + 6.28)),
    Cell(Position("(foo+bar)", 2), Content(ContinuousSchema[Double](), 6.28 + 12.56)),
    Cell(Position("(foo+bar)", 3), Content(ContinuousSchema[Double](), 9.42 + 18.84)),
    Cell(Position("(foo+baz)", 1), Content(ContinuousSchema[Double](), 3.14 + 9.42)),
    Cell(Position("(foo+baz)", 2), Content(ContinuousSchema[Double](), 6.28 + 18.84)),
    Cell(Position("(qux+bar)", 1), Content(ContinuousSchema[Double](), 12.56 + 6.28)),
    Cell(Position("(qux+baz)", 1), Content(ContinuousSchema[Double](), 12.56 + 9.42)),
    Cell(Position("(qux+foo)", 1), Content(ContinuousSchema[Double](), 12.56 + 3.14))
  )

  val result6 = List(
    Cell(Position("(baz+bar)", 1, "xyz"), Content(ContinuousSchema[Double](), 9.42 + 6.28)),
    Cell(Position("(baz+bar)", 2, "xyz"), Content(ContinuousSchema[Double](), 18.84 + 12.56)),
    Cell(Position("(foo+bar)", 1, "xyz"), Content(ContinuousSchema[Double](), 3.14 + 6.28)),
    Cell(Position("(foo+bar)", 2, "xyz"), Content(ContinuousSchema[Double](), 6.28 + 12.56)),
    Cell(Position("(foo+bar)", 3, "xyz"), Content(ContinuousSchema[Double](), 9.42 + 18.84)),
    Cell(Position("(foo+baz)", 1, "xyz"), Content(ContinuousSchema[Double](), 3.14 + 9.42)),
    Cell(Position("(foo+baz)", 2, "xyz"), Content(ContinuousSchema[Double](), 6.28 + 18.84)),
    Cell(Position("(qux+bar)", 1, "xyz"), Content(ContinuousSchema[Double](), 12.56 + 6.28)),
    Cell(Position("(qux+baz)", 1, "xyz"), Content(ContinuousSchema[Double](), 12.56 + 9.42)),
    Cell(Position("(qux+foo)", 1, "xyz"), Content(ContinuousSchema[Double](), 12.56 + 3.14))
  )

  val result7 = List(
    Cell(Position("(2|xyz+1|xyz)", "bar"), Content(ContinuousSchema[Double](), 12.56 + 6.28)),
    Cell(Position("(2|xyz+1|xyz)", "baz"), Content(ContinuousSchema[Double](), 18.84 + 9.42)),
    Cell(Position("(2|xyz+1|xyz)", "foo"), Content(ContinuousSchema[Double](), 6.28 + 3.14)),
    Cell(Position("(2|xyz-1|xyz)", "bar"), Content(ContinuousSchema[Double](), 12.56 - 6.28)),
    Cell(Position("(2|xyz-1|xyz)", "baz"), Content(ContinuousSchema[Double](), 18.84 - 9.42)),
    Cell(Position("(2|xyz-1|xyz)", "foo"), Content(ContinuousSchema[Double](), 6.28 - 3.14)),
    Cell(Position("(3|xyz+1|xyz)", "bar"), Content(ContinuousSchema[Double](), 18.84 + 6.28)),
    Cell(Position("(3|xyz+1|xyz)", "foo"), Content(ContinuousSchema[Double](), 9.42 + 3.14)),
    Cell(Position("(3|xyz+2|xyz)", "bar"), Content(ContinuousSchema[Double](), 18.84 + 12.56)),
    Cell(Position("(3|xyz+2|xyz)", "foo"), Content(ContinuousSchema[Double](), 9.42 + 6.28)),
    Cell(Position("(3|xyz-1|xyz)", "bar"), Content(ContinuousSchema[Double](), 18.84 - 6.28)),
    Cell(Position("(3|xyz-1|xyz)", "foo"), Content(ContinuousSchema[Double](), 9.42 - 3.14)),
    Cell(Position("(3|xyz-2|xyz)", "bar"), Content(ContinuousSchema[Double](), 18.84 - 12.56)),
    Cell(Position("(3|xyz-2|xyz)", "foo"), Content(ContinuousSchema[Double](), 9.42 - 6.28)),
    Cell(Position("(4|xyz+1|xyz)", "foo"), Content(ContinuousSchema[Double](), 12.56 + 3.14)),
    Cell(Position("(4|xyz+2|xyz)", "foo"), Content(ContinuousSchema[Double](), 12.56 + 6.28)),
    Cell(Position("(4|xyz+3|xyz)", "foo"), Content(ContinuousSchema[Double](), 12.56 + 9.42)),
    Cell(Position("(4|xyz-1|xyz)", "foo"), Content(ContinuousSchema[Double](), 12.56 - 3.14)),
    Cell(Position("(4|xyz-2|xyz)", "foo"), Content(ContinuousSchema[Double](), 12.56 - 6.28)),
    Cell(Position("(4|xyz-3|xyz)", "foo"), Content(ContinuousSchema[Double](), 12.56 - 9.42))
  )

  val result8 = List(
    Cell(Position("(2+1)", "bar", "xyz"), Content(ContinuousSchema[Double](), 12.56 + 6.28)),
    Cell(Position("(2+1)", "baz", "xyz"), Content(ContinuousSchema[Double](), 18.84 + 9.42)),
    Cell(Position("(2+1)", "foo", "xyz"), Content(ContinuousSchema[Double](), 6.28 + 3.14)),
    Cell(Position("(2-1)", "bar", "xyz"), Content(ContinuousSchema[Double](), 12.56 - 6.28)),
    Cell(Position("(2-1)", "baz", "xyz"), Content(ContinuousSchema[Double](), 18.84 - 9.42)),
    Cell(Position("(2-1)", "foo", "xyz"), Content(ContinuousSchema[Double](), 6.28 - 3.14)),
    Cell(Position("(3+1)", "bar", "xyz"), Content(ContinuousSchema[Double](), 18.84 + 6.28)),
    Cell(Position("(3+1)", "foo", "xyz"), Content(ContinuousSchema[Double](), 9.42 + 3.14)),
    Cell(Position("(3+2)", "bar", "xyz"), Content(ContinuousSchema[Double](), 18.84 + 12.56)),
    Cell(Position("(3+2)", "foo", "xyz"), Content(ContinuousSchema[Double](), 9.42 + 6.28)),
    Cell(Position("(3-1)", "bar", "xyz"), Content(ContinuousSchema[Double](), 18.84 - 6.28)),
    Cell(Position("(3-1)", "foo", "xyz"), Content(ContinuousSchema[Double](), 9.42 - 3.14)),
    Cell(Position("(3-2)", "bar", "xyz"), Content(ContinuousSchema[Double](), 18.84 - 12.56)),
    Cell(Position("(3-2)", "foo", "xyz"), Content(ContinuousSchema[Double](), 9.42 - 6.28)),
    Cell(Position("(4+1)", "foo", "xyz"), Content(ContinuousSchema[Double](), 12.56 + 3.14)),
    Cell(Position("(4+2)", "foo", "xyz"), Content(ContinuousSchema[Double](), 12.56 + 6.28)),
    Cell(Position("(4+3)", "foo", "xyz"), Content(ContinuousSchema[Double](), 12.56 + 9.42)),
    Cell(Position("(4-1)", "foo", "xyz"), Content(ContinuousSchema[Double](), 12.56 - 3.14)),
    Cell(Position("(4-2)", "foo", "xyz"), Content(ContinuousSchema[Double](), 12.56 - 6.28)),
    Cell(Position("(4-3)", "foo", "xyz"), Content(ContinuousSchema[Double](), 12.56 - 9.42))
  )

  val result9 = List(
    Cell(Position("(baz|xyz+bar|xyz)", 1), Content(ContinuousSchema[Double](), 9.42 + 6.28)),
    Cell(Position("(baz|xyz+bar|xyz)", 2), Content(ContinuousSchema[Double](), 18.84 + 12.56)),
    Cell(Position("(foo|xyz+bar|xyz)", 1), Content(ContinuousSchema[Double](), 3.14 + 6.28)),
    Cell(Position("(foo|xyz+bar|xyz)", 2), Content(ContinuousSchema[Double](), 6.28 + 12.56)),
    Cell(Position("(foo|xyz+bar|xyz)", 3), Content(ContinuousSchema[Double](), 9.42 + 18.84)),
    Cell(Position("(foo|xyz+baz|xyz)", 1), Content(ContinuousSchema[Double](), 3.14 + 9.42)),
    Cell(Position("(foo|xyz+baz|xyz)", 2), Content(ContinuousSchema[Double](), 6.28 + 18.84)),
    Cell(Position("(qux|xyz+bar|xyz)", 1), Content(ContinuousSchema[Double](), 12.56 + 6.28)),
    Cell(Position("(qux|xyz+baz|xyz)", 1), Content(ContinuousSchema[Double](), 12.56 + 9.42)),
    Cell(Position("(qux|xyz+foo|xyz)", 1), Content(ContinuousSchema[Double](), 12.56 + 3.14))
  )

  val result10 = List()

  val result11 = List(
    Cell(Position("(bar|2+bar|1)", "xyz"), Content(ContinuousSchema[Double](), 12.56 + 6.28)),
    Cell(Position("(bar|3+bar|1)", "xyz"), Content(ContinuousSchema[Double](), 18.84 + 6.28)),
    Cell(Position("(bar|3+bar|2)", "xyz"), Content(ContinuousSchema[Double](), 18.84 + 12.56)),
    Cell(Position("(baz|1+bar|1)", "xyz"), Content(ContinuousSchema[Double](), 9.42 + 6.28)),
    Cell(Position("(baz|1+bar|2)", "xyz"), Content(ContinuousSchema[Double](), 9.42 + 12.56)),
    Cell(Position("(baz|1+bar|3)", "xyz"), Content(ContinuousSchema[Double](), 9.42 + 18.84)),
    Cell(Position("(baz|2+bar|1)", "xyz"), Content(ContinuousSchema[Double](), 18.84 + 6.28)),
    Cell(Position("(baz|2+bar|2)", "xyz"), Content(ContinuousSchema[Double](), 18.84 + 12.56)),
    Cell(Position("(baz|2+bar|3)", "xyz"), Content(ContinuousSchema[Double](), 18.84 + 18.84)),
    Cell(Position("(baz|2+baz|1)", "xyz"), Content(ContinuousSchema[Double](), 18.84 + 9.42)),
    Cell(Position("(foo|1+bar|1)", "xyz"), Content(ContinuousSchema[Double](), 3.14 + 6.28)),
    Cell(Position("(foo|1+bar|2)", "xyz"), Content(ContinuousSchema[Double](), 3.14 + 12.56)),
    Cell(Position("(foo|1+bar|3)", "xyz"), Content(ContinuousSchema[Double](), 3.14 + 18.84)),
    Cell(Position("(foo|1+baz|1)", "xyz"), Content(ContinuousSchema[Double](), 3.14 + 9.42)),
    Cell(Position("(foo|1+baz|2)", "xyz"), Content(ContinuousSchema[Double](), 3.14 + 18.84)),
    Cell(Position("(foo|2+bar|1)", "xyz"), Content(ContinuousSchema[Double](), 6.28 + 6.28)),
    Cell(Position("(foo|2+bar|2)", "xyz"), Content(ContinuousSchema[Double](), 6.28 + 12.56)),
    Cell(Position("(foo|2+bar|3)", "xyz"), Content(ContinuousSchema[Double](), 6.28 + 18.84)),
    Cell(Position("(foo|2+baz|1)", "xyz"), Content(ContinuousSchema[Double](), 6.28 + 9.42)),
    Cell(Position("(foo|2+baz|2)", "xyz"), Content(ContinuousSchema[Double](), 6.28 + 18.84)),
    Cell(Position("(foo|2+foo|1)", "xyz"), Content(ContinuousSchema[Double](), 6.28 + 3.14)),
    Cell(Position("(foo|3+bar|1)", "xyz"), Content(ContinuousSchema[Double](), 9.42 + 6.28)),
    Cell(Position("(foo|3+bar|2)", "xyz"), Content(ContinuousSchema[Double](), 9.42 + 12.56)),
    Cell(Position("(foo|3+bar|3)", "xyz"), Content(ContinuousSchema[Double](), 9.42 + 18.84)),
    Cell(Position("(foo|3+baz|1)", "xyz"), Content(ContinuousSchema[Double](), 9.42 + 9.42)),
    Cell(Position("(foo|3+baz|2)", "xyz"), Content(ContinuousSchema[Double](), 9.42 + 18.84)),
    Cell(Position("(foo|3+foo|1)", "xyz"), Content(ContinuousSchema[Double](), 9.42 + 3.14)),
    Cell(Position("(foo|3+foo|2)", "xyz"), Content(ContinuousSchema[Double](), 9.42 + 6.28)),
    Cell(Position("(foo|4+bar|1)", "xyz"), Content(ContinuousSchema[Double](), 12.56 + 6.28)),
    Cell(Position("(foo|4+bar|2)", "xyz"), Content(ContinuousSchema[Double](), 12.56 + 12.56)),
    Cell(Position("(foo|4+bar|3)", "xyz"), Content(ContinuousSchema[Double](), 12.56 + 18.84)),
    Cell(Position("(foo|4+baz|1)", "xyz"), Content(ContinuousSchema[Double](), 12.56 + 9.42)),
    Cell(Position("(foo|4+baz|2)", "xyz"), Content(ContinuousSchema[Double](), 12.56 + 18.84)),
    Cell(Position("(foo|4+foo|1)", "xyz"), Content(ContinuousSchema[Double](), 12.56 + 3.14)),
    Cell(Position("(foo|4+foo|2)", "xyz"), Content(ContinuousSchema[Double](), 12.56 + 6.28)),
    Cell(Position("(foo|4+foo|3)", "xyz"), Content(ContinuousSchema[Double](), 12.56 + 9.42)),
    Cell(Position("(qux|1+bar|1)", "xyz"), Content(ContinuousSchema[Double](), 12.56 + 6.28)),
    Cell(Position("(qux|1+bar|2)", "xyz"), Content(ContinuousSchema[Double](), 12.56 + 12.56)),
    Cell(Position("(qux|1+bar|3)", "xyz"), Content(ContinuousSchema[Double](), 12.56 + 18.84)),
    Cell(Position("(qux|1+baz|1)", "xyz"), Content(ContinuousSchema[Double](), 12.56 + 9.42)),
    Cell(Position("(qux|1+baz|2)", "xyz"), Content(ContinuousSchema[Double](), 12.56 + 18.84)),
    Cell(Position("(qux|1+foo|1)", "xyz"), Content(ContinuousSchema[Double](), 12.56 + 3.14)),
    Cell(Position("(qux|1+foo|2)", "xyz"), Content(ContinuousSchema[Double](), 12.56 + 6.28)),
    Cell(Position("(qux|1+foo|3)", "xyz"), Content(ContinuousSchema[Double](), 12.56 + 9.42)),
    Cell(Position("(qux|1+foo|4)", "xyz"), Content(ContinuousSchema[Double](), 12.56 + 12.56))
  )

  val result12 = List(
    Cell(Position("(baz+bar)"), Content(ContinuousSchema[Double](), 9.42 + 6.28 + 1)),
    Cell(Position("(foo+bar)"), Content(ContinuousSchema[Double](), 3.14 + 6.28 + 1)),
    Cell(Position("(foo+baz)"), Content(ContinuousSchema[Double](), 3.14 + 9.42 + 1)),
    Cell(Position("(qux+bar)"), Content(ContinuousSchema[Double](), 12.56 + 6.28 + 1)),
    Cell(Position("(qux+baz)"), Content(ContinuousSchema[Double](), 12.56 + 9.42 + 1)),
    Cell(Position("(qux+foo)"), Content(ContinuousSchema[Double](), 12.56 + 3.14 + 1))
  )

  val result13 = List(
    Cell(Position("(baz+bar)", 1), Content(ContinuousSchema[Double](), 9.42 + 6.28 + 1)),
    Cell(Position("(baz+bar)", 2), Content(ContinuousSchema[Double](), 18.84 + 12.56 + 1)),
    Cell(Position("(foo+bar)", 1), Content(ContinuousSchema[Double](), 3.14 + 6.28 + 1)),
    Cell(Position("(foo+bar)", 2), Content(ContinuousSchema[Double](), 6.28 + 12.56 + 1)),
    Cell(Position("(foo+bar)", 3), Content(ContinuousSchema[Double](), 9.42 + 18.84 + 1)),
    Cell(Position("(foo+baz)", 1), Content(ContinuousSchema[Double](), 3.14 + 9.42 + 1)),
    Cell(Position("(foo+baz)", 2), Content(ContinuousSchema[Double](), 6.28 + 18.84 + 1)),
    Cell(Position("(qux+bar)", 1), Content(ContinuousSchema[Double](), 12.56 + 6.28 + 1)),
    Cell(Position("(qux+baz)", 1), Content(ContinuousSchema[Double](), 12.56 + 9.42 + 1)),
    Cell(Position("(qux+foo)", 1), Content(ContinuousSchema[Double](), 12.56 + 3.14 + 1))
  )

  val result14 = List(
    Cell(Position("(2+1)", "bar"), Content(ContinuousSchema[Double](), 12.56 + 6.28 + 1)),
    Cell(Position("(2+1)", "baz"), Content(ContinuousSchema[Double](), 18.84 + 9.42 + 1)),
    Cell(Position("(2+1)", "foo"), Content(ContinuousSchema[Double](), 6.28 + 3.14 + 1)),
    Cell(Position("(2-1)", "bar"), Content(ContinuousSchema[Double](), 12.56 - 6.28 - 1)),
    Cell(Position("(2-1)", "baz"), Content(ContinuousSchema[Double](), 18.84 - 9.42 - 1)),
    Cell(Position("(2-1)", "foo"), Content(ContinuousSchema[Double](), 6.28 - 3.14 - 1)),
    Cell(Position("(3+1)", "bar"), Content(ContinuousSchema[Double](), 18.84 + 6.28 + 1)),
    Cell(Position("(3+1)", "foo"), Content(ContinuousSchema[Double](), 9.42 + 3.14 + 1)),
    Cell(Position("(3+2)", "bar"), Content(ContinuousSchema[Double](), 18.84 + 12.56 + 1)),
    Cell(Position("(3+2)", "foo"), Content(ContinuousSchema[Double](), 9.42 + 6.28 + 1)),
    Cell(Position("(3-1)", "bar"), Content(ContinuousSchema[Double](), 18.84 - 6.28 - 1)),
    Cell(Position("(3-1)", "foo"), Content(ContinuousSchema[Double](), 9.42 - 3.14 - 1)),
    Cell(Position("(3-2)", "bar"), Content(ContinuousSchema[Double](), 18.84 - 12.56 - 1)),
    Cell(Position("(3-2)", "foo"), Content(ContinuousSchema[Double](), 9.42 - 6.28 - 1)),
    Cell(Position("(4+1)", "foo"), Content(ContinuousSchema[Double](), 12.56 + 3.14 + 1)),
    Cell(Position("(4+2)", "foo"), Content(ContinuousSchema[Double](), 12.56 + 6.28 + 1)),
    Cell(Position("(4+3)", "foo"), Content(ContinuousSchema[Double](), 12.56 + 9.42 + 1)),
    Cell(Position("(4-1)", "foo"), Content(ContinuousSchema[Double](), 12.56 - 3.14 - 1)),
    Cell(Position("(4-2)", "foo"), Content(ContinuousSchema[Double](), 12.56 - 6.28 - 1)),
    Cell(Position("(4-3)", "foo"), Content(ContinuousSchema[Double](), 12.56 - 9.42 - 1))
  )

  val result15 = List(
    Cell(Position("(2+1)", "bar"), Content(ContinuousSchema[Double](), 12.56 + 6.28 + 1)),
    Cell(Position("(2+1)", "baz"), Content(ContinuousSchema[Double](), 18.84 + 9.42 + 1)),
    Cell(Position("(2+1)", "foo"), Content(ContinuousSchema[Double](), 6.28 + 3.14 + 1)),
    Cell(Position("(2-1)", "bar"), Content(ContinuousSchema[Double](), 12.56 - 6.28 - 1)),
    Cell(Position("(2-1)", "baz"), Content(ContinuousSchema[Double](), 18.84 - 9.42 - 1)),
    Cell(Position("(2-1)", "foo"), Content(ContinuousSchema[Double](), 6.28 - 3.14 - 1)),
    Cell(Position("(3+1)", "bar"), Content(ContinuousSchema[Double](), 18.84 + 6.28 + 1)),
    Cell(Position("(3+1)", "foo"), Content(ContinuousSchema[Double](), 9.42 + 3.14 + 1)),
    Cell(Position("(3+2)", "bar"), Content(ContinuousSchema[Double](), 18.84 + 12.56 + 1)),
    Cell(Position("(3+2)", "foo"), Content(ContinuousSchema[Double](), 9.42 + 6.28 + 1)),
    Cell(Position("(3-1)", "bar"), Content(ContinuousSchema[Double](), 18.84 - 6.28 - 1)),
    Cell(Position("(3-1)", "foo"), Content(ContinuousSchema[Double](), 9.42 - 3.14 - 1)),
    Cell(Position("(3-2)", "bar"), Content(ContinuousSchema[Double](), 18.84 - 12.56 - 1)),
    Cell(Position("(3-2)", "foo"), Content(ContinuousSchema[Double](), 9.42 - 6.28 - 1)),
    Cell(Position("(4+1)", "foo"), Content(ContinuousSchema[Double](), 12.56 + 3.14 + 1)),
    Cell(Position("(4+2)", "foo"), Content(ContinuousSchema[Double](), 12.56 + 6.28 + 1)),
    Cell(Position("(4+3)", "foo"), Content(ContinuousSchema[Double](), 12.56 + 9.42 + 1)),
    Cell(Position("(4-1)", "foo"), Content(ContinuousSchema[Double](), 12.56 - 3.14 - 1)),
    Cell(Position("(4-2)", "foo"), Content(ContinuousSchema[Double](), 12.56 - 6.28 - 1)),
    Cell(Position("(4-3)", "foo"), Content(ContinuousSchema[Double](), 12.56 - 9.42 - 1))
  )

  val result16 = List(
    Cell(Position("(baz+bar)", 1), Content(ContinuousSchema[Double](), 9.42 + 6.28 + 1)),
    Cell(Position("(baz+bar)", 2), Content(ContinuousSchema[Double](), 18.84 + 12.56 + 1)),
    Cell(Position("(foo+bar)", 1), Content(ContinuousSchema[Double](), 3.14 + 6.28 + 1)),
    Cell(Position("(foo+bar)", 2), Content(ContinuousSchema[Double](), 6.28 + 12.56 + 1)),
    Cell(Position("(foo+bar)", 3), Content(ContinuousSchema[Double](), 9.42 + 18.84 + 1)),
    Cell(Position("(foo+baz)", 1), Content(ContinuousSchema[Double](), 3.14 + 9.42 + 1)),
    Cell(Position("(foo+baz)", 2), Content(ContinuousSchema[Double](), 6.28 + 18.84 + 1)),
    Cell(Position("(qux+bar)", 1), Content(ContinuousSchema[Double](), 12.56 + 6.28 + 1)),
    Cell(Position("(qux+baz)", 1), Content(ContinuousSchema[Double](), 12.56 + 9.42 + 1)),
    Cell(Position("(qux+foo)", 1), Content(ContinuousSchema[Double](), 12.56 + 3.14 + 1))
  )

  val result17 = List(
    Cell(Position("(baz+bar)", 1, "xyz"), Content(ContinuousSchema[Double](), 9.42 + 6.28 + 1)),
    Cell(Position("(baz+bar)", 2, "xyz"), Content(ContinuousSchema[Double](), 18.84 + 12.56 + 1)),
    Cell(Position("(foo+bar)", 1, "xyz"), Content(ContinuousSchema[Double](), 3.14 + 6.28 + 1)),
    Cell(Position("(foo+bar)", 2, "xyz"), Content(ContinuousSchema[Double](), 6.28 + 12.56 + 1)),
    Cell(Position("(foo+bar)", 3, "xyz"), Content(ContinuousSchema[Double](), 9.42 + 18.84 + 1)),
    Cell(Position("(foo+baz)", 1, "xyz"), Content(ContinuousSchema[Double](), 3.14 + 9.42 + 1)),
    Cell(Position("(foo+baz)", 2, "xyz"), Content(ContinuousSchema[Double](), 6.28 + 18.84 + 1)),
    Cell(Position("(qux+bar)", 1, "xyz"), Content(ContinuousSchema[Double](), 12.56 + 6.28 + 1)),
    Cell(Position("(qux+baz)", 1, "xyz"), Content(ContinuousSchema[Double](), 12.56 + 9.42 + 1)),
    Cell(Position("(qux+foo)", 1, "xyz"), Content(ContinuousSchema[Double](), 12.56 + 3.14 + 1))
  )

  val result18 = List(
    Cell(Position("(2|xyz+1|xyz)", "bar"), Content(ContinuousSchema[Double](), 12.56 + 6.28 + 1)),
    Cell(Position("(2|xyz+1|xyz)", "baz"), Content(ContinuousSchema[Double](), 18.84 + 9.42 + 1)),
    Cell(Position("(2|xyz+1|xyz)", "foo"), Content(ContinuousSchema[Double](), 6.28 + 3.14 + 1)),
    Cell(Position("(2|xyz-1|xyz)", "bar"), Content(ContinuousSchema[Double](), 12.56 - 6.28 - 1)),
    Cell(Position("(2|xyz-1|xyz)", "baz"), Content(ContinuousSchema[Double](), 18.84 - 9.42 - 1)),
    Cell(Position("(2|xyz-1|xyz)", "foo"), Content(ContinuousSchema[Double](), 6.28 - 3.14 - 1)),
    Cell(Position("(3|xyz+1|xyz)", "bar"), Content(ContinuousSchema[Double](), 18.84 + 6.28 + 1)),
    Cell(Position("(3|xyz+1|xyz)", "foo"), Content(ContinuousSchema[Double](), 9.42 + 3.14 + 1)),
    Cell(Position("(3|xyz+2|xyz)", "bar"), Content(ContinuousSchema[Double](), 18.84 + 12.56 + 1)),
    Cell(Position("(3|xyz+2|xyz)", "foo"), Content(ContinuousSchema[Double](), 9.42 + 6.28 + 1)),
    Cell(Position("(3|xyz-1|xyz)", "bar"), Content(ContinuousSchema[Double](), 18.84 - 6.28 - 1)),
    Cell(Position("(3|xyz-1|xyz)", "foo"), Content(ContinuousSchema[Double](), 9.42 - 3.14 - 1)),
    Cell(Position("(3|xyz-2|xyz)", "bar"), Content(ContinuousSchema[Double](), 18.84 - 12.56 - 1)),
    Cell(Position("(3|xyz-2|xyz)", "foo"), Content(ContinuousSchema[Double](), 9.42 - 6.28 - 1)),
    Cell(Position("(4|xyz+1|xyz)", "foo"), Content(ContinuousSchema[Double](), 12.56 + 3.14 + 1)),
    Cell(Position("(4|xyz+2|xyz)", "foo"), Content(ContinuousSchema[Double](), 12.56 + 6.28 + 1)),
    Cell(Position("(4|xyz+3|xyz)", "foo"), Content(ContinuousSchema[Double](), 12.56 + 9.42 + 1)),
    Cell(Position("(4|xyz-1|xyz)", "foo"), Content(ContinuousSchema[Double](), 12.56 - 3.14 - 1)),
    Cell(Position("(4|xyz-2|xyz)", "foo"), Content(ContinuousSchema[Double](), 12.56 - 6.28 - 1)),
    Cell(Position("(4|xyz-3|xyz)", "foo"), Content(ContinuousSchema[Double](), 12.56 - 9.42 - 1))
  )

  val result19 = List(
    Cell(Position("(2+1)", "bar", "xyz"), Content(ContinuousSchema[Double](), 12.56 + 6.28 + 1)),
    Cell(Position("(2+1)", "baz", "xyz"), Content(ContinuousSchema[Double](), 18.84 + 9.42 + 1)),
    Cell(Position("(2+1)", "foo", "xyz"), Content(ContinuousSchema[Double](), 6.28 + 3.14 + 1)),
    Cell(Position("(2-1)", "bar", "xyz"), Content(ContinuousSchema[Double](), 12.56 - 6.28 - 1)),
    Cell(Position("(2-1)", "baz", "xyz"), Content(ContinuousSchema[Double](), 18.84 - 9.42 - 1)),
    Cell(Position("(2-1)", "foo", "xyz"), Content(ContinuousSchema[Double](), 6.28 - 3.14 - 1)),
    Cell(Position("(3+1)", "bar", "xyz"), Content(ContinuousSchema[Double](), 18.84 + 6.28 + 1)),
    Cell(Position("(3+1)", "foo", "xyz"), Content(ContinuousSchema[Double](), 9.42 + 3.14 + 1)),
    Cell(Position("(3+2)", "bar", "xyz"), Content(ContinuousSchema[Double](), 18.84 + 12.56 + 1)),
    Cell(Position("(3+2)", "foo", "xyz"), Content(ContinuousSchema[Double](), 9.42 + 6.28 + 1)),
    Cell(Position("(3-1)", "bar", "xyz"), Content(ContinuousSchema[Double](), 18.84 - 6.28 - 1)),
    Cell(Position("(3-1)", "foo", "xyz"), Content(ContinuousSchema[Double](), 9.42 - 3.14 - 1)),
    Cell(Position("(3-2)", "bar", "xyz"), Content(ContinuousSchema[Double](), 18.84 - 12.56 - 1)),
    Cell(Position("(3-2)", "foo", "xyz"), Content(ContinuousSchema[Double](), 9.42 - 6.28 - 1)),
    Cell(Position("(4+1)", "foo", "xyz"), Content(ContinuousSchema[Double](), 12.56 + 3.14 + 1)),
    Cell(Position("(4+2)", "foo", "xyz"), Content(ContinuousSchema[Double](), 12.56 + 6.28 + 1)),
    Cell(Position("(4+3)", "foo", "xyz"), Content(ContinuousSchema[Double](), 12.56 + 9.42 + 1)),
    Cell(Position("(4-1)", "foo", "xyz"), Content(ContinuousSchema[Double](), 12.56 - 3.14 - 1)),
    Cell(Position("(4-2)", "foo", "xyz"), Content(ContinuousSchema[Double](), 12.56 - 6.28 - 1)),
    Cell(Position("(4-3)", "foo", "xyz"), Content(ContinuousSchema[Double](), 12.56 - 9.42 - 1))
  )

  val result20 = List(
    Cell(Position("(baz|xyz+bar|xyz)", 1), Content(ContinuousSchema[Double](), 9.42 + 6.28 + 1)),
    Cell(Position("(baz|xyz+bar|xyz)", 2), Content(ContinuousSchema[Double](), 18.84 + 12.56 + 1)),
    Cell(Position("(foo|xyz+bar|xyz)", 1), Content(ContinuousSchema[Double](), 3.14 + 6.28 + 1)),
    Cell(Position("(foo|xyz+bar|xyz)", 2), Content(ContinuousSchema[Double](), 6.28 + 12.56 + 1)),
    Cell(Position("(foo|xyz+bar|xyz)", 3), Content(ContinuousSchema[Double](), 9.42 + 18.84 + 1)),
    Cell(Position("(foo|xyz+baz|xyz)", 1), Content(ContinuousSchema[Double](), 3.14 + 9.42 + 1)),
    Cell(Position("(foo|xyz+baz|xyz)", 2), Content(ContinuousSchema[Double](), 6.28 + 18.84 + 1)),
    Cell(Position("(qux|xyz+bar|xyz)", 1), Content(ContinuousSchema[Double](), 12.56 + 6.28 + 1)),
    Cell(Position("(qux|xyz+baz|xyz)", 1), Content(ContinuousSchema[Double](), 12.56 + 9.42 + 1)),
    Cell(Position("(qux|xyz+foo|xyz)", 1), Content(ContinuousSchema[Double](), 12.56 + 3.14 + 1))
  )

  val result21 = List()

  val result22 = List(
    Cell(Position("(bar|2+bar|1)", "xyz"), Content(ContinuousSchema[Double](), 12.56 + 6.28 + 1)),
    Cell(Position("(bar|3+bar|1)", "xyz"), Content(ContinuousSchema[Double](), 18.84 + 6.28 + 1)),
    Cell(Position("(bar|3+bar|2)", "xyz"), Content(ContinuousSchema[Double](), 18.84 + 12.56 + 1)),
    Cell(Position("(baz|1+bar|1)", "xyz"), Content(ContinuousSchema[Double](), 9.42 + 6.28 + 1)),
    Cell(Position("(baz|1+bar|2)", "xyz"), Content(ContinuousSchema[Double](), 9.42 + 12.56 + 1)),
    Cell(Position("(baz|1+bar|3)", "xyz"), Content(ContinuousSchema[Double](), 9.42 + 18.84 + 1)),
    Cell(Position("(baz|2+bar|1)", "xyz"), Content(ContinuousSchema[Double](), 18.84 + 6.28 + 1)),
    Cell(Position("(baz|2+bar|2)", "xyz"), Content(ContinuousSchema[Double](), 18.84 + 12.56 + 1)),
    Cell(Position("(baz|2+bar|3)", "xyz"), Content(ContinuousSchema[Double](), 18.84 + 18.84 + 1)),
    Cell(Position("(baz|2+baz|1)", "xyz"), Content(ContinuousSchema[Double](), 18.84 + 9.42 + 1)),
    Cell(Position("(foo|1+bar|1)", "xyz"), Content(ContinuousSchema[Double](), 3.14 + 6.28 + 1)),
    Cell(Position("(foo|1+bar|2)", "xyz"), Content(ContinuousSchema[Double](), 3.14 + 12.56 + 1)),
    Cell(Position("(foo|1+bar|3)", "xyz"), Content(ContinuousSchema[Double](), 3.14 + 18.84 + 1)),
    Cell(Position("(foo|1+baz|1)", "xyz"), Content(ContinuousSchema[Double](), 3.14 + 9.42 + 1)),
    Cell(Position("(foo|1+baz|2)", "xyz"), Content(ContinuousSchema[Double](), 3.14 + 18.84 + 1)),
    Cell(Position("(foo|2+bar|1)", "xyz"), Content(ContinuousSchema[Double](), 6.28 + 6.28 + 1)),
    Cell(Position("(foo|2+bar|2)", "xyz"), Content(ContinuousSchema[Double](), 6.28 + 12.56 + 1)),
    Cell(Position("(foo|2+bar|3)", "xyz"), Content(ContinuousSchema[Double](), 6.28 + 18.84 + 1)),
    Cell(Position("(foo|2+baz|1)", "xyz"), Content(ContinuousSchema[Double](), 6.28 + 9.42 + 1)),
    Cell(Position("(foo|2+baz|2)", "xyz"), Content(ContinuousSchema[Double](), 6.28 + 18.84 + 1)),
    Cell(Position("(foo|2+foo|1)", "xyz"), Content(ContinuousSchema[Double](), 6.28 + 3.14 + 1)),
    Cell(Position("(foo|3+bar|1)", "xyz"), Content(ContinuousSchema[Double](), 9.42 + 6.28 + 1)),
    Cell(Position("(foo|3+bar|2)", "xyz"), Content(ContinuousSchema[Double](), 9.42 + 12.56 + 1)),
    Cell(Position("(foo|3+bar|3)", "xyz"), Content(ContinuousSchema[Double](), 9.42 + 18.84 + 1)),
    Cell(Position("(foo|3+baz|1)", "xyz"), Content(ContinuousSchema[Double](), 9.42 + 9.42 + 1)),
    Cell(Position("(foo|3+baz|2)", "xyz"), Content(ContinuousSchema[Double](), 9.42 + 18.84 + 1)),
    Cell(Position("(foo|3+foo|1)", "xyz"), Content(ContinuousSchema[Double](), 9.42 + 3.14 + 1)),
    Cell(Position("(foo|3+foo|2)", "xyz"), Content(ContinuousSchema[Double](), 9.42 + 6.28 + 1)),
    Cell(Position("(foo|4+bar|1)", "xyz"), Content(ContinuousSchema[Double](), 12.56 + 6.28 + 1)),
    Cell(Position("(foo|4+bar|2)", "xyz"), Content(ContinuousSchema[Double](), 12.56 + 12.56 + 1)),
    Cell(Position("(foo|4+bar|3)", "xyz"), Content(ContinuousSchema[Double](), 12.56 + 18.84 + 1)),
    Cell(Position("(foo|4+baz|1)", "xyz"), Content(ContinuousSchema[Double](), 12.56 + 9.42 + 1)),
    Cell(Position("(foo|4+baz|2)", "xyz"), Content(ContinuousSchema[Double](), 12.56 + 18.84 + 1)),
    Cell(Position("(foo|4+foo|1)", "xyz"), Content(ContinuousSchema[Double](), 12.56 + 3.14 + 1)),
    Cell(Position("(foo|4+foo|2)", "xyz"), Content(ContinuousSchema[Double](), 12.56 + 6.28 + 1)),
    Cell(Position("(foo|4+foo|3)", "xyz"), Content(ContinuousSchema[Double](), 12.56 + 9.42 + 1)),
    Cell(Position("(qux|1+bar|1)", "xyz"), Content(ContinuousSchema[Double](), 12.56 + 6.28 + 1)),
    Cell(Position("(qux|1+bar|2)", "xyz"), Content(ContinuousSchema[Double](), 12.56 + 12.56 + 1)),
    Cell(Position("(qux|1+bar|3)", "xyz"), Content(ContinuousSchema[Double](), 12.56 + 18.84 + 1)),
    Cell(Position("(qux|1+baz|1)", "xyz"), Content(ContinuousSchema[Double](), 12.56 + 9.42 + 1)),
    Cell(Position("(qux|1+baz|2)", "xyz"), Content(ContinuousSchema[Double](), 12.56 + 18.84 + 1)),
    Cell(Position("(qux|1+foo|1)", "xyz"), Content(ContinuousSchema[Double](), 12.56 + 3.14 + 1)),
    Cell(Position("(qux|1+foo|2)", "xyz"), Content(ContinuousSchema[Double](), 12.56 + 6.28 + 1)),
    Cell(Position("(qux|1+foo|3)", "xyz"), Content(ContinuousSchema[Double](), 12.56 + 9.42 + 1)),
    Cell(Position("(qux|1+foo|4)", "xyz"), Content(ContinuousSchema[Double](), 12.56 + 12.56 + 1))
  )

  val result23 = List(
    Cell(Position("(baz+bar)"), Content(ContinuousSchema[Double](), 9.42 + 1)),
    Cell(Position("(foo+bar)"), Content(ContinuousSchema[Double](), 3.14 + 1)),
    Cell(Position("(foo+baz)"), Content(ContinuousSchema[Double](), 3.14 + 2)),
    Cell(Position("(qux+bar)"), Content(ContinuousSchema[Double](), 12.56 + 1)),
    Cell(Position("(qux+baz)"), Content(ContinuousSchema[Double](), 12.56 + 2))
  )

  val result24 = List(
    Cell(Position("(baz+bar)", 1), Content(ContinuousSchema[Double](), 9.42 + 1)),
    Cell(Position("(baz+bar)", 2), Content(ContinuousSchema[Double](), 18.84 + 2)),
    Cell(Position("(foo+bar)", 1), Content(ContinuousSchema[Double](), 3.14 + 1)),
    Cell(Position("(foo+bar)", 2), Content(ContinuousSchema[Double](), 6.28 + 2)),
    Cell(Position("(foo+bar)", 3), Content(ContinuousSchema[Double](), 9.42 + 3)),
    Cell(Position("(foo+baz)", 1), Content(ContinuousSchema[Double](), 3.14 + 4)),
    Cell(Position("(foo+baz)", 2), Content(ContinuousSchema[Double](), 6.28 + 5)),
    Cell(Position("(qux+bar)", 1), Content(ContinuousSchema[Double](), 12.56 + 1)),
    Cell(Position("(qux+baz)", 1), Content(ContinuousSchema[Double](), 12.56 + 4))
  )

  val result25 = List(
    Cell(Position("(3+2)", "bar"), Content(ContinuousSchema[Double](), 18.84 + 1)),
    Cell(Position("(3+2)", "foo"), Content(ContinuousSchema[Double](), 9.42 + 3)),
    Cell(Position("(3-2)", "bar"), Content(ContinuousSchema[Double](), 18.84 - 1)),
    Cell(Position("(3-2)", "foo"), Content(ContinuousSchema[Double](), 9.42 - 3)),
    Cell(Position("(4+2)", "foo"), Content(ContinuousSchema[Double](), 12.56 + 3)),
    Cell(Position("(4-2)", "foo"), Content(ContinuousSchema[Double](), 12.56 - 3))
  )

  val result26 = List(
    Cell(Position("(3+2)", "bar"), Content(ContinuousSchema[Double](), 18.84 + 1)),
    Cell(Position("(3+2)", "foo"), Content(ContinuousSchema[Double](), 9.42 + 3)),
    Cell(Position("(3-2)", "bar"), Content(ContinuousSchema[Double](), 18.84 - 1)),
    Cell(Position("(3-2)", "foo"), Content(ContinuousSchema[Double](), 9.42 - 3)),
    Cell(Position("(4+2)", "foo"), Content(ContinuousSchema[Double](), 12.56 + 3)),
    Cell(Position("(4-2)", "foo"), Content(ContinuousSchema[Double](), 12.56 - 3))
  )

  val result27 = List(
    Cell(Position("(baz+bar)", 1), Content(ContinuousSchema[Double](), 9.42 + 1)),
    Cell(Position("(baz+bar)", 2), Content(ContinuousSchema[Double](), 18.84 + 2)),
    Cell(Position("(foo+bar)", 1), Content(ContinuousSchema[Double](), 3.14 + 1)),
    Cell(Position("(foo+bar)", 2), Content(ContinuousSchema[Double](), 6.28 + 2)),
    Cell(Position("(foo+bar)", 3), Content(ContinuousSchema[Double](), 9.42 + 3)),
    Cell(Position("(foo+baz)", 1), Content(ContinuousSchema[Double](), 3.14 + 4)),
    Cell(Position("(foo+baz)", 2), Content(ContinuousSchema[Double](), 6.28 + 5)),
    Cell(Position("(qux+bar)", 1), Content(ContinuousSchema[Double](), 12.56 + 1)),
    Cell(Position("(qux+baz)", 1), Content(ContinuousSchema[Double](), 12.56 + 4))
  )

  val result28 = List(
    Cell(Position("(baz+bar)", 1, "xyz"), Content(ContinuousSchema[Double](), 9.42 + 1)),
    Cell(Position("(baz+bar)", 2, "xyz"), Content(ContinuousSchema[Double](), 18.84 + 2)),
    Cell(Position("(foo+bar)", 1, "xyz"), Content(ContinuousSchema[Double](), 3.14 + 1)),
    Cell(Position("(foo+bar)", 2, "xyz"), Content(ContinuousSchema[Double](), 6.28 + 2)),
    Cell(Position("(foo+bar)", 3, "xyz"), Content(ContinuousSchema[Double](), 9.42 + 3)),
    Cell(Position("(foo+baz)", 1, "xyz"), Content(ContinuousSchema[Double](), 3.14 + 4)),
    Cell(Position("(foo+baz)", 2, "xyz"), Content(ContinuousSchema[Double](), 6.28 + 5)),
    Cell(Position("(qux+bar)", 1, "xyz"), Content(ContinuousSchema[Double](), 12.56 + 1)),
    Cell(Position("(qux+baz)", 1, "xyz"), Content(ContinuousSchema[Double](), 12.56 + 4))
  )

  val result29 = List(
    Cell(Position("(3|xyz+2|xyz)", "bar"), Content(ContinuousSchema[Double](), 18.84 + 1)),
    Cell(Position("(3|xyz+2|xyz)", "foo"), Content(ContinuousSchema[Double](), 9.42 + 3)),
    Cell(Position("(3|xyz-2|xyz)", "bar"), Content(ContinuousSchema[Double](), 18.84 - 1)),
    Cell(Position("(3|xyz-2|xyz)", "foo"), Content(ContinuousSchema[Double](), 9.42 - 3)),
    Cell(Position("(4|xyz+2|xyz)", "foo"), Content(ContinuousSchema[Double](), 12.56 + 3)),
    Cell(Position("(4|xyz-2|xyz)", "foo"), Content(ContinuousSchema[Double](), 12.56 - 3))
  )

  val result30 = List(
    Cell(Position("(3+2)", "bar", "xyz"), Content(ContinuousSchema[Double](), 18.84 + 1)),
    Cell(Position("(3+2)", "foo", "xyz"), Content(ContinuousSchema[Double](), 9.42 + 3)),
    Cell(Position("(3-2)", "bar", "xyz"), Content(ContinuousSchema[Double](), 18.84 - 1)),
    Cell(Position("(3-2)", "foo", "xyz"), Content(ContinuousSchema[Double](), 9.42 - 3)),
    Cell(Position("(4+2)", "foo", "xyz"), Content(ContinuousSchema[Double](), 12.56 + 3)),
    Cell(Position("(4-2)", "foo", "xyz"), Content(ContinuousSchema[Double](), 12.56 - 3))
  )

  val result31 = List(
    Cell(Position("(baz|xyz+bar|xyz)", 1), Content(ContinuousSchema[Double](), 9.42 + 1)),
    Cell(Position("(baz|xyz+bar|xyz)", 2), Content(ContinuousSchema[Double](), 18.84 + 2)),
    Cell(Position("(foo|xyz+bar|xyz)", 1), Content(ContinuousSchema[Double](), 3.14 + 1)),
    Cell(Position("(foo|xyz+bar|xyz)", 2), Content(ContinuousSchema[Double](), 6.28 + 2)),
    Cell(Position("(foo|xyz+bar|xyz)", 3), Content(ContinuousSchema[Double](), 9.42 + 3)),
    Cell(Position("(foo|xyz+baz|xyz)", 1), Content(ContinuousSchema[Double](), 3.14 + 4)),
    Cell(Position("(foo|xyz+baz|xyz)", 2), Content(ContinuousSchema[Double](), 6.28 + 5)),
    Cell(Position("(qux|xyz+bar|xyz)", 1), Content(ContinuousSchema[Double](), 12.56 + 1)),
    Cell(Position("(qux|xyz+baz|xyz)", 1), Content(ContinuousSchema[Double](), 12.56 + 4))
  )

  val result32 = List()

  val result33 = List(
    Cell(Position("(bar|2+bar|1)", "xyz"), Content(ContinuousSchema[Double](), 12.56 + 1)),
    Cell(Position("(bar|3+bar|1)", "xyz"), Content(ContinuousSchema[Double](), 18.84 + 1)),
    Cell(Position("(bar|3+bar|2)", "xyz"), Content(ContinuousSchema[Double](), 18.84 + 2)),
    Cell(Position("(baz|1+bar|1)", "xyz"), Content(ContinuousSchema[Double](), 9.42 + 1)),
    Cell(Position("(baz|1+bar|2)", "xyz"), Content(ContinuousSchema[Double](), 9.42 + 2)),
    Cell(Position("(baz|1+bar|3)", "xyz"), Content(ContinuousSchema[Double](), 9.42 + 3)),
    Cell(Position("(baz|2+bar|1)", "xyz"), Content(ContinuousSchema[Double](), 18.84 + 1)),
    Cell(Position("(baz|2+bar|2)", "xyz"), Content(ContinuousSchema[Double](), 18.84 + 2)),
    Cell(Position("(baz|2+bar|3)", "xyz"), Content(ContinuousSchema[Double](), 18.84 + 3)),
    Cell(Position("(baz|2+baz|1)", "xyz"), Content(ContinuousSchema[Double](), 18.84 + 4)),
    Cell(Position("(foo|1+bar|1)", "xyz"), Content(ContinuousSchema[Double](), 3.14 + 1)),
    Cell(Position("(foo|1+bar|2)", "xyz"), Content(ContinuousSchema[Double](), 3.14 + 2)),
    Cell(Position("(foo|1+bar|3)", "xyz"), Content(ContinuousSchema[Double](), 3.14 + 3)),
    Cell(Position("(foo|1+baz|1)", "xyz"), Content(ContinuousSchema[Double](), 3.14 + 4)),
    Cell(Position("(foo|1+baz|2)", "xyz"), Content(ContinuousSchema[Double](), 3.14 + 5)),
    Cell(Position("(foo|2+bar|1)", "xyz"), Content(ContinuousSchema[Double](), 6.28 + 1)),
    Cell(Position("(foo|2+bar|2)", "xyz"), Content(ContinuousSchema[Double](), 6.28 + 2)),
    Cell(Position("(foo|2+bar|3)", "xyz"), Content(ContinuousSchema[Double](), 6.28 + 3)),
    Cell(Position("(foo|2+baz|1)", "xyz"), Content(ContinuousSchema[Double](), 6.28 + 4)),
    Cell(Position("(foo|2+baz|2)", "xyz"), Content(ContinuousSchema[Double](), 6.28 + 5)),
    Cell(Position("(foo|3+bar|1)", "xyz"), Content(ContinuousSchema[Double](), 9.42 + 1)),
    Cell(Position("(foo|3+bar|2)", "xyz"), Content(ContinuousSchema[Double](), 9.42 + 2)),
    Cell(Position("(foo|3+bar|3)", "xyz"), Content(ContinuousSchema[Double](), 9.42 + 3)),
    Cell(Position("(foo|3+baz|1)", "xyz"), Content(ContinuousSchema[Double](), 9.42 + 4)),
    Cell(Position("(foo|3+baz|2)", "xyz"), Content(ContinuousSchema[Double](), 9.42 + 5)),
    Cell(Position("(foo|4+bar|1)", "xyz"), Content(ContinuousSchema[Double](), 12.56 + 1)),
    Cell(Position("(foo|4+bar|2)", "xyz"), Content(ContinuousSchema[Double](), 12.56 + 2)),
    Cell(Position("(foo|4+bar|3)", "xyz"), Content(ContinuousSchema[Double](), 12.56 + 3)),
    Cell(Position("(foo|4+baz|1)", "xyz"), Content(ContinuousSchema[Double](), 12.56 + 4)),
    Cell(Position("(foo|4+baz|2)", "xyz"), Content(ContinuousSchema[Double](), 12.56 + 5)),
    Cell(Position("(qux|1+bar|1)", "xyz"), Content(ContinuousSchema[Double](), 12.56 + 1)),
    Cell(Position("(qux|1+bar|2)", "xyz"), Content(ContinuousSchema[Double](), 12.56 + 2)),
    Cell(Position("(qux|1+bar|3)", "xyz"), Content(ContinuousSchema[Double](), 12.56 + 3)),
    Cell(Position("(qux|1+baz|1)", "xyz"), Content(ContinuousSchema[Double](), 12.56 + 4)),
    Cell(Position("(qux|1+baz|2)", "xyz"), Content(ContinuousSchema[Double](), 12.56 + 5))
  )

  val result34 = List(
    Cell(Position("(baz+bar)"), Content(ContinuousSchema[Double](), 9.42 + 1 + 1)),
    Cell(Position("(foo+bar)"), Content(ContinuousSchema[Double](), 3.14 + 1 + 1)),
    Cell(Position("(foo+baz)"), Content(ContinuousSchema[Double](), 3.14 + 2 + 1)),
    Cell(Position("(qux+bar)"), Content(ContinuousSchema[Double](), 12.56 + 1 + 1)),
    Cell(Position("(qux+baz)"), Content(ContinuousSchema[Double](), 12.56 + 2 + 1))
  )

  val result35 = List(
    Cell(Position("(baz+bar)", 1), Content(ContinuousSchema[Double](), 9.42 + 1 + 1)),
    Cell(Position("(baz+bar)", 2), Content(ContinuousSchema[Double](), 18.84 + 2 + 1)),
    Cell(Position("(foo+bar)", 1), Content(ContinuousSchema[Double](), 3.14 + 1 + 1)),
    Cell(Position("(foo+bar)", 2), Content(ContinuousSchema[Double](), 6.28 + 2 + 1)),
    Cell(Position("(foo+bar)", 3), Content(ContinuousSchema[Double](), 9.42 + 3 + 1)),
    Cell(Position("(foo+baz)", 1), Content(ContinuousSchema[Double](), 3.14 + 4 + 1)),
    Cell(Position("(foo+baz)", 2), Content(ContinuousSchema[Double](), 6.28 + 5 + 1)),
    Cell(Position("(qux+bar)", 1), Content(ContinuousSchema[Double](), 12.56 + 1 + 1)),
    Cell(Position("(qux+baz)", 1), Content(ContinuousSchema[Double](), 12.56 + 4 + 1))
  )

  val result36 = List(
    Cell(Position("(3+2)", "bar"), Content(ContinuousSchema[Double](), 18.84 + 1 + 1)),
    Cell(Position("(3+2)", "foo"), Content(ContinuousSchema[Double](), 9.42 + 3 + 1)),
    Cell(Position("(3-2)", "bar"), Content(ContinuousSchema[Double](), 18.84 - 1 - 1)),
    Cell(Position("(3-2)", "foo"), Content(ContinuousSchema[Double](), 9.42 - 3 - 1)),
    Cell(Position("(4+2)", "foo"), Content(ContinuousSchema[Double](), 12.56 + 3 + 1)),
    Cell(Position("(4-2)", "foo"), Content(ContinuousSchema[Double](), 12.56 - 3 - 1))
  )

  val result37 = List(
    Cell(Position("(3+2)", "bar"), Content(ContinuousSchema[Double](), 18.84 + 1 + 1)),
    Cell(Position("(3+2)", "foo"), Content(ContinuousSchema[Double](), 9.42 + 3 + 1)),
    Cell(Position("(3-2)", "bar"), Content(ContinuousSchema[Double](), 18.84 - 1 - 1)),
    Cell(Position("(3-2)", "foo"), Content(ContinuousSchema[Double](), 9.42 - 3 - 1)),
    Cell(Position("(4+2)", "foo"), Content(ContinuousSchema[Double](), 12.56 + 3 + 1)),
    Cell(Position("(4-2)", "foo"), Content(ContinuousSchema[Double](), 12.56 - 3 - 1))
  )

  val result38 = List(
    Cell(Position("(baz+bar)", 1), Content(ContinuousSchema[Double](), 9.42 + 1 + 1)),
    Cell(Position("(baz+bar)", 2), Content(ContinuousSchema[Double](), 18.84 + 2 + 1)),
    Cell(Position("(foo+bar)", 1), Content(ContinuousSchema[Double](), 3.14 + 1 + 1)),
    Cell(Position("(foo+bar)", 2), Content(ContinuousSchema[Double](), 6.28 + 2 + 1)),
    Cell(Position("(foo+bar)", 3), Content(ContinuousSchema[Double](), 9.42 + 3 + 1)),
    Cell(Position("(foo+baz)", 1), Content(ContinuousSchema[Double](), 3.14 + 4 + 1)),
    Cell(Position("(foo+baz)", 2), Content(ContinuousSchema[Double](), 6.28 + 5 + 1)),
    Cell(Position("(qux+bar)", 1), Content(ContinuousSchema[Double](), 12.56 + 1 + 1)),
    Cell(Position("(qux+baz)", 1), Content(ContinuousSchema[Double](), 12.56 + 4 + 1))
  )

  val result39 = List(
    Cell(Position("(baz+bar)", 1, "xyz"), Content(ContinuousSchema[Double](), 9.42 + 1 + 1)),
    Cell(Position("(baz+bar)", 2, "xyz"), Content(ContinuousSchema[Double](), 18.84 + 2 + 1)),
    Cell(Position("(foo+bar)", 1, "xyz"), Content(ContinuousSchema[Double](), 3.14 + 1 + 1)),
    Cell(Position("(foo+bar)", 2, "xyz"), Content(ContinuousSchema[Double](), 6.28 + 2 + 1)),
    Cell(Position("(foo+bar)", 3, "xyz"), Content(ContinuousSchema[Double](), 9.42 + 3 + 1)),
    Cell(Position("(foo+baz)", 1, "xyz"), Content(ContinuousSchema[Double](), 3.14 + 4 + 1)),
    Cell(Position("(foo+baz)", 2, "xyz"), Content(ContinuousSchema[Double](), 6.28 + 5 + 1)),
    Cell(Position("(qux+bar)", 1, "xyz"), Content(ContinuousSchema[Double](), 12.56 + 1 + 1)),
    Cell(Position("(qux+baz)", 1, "xyz"), Content(ContinuousSchema[Double](), 12.56 + 4 + 1))
  )

  val result40 = List(
    Cell(Position("(3|xyz+2|xyz)", "bar"), Content(ContinuousSchema[Double](), 18.84 + 1 + 1)),
    Cell(Position("(3|xyz+2|xyz)", "foo"), Content(ContinuousSchema[Double](), 9.42 + 3 + 1)),
    Cell(Position("(3|xyz-2|xyz)", "bar"), Content(ContinuousSchema[Double](), 18.84 - 1 - 1)),
    Cell(Position("(3|xyz-2|xyz)", "foo"), Content(ContinuousSchema[Double](), 9.42 - 3 - 1)),
    Cell(Position("(4|xyz+2|xyz)", "foo"), Content(ContinuousSchema[Double](), 12.56 + 3 + 1)),
    Cell(Position("(4|xyz-2|xyz)", "foo"), Content(ContinuousSchema[Double](), 12.56 - 3 - 1))
  )

  val result41 = List(
    Cell(Position("(3+2)", "bar", "xyz"), Content(ContinuousSchema[Double](), 18.84 + 1 + 1)),
    Cell(Position("(3+2)", "foo", "xyz"), Content(ContinuousSchema[Double](), 9.42 + 3 + 1)),
    Cell(Position("(3-2)", "bar", "xyz"), Content(ContinuousSchema[Double](), 18.84 - 1 - 1)),
    Cell(Position("(3-2)", "foo", "xyz"), Content(ContinuousSchema[Double](), 9.42 - 3 - 1)),
    Cell(Position("(4+2)", "foo", "xyz"), Content(ContinuousSchema[Double](), 12.56 + 3 + 1)),
    Cell(Position("(4-2)", "foo", "xyz"), Content(ContinuousSchema[Double](), 12.56 - 3 - 1))
  )

  val result42 = List(
    Cell(Position("(baz|xyz+bar|xyz)", 1), Content(ContinuousSchema[Double](), 9.42 + 1 + 1)),
    Cell(Position("(baz|xyz+bar|xyz)", 2), Content(ContinuousSchema[Double](), 18.84 + 2 + 1)),
    Cell(Position("(foo|xyz+bar|xyz)", 1), Content(ContinuousSchema[Double](), 3.14 + 1 + 1)),
    Cell(Position("(foo|xyz+bar|xyz)", 2), Content(ContinuousSchema[Double](), 6.28 + 2 + 1)),
    Cell(Position("(foo|xyz+bar|xyz)", 3), Content(ContinuousSchema[Double](), 9.42 + 3 + 1)),
    Cell(Position("(foo|xyz+baz|xyz)", 1), Content(ContinuousSchema[Double](), 3.14 + 4 + 1)),
    Cell(Position("(foo|xyz+baz|xyz)", 2), Content(ContinuousSchema[Double](), 6.28 + 5 + 1)),
    Cell(Position("(qux|xyz+bar|xyz)", 1), Content(ContinuousSchema[Double](), 12.56 + 1 + 1)),
    Cell(Position("(qux|xyz+baz|xyz)", 1), Content(ContinuousSchema[Double](), 12.56 + 4 + 1))
  )

  val result43 = List()

  val result44 = List(
    Cell(Position("(bar|2+bar|1)", "xyz"), Content(ContinuousSchema[Double](), 12.56 + 1 + 1)),
    Cell(Position("(bar|3+bar|1)", "xyz"), Content(ContinuousSchema[Double](), 18.84 + 1 + 1)),
    Cell(Position("(bar|3+bar|2)", "xyz"), Content(ContinuousSchema[Double](), 18.84 + 2 + 1)),
    Cell(Position("(baz|1+bar|1)", "xyz"), Content(ContinuousSchema[Double](), 9.42 + 1 + 1)),
    Cell(Position("(baz|1+bar|2)", "xyz"), Content(ContinuousSchema[Double](), 9.42 + 2 + 1)),
    Cell(Position("(baz|1+bar|3)", "xyz"), Content(ContinuousSchema[Double](), 9.42 + 3 + 1)),
    Cell(Position("(baz|2+bar|1)", "xyz"), Content(ContinuousSchema[Double](), 18.84 + 1 + 1)),
    Cell(Position("(baz|2+bar|2)", "xyz"), Content(ContinuousSchema[Double](), 18.84 + 2 + 1)),
    Cell(Position("(baz|2+bar|3)", "xyz"), Content(ContinuousSchema[Double](), 18.84 + 3 + 1)),
    Cell(Position("(baz|2+baz|1)", "xyz"), Content(ContinuousSchema[Double](), 18.84 + 4 + 1)),
    Cell(Position("(foo|1+bar|1)", "xyz"), Content(ContinuousSchema[Double](), 3.14 + 1 + 1)),
    Cell(Position("(foo|1+bar|2)", "xyz"), Content(ContinuousSchema[Double](), 3.14 + 2 + 1)),
    Cell(Position("(foo|1+bar|3)", "xyz"), Content(ContinuousSchema[Double](), 3.14 + 3 + 1)),
    Cell(Position("(foo|1+baz|1)", "xyz"), Content(ContinuousSchema[Double](), 3.14 + 4 + 1)),
    Cell(Position("(foo|1+baz|2)", "xyz"), Content(ContinuousSchema[Double](), 3.14 + 5 + 1)),
    Cell(Position("(foo|2+bar|1)", "xyz"), Content(ContinuousSchema[Double](), 6.28 + 1 + 1)),
    Cell(Position("(foo|2+bar|2)", "xyz"), Content(ContinuousSchema[Double](), 6.28 + 2 + 1)),
    Cell(Position("(foo|2+bar|3)", "xyz"), Content(ContinuousSchema[Double](), 6.28 + 3 + 1)),
    Cell(Position("(foo|2+baz|1)", "xyz"), Content(ContinuousSchema[Double](), 6.28 + 4 + 1)),
    Cell(Position("(foo|2+baz|2)", "xyz"), Content(ContinuousSchema[Double](), 6.28 + 5 + 1)),
    Cell(Position("(foo|3+bar|1)", "xyz"), Content(ContinuousSchema[Double](), 9.42 + 1 + 1)),
    Cell(Position("(foo|3+bar|2)", "xyz"), Content(ContinuousSchema[Double](), 9.42 + 2 + 1)),
    Cell(Position("(foo|3+bar|3)", "xyz"), Content(ContinuousSchema[Double](), 9.42 + 3 + 1)),
    Cell(Position("(foo|3+baz|1)", "xyz"), Content(ContinuousSchema[Double](), 9.42 + 4 + 1)),
    Cell(Position("(foo|3+baz|2)", "xyz"), Content(ContinuousSchema[Double](), 9.42 + 5 + 1)),
    Cell(Position("(foo|4+bar|1)", "xyz"), Content(ContinuousSchema[Double](), 12.56 + 1 + 1)),
    Cell(Position("(foo|4+bar|2)", "xyz"), Content(ContinuousSchema[Double](), 12.56 + 2 + 1)),
    Cell(Position("(foo|4+bar|3)", "xyz"), Content(ContinuousSchema[Double](), 12.56 + 3 + 1)),
    Cell(Position("(foo|4+baz|1)", "xyz"), Content(ContinuousSchema[Double](), 12.56 + 4 + 1)),
    Cell(Position("(foo|4+baz|2)", "xyz"), Content(ContinuousSchema[Double](), 12.56 + 5 + 1)),
    Cell(Position("(qux|1+bar|1)", "xyz"), Content(ContinuousSchema[Double](), 12.56 + 1 + 1)),
    Cell(Position("(qux|1+bar|2)", "xyz"), Content(ContinuousSchema[Double](), 12.56 + 2 + 1)),
    Cell(Position("(qux|1+bar|3)", "xyz"), Content(ContinuousSchema[Double](), 12.56 + 3 + 1)),
    Cell(Position("(qux|1+baz|1)", "xyz"), Content(ContinuousSchema[Double](), 12.56 + 4 + 1)),
    Cell(Position("(qux|1+baz|2)", "xyz"), Content(ContinuousSchema[Double](), 12.56 + 5 + 1))
  )

  def plus[L <: Nat, P <: Nat](slice: Slice[L, P])(implicit ev: Diff.Aux[P, _1, L]) =
    Locate.PrependPairwiseSelectedStringToRemainder[L, P](slice, "(%1$s+%2$s)")

  def minus[L <: Nat, P <: Nat](slice: Slice[L, P])(implicit ev: Diff.Aux[P, _1, L]) =
    Locate.PrependPairwiseSelectedStringToRemainder[L, P](slice, "(%1$s-%2$s)")
}

object TestMatrixPairwise {

  case class PlusX[P <: Nat, Q <: Nat](pos: Locate.FromPairwiseCells[P, Q]) extends OperatorWithValue[P, Q] {
    type V = Double

    val plus = Plus(pos)

    def computeWithValue(left: Cell[P], right: Cell[P], ext: V): TraversableOnce[Cell[Q]] = plus
      .compute(left, right)
      .map { case Cell(pos, Content(_, DoubleValue(d, _))) => Cell(pos, Content(ContinuousSchema[Double](), d + ext)) }
  }

  case class MinusX[P <: Nat, Q <: Nat](pos: Locate.FromPairwiseCells[P, Q]) extends OperatorWithValue[P, Q] {
    type V = Double

    val minus = Minus(pos)

    def computeWithValue(left: Cell[P], right: Cell[P], ext: V): TraversableOnce[Cell[Q]] = minus
      .compute(left, right)
      .map { case Cell(pos, Content(_, DoubleValue(d, _))) => Cell(pos, Content(ContinuousSchema[Double](), d - ext)) }
  }
}

class TestScaldingMatrixPairwise extends TestMatrixPairwise {

  "A Matrix.pairwise" should "return its first over pairwise in 1D" in {
    toPipe(num1)
      .pairwise(Over(_1), InMemory())(Lower, Plus(plus(Over(_1))))
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its first over pairwise in 2D" in {
    toPipe(num2)
      .pairwise(Over(_1), Default())(Lower, Plus(plus[_1, _2](Over(_1))))
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its first along pairwise in 2D" in {
    toPipe(num2)
      .pairwise(Along(_1), Ternary(InMemory(), Default(12), Default(12)))(
        Lower,
        List(Plus(plus[_1, _2](Along(_1))), Minus(minus[_1, _2](Along(_1))))
      )
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its second over pairwise in 2D" in {
    toPipe(num2)
      .pairwise(Over(_2), Ternary(InMemory(), Default(12), Unbalanced(12)))(
        Lower,
        Plus(plus[_1, _2](Over(_2))),
        Minus(minus[_1, _2](Over(_2)))
      )
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its second along pairwise in 2D" in {
    toPipe(num2)
      .pairwise(Along(_2), Ternary(InMemory(), Unbalanced(12), Default(12)))(
        Lower,
        Plus(plus[_1, _2](Along(_2)))
      )
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its first over pairwise in 3D" in {
    toPipe(num3)
      .pairwise(Over(_1), Ternary(InMemory(), Unbalanced(12), Unbalanced(12)))(Lower, Plus(plus[_2, _3](Over(_1))))
      .toList.sortBy(_.position) shouldBe result6
  }

  it should "return its first along pairwise in 3D" in {
    toPipe(num3)
      .pairwise(Along(_1), Ternary(Default(12), Default(12), Default(12)))(
        Lower,
        List(Plus(plus[_2, _3](Along(_1))), Minus(minus[_2, _3](Along(_1))))
      )
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its second over pairwise in 3D" in {
    toPipe(num3)
      .pairwise(Over(_2), Ternary(Default(12), Default(12), Unbalanced(12)))(
        Lower,
        Plus(plus[_2, _3](Over(_2))),
        Minus(minus[_2, _3](Over(_2)))
      )
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its second along pairwise in 3D" in {
    toPipe(num3)
      .pairwise(Along(_2), Ternary(Default(12), Unbalanced(12), Default(12)))(
        Lower,
        Plus(plus[_2, _3](Along(_2)))
      )
      .toList.sortBy(_.position) shouldBe result9
  }

  it should "return its third over pairwise in 3D" in {
    toPipe(num3)
      .pairwise(Over(_3), Ternary(Default(12), Unbalanced(12), Unbalanced(12)))(
        Lower,
        List(Plus(plus[_2, _3](Over(_3))), Minus(minus[_2, _3](Over(_3))))
      )
      .toList.sortBy(_.position) shouldBe result10
  }

  it should "return its third along pairwise in 3D" in {
    toPipe(num3)
      .pairwise(Along(_3), InMemory())(Lower, Plus(plus[_2, _3](Along(_3))))
      .toList.sortBy(_.position) shouldBe result11
  }

  "A Matrix.pairwiseWithValue" should "return its first over pairwise in 1D" in {
    toPipe(num1)
      .pairwiseWithValue(Over(_1), Default())(
        Lower,
        ValuePipe(ext),
        TestMatrixPairwise.PlusX(plus(Over(_1)))
      )
      .toList.sortBy(_.position) shouldBe result12
  }

  it should "return its first over pairwise in 2D" in {
    toPipe(num2)
      .pairwiseWithValue(Over(_1), Ternary(InMemory(), Default(12), Default(12)))(
        Lower,
        ValuePipe(ext),
        TestMatrixPairwise.PlusX(plus[_1, _2](Over(_1)))
      )
      .toList.sortBy(_.position) shouldBe result13
  }

  it should "return its first along pairwise in 2D" in {
    toPipe(num2)
      .pairwiseWithValue(Along(_1), Ternary(InMemory(), Default(12), Unbalanced(12)))(
        Lower,
        ValuePipe(ext),
        TestMatrixPairwise.PlusX(plus[_1, _2](Along(_1))),
        TestMatrixPairwise.MinusX(minus[_1, _2](Along(_1)))
      )
      .toList.sortBy(_.position) shouldBe result14
  }

  it should "return its second over pairwise in 2D" in {
    toPipe(num2)
      .pairwiseWithValue(Over(_2), Ternary(InMemory(), Unbalanced(12), Default(12)))(
        Lower,
        ValuePipe(ext),
        List(
          TestMatrixPairwise.PlusX(plus[_1, _2](Over(_2))),
          TestMatrixPairwise.MinusX(minus[_1, _2](Over(_2)))
        )
      )
      .toList.sortBy(_.position) shouldBe result15
  }

  it should "return its second along pairwise in 2D" in {
    toPipe(num2)
      .pairwiseWithValue(Along(_2), Ternary(InMemory(), Unbalanced(12), Unbalanced(12)))(
        Lower,
        ValuePipe(ext),
        TestMatrixPairwise.PlusX(plus[_1, _2](Along(_2)))
      )
      .toList.sortBy(_.position) shouldBe result16
  }

  it should "return its first over pairwise in 3D" in {
    toPipe(num3)
      .pairwiseWithValue(Over(_1), Ternary(Default(12), Default(12), Default(12)))(
        Lower,
        ValuePipe(ext),
        TestMatrixPairwise.PlusX(plus[_2, _3](Over(_1)))
      )
      .toList.sortBy(_.position) shouldBe result17
  }

  it should "return its first along pairwise in 3D" in {
    toPipe(num3)
      .pairwiseWithValue(Along(_1), Ternary(Default(12), Default(12), Unbalanced(12)))(
        Lower,
        ValuePipe(ext),
        TestMatrixPairwise.PlusX(plus[_2, _3](Along(_1))),
        TestMatrixPairwise.MinusX(minus[_2, _3](Along(_1)))
      )
      .toList.sortBy(_.position) shouldBe result18
  }

  it should "return its second over pairwise in 3D" in {
    toPipe(num3)
      .pairwiseWithValue(Over(_2), Ternary(Default(12), Unbalanced(12), Default(12)))(
        Lower,
        ValuePipe(ext),
        List(
          TestMatrixPairwise.PlusX(plus[_2, _3](Over(_2))),
          TestMatrixPairwise.MinusX(minus[_2, _3](Over(_2)))
        )
      )
      .toList.sortBy(_.position) shouldBe result19
  }

  it should "return its second along pairwise in 3D" in {
    toPipe(num3)
      .pairwiseWithValue(Along(_2), Ternary(Default(12), Unbalanced(12), Unbalanced(12)))(
        Lower,
        ValuePipe(ext),
        TestMatrixPairwise.PlusX(plus[_2, _3](Along(_2)))
      )
      .toList.sortBy(_.position) shouldBe result20
  }

  it should "return its third over pairwise in 3D" in {
    toPipe(num3)
      .pairwiseWithValue(Over(_3), InMemory())(
        Lower,
        ValuePipe(ext),
        TestMatrixPairwise.PlusX(plus[_2, _3](Over(_3))),
        TestMatrixPairwise.MinusX(minus[_2, _3](Over(_3)))
      )
      .toList.sortBy(_.position) shouldBe result21
  }

  it should "return its third along pairwise in 3D" in {
    toPipe(num3)
      .pairwiseWithValue(Along(_3), Default())(
        Lower,
        ValuePipe(ext),
        TestMatrixPairwise.PlusX(plus[_2, _3](Along(_3)))
      )
      .toList.sortBy(_.position) shouldBe result22
  }

  "A Matrix.pairwiseBetween" should "return its first over pairwise in 1D" in {
    toPipe(num1)
      .pairwiseBetween(Over(_1), Ternary(InMemory(), Default(12), Default(12)))(
        Lower,
        toPipe(dataA),
        Plus(plus(Over(_1)))
      )
      .toList.sortBy(_.position) shouldBe result23
  }

  it should "return its first over pairwise in 2D" in {
    toPipe(num2)
      .pairwiseBetween(Over(_1), Ternary(InMemory(), Default(12), Unbalanced(12)))(
        Lower,
        toPipe(dataB),
        Plus(plus[_1, _2](Over(_1)))
      )
      .toList.sortBy(_.position) shouldBe result24
  }

  it should "return its first along pairwise in 2D" in {
    toPipe(num2)
      .pairwiseBetween(Along(_1), Ternary(InMemory(), Unbalanced(12), Default(12)))(
        Lower,
        toPipe(dataC),
        List(Plus(plus[_1, _2](Along(_1))), Minus(minus[_1, _2](Along(_1))))
      )
      .toList.sortBy(_.position) shouldBe result25
  }

  it should "return its second over pairwise in 2D" in {
    toPipe(num2)
      .pairwiseBetween(Over(_2), Ternary(InMemory(), Unbalanced(12), Unbalanced(12)))(
        Lower,
        toPipe(dataD),
        Plus(plus[_1, _2](Over(_2))),
        Minus(minus[_1, _2](Over(_2)))
      )
      .toList.sortBy(_.position) shouldBe result26
  }

  it should "return its second along pairwise in 2D" in {
    toPipe(num2)
      .pairwiseBetween(Along(_2), Ternary(Default(12), Default(12), Default(12)))(
        Lower,
        toPipe(dataE),
        Plus(plus[_1, _2](Along(_2)))
      )
      .toList.sortBy(_.position) shouldBe result27
  }

  it should "return its first over pairwise in 3D" in {
    toPipe(num3)
      .pairwiseBetween(Over(_1), Ternary(Default(12), Default(12), Unbalanced(12)))(
        Lower,
        toPipe(dataF),
        Plus(plus[_2, _3](Over(_1)))
      )
      .toList.sortBy(_.position) shouldBe result28
  }

  it should "return its first along pairwise in 3D" in {
    toPipe(num3)
      .pairwiseBetween(Along(_1), Ternary(Default(12), Unbalanced(12), Default(12)))(
        Lower,
        toPipe(dataG),
        List(Plus(plus[_2, _3](Along(_1))), Minus(minus[_2, _3](Along(_1))))
      )
      .toList.sortBy(_.position) shouldBe result29
  }

  it should "return its second over pairwise in 3D" in {
    toPipe(num3)
      .pairwiseBetween(Over(_2), Ternary(Default(12), Unbalanced(12), Unbalanced(12)))(
        Lower,
        toPipe(dataH),
        Plus(plus[_2, _3](Over(_2))),
        Minus(minus[_2, _3](Over(_2)))
      )
      .toList.sortBy(_.position) shouldBe result30
  }

  it should "return its second along pairwise in 3D" in {
    toPipe(num3)
      .pairwiseBetween(Along(_2), InMemory())(Lower, toPipe(dataI), Plus(plus[_2, _3](Along(_2))))
      .toList.sortBy(_.position) shouldBe result31
  }

  it should "return its third over pairwise in 3D" in {
    toPipe(num3)
      .pairwiseBetween(Over(_3), Default())(
        Lower,
        toPipe(dataJ),
        List(Plus(plus[_2, _3](Over(_3))), Minus(minus[_2, _3](Over(_3))))
      )
      .toList.sortBy(_.position) shouldBe result32
  }

  it should "return its third along pairwise in 3D" in {
    toPipe(num3)
      .pairwiseBetween(Along(_3), Ternary(InMemory(), Default(12), Default(12)))(
        Lower,
        toPipe(dataK),
        Plus(plus[_2, _3](Along(_3)))
      )
      .toList.sortBy(_.position) shouldBe result33
  }

  "A Matrix.pairwiseBetweenWithValue" should "return its first over pairwise in 1D" in {
    toPipe(num1)
      .pairwiseBetweenWithValue(Over(_1), Ternary(InMemory(), Default(12), Unbalanced(12)))(
        Lower,
        toPipe(dataL),
        ValuePipe(ext),
        TestMatrixPairwise.PlusX(plus(Over(_1)))
      )
      .toList.sortBy(_.position) shouldBe result34
  }

  it should "return its first over pairwise in 2D" in {
    toPipe(num2)
      .pairwiseBetweenWithValue(Over(_1), Ternary(InMemory(), Unbalanced(12), Default(12)))(
        Lower,
        toPipe(dataM),
        ValuePipe(ext),
        TestMatrixPairwise.PlusX(plus[_1, _2](Over(_1)))
      )
      .toList.sortBy(_.position) shouldBe result35
  }

  it should "return its first along pairwise in 2D" in {
    toPipe(num2)
      .pairwiseBetweenWithValue(Along(_1), Ternary(InMemory(), Unbalanced(12), Unbalanced(12)))(
        Lower,
        toPipe(dataN),
        ValuePipe(ext),
        TestMatrixPairwise.PlusX(plus[_1, _2](Along(_1))),
        TestMatrixPairwise.MinusX(minus[_1, _2](Along(_1)))
      )
      .toList.sortBy(_.position) shouldBe result36
  }

  it should "return its second over pairwise in 2D" in {
    toPipe(num2)
      .pairwiseBetweenWithValue(Over(_2), Ternary(Default(12), Default(12), Default(12)))(
        Lower,
        toPipe(dataO),
        ValuePipe(ext),
        List(
          TestMatrixPairwise.PlusX(plus[_1, _2](Over(_2))),
          TestMatrixPairwise.MinusX(minus[_1, _2](Over(_2)))
        )
      )
      .toList.sortBy(_.position) shouldBe result37
  }

  it should "return its second along pairwise in 2D" in {
    toPipe(num2)
      .pairwiseBetweenWithValue(Along(_2), Ternary(Default(12), Default(12), Unbalanced(12)))(
        Lower,
        toPipe(dataP),
        ValuePipe(ext),
        TestMatrixPairwise.PlusX(plus[_1, _2](Along(_2)))
      )
      .toList.sortBy(_.position) shouldBe result38
  }

  it should "return its first over pairwise in 3D" in {
    toPipe(num3)
      .pairwiseBetweenWithValue(Over(_1), Ternary(Default(12), Unbalanced(12), Default(12)))(
        Lower,
        toPipe(dataQ),
        ValuePipe(ext),
        TestMatrixPairwise.PlusX(plus[_2, _3](Over(_1)))
      )
      .toList.sortBy(_.position) shouldBe result39
  }

  it should "return its first along pairwise in 3D" in {
    toPipe(num3)
      .pairwiseBetweenWithValue(Along(_1), Ternary(Default(12), Unbalanced(12), Unbalanced(12)))(
        Lower,
        toPipe(dataR),
        ValuePipe(ext),
        TestMatrixPairwise.PlusX(plus[_2, _3](Along(_1))),
        TestMatrixPairwise.MinusX(minus[_2, _3](Along(_1)))
      )
      .toList.sortBy(_.position) shouldBe result40
  }

  it should "return its second over pairwise in 3D" in {
    toPipe(num3)
      .pairwiseBetweenWithValue(Over(_2), InMemory())(
        Lower,
        toPipe(dataS),
        ValuePipe(ext),
        List(
          TestMatrixPairwise.PlusX(plus[_2, _3](Over(_2))),
          TestMatrixPairwise.MinusX(minus[_2, _3](Over(_2)))
        )
      )
      .toList.sortBy(_.position) shouldBe result41
  }

  it should "return its second along pairwise in 3D" in {
    toPipe(num3)
      .pairwiseBetweenWithValue(Along(_2), Default())(
        Lower,
        toPipe(dataT),
        ValuePipe(ext),
        TestMatrixPairwise.PlusX(plus[_2, _3](Along(_2)))
      )
      .toList.sortBy(_.position) shouldBe result42
  }

  it should "return its third over pairwise in 3D" in {
    toPipe(num3)
      .pairwiseBetweenWithValue(Over(_3), Ternary(InMemory(), Default(12), Default(12)))(
        Lower,
        toPipe(dataU),
        ValuePipe(ext),
        TestMatrixPairwise.PlusX(plus[_2, _3](Over(_3))),
        TestMatrixPairwise.MinusX(minus[_2, _3](Over(_3)))
      )
      .toList.sortBy(_.position) shouldBe result43
  }

  it should "return its third along pairwise in 3D" in {
    toPipe(num3)
      .pairwiseBetweenWithValue(Along(_3), Ternary(InMemory(), Default(12), Unbalanced(12)))(
        Lower,
        toPipe(dataV),
        ValuePipe(ext),
        TestMatrixPairwise.PlusX(plus[_2, _3](Along(_3)))
      )
      .toList.sortBy(_.position) shouldBe result44
  }

  it should "return empty data - InMemory" in {
    toPipe(num3)
      .pairwiseBetween(Along(_3), InMemory())(Lower, TypedPipe.empty, Plus(plus[_2, _3](Along(_3))))
      .toList.sortBy(_.position) shouldBe List()
  }

  it should "return empty data - Default" in {
    toPipe(num3)
      .pairwiseBetween(Along(_3), Default())(Lower, TypedPipe.empty, Plus(plus[_2, _3](Along(_3))))
      .toList.sortBy(_.position) shouldBe List()
  }
}

class TestSparkMatrixPairwise extends TestMatrixPairwise {

  "A Matrix.pairwise" should "return its first over pairwise in 1D" in {
    toRDD(num1)
      .pairwise(Over(_1), InMemory())(Lower, Plus(plus(Over(_1))))
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its first over pairwise in 2D" in {
    toRDD(num2)
      .pairwise(Over(_1), Default())(Lower, Plus(plus[_1, _2](Over(_1))))
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its first along pairwise in 2D" in {
    toRDD(num2)
      .pairwise(Along(_1), Ternary(InMemory(), Default(12), Default(12)))(
        Lower,
        List(Plus(plus[_1, _2](Along(_1))), Minus(minus[_1, _2](Along(_1))))
      )
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its second over pairwise in 2D" in {
    toRDD(num2)
      .pairwise(Over(_2), Ternary(Default(12), Default(12), Default(12)))(
        Lower,
        List(Plus(plus[_1, _2](Over(_2))), Minus(minus[_1, _2](Over(_2))))
      )
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its second along pairwise in 2D" in {
    toRDD(num2)
      .pairwise(Along(_2), InMemory())(Lower, Plus(plus[_1, _2](Along(_2))))
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its first over pairwise in 3D" in {
    toRDD(num3)
      .pairwise(Over(_1), Default())(Lower, Plus(plus[_2, _3](Over(_1))))
      .toList.sortBy(_.position) shouldBe result6
  }

  it should "return its first along pairwise in 3D" in {
    toRDD(num3)
      .pairwise(Along(_1), Ternary(InMemory(), Default(12), Default(12)))(
        Lower,
        Plus(plus[_2, _3](Along(_1))), Minus(minus[_2, _3](Along(_1)))
      )
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its second over pairwise in 3D" in {
    toRDD(num3)
      .pairwise(Over(_2), Ternary(Default(12), Default(12), Default(12)))(
        Lower,
        List(Plus(plus[_2, _3](Over(_2))), Minus(minus[_2, _3](Over(_2))))
      )
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its second along pairwise in 3D" in {
    toRDD(num3)
      .pairwise(Along(_2), InMemory())(Lower, Plus(plus[_2, _3](Along(_2))))
      .toList.sortBy(_.position) shouldBe result9
  }

  it should "return its third over pairwise in 3D" in {
    toRDD(num3)
      .pairwise(Over(_3), Default())(Lower, Plus(plus[_2, _3](Over(_3))), Minus(minus[_2, _3](Over(_3))))
      .toList.sortBy(_.position) shouldBe result10
  }

  it should "return its third along pairwise in 3D" in {
    toRDD(num3)
      .pairwise(Along(_3), Ternary(InMemory(), Default(12), Default(12)))(Lower, Plus(plus[_2, _3](Along(_3))))
      .toList.sortBy(_.position) shouldBe result11
  }

  "A Matrix.pairwiseWithValue" should "return its first over pairwise in 1D" in {
    toRDD(num1)
      .pairwiseWithValue(Over(_1), Ternary(Default(12), Default(12), Default(12)))(
        Lower,
        ext,
        TestMatrixPairwise.PlusX(plus(Over(_1)))
      )
      .toList.sortBy(_.position) shouldBe result12
  }

  it should "return its first over pairwise in 2D" in {
    toRDD(num2)
      .pairwiseWithValue(Over(_1), InMemory())(Lower, ext, TestMatrixPairwise.PlusX(plus[_1, _2](Over(_1))))
      .toList.sortBy(_.position) shouldBe result13
  }

  it should "return its first along pairwise in 2D" in {
    toRDD(num2)
      .pairwiseWithValue(Along(_1), Default())(
        Lower,
        ext,
        List(
          TestMatrixPairwise.PlusX(plus[_1, _2](Along(_1))),
          TestMatrixPairwise.MinusX(minus[_1, _2](Along(_1)))
        )
      )
      .toList.sortBy(_.position) shouldBe result14
  }

  it should "return its second over pairwise in 2D" in {
    toRDD(num2)
      .pairwiseWithValue(Over(_2), Ternary(InMemory(), Default(12), Default(12)))(
        Lower,
        ext,
        TestMatrixPairwise.PlusX(plus[_1, _2](Over(_2))),
        TestMatrixPairwise.MinusX(minus[_1, _2](Over(_2)))
      )
      .toList.sortBy(_.position) shouldBe result15
  }

  it should "return its second along pairwise in 2D" in {
    toRDD(num2)
      .pairwiseWithValue(Along(_2), Ternary(Default(12), Default(12), Default(12)))(
        Lower,
        ext,
        TestMatrixPairwise.PlusX(plus[_1, _2](Along(_2)))
      )
      .toList.sortBy(_.position) shouldBe result16
  }

  it should "return its first over pairwise in 3D" in {
    toRDD(num3)
      .pairwiseWithValue(Over(_1), InMemory())(Lower, ext, TestMatrixPairwise.PlusX(plus[_2, _3](Over(_1))))
      .toList.sortBy(_.position) shouldBe result17
  }

  it should "return its first along pairwise in 3D" in {
    toRDD(num3)
      .pairwiseWithValue(Along(_1), Default())(
        Lower,
        ext,
        List(
          TestMatrixPairwise.PlusX(plus[_2, _3](Along(_1))),
          TestMatrixPairwise.MinusX(minus[_2, _3](Along(_1)))
        )
      )
      .toList.sortBy(_.position) shouldBe result18
  }

  it should "return its second over pairwise in 3D" in {
    toRDD(num3)
      .pairwiseWithValue(Over(_2), Ternary(InMemory(), Default(12), Default(12)))(
        Lower,
        ext,
        TestMatrixPairwise.PlusX(plus[_2, _3](Over(_2))),
        TestMatrixPairwise.MinusX(minus[_2, _3](Over(_2)))
      )
      .toList.sortBy(_.position) shouldBe result19
  }

  it should "return its second along pairwise in 3D" in {
    toRDD(num3)
      .pairwiseWithValue(Along(_2), Ternary(Default(12), Default(12), Default(12)))(
        Lower,
        ext,
        TestMatrixPairwise.PlusX(plus[_2, _3](Along(_2)))
      )
      .toList.sortBy(_.position) shouldBe result20
  }

  it should "return its third over pairwise in 3D" in {
    toRDD(num3)
      .pairwiseWithValue(Over(_3), InMemory())(
        Lower,
        ext,
        List(
          TestMatrixPairwise.PlusX(plus[_2, _3](Over(_3))),
          TestMatrixPairwise.MinusX(minus[_2, _3](Over(_3)))
        )
      )
      .toList.sortBy(_.position) shouldBe result21
  }

  it should "return its third along pairwise in 3D" in {
    toRDD(num3)
      .pairwiseWithValue(Along(_3), Default())(Lower, ext, TestMatrixPairwise.PlusX(plus[_2, _3](Along(_3))))
      .toList.sortBy(_.position) shouldBe result22
  }

  "A Matrix.pairwiseBetween" should "return its first over pairwise in 1D" in {
    toRDD(num1)
      .pairwiseBetween(Over(_1), Ternary(InMemory(), Default(12), Default(12)))(
        Lower,
        toRDD(dataA),
        Plus(plus(Over(_1)))
      )
      .toList.sortBy(_.position) shouldBe result23
  }

  it should "return its first over pairwise in 2D" in {
    toRDD(num2)
      .pairwiseBetween(Over(_1), Ternary(Default(12), Default(12), Default(12)))(
        Lower,
        toRDD(dataB),
        Plus(plus[_1, _2](Over(_1)))
      )
      .toList.sortBy(_.position) shouldBe result24
  }

  it should "return its first along pairwise in 2D" in {
    toRDD(num2)
      .pairwiseBetween(Along(_1), InMemory())(
        Lower,
        toRDD(dataC),
        Plus(plus[_1, _2](Along(_1))), Minus(minus[_1, _2](Along(_1)))
      )
      .toList.sortBy(_.position) shouldBe result25
  }

  it should "return its second over pairwise in 2D" in {
    toRDD(num2)
      .pairwiseBetween(Over(_2), Default())(
        Lower,
        toRDD(dataD),
        List(Plus(plus[_1, _2](Over(_2))), Minus(minus[_1, _2](Over(_2))))
      )
      .toList.sortBy(_.position) shouldBe result26
  }

  it should "return its second along pairwise in 2D" in {
    toRDD(num2)
      .pairwiseBetween(Along(_2), Ternary(InMemory(), Default(12), Default(12)))(
        Lower,
        toRDD(dataE),
        Plus(plus[_1, _2](Along(_2)))
      )
      .toList.sortBy(_.position) shouldBe result27
  }

  it should "return its first over pairwise in 3D" in {
    toRDD(num3)
      .pairwiseBetween(Over(_1), Ternary(Default(12), Default(12), Default(12)))(
        Lower,
        toRDD(dataF),
        Plus(plus[_2, _3](Over(_1)))
      )
      .toList.sortBy(_.position) shouldBe result28
  }

  it should "return its first along pairwise in 3D" in {
    toRDD(num3)
      .pairwiseBetween(Along(_1), InMemory())(
        Lower,
        toRDD(dataG),
        Plus(plus[_2, _3](Along(_1))),
        Minus(minus[_2, _3](Along(_1)))
      )
      .toList.sortBy(_.position) shouldBe result29
  }

  it should "return its second over pairwise in 3D" in {
    toRDD(num3)
      .pairwiseBetween(Over(_2), Default())(
        Lower,
        toRDD(dataH),
        List(Plus(plus[_2, _3](Over(_2))), Minus(minus[_2, _3](Over(_2))))
      )
      .toList.sortBy(_.position) shouldBe result30
  }

  it should "return its second along pairwise in 3D" in {
    toRDD(num3)
      .pairwiseBetween(Along(_2), Ternary(InMemory(), Default(12), Default(12)))(
        Lower,
        toRDD(dataI),
        Plus(plus[_2, _3](Along(_2)))
      )
      .toList.sortBy(_.position) shouldBe result31
  }

  it should "return its third over pairwise in 3D" in {
    toRDD(num3)
      .pairwiseBetween(Over(_3), Ternary(Default(12), Default(12), Default(12)))(
        Lower,
        toRDD(dataJ),
        Plus(plus[_2, _3](Over(_3))),
        Minus(minus[_2, _3](Over(_3)))
      )
      .toList.sortBy(_.position) shouldBe result32
  }

  it should "return its third along pairwise in 3D" in {
    toRDD(num3)
      .pairwiseBetween(Along(_3), InMemory())(
        Lower,
        toRDD(dataK),
        Plus(plus[_2, _3](Along(_3)))
      )
      .toList.sortBy(_.position) shouldBe result33
  }

  "A Matrix.pairwiseBetweenWithValue" should "return its first over pairwise in 1D" in {
    toRDD(num1)
      .pairwiseBetweenWithValue(Over(_1), Default())(
        Lower,
        toRDD(dataL),
        ext,
        TestMatrixPairwise.PlusX(plus(Over(_1)))
      )
      .toList.sortBy(_.position) shouldBe result34
  }

  it should "return its first over pairwise in 2D" in {
    toRDD(num2)
      .pairwiseBetweenWithValue(Over(_1), Ternary(InMemory(), Default(12), Default(12)))(
        Lower,
        toRDD(dataM),
        ext,
        TestMatrixPairwise.PlusX(plus[_1, _2](Over(_1)))
      )
      .toList.sortBy(_.position) shouldBe result35
  }

  it should "return its first along pairwise in 2D" in {
    toRDD(num2)
      .pairwiseBetweenWithValue(Along(_1), Ternary(Default(12), Default(12), Default(12)))(
        Lower,
        toRDD(dataN),
        ext,
        List(
          TestMatrixPairwise.PlusX(plus[_1, _2](Along(_1))),
          TestMatrixPairwise.MinusX(minus[_1, _2](Along(_1)))
        )
      )
      .toList.sortBy(_.position) shouldBe result36
  }

  it should "return its second over pairwise in 2D" in {
    toRDD(num2)
      .pairwiseBetweenWithValue(Over(_2), InMemory())(
        Lower,
        toRDD(dataO),
        ext,
        TestMatrixPairwise.PlusX(plus[_1, _2](Over(_2))),
        TestMatrixPairwise.MinusX(minus[_1, _2](Over(_2)))
      )
      .toList.sortBy(_.position) shouldBe result37
  }

  it should "return its second along pairwise in 2D" in {
    toRDD(num2)
      .pairwiseBetweenWithValue(Along(_2), Default())(
        Lower,
        toRDD(dataP),
        ext,
        TestMatrixPairwise.PlusX(plus[_1, _2](Along(_2)))
      )
      .toList.sortBy(_.position) shouldBe result38
  }

  it should "return its first over pairwise in 3D" in {
    toRDD(num3)
      .pairwiseBetweenWithValue(Over(_1), Ternary(InMemory(), Default(12), Default(12)))(
        Lower,
        toRDD(dataQ),
        ext,
        TestMatrixPairwise.PlusX(plus[_2, _3](Over(_1)))
      )
      .toList.sortBy(_.position) shouldBe result39
  }

  it should "return its first along pairwise in 3D" in {
    toRDD(num3)
      .pairwiseBetweenWithValue(Along(_1), Ternary(Default(12), Default(12), Default(12)))(
        Lower,
        toRDD(dataR),
        ext,
        List(
          TestMatrixPairwise.PlusX(plus[_2, _3](Along(_1))),
          TestMatrixPairwise.MinusX(minus[_2, _3](Along(_1)))
        )
      )
      .toList.sortBy(_.position) shouldBe result40
  }

  it should "return its second over pairwise in 3D" in {
    toRDD(num3)
      .pairwiseBetweenWithValue(Over(_2), InMemory())(
        Lower,
        toRDD(dataS),
        ext,
        TestMatrixPairwise.PlusX(plus[_2, _3](Over(_2))),
        TestMatrixPairwise.MinusX(minus[_2, _3](Over(_2)))
      )
      .toList.sortBy(_.position) shouldBe result41
  }

  it should "return its second along pairwise in 3D" in {
    toRDD(num3)
      .pairwiseBetweenWithValue(Along(_2), Default())(
        Lower,
        toRDD(dataT),
        ext,
        TestMatrixPairwise.PlusX(plus[_2, _3](Along(_2)))
      )
      .toList.sortBy(_.position) shouldBe result42
  }

  it should "return its third over pairwise in 3D" in {
    toRDD(num3)
      .pairwiseBetweenWithValue(Over(_3), Ternary(InMemory(), Default(12), Default(12)))(
        Lower,
        toRDD(dataU),
        ext,
        List(
          TestMatrixPairwise.PlusX(plus[_2, _3](Over(_3))),
          TestMatrixPairwise.MinusX(minus[_2, _3](Over(_3)))
        )
      )
      .toList.sortBy(_.position) shouldBe result43
  }

  it should "return its third along pairwise in 3D" in {
    toRDD(num3)
      .pairwiseBetweenWithValue(Along(_3), Ternary(Default(12), Default(12), Default(12)))(
        Lower,
        toRDD(dataV),
        ext,
        TestMatrixPairwise.PlusX(plus[_2, _3](Along(_3)))
      )
      .toList.sortBy(_.position) shouldBe result44
  }

  it should "return empty data - Default" in {
    toRDD(num3)
      .pairwiseBetween(Along(_3), Default())(Lower, toRDD(List.empty[Cell[_3]]), Plus(plus[_2, _3](Along(_3))))
      .toList.sortBy(_.position) shouldBe List()
  }
}

trait TestMatrixChange extends TestMatrix {

  val result1 = List(
    Cell(Position("bar"), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("baz"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("foo"), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position("qux"), Content(OrdinalSchema[String](), "12.56"))
  )

  val result2 = List(
    Cell(Position("bar", 1), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("bar", 2), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", 3), Content(OrdinalSchema[Long](), 19)),
    Cell(Position("baz", 1), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", 2), Content(DiscreteSchema[Long](), 19)),
    Cell(Position("foo", 1), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position("foo", 2), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("foo", 3), Content(ContinuousSchema[Double](), 9.42)),
    Cell(Position("qux", 1), Content(OrdinalSchema[String](), "12.56"))
  )

  val result3 = List(
    Cell(Position("bar", 1), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("bar", 2), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", 3), Content(ContinuousSchema[Double](), 19.0)),
    Cell(Position("baz", 1), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", 2), Content(DiscreteSchema[Long](), 19)),
    Cell(Position("foo", 1), Content(OrdinalSchema[String](), "3.14")),
    Cell(Position("foo", 2), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("foo", 3), Content(ContinuousSchema[Double](), 9.42)),
    Cell(Position("qux", 1), Content(OrdinalSchema[String](), "12.56"))
  )

  val result4 = List(
    Cell(Position("bar", 1), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("bar", 2), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", 3), Content(ContinuousSchema[Double](), 19.0)),
    Cell(Position("baz", 1), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", 2), Content(DiscreteSchema[Long](), 19)),
    Cell(Position("foo", 1), Content(OrdinalSchema[String](), "3.14")),
    Cell(Position("foo", 2), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("foo", 3), Content(ContinuousSchema[Double](), 9.42)),
    Cell(Position("qux", 1), Content(OrdinalSchema[String](), "12.56"))
  )

  val result5 = List(
    Cell(Position("bar", 1), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("bar", 2), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", 3), Content(OrdinalSchema[Long](), 19)),
    Cell(Position("baz", 1), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", 2), Content(DiscreteSchema[Long](), 19)),
    Cell(Position("foo", 1), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position("foo", 2), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("foo", 3), Content(ContinuousSchema[Double](), 9.42)),
    Cell(Position("qux", 1), Content(OrdinalSchema[String](), "12.56"))
  )

  val result6 = List(
    Cell(Position("bar", 1, "xyz"), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("bar", 2, "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", 3, "xyz"), Content(OrdinalSchema[Long](), 19)),
    Cell(Position("baz", 1, "xyz"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", 2, "xyz"), Content(DiscreteSchema[Long](), 19)),
    Cell(Position("foo", 1, "xyz"), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position("foo", 2, "xyz"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("foo", 3, "xyz"), Content(ContinuousSchema[Double](), 9.42)),
    Cell(Position("qux", 1, "xyz"), Content(OrdinalSchema[String](), "12.56"))
  )

  val result7 = List(
    Cell(Position("bar", 1, "xyz"), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("bar", 2, "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", 3, "xyz"), Content(ContinuousSchema[Double](), 19.0)),
    Cell(Position("baz", 1, "xyz"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", 2, "xyz"), Content(DiscreteSchema[Long](), 19)),
    Cell(Position("foo", 1, "xyz"), Content(OrdinalSchema[String](), "3.14")),
    Cell(Position("foo", 2, "xyz"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("foo", 3, "xyz"), Content(ContinuousSchema[Double](), 9.42)),
    Cell(Position("qux", 1, "xyz"), Content(OrdinalSchema[String](), "12.56"))
  )

  val result8 = List(
    Cell(Position("bar", 1, "xyz"), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("bar", 2, "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", 3, "xyz"), Content(ContinuousSchema[Double](), 19.0)),
    Cell(Position("baz", 1, "xyz"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", 2, "xyz"), Content(DiscreteSchema[Long](), 19)),
    Cell(Position("foo", 1, "xyz"), Content(OrdinalSchema[String](), "3.14")),
    Cell(Position("foo", 2, "xyz"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("foo", 3, "xyz"), Content(ContinuousSchema[Double](), 9.42)),
    Cell(Position("qux", 1, "xyz"), Content(OrdinalSchema[String](), "12.56"))
  )

  val result9 = List(
    Cell(Position("bar", 1, "xyz"), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("bar", 2, "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", 3, "xyz"), Content(OrdinalSchema[Long](), 19)),
    Cell(Position("baz", 1, "xyz"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", 2, "xyz"), Content(DiscreteSchema[Long](), 19)),
    Cell(Position("foo", 1, "xyz"), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position("foo", 2, "xyz"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("foo", 3, "xyz"), Content(ContinuousSchema[Double](), 9.42)),
    Cell(Position("qux", 1, "xyz"), Content(OrdinalSchema[String](), "12.56"))
  )

  val result10 = List(
    Cell(Position("bar", 1, "xyz"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("bar", 2, "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", 3, "xyz"), Content(ContinuousSchema[Double](), 19.0)),
    Cell(Position("baz", 1, "xyz"), Content(ContinuousSchema[Double](), 9.42)),
    Cell(Position("baz", 2, "xyz"), Content(ContinuousSchema[Double](), 19.0)),
    Cell(Position("foo", 1, "xyz"), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position("foo", 2, "xyz"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("foo", 3, "xyz"), Content(ContinuousSchema[Double](), 9.42)),
    Cell(Position("qux", 1, "xyz"), Content(ContinuousSchema[Double](), 12.56))
  )

  val result11 = List(
    Cell(Position("bar", 1, "xyz"), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("bar", 2, "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", 3, "xyz"), Content(OrdinalSchema[Long](), 19)),
    Cell(Position("baz", 1, "xyz"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", 2, "xyz"), Content(DiscreteSchema[Long](), 19)),
    Cell(Position("foo", 1, "xyz"), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position("foo", 2, "xyz"), Content(ContinuousSchema[Double](), 6.28)),
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

  // errors data2/data3 : Date -> Double

  val error2 = List("unable to change: foo|4|date(yyyy-MM-dd)|date|2000-01-01")

  val error3 = List("unable to change: foo|4|xyz|date(yyyy-MM-dd)|date|2000-01-01")
}

object TestMatrixChange {

  def writer[P <: Nat](cell: Cell[P]) = List("unable to change: " + cell.toShortString("|"))
}

class TestScaldingMatrixChange extends TestMatrixChange {

  "A Matrix.change" should "return its first over data in 1D" in {
    val (data, errors) = toPipe(data1)
      .change(Over(_1), InMemory())(
        "foo",
        Content.parser(DoubleCodec, ContinuousSchema[Double]()),
        TestMatrixChange.writer
      )

    data.toList.sortBy(_.position) shouldBe result1
    errors.toList shouldBe List()
  }

  it should "return its first over data in 2D" in {
    val (data, errors) = toPipe(data2)
      .change(Over(_1), Default())(
        "foo",
        Content.parser(DoubleCodec, ContinuousSchema[Double]()),
        TestMatrixChange.writer
      )

    data.toList.sortBy(_.position) shouldBe result2
    errors.toList shouldBe error2
  }

  it should "return its first along data in 2D" in {
    val (data, errors) = toPipe(data2)
      .change(Along(_1), Default(12))(
        List(3, 4),
        Content.parser(DoubleCodec, ContinuousSchema[Double]()),
        TestMatrixChange.writer
      )

    data.toList.sortBy(_.position) shouldBe result3
    errors.toList shouldBe error2
  }

  it should "return its second over data in 2D" in {
    val (data, errors) = toPipe(data2)
      .change(Over(_2), Unbalanced(12))(
        List(3, 4),
        Content.parser(DoubleCodec, ContinuousSchema[Double]()),
        TestMatrixChange.writer
      )

    data.toList.sortBy(_.position) shouldBe result4
    errors.toList shouldBe error2
  }

  it should "return its second along data in 2D" in {
    val (data, errors) = toPipe(data2)
      .change(Along(_2), InMemory())(
        "foo",
        Content.parser(DoubleCodec, ContinuousSchema[Double]()),
        TestMatrixChange.writer
      )

    data.toList.sortBy(_.position) shouldBe result5
    errors.toList shouldBe error2
  }

  it should "return its first over data in 3D" in {
    val (data, errors) = toPipe(data3)
      .change(Over(_1), Default())(
        "foo",
        Content.parser(DoubleCodec, ContinuousSchema[Double]()),
        TestMatrixChange.writer
      )

    data.toList.sortBy(_.position) shouldBe result6
    errors.toList shouldBe error3
  }

  it should "return its first along data in 3D" in {
    val (data, errors) = toPipe(data3)
      .change(Along(_1), Default(12))(
        List(Position(3, "xyz"), Position(4, "xyz")),
        Content.parser(DoubleCodec, ContinuousSchema[Double]()),
        TestMatrixChange.writer
      )

    data.toList.sortBy(_.position) shouldBe result7
    errors.toList shouldBe error3
  }

  it should "return its second over data in 3D" in {
    val (data, errors) = toPipe(data3)
      .change(Over(_2), Unbalanced(12))(
        List(3, 4),
        Content.parser(DoubleCodec, ContinuousSchema[Double]()),
        TestMatrixChange.writer
      )

    data.toList.sortBy(_.position) shouldBe result8
    errors.toList shouldBe error3
  }

  it should "return its second along data in 3D" in {
    val (data, errors) = toPipe(data3)
      .change(Along(_2), InMemory())(
        Position("foo", "xyz"),
        Content.parser(DoubleCodec, ContinuousSchema[Double]()),
        TestMatrixChange.writer
      )

    data.toList.sortBy(_.position) shouldBe result9
    errors.toList shouldBe error3
  }

  it should "return its third over data in 3D" in {
    val (data, errors) = toPipe(data3)
      .change(Over(_3), Default())(
        List("xyz"),
        Content.parser(DoubleCodec, ContinuousSchema[Double]()),
        TestMatrixChange.writer
      )

    data.toList.sortBy(_.position) shouldBe result10
    errors.toList shouldBe error3
  }

  it should "return its third along data in 3D" in {
    val (data, errors) = toPipe(data3)
      .change(Along(_3), Default(12))(
        Position("foo", 1),
        Content.parser(DoubleCodec, ContinuousSchema[Double]()),
        TestMatrixChange.writer
      )

    data.toList.sortBy(_.position) shouldBe result11
    errors.toList shouldBe List()
  }

  it should "return with empty data - InMemory" in {
    val (data, errors) = toPipe(data3)
      .change(Over(_1), InMemory())(
        List.empty[Position[_1]],
        Content.parser(DoubleCodec, ContinuousSchema[Double]()),
        TestMatrixChange.writer
      )

    data.toList.sortBy(_.position) shouldBe data3.sortBy(_.position)
    errors.toList shouldBe List()
  }

  it should "return with empty data - Default" in {
    val (data, errors) = toPipe(data3)
      .change(Over(_1), Default())(
        List.empty[Position[_1]],
        Content.parser(DoubleCodec, ContinuousSchema[Double]()),
        TestMatrixChange.writer
      )

    data.toList.sortBy(_.position) shouldBe data3.sortBy(_.position)
    errors.toList shouldBe List()
  }
}

class TestSparkMatrixChange extends TestMatrixChange {

  "A Matrix.change" should "return its first over data in 1D" in {
    val (data, errors) = toRDD(data1)
      .change(Over(_1), InMemory())(
        "foo",
        Content.parser(DoubleCodec, ContinuousSchema[Double]()),
        TestMatrixChange.writer
      )

    data.toList.sortBy(_.position) shouldBe result1
    errors.toList shouldBe List()
  }

  it should "return its first over data in 2D" in {
    val (data, errors) = toRDD(data2)
      .change(Over(_1), Default())(
        "foo",
        Content.parser(DoubleCodec, ContinuousSchema[Double]()),
        TestMatrixChange.writer
      )

    data.toList.sortBy(_.position) shouldBe result2
    errors.toList shouldBe error2
  }

  it should "return its first along data in 2D" in {
    val (data, errors) = toRDD(data2)
      .change(Along(_1), Default(12))(
        List(3, 4),
        Content.parser(DoubleCodec, ContinuousSchema[Double]()),
        TestMatrixChange.writer
      )

    data.toList.sortBy(_.position) shouldBe result3
    errors.toList shouldBe error2
  }

  it should "return its second over data in 2D" in {
    val (data, errors) = toRDD(data2)
      .change(Over(_2), InMemory())(
        List(3, 4),
        Content.parser(DoubleCodec, ContinuousSchema[Double]()),
        TestMatrixChange.writer
      )

    data.toList.sortBy(_.position) shouldBe result4
    errors.toList shouldBe error2
  }

  it should "return its second along data in 2D" in {
    val (data, errors) = toRDD(data2)
      .change(Along(_2), Default())(
        "foo",
        Content.parser(DoubleCodec, ContinuousSchema[Double]()),
        TestMatrixChange.writer
      )

    data.toList.sortBy(_.position) shouldBe result5
    errors.toList shouldBe error2
  }

  it should "return its first over data in 3D" in {
    val (data, errors) = toRDD(data3)
      .change(Over(_1), Default(12))(
        "foo",
        Content.parser(DoubleCodec, ContinuousSchema[Double]()),
        TestMatrixChange.writer
      )

    data.toList.sortBy(_.position) shouldBe result6
    errors.toList shouldBe error3
  }

  it should "return its first along data in 3D" in {
    val (data, errors) = toRDD(data3)
      .change(Along(_1), InMemory())(
        List(Position(3, "xyz"), Position(4, "xyz")),
        Content.parser(DoubleCodec, ContinuousSchema[Double]()),
        TestMatrixChange.writer
      )

    data.toList.sortBy(_.position) shouldBe result7
    errors.toList shouldBe error3
  }

  it should "return its second over data in 3D" in {
    val (data, errors) = toRDD(data3)
      .change(Over(_2), Default())(
        List(3, 4),
        Content.parser(DoubleCodec, ContinuousSchema[Double]()),
        TestMatrixChange.writer
      )

    data.toList.sortBy(_.position) shouldBe result8
    errors.toList shouldBe error3
  }

  it should "return its second along data in 3D" in {
    val (data, errors) = toRDD(data3)
      .change(Along(_2), Default(12))(
        Position("foo", "xyz"),
        Content.parser(DoubleCodec, ContinuousSchema[Double]()),
        TestMatrixChange.writer
      )

    data.toList.sortBy(_.position) shouldBe result9
    errors.toList shouldBe error3
  }

  it should "return its third over data in 3D" in {
    val (data, errors) = toRDD(data3)
      .change(Over(_3), InMemory())(
        List("xyz"),
        Content.parser(DoubleCodec, ContinuousSchema[Double]()),
        TestMatrixChange.writer
      )

    data.toList.sortBy(_.position) shouldBe result10
    errors.toList shouldBe error3
  }

  it should "return its third along data in 3D" in {
    val (data, errors) = toRDD(data3)
      .change(Along(_3), Default())(
        Position("foo", 1),
        Content.parser(DoubleCodec, ContinuousSchema[Double]()),
        TestMatrixChange.writer
      )

    data.toList.sortBy(_.position) shouldBe result11
    errors.toList shouldBe List()
  }

  it should "return with empty data - Default" in {
    val (data, errors) = toRDD(data3)
      .change(Over(_1), Default())(
        List.empty[Position[_1]],
        Content.parser(DoubleCodec, ContinuousSchema[Double]()),
        TestMatrixChange.writer
      )

    data.toList.sortBy(_.position) shouldBe data3.sortBy(_.position)
    errors.toList shouldBe List()
  }
}

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
    Cell(Position("bar", 3), Content(OrdinalSchema[Long](), 19)),
    Cell(Position("baz", 1), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", 2), Content(DiscreteSchema[Long](), 19)),
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
    Cell(Position("bar", 3), Content(OrdinalSchema[Long](), 19)),
    Cell(Position("baz", 1), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", 2), Content(DiscreteSchema[Long](), 19)),
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
    Cell(Position("bar", 3), Content(OrdinalSchema[Long](), 19)),
    Cell(Position("baz", 1), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", 2), Content(DiscreteSchema[Long](), 19)),
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
    Cell(Position("bar", 3, "xyz"), Content(OrdinalSchema[Long](), 19)),
    Cell(Position("baz", 1, "xyz"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", 2, "xyz"), Content(DiscreteSchema[Long](), 19)),
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
    Cell(Position("bar", 3, "xyz"), Content(OrdinalSchema[Long](), 19)),
    Cell(Position("baz", 1, "xyz"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", 2, "xyz"), Content(DiscreteSchema[Long](), 19)),
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
    Cell(Position("bar", 3, "xyz"), Content(OrdinalSchema[Long](), 19)),
    Cell(Position("baz", 1, "xyz"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", 2, "xyz"), Content(DiscreteSchema[Long](), 19)),
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

class TestScaldingMatrixSet extends TestMatrixSet {

  "A Matrix.set" should "return its updated data in 1D" in {
    toPipe(data1)
      .set(Cell(Position("foo"), Content(ContinuousSchema[Double](), 1.0)), Default())
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its updated and added data in 1D" in {
    toPipe(data1)
      .set(List("foo", "quxx").map(pos => Cell(Position(pos), Content(ContinuousSchema[Double](), 1.0))), Default(12))
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its matrix updated data in 1D" in {
    toPipe(data1)
      .set(toPipe(dataA), Default())
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its updated data in 2D" in {
    toPipe(data2)
      .set(Cell(Position("foo", 2), Content(ContinuousSchema[Double](), 1.0)), Default(12))
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its updated and added data in 2D" in {
    toPipe(data2)
      .set(
        List(Position("foo", 2), Position("quxx", 5)).map(pos => Cell(pos, Content(ContinuousSchema[Double](), 1.0))),
        Default()
      )
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its matrix updated data in 2D" in {
    toPipe(data2)
      .set(toPipe(dataB), Default(12))
      .toList.sortBy(_.position) shouldBe result6
  }

  it should "return its updated data in 3D" in {
    toPipe(data3)
      .set(Cell(Position("foo", 2, "xyz"), Content(ContinuousSchema[Double](), 1.0)), Default())
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its updated and added data in 3D" in {
    toPipe(data3)
      .set(
        List(
          Position("foo", 2, "xyz"),
          Position("quxx", 5, "abc")
        ).map(pos => Cell(pos, Content(ContinuousSchema[Double](), 1.0))),
        Default(12)
      )
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its matrix updated data in 3D" in {
    toPipe(data3)
      .set(toPipe(dataC), Default())
      .toList.sortBy(_.position) shouldBe result9
  }
}

class TestSparkMatrixSet extends TestMatrixSet {

  "A Matrix.set" should "return its updated data in 1D" in {
    toRDD(data1)
      .set(Cell(Position("foo"), Content(ContinuousSchema[Double](), 1.0)), Default())
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its updated and added data in 1D" in {
    toRDD(data1)
      .set(List("foo", "quxx").map(pos => Cell(Position(pos), Content(ContinuousSchema[Double](), 1.0))), Default(12))
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its matrix updated data in 1D" in {
    toRDD(data1)
      .set(toRDD(dataA), Default())
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its updated data in 2D" in {
    toRDD(data2)
      .set(Cell(Position("foo", 2), Content(ContinuousSchema[Double](), 1.0)), Default(12))
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its updated and added data in 2D" in {
    toRDD(data2)
      .set(
        List(Position("foo", 2), Position("quxx", 5)).map(pos => Cell(pos, Content(ContinuousSchema[Double](), 1.0))),
        Default()
      )
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its matrix updated data in 2D" in {
    toRDD(data2)
      .set(toRDD(dataB), Default(12))
      .toList.sortBy(_.position) shouldBe result6
  }

  it should "return its updated data in 3D" in {
    toRDD(data3)
      .set(Cell(Position("foo", 2, "xyz"), Content(ContinuousSchema[Double](), 1.0)), Default())
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its updated and added data in 3D" in {
    toRDD(data3)
      .set(
        List(
          Position("foo", 2, "xyz"),
          Position("quxx", 5, "abc")
        ).map(pos => Cell(pos, Content(ContinuousSchema[Double](), 1.0))),
        Default(12)
      )
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its matrix updated data in 3D" in {
    toRDD(data3)
      .set(toRDD(dataC), Default())
      .toList.sortBy(_.position) shouldBe result9
  }
}

trait TestMatrixTransform extends TestMatrix {

  val ext = Map(
    Position("foo") -> Map(
      Position("max.abs") -> Content(ContinuousSchema[Double](), 3.14),
      Position("mean") -> Content(ContinuousSchema[Double](), 3.14),
      Position("sd") -> Content(ContinuousSchema[Double](), 1.0)
    ),
    Position("bar") -> Map(
      Position("max.abs") -> Content(ContinuousSchema[Double](), 6.28),
      Position("mean") -> Content(ContinuousSchema[Double](), 3.14),
      Position("sd") -> Content(ContinuousSchema[Double](), 2.0)
    ),
    Position("baz") -> Map(
      Position("max.abs") -> Content(ContinuousSchema[Double](), 9.42),
      Position("mean") -> Content(ContinuousSchema[Double](), 3.14),
      Position("sd") -> Content(ContinuousSchema[Double](), 3.0)
    ),
    Position("qux") -> Map(
      Position("max.abs") -> Content(ContinuousSchema[Double](), 12.56),
      Position("mean") -> Content(ContinuousSchema[Double](), 3.14),
      Position("sd") -> Content(ContinuousSchema[Double](), 4.0)
    )
  )

  val result1 = List(
    Cell(Position("bar.ind"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position("baz.ind"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position("foo.ind"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position("qux.ind"), Content(DiscreteSchema[Long](), 1))
  )

  val result2 = List(
    Cell(Position("bar.ind", 1), Content(DiscreteSchema[Long](), 1)),
    Cell(Position("bar.ind", 2), Content(DiscreteSchema[Long](), 1)),
    Cell(Position("bar.ind", 3), Content(DiscreteSchema[Long](), 1)),
    Cell(Position("bar=19", 3), Content(DiscreteSchema[Long](), 1)),
    Cell(Position("bar=6.28", 1), Content(DiscreteSchema[Long](), 1)),
    Cell(Position("baz.ind", 1), Content(DiscreteSchema[Long](), 1)),
    Cell(Position("baz.ind", 2), Content(DiscreteSchema[Long](), 1)),
    Cell(Position("baz=9.42", 1), Content(DiscreteSchema[Long](), 1)),
    Cell(Position("foo.ind", 1), Content(DiscreteSchema[Long](), 1)),
    Cell(Position("foo.ind", 2), Content(DiscreteSchema[Long](), 1)),
    Cell(Position("foo.ind", 3), Content(DiscreteSchema[Long](), 1)),
    Cell(Position("foo.ind", 4), Content(DiscreteSchema[Long](), 1)),
    Cell(Position("foo=3.14", 1), Content(DiscreteSchema[Long](), 1)),
    Cell(Position("foo=9.42", 3), Content(DiscreteSchema[Long](), 1)),
    Cell(Position("qux.ind", 1), Content(DiscreteSchema[Long](), 1)),
    Cell(Position("qux=12.56", 1), Content(DiscreteSchema[Long](), 1))
  )

  val result3 = List(
    Cell(Position("bar.ind", 1, "xyz"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position("bar.ind", 2, "xyz"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position("bar.ind", 3, "xyz"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position("bar=19", 3, "xyz"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position("bar=6.28", 1, "xyz"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position("baz.ind", 1, "xyz"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position("baz.ind", 2, "xyz"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position("baz=9.42", 1, "xyz"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position("foo.ind", 1, "xyz"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position("foo.ind", 2, "xyz"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position("foo.ind", 3, "xyz"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position("foo.ind", 4, "xyz"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position("foo=3.14", 1, "xyz"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position("foo=9.42", 3, "xyz"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position("qux.ind", 1, "xyz"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position("qux=12.56", 1, "xyz"), Content(DiscreteSchema[Long](), 1))
  )

  val result4 = List(
    Cell(Position("bar.n"), Content(ContinuousSchema[Double](), 6.28 / 6.28)),
    Cell(Position("bar.s"), Content(ContinuousSchema[Double](), (6.28 - 3.14) / 2)),
    Cell(Position("baz.n"), Content(ContinuousSchema[Double](), 9.42 / 9.42)),
    Cell(Position("baz.s"), Content(ContinuousSchema[Double](), (9.42 - 3.14) / 3)),
    Cell(Position("foo.n"), Content(ContinuousSchema[Double](), 3.14 / 3.14)),
    Cell(Position("foo.s"), Content(ContinuousSchema[Double](), (3.14 - 3.14) / 1)),
    Cell(Position("qux.n"), Content(ContinuousSchema[Double](), 12.56 / 12.56)),
    Cell(Position("qux.s"), Content(ContinuousSchema[Double](), (12.56 - 3.14) / 4))
  )

  val result5 = List(
    Cell(Position("bar.n", 1), Content(ContinuousSchema[Double](), 6.28 / 6.28)),
    Cell(Position("bar.n", 2), Content(ContinuousSchema[Double](), 12.56 / 6.28)),
    Cell(Position("bar.n", 3), Content(ContinuousSchema[Double](), 18.84 / 6.28)),
    Cell(Position("baz.n", 1), Content(ContinuousSchema[Double](), 9.42 / 9.42)),
    Cell(Position("baz.n", 2), Content(ContinuousSchema[Double](), 18.84 / 9.42)),
    Cell(Position("foo.n", 1), Content(ContinuousSchema[Double](), 3.14 / 3.14)),
    Cell(Position("foo.n", 2), Content(ContinuousSchema[Double](), 6.28 / 3.14)),
    Cell(Position("foo.n", 3), Content(ContinuousSchema[Double](), 9.42 / 3.14)),
    Cell(Position("foo.n", 4), Content(ContinuousSchema[Double](), 12.56 / 3.14)),
    Cell(Position("qux.n", 1), Content(ContinuousSchema[Double](), 12.56 / 12.56))
  )

  val result6 = List(
    Cell(Position("bar.n", 1, "xyz"), Content(ContinuousSchema[Double](), 6.28 / 6.28)),
    Cell(Position("bar.n", 2, "xyz"), Content(ContinuousSchema[Double](), 12.56 / 6.28)),
    Cell(Position("bar.n", 3, "xyz"), Content(ContinuousSchema[Double](), 18.84 / 6.28)),
    Cell(Position("baz.n", 1, "xyz"), Content(ContinuousSchema[Double](), 9.42 / 9.42)),
    Cell(Position("baz.n", 2, "xyz"), Content(ContinuousSchema[Double](), 18.84 / 9.42)),
    Cell(Position("foo.n", 1, "xyz"), Content(ContinuousSchema[Double](), 3.14 / 3.14)),
    Cell(Position("foo.n", 2, "xyz"), Content(ContinuousSchema[Double](), 6.28 / 3.14)),
    Cell(Position("foo.n", 3, "xyz"), Content(ContinuousSchema[Double](), 9.42 / 3.14)),
    Cell(Position("foo.n", 4, "xyz"), Content(ContinuousSchema[Double](), 12.56 / 3.14)),
    Cell(Position("qux.n", 1, "xyz"), Content(ContinuousSchema[Double](), 12.56 / 12.56))
  )

  val result7 = List(
    Cell(Position("bar.ind", "ind"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position("baz.ind", "ind"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position("foo.ind", "ind"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position("qux.ind", "ind"), Content(DiscreteSchema[Long](), 1))
  )

  val result8 = List(
    Cell(Position("bar.ind", 1, "ind"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position("bar.ind", 2, "ind"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position("bar.ind", 3, "ind"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position("bar=19", 3, "bin"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position("bar=6.28", 1, "bin"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position("baz.ind", 1, "ind"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position("baz.ind", 2, "ind"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position("baz=9.42", 1, "bin"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position("foo.ind", 1, "ind"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position("foo.ind", 2, "ind"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position("foo.ind", 3, "ind"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position("foo.ind", 4, "ind"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position("foo=3.14", 1, "bin"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position("foo=9.42", 3, "bin"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position("qux.ind", 1, "ind"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position("qux=12.56", 1, "bin"), Content(DiscreteSchema[Long](), 1))
  )

  val result9 = List(
    Cell(Position("bar.ind", 1, "xyz", "ind"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position("bar.ind", 2, "xyz", "ind"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position("bar.ind", 3, "xyz", "ind"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position("bar=19", 3, "xyz", "bin"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position("bar=6.28", 1, "xyz", "bin"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position("baz.ind", 1, "xyz", "ind"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position("baz.ind", 2, "xyz", "ind"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position("baz=9.42", 1, "xyz", "bin"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position("foo.ind", 1, "xyz", "ind"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position("foo.ind", 2, "xyz", "ind"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position("foo.ind", 3, "xyz", "ind"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position("foo.ind", 4, "xyz", "ind"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position("foo=3.14", 1, "xyz", "bin"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position("foo=9.42", 3, "xyz", "bin"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position("qux.ind", 1, "xyz", "ind"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position("qux=12.56", 1, "xyz", "bin"), Content(DiscreteSchema[Long](), 1))
  )

  val result10 = List(
    Cell(Position("bar.n", "nrm"), Content(ContinuousSchema[Double](), 6.28 / 6.28)),
    Cell(Position("bar.s", "std"), Content(ContinuousSchema[Double](), (6.28 - 3.14) / 2)),
    Cell(Position("baz.n", "nrm"), Content(ContinuousSchema[Double](), 9.42 / 9.42)),
    Cell(Position("baz.s", "std"), Content(ContinuousSchema[Double](), (9.42 - 3.14) / 3)),
    Cell(Position("foo.n", "nrm"), Content(ContinuousSchema[Double](), 3.14 / 3.14)),
    Cell(Position("foo.s", "std"), Content(ContinuousSchema[Double](), (3.14 - 3.14) / 1)),
    Cell(Position("qux.n", "nrm"), Content(ContinuousSchema[Double](), 12.56 / 12.56)),
    Cell(Position("qux.s", "std"), Content(ContinuousSchema[Double](), (12.56 - 3.14) / 4))
  )

  val result11 = List(
    Cell(Position("bar.n", 1, "nrm"), Content(ContinuousSchema[Double](), 6.28 / 6.28)),
    Cell(Position("bar.n", 2, "nrm"), Content(ContinuousSchema[Double](), 12.56 / 6.28)),
    Cell(Position("bar.n", 3, "nrm"), Content(ContinuousSchema[Double](), 18.84 / 6.28)),
    Cell(Position("baz.n", 1, "nrm"), Content(ContinuousSchema[Double](), 9.42 / 9.42)),
    Cell(Position("baz.n", 2, "nrm"), Content(ContinuousSchema[Double](), 18.84 / 9.42)),
    Cell(Position("foo.n", 1, "nrm"), Content(ContinuousSchema[Double](), 3.14 / 3.14)),
    Cell(Position("foo.n", 2, "nrm"), Content(ContinuousSchema[Double](), 6.28 / 3.14)),
    Cell(Position("foo.n", 3, "nrm"), Content(ContinuousSchema[Double](), 9.42 / 3.14)),
    Cell(Position("foo.n", 4, "nrm"), Content(ContinuousSchema[Double](), 12.56 / 3.14)),
    Cell(Position("qux.n", 1, "nrm"), Content(ContinuousSchema[Double](), 12.56 / 12.56))
  )

  val result12 = List(
    Cell(Position("bar.n", 1, "xyz", "nrm"), Content(ContinuousSchema[Double](), 6.28 / 6.28)),
    Cell(Position("bar.n", 2, "xyz", "nrm"), Content(ContinuousSchema[Double](), 12.56 / 6.28)),
    Cell(Position("bar.n", 3, "xyz", "nrm"), Content(ContinuousSchema[Double](), 18.84 / 6.28)),
    Cell(Position("baz.n", 1, "xyz", "nrm"), Content(ContinuousSchema[Double](), 9.42 / 9.42)),
    Cell(Position("baz.n", 2, "xyz", "nrm"), Content(ContinuousSchema[Double](), 18.84 / 9.42)),
    Cell(Position("foo.n", 1, "xyz", "nrm"), Content(ContinuousSchema[Double](), 3.14 / 3.14)),
    Cell(Position("foo.n", 2, "xyz", "nrm"), Content(ContinuousSchema[Double](), 6.28 / 3.14)),
    Cell(Position("foo.n", 3, "xyz", "nrm"), Content(ContinuousSchema[Double](), 9.42 / 3.14)),
    Cell(Position("foo.n", 4, "xyz", "nrm"), Content(ContinuousSchema[Double](), 12.56 / 3.14)),
    Cell(Position("qux.n", 1, "xyz", "nrm"), Content(ContinuousSchema[Double](), 12.56 / 12.56))
  )

  type W = Map[Position[_1], Map[Position[_1], Content]]

  def extractor[P <: Nat](key: String)(implicit ev: LTEq[_1, P]) =
    ExtractWithDimensionAndKey[P, Content](_1, key).andThenPresent(_.value.asDouble)
}

class TestScaldingMatrixTransform extends TestMatrixTransform {

  "A Matrix.transform" should "return its transformed data in 1D" in {
    toPipe(data1)
      .transform(Indicator().andThenRelocate(Locate.RenameDimension(_1, "%1$s.ind")))
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its transformed data in 2D" in {
    toPipe(data2)
      .transform(
        List(
          Indicator[_2]().andThenRelocate(Locate.RenameDimension(_1, "%1$s.ind")),
          Binarise[_2](Locate.RenameDimensionWithContent(_1))
        )
      )
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its transformed data in 3D" in {
    toPipe(data3)
      .transform(
        Indicator[_3]().andThenRelocate(Locate.RenameDimension(_1, "%1$s.ind")),
        Binarise[_3](Locate.RenameDimensionWithContent(_1))
      )
      .toList.sortBy(_.position) shouldBe result3
  }

  "A Matrix.transformWithValue" should "return its transformed data in 1D" in {
    toPipe(num1)
      .transformWithValue(
        ValuePipe(ext),
        List(
          Normalise[_1, W](extractor("max.abs")).andThenRelocate(Locate.RenameDimension(_1, "%1$s.n")),
          Standardise[_1, W](extractor("mean"), extractor("sd")).andThenRelocate(Locate.RenameDimension(_1, "%1$s.s"))
        )
      )
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its transformed data in 2D" in {
    toPipe(num2)
      .transformWithValue(
        ValuePipe(ext),
        Normalise[_2, W](extractor("max.abs")).andThenRelocate(Locate.RenameDimension(_1, "%1$s.n"))
      )
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its transformed data in 3D" in {
    toPipe(num3)
      .transformWithValue(
        ValuePipe(ext),
        Normalise[_3, W](extractor("max.abs")).andThenRelocate(Locate.RenameDimension(_1, "%1$s.n"))
      )
      .toList.sortBy(_.position) shouldBe result6
  }

  "A Matrix.transformAndExpand" should "return its transformed data in 1D" in {
    toPipe(data1)
      .transform(
        Indicator[_1]()
          .andThenRelocate(Locate.RenameDimension(_1, "%1$s.ind"))
          .andThenRelocate(c => c.position.append("ind").toOption)
      )
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its transformed data in 2D" in {
    toPipe(data2)
      .transform(
        Indicator[_2]()
          .andThenRelocate(Locate.RenameDimension(_1, "%1$s.ind"))
          .andThenRelocate(c => c.position.append("ind").toOption),
        Binarise[_2](Locate.RenameDimensionWithContent(_1)).andThenRelocate(c => c.position.append("bin").toOption)
      )
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its transformed data in 3D" in {
    toPipe(data3)
      .transform(
        List(
          Indicator[_3]()
            .andThenRelocate(Locate.RenameDimension(_1, "%1$s.ind"))
            .andThenRelocate(c => c.position.append("ind").toOption),
          Binarise[_3](Locate.RenameDimensionWithContent(_1)).andThenRelocate(c => c.position.append("bin").toOption)
        )
      )
      .toList.sortBy(_.position) shouldBe result9
  }

  "A Matrix.transformAndExpandWithValue" should "return its transformed data in 1D" in {
    toPipe(num1)
      .transformWithValue(
        ValuePipe(ext),
        Normalise[_1, W](extractor("max.abs"))
          .andThenRelocate(Locate.RenameDimension(_1, "%1$s.n"))
          .andThenRelocateWithValue((c, _) => c.position.append("nrm").toOption),
        Standardise[_1, W](extractor("mean"), extractor("sd"))
          .andThenRelocate(Locate.RenameDimension(_1, "%1$s.s"))
          .andThenRelocateWithValue((c, _) => c.position.append("std").toOption)
      )
      .toList.sortBy(_.position) shouldBe result10
  }

  it should "return its transformed data in 2D" in {
    toPipe(num2)
      .transformWithValue(
        ValuePipe(ext),
        Normalise[_2, W](extractor("max.abs"))
          .andThenRelocate(Locate.RenameDimension(_1, "%1$s.n"))
          .andThenRelocateWithValue((c, _) => c.position.append("nrm").toOption)
      )
      .toList.sortBy(_.position) shouldBe result11
  }

  it should "return its transformed data in 3D" in {
    toPipe(num3)
      .transformWithValue(
        ValuePipe(ext),
        Normalise[_3, W](extractor("max.abs"))
          .andThenRelocate(Locate.RenameDimension(_1, "%1$s.n"))
          .andThenRelocateWithValue((c, _) => c.position.append("nrm").toOption)
      )
      .toList.sortBy(_.position) shouldBe result12
  }
}

class TestSparkMatrixTransform extends TestMatrixTransform {

  "A Matrix.transform" should "return its transformed data in 1D" in {
    toRDD(data1)
      .transform(Indicator[_1]().andThenRelocate(Locate.RenameDimension(_1, "%1$s.ind")))
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its transformed data in 2D" in {
    toRDD(data2)
      .transform(
        List(
          Indicator[_2]().andThenRelocate(Locate.RenameDimension(_1, "%1$s.ind")),
          Binarise[_2](Locate.RenameDimensionWithContent(_1))
        )
      )
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its transformed data in 3D" in {
    toRDD(data3)
      .transform(
        Indicator[_3]().andThenRelocate(Locate.RenameDimension(_1, "%1$s.ind")),
        Binarise[_3](Locate.RenameDimensionWithContent(_1))
      )
      .toList.sortBy(_.position) shouldBe result3
  }

  "A Matrix.transformWithValue" should "return its transformed data in 1D" in {
    toRDD(num1)
      .transformWithValue(
        ext,
        List(
          Normalise[_1, W](extractor("max.abs")).andThenRelocate(Locate.RenameDimension(_1, "%1$s.n")),
          Standardise[_1, W](extractor("mean"), extractor("sd")).andThenRelocate(Locate.RenameDimension(_1, "%1$s.s"))
        )
      )
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its transformed data in 2D" in {
    toRDD(num2)
      .transformWithValue(
        ext,
        Normalise[_2, W](extractor("max.abs")).andThenRelocate(Locate.RenameDimension(_1, "%1$s.n"))
      )
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its transformed data in 3D" in {
    toRDD(num3)
      .transformWithValue(
        ext,
        Normalise[_3, W](extractor("max.abs")).andThenRelocate(Locate.RenameDimension(_1, "%1$s.n"))
      )
      .toList.sortBy(_.position) shouldBe result6
  }

  "A Matrix.transformAndExpand" should "return its transformed data in 1D" in {
    toRDD(data1)
      .transform(
        Indicator[_1]()
          .andThenRelocate(Locate.RenameDimension(_1, "%1$s.ind"))
          .andThenRelocate(c => c.position.append("ind").toOption)
      )
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its transformed data in 2D" in {
    toRDD(data2)
      .transform(
        Indicator[_2]()
          .andThenRelocate(Locate.RenameDimension(_1, "%1$s.ind"))
          .andThenRelocate(c => c.position.append("ind").toOption),
        Binarise[_2](Locate.RenameDimensionWithContent(_1)).andThenRelocate(c => c.position.append("bin").toOption)
      )
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its transformed data in 3D" in {
    toRDD(data3)
      .transform(
        List(
          Indicator[_3]()
            .andThenRelocate(Locate.RenameDimension(_1, "%1$s.ind"))
            .andThenRelocate(c => c.position.append("ind").toOption),
          Binarise[_3](Locate.RenameDimensionWithContent(_1)).andThenRelocate(c => c.position.append("bin").toOption)
        )
      )
      .toList.sortBy(_.position) shouldBe result9
  }

  "A Matrix.transformAndExpandWithValue" should "return its transformed data in 1D" in {
    toRDD(num1)
      .transformWithValue(
        ext,
        Normalise[_1, W](extractor("max.abs"))
          .andThenRelocate(Locate.RenameDimension(_1, "%1$s.n"))
          .andThenRelocateWithValue((c, _) => c.position.append("nrm").toOption),
        Standardise[_1, W](extractor("mean"), extractor("sd"))
          .andThenRelocate(Locate.RenameDimension(_1, "%1$s.s"))
          .andThenRelocateWithValue((c, _) => c.position.append("std").toOption)
      )
      .toList.sortBy(_.position) shouldBe result10
  }

  it should "return its transformed data in 2D" in {
    toRDD(num2)
      .transformWithValue(
        ext,
        Normalise[_2, W](extractor("max.abs"))
          .andThenRelocate(Locate.RenameDimension(_1, "%1$s.n"))
          .andThenRelocateWithValue((c, _) => c.position.append("nrm").toOption)
      )
      .toList.sortBy(_.position) shouldBe result11
  }

  it should "return its transformed data in 3D" in {
    toRDD(num3)
      .transformWithValue(
        ext,
        Normalise[_3, W](extractor("max.abs"))
          .andThenRelocate(Locate.RenameDimension(_1, "%1$s.n"))
          .andThenRelocateWithValue((c, _) => c.position.append("nrm").toOption)
      )
      .toList.sortBy(_.position) shouldBe result12
  }
}

trait TestMatrixSlide extends TestMatrix {

  val ext = Map("one" -> 1, "two" -> 2)

  val result1 = List(
    Cell(Position("1*(bar-baz)"), Content(ContinuousSchema[Double](), 6.28 - 9.42)),
    Cell(Position("1*(baz-foo)"), Content(ContinuousSchema[Double](), 9.42 - 3.14)),
    Cell(Position("1*(foo-qux)"), Content(ContinuousSchema[Double](), 3.14 - 12.56)),
    Cell(Position("2*(bar-baz)"), Content(ContinuousSchema[Double](), 2 * (6.28 - 9.42))),
    Cell(Position("2*(baz-foo)"), Content(ContinuousSchema[Double](), 2 * (9.42 - 3.14))),
    Cell(Position("2*(foo-qux)"), Content(ContinuousSchema[Double](), 2 * (3.14 - 12.56)))
  )

  val result2 = List(
    Cell(Position("bar", "1*(1-2)"), Content(ContinuousSchema[Double](), 6.28 - 12.56)),
    Cell(Position("bar", "1*(2-3)"), Content(ContinuousSchema[Double](), 12.56 - 18.84)),
    Cell(Position("baz", "1*(1-2)"), Content(ContinuousSchema[Double](), 9.42 - 18.84)),
    Cell(Position("foo", "1*(1-2)"), Content(ContinuousSchema[Double](), 3.14 - 6.28)),
    Cell(Position("foo", "1*(2-3)"), Content(ContinuousSchema[Double](), 6.28 - 9.42)),
    Cell(Position("foo", "1*(3-4)"), Content(ContinuousSchema[Double](), 9.42 - 12.56))
  )

  val result3 = List(
    Cell(Position(1, "1*(baz-bar)"), Content(ContinuousSchema[Double](), 9.42 - 6.28)),
    Cell(Position(1, "1*(foo-baz)"), Content(ContinuousSchema[Double](), 3.14 - 9.42)),
    Cell(Position(1, "1*(qux-foo)"), Content(ContinuousSchema[Double](), 12.56 - 3.14)),
    Cell(Position(2, "1*(baz-bar)"), Content(ContinuousSchema[Double](), 18.84 - 12.56)),
    Cell(Position(2, "1*(foo-baz)"), Content(ContinuousSchema[Double](), 6.28 - 18.84)),
    Cell(Position(3, "1*(foo-bar)"), Content(ContinuousSchema[Double](), 9.42 - 18.84))
  )

  val result4 = List(
    Cell(Position(1, "1*(baz-bar)"), Content(ContinuousSchema[Double](), 9.42 - 6.28)),
    Cell(Position(1, "1*(foo-baz)"), Content(ContinuousSchema[Double](), 3.14 - 9.42)),
    Cell(Position(1, "1*(qux-foo)"), Content(ContinuousSchema[Double](), 12.56 - 3.14)),
    Cell(Position(2, "1*(baz-bar)"), Content(ContinuousSchema[Double](), 18.84 - 12.56)),
    Cell(Position(2, "1*(foo-baz)"), Content(ContinuousSchema[Double](), 6.28 - 18.84)),
    Cell(Position(3, "1*(foo-bar)"), Content(ContinuousSchema[Double](), 9.42 - 18.84))
  )

  val result5 = List(
    Cell(Position("bar", "1*(1-2)"), Content(ContinuousSchema[Double](), 6.28 - 12.56)),
    Cell(Position("bar", "1*(2-3)"), Content(ContinuousSchema[Double](), 12.56 - 18.84)),
    Cell(Position("baz", "1*(1-2)"), Content(ContinuousSchema[Double](), 9.42 - 18.84)),
    Cell(Position("foo", "1*(1-2)"), Content(ContinuousSchema[Double](), 3.14 - 6.28)),
    Cell(Position("foo", "1*(2-3)"), Content(ContinuousSchema[Double](), 6.28 - 9.42)),
    Cell(Position("foo", "1*(3-4)"), Content(ContinuousSchema[Double](), 9.42 - 12.56))
  )

  val result6 = List(
    Cell(Position("bar", "1*(1|xyz-2|xyz)"), Content(ContinuousSchema[Double](), 6.28 - 12.56)),
    Cell(Position("bar", "1*(2|xyz-3|xyz)"), Content(ContinuousSchema[Double](), 12.56 - 18.84)),
    Cell(Position("baz", "1*(1|xyz-2|xyz)"), Content(ContinuousSchema[Double](), 9.42 - 18.84)),
    Cell(Position("foo", "1*(1|xyz-2|xyz)"), Content(ContinuousSchema[Double](), 3.14 - 6.28)),
    Cell(Position("foo", "1*(2|xyz-3|xyz)"), Content(ContinuousSchema[Double](), 6.28 - 9.42)),
    Cell(Position("foo", "1*(3|xyz-4|xyz)"), Content(ContinuousSchema[Double](), 9.42 - 12.56))
  )

  val result7 = List(
    Cell(Position(1, "xyz", "1*(baz-bar)"), Content(ContinuousSchema[Double](), 9.42 - 6.28)),
    Cell(Position(1, "xyz", "1*(foo-baz)"), Content(ContinuousSchema[Double](), 3.14 - 9.42)),
    Cell(Position(1, "xyz", "1*(qux-foo)"), Content(ContinuousSchema[Double](), 12.56 - 3.14)),
    Cell(Position(2, "xyz", "1*(baz-bar)"), Content(ContinuousSchema[Double](), 18.84 - 12.56)),
    Cell(Position(2, "xyz", "1*(foo-baz)"), Content(ContinuousSchema[Double](), 6.28 - 18.84)),
    Cell(Position(3, "xyz", "1*(foo-bar)"), Content(ContinuousSchema[Double](), 9.42 - 18.84))
  )

  val result8 = List(
    Cell(Position(1, "1*(baz|xyz-bar|xyz)"), Content(ContinuousSchema[Double](), 9.42 - 6.28)),
    Cell(Position(1, "1*(foo|xyz-baz|xyz)"), Content(ContinuousSchema[Double](), 3.14 - 9.42)),
    Cell(Position(1, "1*(qux|xyz-foo|xyz)"), Content(ContinuousSchema[Double](), 12.56 - 3.14)),
    Cell(Position(2, "1*(baz|xyz-bar|xyz)"), Content(ContinuousSchema[Double](), 18.84 - 12.56)),
    Cell(Position(2, "1*(foo|xyz-baz|xyz)"), Content(ContinuousSchema[Double](), 6.28 - 18.84)),
    Cell(Position(3, "1*(foo|xyz-bar|xyz)"), Content(ContinuousSchema[Double](), 9.42 - 18.84))
  )

  val result9 = List(
    Cell(Position("bar", "xyz", "1*(1-2)"), Content(ContinuousSchema[Double](), 6.28 - 12.56)),
    Cell(Position("bar", "xyz", "1*(2-3)"), Content(ContinuousSchema[Double](), 12.56 - 18.84)),
    Cell(Position("baz", "xyz", "1*(1-2)"), Content(ContinuousSchema[Double](), 9.42 - 18.84)),
    Cell(Position("foo", "xyz", "1*(1-2)"), Content(ContinuousSchema[Double](), 3.14 - 6.28)),
    Cell(Position("foo", "xyz", "1*(2-3)"), Content(ContinuousSchema[Double](), 6.28 - 9.42)),
    Cell(Position("foo", "xyz", "1*(3-4)"), Content(ContinuousSchema[Double](), 9.42 - 12.56))
  )

  val result10 = List(
    Cell(Position("xyz", "1*(bar|1-bar|2)"), Content(ContinuousSchema[Double](), 6.28 - 12.56)),
    Cell(Position("xyz", "1*(bar|2-bar|3)"), Content(ContinuousSchema[Double](), 12.56 - 18.84)),
    Cell(Position("xyz", "1*(bar|3-baz|1)"), Content(ContinuousSchema[Double](), 18.84 - 9.42)),
    Cell(Position("xyz", "1*(baz|1-baz|2)"), Content(ContinuousSchema[Double](), 9.42 - 18.84)),
    Cell(Position("xyz", "1*(baz|2-foo|1)"), Content(ContinuousSchema[Double](), 18.84 - 3.14)),
    Cell(Position("xyz", "1*(foo|1-foo|2)"), Content(ContinuousSchema[Double](), 3.14 - 6.28)),
    Cell(Position("xyz", "1*(foo|2-foo|3)"), Content(ContinuousSchema[Double](), 6.28 - 9.42)),
    Cell(Position("xyz", "1*(foo|3-foo|4)"), Content(ContinuousSchema[Double](), 9.42 - 12.56)),
    Cell(Position("xyz", "1*(foo|4-qux|1)"), Content(ContinuousSchema[Double](), 12.56 - 12.56))
  )

  val result11 = List()

  val result12 = List(
    Cell(Position("1*(baz-bar)"), Content(ContinuousSchema[Double](), 9.42 - 6.28)),
    Cell(Position("1*(foo-baz)"), Content(ContinuousSchema[Double](), 3.14 - 9.42)),
    Cell(Position("1*(qux-foo)"), Content(ContinuousSchema[Double](), 12.56 - 3.14)),
    Cell(Position("2*(baz-bar)"), Content(ContinuousSchema[Double](), 2 * (9.42 - 6.28))),
    Cell(Position("2*(foo-baz)"), Content(ContinuousSchema[Double](), 2 * (3.14 - 9.42))),
    Cell(Position("2*(qux-foo)"), Content(ContinuousSchema[Double](), 2 * (12.56 - 3.14)))
  )

  val result13 = List(
    Cell(Position("bar", "1*(1-2)"), Content(ContinuousSchema[Double](), 6.28 - 12.56)),
    Cell(Position("bar", "1*(2-3)"), Content(ContinuousSchema[Double](), 12.56 - 18.84)),
    Cell(Position("baz", "1*(1-2)"), Content(ContinuousSchema[Double](), 9.42 - 18.84)),
    Cell(Position("foo", "1*(1-2)"), Content(ContinuousSchema[Double](), 3.14 - 6.28)),
    Cell(Position("foo", "1*(2-3)"), Content(ContinuousSchema[Double](), 6.28 - 9.42)),
    Cell(Position("foo", "1*(3-4)"), Content(ContinuousSchema[Double](), 9.42 - 12.56))
  )

  val result14 = List(
    Cell(Position(1, "1*(bar-baz)"), Content(ContinuousSchema[Double](), 6.28 - 9.42)),
    Cell(Position(1, "1*(baz-foo)"), Content(ContinuousSchema[Double](), 9.42 - 3.14)),
    Cell(Position(1, "1*(foo-qux)"), Content(ContinuousSchema[Double](), 3.14 - 12.56)),
    Cell(Position(2, "1*(bar-baz)"), Content(ContinuousSchema[Double](), 12.56 - 18.84)),
    Cell(Position(2, "1*(baz-foo)"), Content(ContinuousSchema[Double](), 18.84 - 6.28)),
    Cell(Position(3, "1*(bar-foo)"), Content(ContinuousSchema[Double](), 18.84 - 9.42))
  )

  val result15 = List(
    Cell(Position(1, "1*(baz-bar)"), Content(ContinuousSchema[Double](), 9.42 - 6.28)),
    Cell(Position(1, "1*(foo-baz)"), Content(ContinuousSchema[Double](), 3.14 - 9.42)),
    Cell(Position(1, "1*(qux-foo)"), Content(ContinuousSchema[Double](), 12.56 - 3.14)),
    Cell(Position(2, "1*(baz-bar)"), Content(ContinuousSchema[Double](), 18.84 - 12.56)),
    Cell(Position(2, "1*(foo-baz)"), Content(ContinuousSchema[Double](), 6.28 - 18.84)),
    Cell(Position(3, "1*(foo-bar)"), Content(ContinuousSchema[Double](), 9.42 - 18.84))
  )

  val result16 = List(
    Cell(Position("bar", "1*(2-1)"), Content(ContinuousSchema[Double](), 12.56 - 6.28)),
    Cell(Position("bar", "1*(3-2)"), Content(ContinuousSchema[Double](), 18.84 - 12.56)),
    Cell(Position("baz", "1*(2-1)"), Content(ContinuousSchema[Double](), 18.84 - 9.42)),
    Cell(Position("foo", "1*(2-1)"), Content(ContinuousSchema[Double](), 6.28 - 3.14)),
    Cell(Position("foo", "1*(3-2)"), Content(ContinuousSchema[Double](), 9.42 - 6.28)),
    Cell(Position("foo", "1*(4-3)"), Content(ContinuousSchema[Double](), 12.56 - 9.42))
  )

  val result17 = List(
    Cell(Position("bar", "1*(1|xyz-2|xyz)"), Content(ContinuousSchema[Double](), 6.28 - 12.56)),
    Cell(Position("bar", "1*(2|xyz-3|xyz)"), Content(ContinuousSchema[Double](), 12.56 - 18.84)),
    Cell(Position("baz", "1*(1|xyz-2|xyz)"), Content(ContinuousSchema[Double](), 9.42 - 18.84)),
    Cell(Position("foo", "1*(1|xyz-2|xyz)"), Content(ContinuousSchema[Double](), 3.14 - 6.28)),
    Cell(Position("foo", "1*(2|xyz-3|xyz)"), Content(ContinuousSchema[Double](), 6.28 - 9.42)),
    Cell(Position("foo", "1*(3|xyz-4|xyz)"), Content(ContinuousSchema[Double](), 9.42 - 12.56))
  )

  val result18 = List(
    Cell(Position(1, "xyz", "1*(bar-baz)"), Content(ContinuousSchema[Double](), 6.28 - 9.42)),
    Cell(Position(1, "xyz", "1*(baz-foo)"), Content(ContinuousSchema[Double](), 9.42 - 3.14)),
    Cell(Position(1, "xyz", "1*(foo-qux)"), Content(ContinuousSchema[Double](), 3.14 - 12.56)),
    Cell(Position(2, "xyz", "1*(bar-baz)"), Content(ContinuousSchema[Double](), 12.56 - 18.84)),
    Cell(Position(2, "xyz", "1*(baz-foo)"), Content(ContinuousSchema[Double](), 18.84 - 6.28)),
    Cell(Position(3, "xyz", "1*(bar-foo)"), Content(ContinuousSchema[Double](), 18.84 - 9.42))
  )

  val result19 = List(
    Cell(Position(1, "1*(baz|xyz-bar|xyz)"), Content(ContinuousSchema[Double](), 9.42 - 6.28)),
    Cell(Position(1, "1*(foo|xyz-baz|xyz)"), Content(ContinuousSchema[Double](), 3.14 - 9.42)),
    Cell(Position(1, "1*(qux|xyz-foo|xyz)"), Content(ContinuousSchema[Double](), 12.56 - 3.14)),
    Cell(Position(2, "1*(baz|xyz-bar|xyz)"), Content(ContinuousSchema[Double](), 18.84 - 12.56)),
    Cell(Position(2, "1*(foo|xyz-baz|xyz)"), Content(ContinuousSchema[Double](), 6.28 - 18.84)),
    Cell(Position(3, "1*(foo|xyz-bar|xyz)"), Content(ContinuousSchema[Double](), 9.42 - 18.84))
  )

  val result20 = List(
    Cell(Position("bar", "xyz", "1*(2-1)"), Content(ContinuousSchema[Double](), 12.56 - 6.28)),
    Cell(Position("bar", "xyz", "1*(3-2)"), Content(ContinuousSchema[Double](), 18.84 - 12.56)),
    Cell(Position("baz", "xyz", "1*(2-1)"), Content(ContinuousSchema[Double](), 18.84 - 9.42)),
    Cell(Position("foo", "xyz", "1*(2-1)"), Content(ContinuousSchema[Double](), 6.28 - 3.14)),
    Cell(Position("foo", "xyz", "1*(3-2)"), Content(ContinuousSchema[Double](), 9.42 - 6.28)),
    Cell(Position("foo", "xyz", "1*(4-3)"), Content(ContinuousSchema[Double](), 12.56 - 9.42))
  )

  val result21 = List(
    Cell(Position("xyz", "1*(bar|1-bar|2)"), Content(ContinuousSchema[Double](), 6.28 - 12.56)),
    Cell(Position("xyz", "1*(bar|2-bar|3)"), Content(ContinuousSchema[Double](), 12.56 - 18.84)),
    Cell(Position("xyz", "1*(bar|3-baz|1)"), Content(ContinuousSchema[Double](), 18.84 - 9.42)),
    Cell(Position("xyz", "1*(baz|1-baz|2)"), Content(ContinuousSchema[Double](), 9.42 - 18.84)),
    Cell(Position("xyz", "1*(baz|2-foo|1)"), Content(ContinuousSchema[Double](), 18.84 - 3.14)),
    Cell(Position("xyz", "1*(foo|1-foo|2)"), Content(ContinuousSchema[Double](), 3.14 - 6.28)),
    Cell(Position("xyz", "1*(foo|2-foo|3)"), Content(ContinuousSchema[Double](), 6.28 - 9.42)),
    Cell(Position("xyz", "1*(foo|3-foo|4)"), Content(ContinuousSchema[Double](), 9.42 - 12.56)),
    Cell(Position("xyz", "1*(foo|4-qux|1)"), Content(ContinuousSchema[Double](), 12.56 - 12.56))
  )

  val result22 = List()
}

object TestMatrixSlide {

  case class Delta[P <: Nat, S <: Nat, R <: Nat](times: Int) extends Window[P, S, R, Succ[S]] {
    type I = Option[Double]
    type T = (Option[Double], Position[R])
    type O = (Double, Position[R], Position[R])

    def prepare(cell: Cell[P]): I = cell.content.value.asDouble

    def initialise(rem: Position[R], in: I): (T, TraversableOnce[O]) = ((in, rem), List())

    def update(rem: Position[R], in: I, t: T): (T, TraversableOnce[O]) = ((in, rem), (in, t._1) match {
     case (Some(dc), Some(dt)) => List((dc - dt, rem, t._2))
     case _ => List()
   })

    def present(pos: Position[S], out: O): TraversableOnce[Cell[Succ[S]]] = List(
      Cell(
        pos.append(times + "*(" + out._2.toShortString("|") + "-" + out._3.toShortString("|") + ")"),
        Content(ContinuousSchema[Double](), times * out._1)
      )
    )
  }

  case class DeltaWithValue[P <: Nat, S <: Nat, R <: Nat](key: String) extends WindowWithValue[P, S, R, Succ[S]] {
    type V = Map[String, Int]
    type I = Option[Double]
    type T = (Option[Double], Position[R])
    type O = (Double, Position[R], Position[R])

    def prepareWithValue(cell: Cell[P], ext: V): I = cell.content.value.asDouble

    def initialise(rem: Position[R], in: I): (T, TraversableOnce[O]) = ((in, rem), List())

    def update(rem: Position[R], in: I, t: T): (T, TraversableOnce[O]) = ((in, rem), (in, t._1) match {
     case (Some(dc), Some(dt)) => List((dc - dt, rem, t._2))
     case _ => List()
   })

    def presentWithValue(pos: Position[S], out: O, ext: V): TraversableOnce[Cell[Succ[S]]] = List(
      Cell(
        pos.append(ext(key) + "*(" + out._2.toShortString("|") + "-" + out._3.toShortString("|") + ")"),
        Content(ContinuousSchema[Double](), ext(key) * out._1)
      )
    )
  }
}

class TestScaldingMatrixSlide extends TestMatrixSlide {

  "A Matrix.slide" should "return its first along derived data in 1D" in {
    toPipe(num1)
      .slide(Along(_1), Default())(
        false,
        List(TestMatrixSlide.Delta[_1, _0, _1](1), TestMatrixSlide.Delta[_1, _0, _1](2))
      )
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its first over derived data in 2D" in {
    toPipe(num2)
      .slide(Over(_1), Default(12))(false, TestMatrixSlide.Delta(1))
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its first along derived data in 2D" in {
    toPipe(num2)
      .slide(Along(_1), Redistribute(12))(true, TestMatrixSlide.Delta(1))
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its second over derived data in 2D" in {
    toPipe(num2)
      .slide(Over(_2), Default())(true, TestMatrixSlide.Delta(1))
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its second along derived data in 2D" in {
    toPipe(num2)
      .slide(Along(_2), Default(12))(false, TestMatrixSlide.Delta(1))
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its first over derived data in 3D" in {
    toPipe(num3)
      .slide(Over(_1), Redistribute(12))(false, TestMatrixSlide.Delta(1))
      .toList.sortBy(_.position) shouldBe result6
  }

  it should "return its first along derived data in 3D" in {
    toPipe(num3)
      .slide(Along(_1), Default())(true, TestMatrixSlide.Delta(1))
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its second over derived data in 3D" in {
    toPipe(num3)
      .slide(Over(_2), Default(12))(true, TestMatrixSlide.Delta(1))
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its second along derived data in 3D" in {
    toPipe(num3)
      .slide(Along(_2), Redistribute(12))(false, TestMatrixSlide.Delta(1))
      .toList.sortBy(_.position) shouldBe result9
  }

  it should "return its third over derived data in 3D" in {
    toPipe(num3)
      .slide(Over(_3), Default())(false, TestMatrixSlide.Delta(1))
      .toList.sortBy(_.position) shouldBe result10
  }

  it should "return its third along derived data in 3D" in {
    toPipe(num3)
      .slide(Along(_3), Default(12))(true, TestMatrixSlide.Delta(1))
      .toList.sortBy(_.position) shouldBe result11
  }

  "A Matrix.slideWithValue" should "return its first along derived data in 1D" in {
    toPipe(num1)
      .slideWithValue(Along(_1), Redistribute(12))(
        true,
        ValuePipe(ext),
        TestMatrixSlide.DeltaWithValue[_1, _0, _1]("one"),
        TestMatrixSlide.DeltaWithValue[_1, _0, _1]("two")
      )
      .toList.sortBy(_.position) shouldBe result12
  }

  it should "return its first over derived data in 2D" in {
    toPipe(num2)
      .slideWithValue(Over(_1), Default())(false, ValuePipe(ext), TestMatrixSlide.DeltaWithValue("one"))
      .toList.sortBy(_.position) shouldBe result13
  }

  it should "return its first along derived data in 2D" in {
    toPipe(num2)
      .slideWithValue(Along(_1), Default(12))(false, ValuePipe(ext), TestMatrixSlide.DeltaWithValue("one"))
      .toList.sortBy(_.position) shouldBe result14
  }

  it should "return its second over derived data in 2D" in {
    toPipe(num2)
      .slideWithValue(Over(_2), Redistribute(12))(true, ValuePipe(ext), TestMatrixSlide.DeltaWithValue("one"))
      .toList.sortBy(_.position) shouldBe result15
  }

  it should "return its second along derived data in 2D" in {
    toPipe(num2)
      .slideWithValue(Along(_2), Default())(true, ValuePipe(ext), TestMatrixSlide.DeltaWithValue("one"))
      .toList.sortBy(_.position) shouldBe result16
  }

  it should "return its first over derived data in 3D" in {
    toPipe(num3)
      .slideWithValue(Over(_1), Default(12))(false, ValuePipe(ext), TestMatrixSlide.DeltaWithValue("one"))
      .toList.sortBy(_.position) shouldBe result17
  }

  it should "return its first along derived data in 3D" in {
    toPipe(num3)
      .slideWithValue(Along(_1), Redistribute(12))(false, ValuePipe(ext), TestMatrixSlide.DeltaWithValue("one"))
      .toList.sortBy(_.position) shouldBe result18
  }

  it should "return its second over derived data in 3D" in {
    toPipe(num3)
      .slideWithValue(Over(_2), Default())(true, ValuePipe(ext), TestMatrixSlide.DeltaWithValue("one"))
      .toList.sortBy(_.position) shouldBe result19
  }

  it should "return its second along derived data in 3D" in {
    toPipe(num3)
      .slideWithValue(Along(_2), Default(12))(true, ValuePipe(ext), TestMatrixSlide.DeltaWithValue("one"))
      .toList.sortBy(_.position) shouldBe result20
  }

  it should "return its third over derived data in 3D" in {
    toPipe(num3)
      .slideWithValue(Over(_3), Redistribute(12))(false, ValuePipe(ext), TestMatrixSlide.DeltaWithValue("one"))
      .toList.sortBy(_.position) shouldBe result21
  }

  it should "return its third along derived data in 3D" in {
    toPipe(num3)
      .slideWithValue(Along(_3), Default())(false, ValuePipe(ext), TestMatrixSlide.DeltaWithValue("one"))
      .toList.sortBy(_.position) shouldBe result22
  }
}

class TestSparkMatrixSlide extends TestMatrixSlide {

  "A Matrix.slide" should "return its first along derived data in 1D" in {
    toRDD(num1)
      .slide(Along(_1), Default())(
        false,
        List(TestMatrixSlide.Delta[_1, _0, _1](1), TestMatrixSlide.Delta[_1, _0, _1](2))
      )
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its first over derived data in 2D" in {
    toRDD(num2)
      .slide(Over(_1), Default(12))(false, TestMatrixSlide.Delta(1))
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its first along derived data in 2D" in {
    toRDD(num2)
      .slide(Along(_1), Default())(true, TestMatrixSlide.Delta(1))
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its second over derived data in 2D" in {
    toRDD(num2)
      .slide(Over(_2), Default())(true, TestMatrixSlide.Delta(1))
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its second along derived data in 2D" in {
    toRDD(num2)
      .slide(Along(_2), Default(12))(false, TestMatrixSlide.Delta(1))
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its first over derived data in 3D" in {
    toRDD(num3)
      .slide(Over(_1), Default())(false, TestMatrixSlide.Delta(1))
      .toList.sortBy(_.position) shouldBe result6
  }

  it should "return its first along derived data in 3D" in {
    toRDD(num3)
      .slide(Along(_1), Default(12))(true, TestMatrixSlide.Delta(1))
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its second over derived data in 3D" in {
    toRDD(num3)
      .slide(Over(_2), Default())(true, TestMatrixSlide.Delta(1))
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its second along derived data in 3D" in {
    toRDD(num3)
      .slide(Along(_2), Default(12))(false, TestMatrixSlide.Delta(1))
      .toList.sortBy(_.position) shouldBe result9
  }

  it should "return its third over derived data in 3D" in {
    toRDD(num3)
      .slide(Over(_3), Default())(false, TestMatrixSlide.Delta(1))
      .toList.sortBy(_.position) shouldBe result10
  }

  it should "return its third along derived data in 3D" in {
    toRDD(num3)
      .slide(Along(_3), Default(12))(true, TestMatrixSlide.Delta(1))
      .toList.sortBy(_.position) shouldBe result11
  }

  "A Matrix.slideWithValue" should "return its first along derived data in 1D" in {
    toRDD(num1)
      .slideWithValue(Along(_1), Default())(
        true,
        ext,
        TestMatrixSlide.DeltaWithValue[_1, _0, _1]("one"),
        TestMatrixSlide.DeltaWithValue[_1, _0, _1]("two")
      )
      .toList.sortBy(_.position) shouldBe result12
  }

  it should "return its first over derived data in 2D" in {
    toRDD(num2)
      .slideWithValue(Over(_1), Default(12))(false, ext, TestMatrixSlide.DeltaWithValue("one"))
      .toList.sortBy(_.position) shouldBe result13
  }

  it should "return its first along derived data in 2D" in {
    toRDD(num2)
      .slideWithValue(Along(_1), Default())(false, ext, TestMatrixSlide.DeltaWithValue("one"))
      .toList.sortBy(_.position) shouldBe result14
  }

  it should "return its second over derived data in 2D" in {
    toRDD(num2)
      .slideWithValue(Over(_2), Default(12))(true, ext, TestMatrixSlide.DeltaWithValue("one"))
      .toList.sortBy(_.position) shouldBe result15
  }

  it should "return its second along derived data in 2D" in {
    toRDD(num2)
      .slideWithValue(Along(_2), Default())(true, ext, TestMatrixSlide.DeltaWithValue("one"))
      .toList.sortBy(_.position) shouldBe result16
  }

  it should "return its first over derived data in 3D" in {
    toRDD(num3)
      .slideWithValue(Over(_1), Default(12))(false, ext, TestMatrixSlide.DeltaWithValue("one"))
      .toList.sortBy(_.position) shouldBe result17
  }

  it should "return its first along derived data in 3D" in {
    toRDD(num3)
      .slideWithValue(Along(_1), Default())(false, ext, TestMatrixSlide.DeltaWithValue("one"))
      .toList.sortBy(_.position) shouldBe result18
  }

  it should "return its second over derived data in 3D" in {
    toRDD(num3)
      .slideWithValue(Over(_2), Default(12))(true, ext, TestMatrixSlide.DeltaWithValue("one"))
      .toList.sortBy(_.position) shouldBe result19
  }

  it should "return its second along derived data in 3D" in {
    toRDD(num3)
      .slideWithValue(Along(_2), Default())(true, ext, TestMatrixSlide.DeltaWithValue("one"))
      .toList.sortBy(_.position) shouldBe result20
  }

  it should "return its third over derived data in 3D" in {
    toRDD(num3)
      .slideWithValue(Over(_3), Default(12))(false, ext, TestMatrixSlide.DeltaWithValue("one"))
      .toList.sortBy(_.position) shouldBe result21
  }

  it should "return its third along derived data in 3D" in {
    toRDD(num3)
      .slideWithValue(Along(_3), Default())(false, ext, TestMatrixSlide.DeltaWithValue("one"))
      .toList.sortBy(_.position) shouldBe result22
  }
}

trait TestMatrixFill extends TestMatrix {

  val result0 = List(
    Cell(Position("bar"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("baz"), Content(ContinuousSchema[Double](), 9.42)),
    Cell(Position("foo"), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position("qux"), Content(ContinuousSchema[Double](), 12.56))
  )

  val result1 = List(
    Cell(Position("bar", 1), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("bar", 2), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", 3), Content(ContinuousSchema[Double](), 18.84)),
    Cell(Position("bar", 4), Content(ContinuousSchema[Double](), 0.0)),
    Cell(Position("baz", 1), Content(ContinuousSchema[Double](), 9.42)),
    Cell(Position("baz", 2), Content(ContinuousSchema[Double](), 18.84)),
    Cell(Position("baz", 3), Content(ContinuousSchema[Double](), 0.0)),
    Cell(Position("baz", 4), Content(ContinuousSchema[Double](), 0.0)),
    Cell(Position("foo", 1), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position("foo", 2), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("foo", 3), Content(ContinuousSchema[Double](), 9.42)),
    Cell(Position("foo", 4), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("qux", 1), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("qux", 2), Content(ContinuousSchema[Double](), 0.0)),
    Cell(Position("qux", 3), Content(ContinuousSchema[Double](), 0.0)),
    Cell(Position("qux", 4), Content(ContinuousSchema[Double](), 0.0))
  )

  val result2 = List(
    Cell(Position("bar", 1, "xyz"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("bar", 2, "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", 3, "xyz"), Content(ContinuousSchema[Double](), 18.84)),
    Cell(Position("bar", 4, "xyz"), Content(ContinuousSchema[Double](), 0.0)),
    Cell(Position("baz", 1, "xyz"), Content(ContinuousSchema[Double](), 9.42)),
    Cell(Position("baz", 2, "xyz"), Content(ContinuousSchema[Double](), 18.84)),
    Cell(Position("baz", 3, "xyz"), Content(ContinuousSchema[Double](), 0.0)),
    Cell(Position("baz", 4, "xyz"), Content(ContinuousSchema[Double](), 0.0)),
    Cell(Position("foo", 1, "xyz"), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position("foo", 2, "xyz"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("foo", 3, "xyz"), Content(ContinuousSchema[Double](), 9.42)),
    Cell(Position("foo", 4, "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("qux", 1, "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("qux", 2, "xyz"), Content(ContinuousSchema[Double](), 0.0)),
    Cell(Position("qux", 3, "xyz"), Content(ContinuousSchema[Double](), 0.0)),
    Cell(Position("qux", 4, "xyz"), Content(ContinuousSchema[Double](), 0.0))
  )

  val result3 = List(
    Cell(Position("bar", 1), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("bar", 2), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", 3), Content(ContinuousSchema[Double](), 18.84)),
    Cell(Position("bar", 4), Content(ContinuousSchema[Double](), (6.28 + 12.56 + 18.84) / 3)),
    Cell(Position("baz", 1), Content(ContinuousSchema[Double](), 9.42)),
    Cell(Position("baz", 2), Content(ContinuousSchema[Double](), 18.84)),
    Cell(Position("baz", 3), Content(ContinuousSchema[Double](), (9.42 + 18.84) / 2)),
    Cell(Position("baz", 4), Content(ContinuousSchema[Double](), (9.42 + 18.84) / 2)),
    Cell(Position("foo", 1), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position("foo", 2), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("foo", 3), Content(ContinuousSchema[Double](), 9.42)),
    Cell(Position("foo", 4), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("qux", 1), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("qux", 2), Content(ContinuousSchema[Double](), (12.56) / 1)),
    Cell(Position("qux", 3), Content(ContinuousSchema[Double](), (12.56) / 1)),
    Cell(Position("qux", 4), Content(ContinuousSchema[Double](), (12.56) / 1))
  )

  val result4 = List(
    Cell(Position("bar", 1), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("bar", 2), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", 3), Content(ContinuousSchema[Double](), 18.84)),
    Cell(Position("bar", 4), Content(ContinuousSchema[Double](), (12.56) / 1)),
    Cell(Position("baz", 1), Content(ContinuousSchema[Double](), 9.42)),
    Cell(Position("baz", 2), Content(ContinuousSchema[Double](), 18.84)),
    Cell(Position("baz", 3), Content(ContinuousSchema[Double](), (9.42 + 18.84) / 2)),
    Cell(Position("baz", 4), Content(ContinuousSchema[Double](), (12.56) / 1)),
    Cell(Position("foo", 1), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position("foo", 2), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("foo", 3), Content(ContinuousSchema[Double](), 9.42)),
    Cell(Position("foo", 4), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("qux", 1), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("qux", 2), Content(ContinuousSchema[Double](), (6.28 + 12.56 + 18.84) / 3)),
    Cell(Position("qux", 3), Content(ContinuousSchema[Double](), (9.42 + 18.84) / 2)),
    Cell(Position("qux", 4), Content(ContinuousSchema[Double](), (12.56) / 1))
  )

  val result5 = List(
    Cell(Position("bar", 1), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("bar", 2), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", 3), Content(ContinuousSchema[Double](), 18.84)),
    Cell(Position("bar", 4), Content(ContinuousSchema[Double](), (12.56) / 1)),
    Cell(Position("baz", 1), Content(ContinuousSchema[Double](), 9.42)),
    Cell(Position("baz", 2), Content(ContinuousSchema[Double](), 18.84)),
    Cell(Position("baz", 3), Content(ContinuousSchema[Double](), (9.42 + 18.84) / 2)),
    Cell(Position("baz", 4), Content(ContinuousSchema[Double](), (12.56) / 1)),
    Cell(Position("foo", 1), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position("foo", 2), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("foo", 3), Content(ContinuousSchema[Double](), 9.42)),
    Cell(Position("foo", 4), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("qux", 1), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("qux", 2), Content(ContinuousSchema[Double](), (6.28 + 12.56 + 18.84) / 3)),
    Cell(Position("qux", 3), Content(ContinuousSchema[Double](), (9.42 + 18.84) / 2)),
    Cell(Position("qux", 4), Content(ContinuousSchema[Double](), (12.56) / 1))
  )

  val result6 = List(
    Cell(Position("bar", 1), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("bar", 2), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", 3), Content(ContinuousSchema[Double](), 18.84)),
    Cell(Position("bar", 4), Content(ContinuousSchema[Double](), (6.28 + 12.56 + 18.84) / 3)),
    Cell(Position("baz", 1), Content(ContinuousSchema[Double](), 9.42)),
    Cell(Position("baz", 2), Content(ContinuousSchema[Double](), 18.84)),
    Cell(Position("baz", 3), Content(ContinuousSchema[Double](), (9.42 + 18.84) / 2)),
    Cell(Position("baz", 4), Content(ContinuousSchema[Double](), (9.42 + 18.84) / 2)),
    Cell(Position("foo", 1), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position("foo", 2), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("foo", 3), Content(ContinuousSchema[Double](), 9.42)),
    Cell(Position("foo", 4), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("qux", 1), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("qux", 2), Content(ContinuousSchema[Double](), (12.56) / 1)),
    Cell(Position("qux", 3), Content(ContinuousSchema[Double](), (12.56) / 1)),
    Cell(Position("qux", 4), Content(ContinuousSchema[Double](), (12.56) / 1))
  )

  val result7 = List(
    Cell(Position("bar", 1, "xyz"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("bar", 2, "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", 3, "xyz"), Content(ContinuousSchema[Double](), 18.84)),
    Cell(Position("bar", 4, "xyz"), Content(ContinuousSchema[Double](), (6.28 + 12.56 + 18.84) / 3)),
    Cell(Position("baz", 1, "xyz"), Content(ContinuousSchema[Double](), 9.42)),
    Cell(Position("baz", 2, "xyz"), Content(ContinuousSchema[Double](), 18.84)),
    Cell(Position("baz", 3, "xyz"), Content(ContinuousSchema[Double](), (9.42 + 18.84) / 2)),
    Cell(Position("baz", 4, "xyz"), Content(ContinuousSchema[Double](), (9.42 + 18.84) / 2)),
    Cell(Position("foo", 1, "xyz"), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position("foo", 2, "xyz"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("foo", 3, "xyz"), Content(ContinuousSchema[Double](), 9.42)),
    Cell(Position("foo", 4, "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("qux", 1, "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("qux", 2, "xyz"), Content(ContinuousSchema[Double](), (12.56) / 1)),
    Cell(Position("qux", 3, "xyz"), Content(ContinuousSchema[Double](), (12.56) / 1)),
    Cell(Position("qux", 4, "xyz"), Content(ContinuousSchema[Double](), (12.56) / 1))
  )

  val result8 = List(
    Cell(Position("bar", 1, "xyz"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("bar", 2, "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", 3, "xyz"), Content(ContinuousSchema[Double](), 18.84)),
    Cell(Position("bar", 4, "xyz"), Content(ContinuousSchema[Double](), (12.56) / 1)),
    Cell(Position("baz", 1, "xyz"), Content(ContinuousSchema[Double](), 9.42)),
    Cell(Position("baz", 2, "xyz"), Content(ContinuousSchema[Double](), 18.84)),
    Cell(Position("baz", 3, "xyz"), Content(ContinuousSchema[Double](), (9.42 + 18.84) / 2)),
    Cell(Position("baz", 4, "xyz"), Content(ContinuousSchema[Double](), (12.56) / 1)),
    Cell(Position("foo", 1, "xyz"), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position("foo", 2, "xyz"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("foo", 3, "xyz"), Content(ContinuousSchema[Double](), 9.42)),
    Cell(Position("foo", 4, "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("qux", 1, "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("qux", 2, "xyz"), Content(ContinuousSchema[Double](), (6.28 + 12.56 + 18.84) / 3)),
    Cell(Position("qux", 3, "xyz"), Content(ContinuousSchema[Double](), (9.42 + 18.84) / 2)),
    Cell(Position("qux", 4, "xyz"), Content(ContinuousSchema[Double](), (12.56) / 1))
  )

  val result9 = List(
    Cell(Position("bar", 1, "xyz"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("bar", 2, "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", 3, "xyz"), Content(ContinuousSchema[Double](), 18.84)),
    Cell(Position("bar", 4, "xyz"), Content(ContinuousSchema[Double](), (12.56) / 1)),
    Cell(Position("baz", 1, "xyz"), Content(ContinuousSchema[Double](), 9.42)),
    Cell(Position("baz", 2, "xyz"), Content(ContinuousSchema[Double](), 18.84)),
    Cell(Position("baz", 3, "xyz"), Content(ContinuousSchema[Double](), (9.42 + 18.84) / 2)),
    Cell(Position("baz", 4, "xyz"), Content(ContinuousSchema[Double](), (12.56) / 1)),
    Cell(Position("foo", 1, "xyz"), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position("foo", 2, "xyz"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("foo", 3, "xyz"), Content(ContinuousSchema[Double](), 9.42)),
    Cell(Position("foo", 4, "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("qux", 1, "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("qux", 2, "xyz"), Content(ContinuousSchema[Double](), (6.28 + 12.56 + 18.84) / 3)),
    Cell(Position("qux", 3, "xyz"), Content(ContinuousSchema[Double](), (9.42 + 18.84) / 2)),
    Cell(Position("qux", 4, "xyz"), Content(ContinuousSchema[Double](), (12.56) / 1))
  )

  val result10 = List(
    Cell(Position("bar", 1, "xyz"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("bar", 2, "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", 3, "xyz"), Content(ContinuousSchema[Double](), 18.84)),
    Cell(Position("bar", 4, "xyz"), Content(ContinuousSchema[Double](), (6.28 + 12.56 + 18.84) / 3)),
    Cell(Position("baz", 1, "xyz"), Content(ContinuousSchema[Double](), 9.42)),
    Cell(Position("baz", 2, "xyz"), Content(ContinuousSchema[Double](), 18.84)),
    Cell(Position("baz", 3, "xyz"), Content(ContinuousSchema[Double](), (9.42 + 18.84) / 2)),
    Cell(Position("baz", 4, "xyz"), Content(ContinuousSchema[Double](), (9.42 + 18.84) / 2)),
    Cell(Position("foo", 1, "xyz"), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position("foo", 2, "xyz"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("foo", 3, "xyz"), Content(ContinuousSchema[Double](), 9.42)),
    Cell(Position("foo", 4, "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("qux", 1, "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("qux", 2, "xyz"), Content(ContinuousSchema[Double](), (12.56) / 1)),
    Cell(Position("qux", 3, "xyz"), Content(ContinuousSchema[Double](), (12.56) / 1)),
    Cell(Position("qux", 4, "xyz"), Content(ContinuousSchema[Double](), (12.56) / 1))
  )

  val result11 = List(
    Cell(Position("bar", 1, "xyz"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("bar", 2, "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", 3, "xyz"), Content(ContinuousSchema[Double](), 18.84)),
    Cell(
      Position("bar", 4, "xyz"),
      Content(ContinuousSchema[Double](), (3.14 + 2 * 6.28 + 2 * 9.42 + 3 * 12.56 + 2 * 18.84) / 10)
    ),
    Cell(Position("baz", 1, "xyz"), Content(ContinuousSchema[Double](), 9.42)),
    Cell(Position("baz", 2, "xyz"), Content(ContinuousSchema[Double](), 18.84)),
    Cell(
      Position("baz", 3, "xyz"),
      Content(ContinuousSchema[Double](), (3.14 + 2 * 6.28 + 2 * 9.42 + 3 * 12.56 + 2 * 18.84) / 10)
    ),
    Cell(
      Position("baz", 4, "xyz"),
      Content(ContinuousSchema[Double](), (3.14 + 2 * 6.28 + 2 * 9.42 + 3 * 12.56 + 2 * 18.84) / 10)
    ),
    Cell(Position("foo", 1, "xyz"), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position("foo", 2, "xyz"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("foo", 3, "xyz"), Content(ContinuousSchema[Double](), 9.42)),
    Cell(Position("foo", 4, "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("qux", 1, "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(
      Position("qux", 2, "xyz"),
      Content(ContinuousSchema[Double](), (3.14 + 2 * 6.28 + 2 * 9.42 + 3 * 12.56 + 2 * 18.84) / 10)
    ),
    Cell(
      Position("qux", 3, "xyz"),
      Content(ContinuousSchema[Double](), (3.14 + 2 * 6.28 + 2 * 9.42 + 3 * 12.56 + 2 * 18.84) / 10)
    ),
    Cell(
      Position("qux", 4, "xyz"),
      Content(ContinuousSchema[Double](), (3.14 + 2 * 6.28 + 2 * 9.42 + 3 * 12.56 + 2 * 18.84) / 10)
    )
  )

  val result12 = List(
    Cell(Position("bar", 1, "xyz"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("bar", 2, "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", 3, "xyz"), Content(ContinuousSchema[Double](), 18.84)),
    Cell(Position("baz", 1, "xyz"), Content(ContinuousSchema[Double](), 9.42)),
    Cell(Position("baz", 2, "xyz"), Content(ContinuousSchema[Double](), 18.84)),
    Cell(Position("foo", 1, "xyz"), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position("foo", 2, "xyz"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("foo", 3, "xyz"), Content(ContinuousSchema[Double](), 9.42)),
    Cell(Position("foo", 4, "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("qux", 1, "xyz"), Content(ContinuousSchema[Double](), 12.56))
  )
}

class TestScaldingMatrixFill extends TestMatrixFill {

  "A Matrix.fill" should "return its filled data in 1D" in {
    toPipe(num1)
      .fillHomogeneous(Content(ContinuousSchema[Double](), 0.0), Default())
      .toList.sortBy(_.position) shouldBe result0
  }

  it should "return its filled data in 2D" in {
    toPipe(num2)
      .fillHomogeneous(Content(ContinuousSchema[Double](), 0.0), Binary(InMemory(12), Default(12)))
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its filled data in 3D" in {
    toPipe(num3)
      .fillHomogeneous(Content(ContinuousSchema[Double](), 0.0), Binary(Default(12), Default(12)))
      .toList.sortBy(_.position) shouldBe result2
  }

  "A Matrix.fill" should "return its first over filled data in 1D" in {
    val cells = toPipe(num1)

    cells
      .fillHeterogeneous(Over(_1), Default())(cells.summarise(Over(_1))(Mean()))
      .toList.sortBy(_.position) shouldBe result0
  }

  it should "return its first along filled data in 1D" in {
    val cells = toPipe(num1)

    cells
      .fillHeterogeneous(Along(_1), Ternary(InMemory(), InMemory(), Default()))(cells.summarise(Along(_1))(Mean()))
      .toList.sortBy(_.position) shouldBe result0
  }

  it should "return its first over filled data in 2D" in {
    val cells = toPipe(num2)

    cells
      .fillHeterogeneous(Over(_1), Ternary(InMemory(), Default(), Default(12)))(cells.summarise(Over(_1))(Mean()))
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its first along filled data in 2D" in {
    val cells = toPipe(num2)

    cells
      .fillHeterogeneous(Along(_1), Ternary(InMemory(), InMemory(), Default(12)))(cells.summarise(Along(_1))(Mean()))
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its second over filled data in 2D" in {
    val cells = toPipe(num2)

    cells
      .fillHeterogeneous(Over(_2), Ternary(InMemory(), Default(), Default(12)))(cells.summarise(Over(_2))(Mean()))
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its second along filled data in 2D" in {
    val cells = toPipe(num2)

    cells
      .fillHeterogeneous(Along(_2), Ternary(InMemory(), Default(12), Default(12)))(cells.summarise(Along(_2))(Mean()))
      .toList.sortBy(_.position) shouldBe result6
  }

  it should "return its first over filled data in 3D" in {
    val cells = toPipe(num3)

    cells
      .fillHeterogeneous(Over(_1), Ternary(Default(), Default(), Default(12)))(cells.summarise(Over(_1))(Mean()))
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its first along filled data in 3D" in {
    val cells = toPipe(num3)

    cells
      .fillHeterogeneous(Along(_1), Ternary(Default(), Default(12), Default(12)))(cells.summarise(Along(_1))(Mean()))
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its second over filled data in 3D" in {
    val cells = toPipe(num3)

    cells
      .fillHeterogeneous(Over(_2), Ternary(Default(12), Default(12), Default(12)))(cells.summarise(Over(_2))(Mean()))
      .toList.sortBy(_.position) shouldBe result9
  }

  it should "return its second along filled data in 3D" in {
    val cells = toPipe(num3)

    cells
      .fillHeterogeneous(Along(_2), Default())(cells.summarise(Along(_2))(Mean()))
      .toList.sortBy(_.position) shouldBe result10
  }

  it should "return its third over filled data in 3D" in {
    val cells = toPipe(num3)

    cells
      .fillHeterogeneous(Over(_3), Ternary(InMemory(), InMemory(), Default()))(cells.summarise(Over(_3))(Mean()))
      .toList.sortBy(_.position) shouldBe result11
  }

  it should "return its third along filled data in 3D" in {
    val cells = toPipe(num3)

    cells
      .fillHeterogeneous(Along(_3), Ternary(InMemory(), Default(), Default()))(cells.summarise(Along(_3))(Mean()))
      .toList.sortBy(_.position) shouldBe result12
  }

  it should "return empty data - InMemory" in {
    toPipe(num3)
      .fillHeterogeneous(Along(_3), Ternary(InMemory(), InMemory(), Default()))(TypedPipe.empty)
      .toList.sortBy(_.position) shouldBe List()
  }

  it should "return empty data - Default" in {
    toPipe(num3)
      .fillHeterogeneous(Along(_3), Default())(TypedPipe.empty)
      .toList.sortBy(_.position) shouldBe List()
  }
}

class TestSparkMatrixFill extends TestMatrixFill {

  "A Matrix.fill" should "return its filled data in 1D" in {
    toRDD(num1)
      .fillHomogeneous(Content(ContinuousSchema[Double](), 0.0), Default())
      .toList.sortBy(_.position) shouldBe result0
  }

  it should "return its filled data in 2D" in {
    toRDD(num2)
      .fillHomogeneous(Content(ContinuousSchema[Double](), 0.0), Binary(InMemory(12), Default(12)))
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its filled data in 3D" in {
    toRDD(num3)
      .fillHomogeneous(Content(ContinuousSchema[Double](), 0.0), Binary(Default(12), Default(12)))
      .toList.sortBy(_.position) shouldBe result2
  }

  "A Matrix.fill" should "return its first over filled data in 1D" in {
    val cells = toRDD(num1)

    cells
      .fillHeterogeneous(Over(_1), Default())(cells.summarise(Over(_1))(Mean()))
      .toList.sortBy(_.position) shouldBe result0
  }

  it should "return its first along filled data in 1D" in {
    val cells = toRDD(num1)

    cells
      .fillHeterogeneous(Along(_1), Ternary(InMemory(), InMemory(), Default()))(cells.summarise(Along(_1))(Mean()))
      .toList.sortBy(_.position) shouldBe result0
  }

  it should "return its first over filled data in 2D" in {
    val cells = toRDD(num2)

    cells
      .fillHeterogeneous(Over(_1), Ternary(InMemory(), Default(), Default()))(cells.summarise(Over(_1))(Mean()))
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its first along filled data in 2D" in {
    val cells = toRDD(num2)

    cells
      .fillHeterogeneous(Along(_1), Ternary(InMemory(), InMemory(), Default(12)))(cells.summarise(Along(_1))(Mean()))
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its second over filled data in 2D" in {
    val cells = toRDD(num2)

    cells
      .fillHeterogeneous(Over(_2), Ternary(InMemory(), Default(), Default(12)))(cells.summarise(Over(_2))(Mean()))
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its second along filled data in 2D" in {
    val cells = toRDD(num2)

    cells
      .fillHeterogeneous(Along(_2), Ternary(InMemory(), Default(12), Default(12)))(cells.summarise(Along(_2))(Mean()))
      .toList.sortBy(_.position) shouldBe result6
  }

  it should "return its first over filled data in 3D" in {
    val cells = toRDD(num3)

    cells
      .fillHeterogeneous(Over(_1), Ternary(Default(), Default(), Default(12)))(cells.summarise(Over(_1))(Mean()))
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its first along filled data in 3D" in {
    val cells = toRDD(num3)

    cells
      .fillHeterogeneous(Along(_1), Ternary(Default(), Default(12), Default(12)))(cells.summarise(Along(_1))(Mean()))
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its second over filled data in 3D" in {
    val cells = toRDD(num3)

    cells
      .fillHeterogeneous(Over(_2), Ternary(Default(12), Default(12), Default(12)))(cells.summarise(Over(_2))(Mean()))
      .toList.sortBy(_.position) shouldBe result9
  }

  it should "return its second along filled data in 3D" in {
    val cells = toRDD(num3)

    cells
      .fillHeterogeneous(Along(_2), Default())(cells.summarise(Along(_2))(Mean()))
      .toList.sortBy(_.position) shouldBe result10
  }

  it should "return its third over filled data in 3D" in {
    val cells = toRDD(num3)

    cells
      .fillHeterogeneous(Over(_3), Ternary(InMemory(), InMemory(), Default()))(cells.summarise(Over(_3))(Mean()))
      .toList.sortBy(_.position) shouldBe result11
  }

  it should "return its third along filled data in 3D" in {
    val cells = toRDD(num3)

    cells
      .fillHeterogeneous(Along(_3), Ternary(InMemory(), Default(), Default()))(cells.summarise(Along(_3))(Mean()))
      .toList.sortBy(_.position) shouldBe result12
  }

  it should "return empty data - InMemory" in {
    toRDD(num3)
      .fillHeterogeneous(Along(_3), Ternary(InMemory(), InMemory(), Default()))(toRDD(List.empty[Cell[_2]]))
      .toList.sortBy(_.position) shouldBe List()
  }

  it should "return empty data - Default" in {
    toRDD(num3)
      .fillHeterogeneous(Along(_3), Default())(toRDD(List.empty[Cell[_2]]))
      .toList.sortBy(_.position) shouldBe List()
  }
}

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
    Cell(Position("bar.new", 3), Content(OrdinalSchema[Long](), 19)),
    Cell(Position("baz.new", 1), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz.new", 2), Content(DiscreteSchema[Long](), 19)),
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
    Cell(Position("bar", "3.new"), Content(OrdinalSchema[Long](), 19)),
    Cell(Position("baz", "1.new"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", "2.new"), Content(DiscreteSchema[Long](), 19)),
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
    Cell(Position("bar.new", 3, "xyz"), Content(OrdinalSchema[Long](), 19)),
    Cell(Position("baz.new", 1, "xyz"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz.new", 2, "xyz"), Content(DiscreteSchema[Long](), 19)),
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
    Cell(Position("bar", "3.new", "xyz"), Content(OrdinalSchema[Long](), 19)),
    Cell(Position("baz", "1.new", "xyz"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", "2.new", "xyz"), Content(DiscreteSchema[Long](), 19)),
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
    Cell(Position("bar", 3, "xyz.new"), Content(OrdinalSchema[Long](), 19)),
    Cell(Position("baz", 1, "xyz.new"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", 2, "xyz.new"), Content(DiscreteSchema[Long](), 19)),
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
    Cell(Position("bar.new", 3), Content(OrdinalSchema[Long](), 19)),
    Cell(Position("baz.new", 1), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz.new", 2), Content(DiscreteSchema[Long](), 19)),
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
    Cell(Position("bar", "3.new"), Content(OrdinalSchema[Long](), 19)),
    Cell(Position("baz", "1.new"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", "2.new"), Content(DiscreteSchema[Long](), 19)),
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
    Cell(Position("bar.new", 3, "xyz"), Content(OrdinalSchema[Long](), 19)),
    Cell(Position("baz.new", 1, "xyz"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz.new", 2, "xyz"), Content(DiscreteSchema[Long](), 19)),
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
    Cell(Position("bar", "3.new", "xyz"), Content(OrdinalSchema[Long](), 19)),
    Cell(Position("baz", "1.new", "xyz"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", "2.new", "xyz"), Content(DiscreteSchema[Long](), 19)),
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
    Cell(Position("bar", 3, "xyz.new"), Content(OrdinalSchema[Long](), 19)),
    Cell(Position("baz", 1, "xyz.new"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", 2, "xyz.new"), Content(DiscreteSchema[Long](), 19)),
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

  def renamer[D <: Nat: ToInt, P <: Nat](dim: D)(implicit ev: LTEq[D, P]) = (cell: Cell[P]) =>
    cell.position.update(dim, cell.position(dim).toShortString + ".new").toOption

  def renamerWithValue[D <: Nat: ToInt, P <: Nat](dim: D)(implicit ev: LTEq[D, P]) = (cell: Cell[P], ext: String) =>
    cell.position.update(dim, cell.position(dim).toShortString + ext).toOption
}

class TestScaldingMatrixRename extends TestMatrixRename {

  "A Matrix.relocate" should "return its first renamed data in 1D" in {
    toPipe(data1)
      .relocate(TestMatrixRename.renamer(_1))
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its first renamed data in 2D" in {
    toPipe(data2)
      .relocate(TestMatrixRename.renamer(_1))
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its second renamed data in 2D" in {
    toPipe(data2)
      .relocate(TestMatrixRename.renamer(_2))
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its first renamed data in 3D" in {
    toPipe(data3)
      .relocate(TestMatrixRename.renamer(_1))
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its second renamed data in 3D" in {
    toPipe(data3)
      .relocate(TestMatrixRename.renamer(_2))
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its third renamed data in 3D" in {
    toPipe(data3)
      .relocate(TestMatrixRename.renamer(_3))
      .toList.sortBy(_.position) shouldBe result6
  }

  "A Matrix.renameWithValue" should "return its first renamed data in 1D" in {
    toPipe(data1)
      .relocateWithValue(ValuePipe(ext), TestMatrixRename.renamerWithValue(_1))
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its first renamed data in 2D" in {
    toPipe(data2)
      .relocateWithValue(ValuePipe(ext), TestMatrixRename.renamerWithValue(_1))
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its second renamed data in 2D" in {
    toPipe(data2)
      .relocateWithValue(ValuePipe(ext), TestMatrixRename.renamerWithValue(_2))
      .toList.sortBy(_.position) shouldBe result9
  }

  it should "return its first renamed data in 3D" in {
    toPipe(data3)
      .relocateWithValue(ValuePipe(ext), TestMatrixRename.renamerWithValue(_1))
      .toList.sortBy(_.position) shouldBe result10
  }

  it should "return its second renamed data in 3D" in {
    toPipe(data3)
      .relocateWithValue(ValuePipe(ext), TestMatrixRename.renamerWithValue(_2))
      .toList.sortBy(_.position) shouldBe result11
  }

  it should "return its third renamed data in 3D" in {
    toPipe(data3)
      .relocateWithValue(ValuePipe(ext), TestMatrixRename.renamerWithValue(_3))
      .toList.sortBy(_.position) shouldBe result12
  }
}

class TestSparkMatrixRename extends TestMatrixRename {

  "A Matrix.rename" should "return its first renamed data in 1D" in {
    toRDD(data1)
      .relocate(TestMatrixRename.renamer(_1))
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its first renamed data in 2D" in {
    toRDD(data2)
      .relocate(TestMatrixRename.renamer(_1))
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its second renamed data in 2D" in {
    toRDD(data2)
      .relocate(TestMatrixRename.renamer(_2))
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its first renamed data in 3D" in {
    toRDD(data3)
      .relocate(TestMatrixRename.renamer(_1))
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its second renamed data in 3D" in {
    toRDD(data3)
      .relocate(TestMatrixRename.renamer(_2))
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its third renamed data in 3D" in {
    toRDD(data3)
      .relocate(TestMatrixRename.renamer(_3))
      .toList.sortBy(_.position) shouldBe result6
  }

  "A Matrix.renameWithValue" should "return its first renamed data in 1D" in {
    toRDD(data1)
      .relocateWithValue(ext, TestMatrixRename.renamerWithValue(_1))
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its first renamed data in 2D" in {
    toRDD(data2)
      .relocateWithValue(ext, TestMatrixRename.renamerWithValue(_1))
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its second renamed data in 2D" in {
    toRDD(data2)
      .relocateWithValue(ext, TestMatrixRename.renamerWithValue(_2))
      .toList.sortBy(_.position) shouldBe result9
  }

  it should "return its first renamed data in 3D" in {
    toRDD(data3)
      .relocateWithValue(ext, TestMatrixRename.renamerWithValue(_1))
      .toList.sortBy(_.position) shouldBe result10
  }

  it should "return its second renamed data in 3D" in {
    toRDD(data3)
      .relocateWithValue(ext, TestMatrixRename.renamerWithValue(_2))
      .toList.sortBy(_.position) shouldBe result11
  }

  it should "return its third renamed data in 3D" in {
    toRDD(data3)
      .relocateWithValue(ext, TestMatrixRename.renamerWithValue(_3))
      .toList.sortBy(_.position) shouldBe result12
  }
}

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
    Cell(Position("bar"), Content(OrdinalSchema[Long](), 19)),
    Cell(Position("baz"), Content(DiscreteSchema[Long](), 19)),
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
    Cell(Position("bar", "xyz"), Content(OrdinalSchema[Long](), 19)),
    Cell(Position("baz", "xyz"), Content(DiscreteSchema[Long](), 19)),
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
    Cell(Position("bar", 3), Content(OrdinalSchema[Long](), 19)),
    Cell(Position("baz", 1), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", 2), Content(DiscreteSchema[Long](), 19)),
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
    Cell(Position("bar"), Content(OrdinalSchema[Long](), 19)),
    Cell(Position("baz"), Content(DiscreteSchema[Long](), 19)),
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
    Cell(Position("bar", "xyz"), Content(OrdinalSchema[Long](), 19)),
    Cell(Position("baz", "xyz"), Content(DiscreteSchema[Long](), 19)),
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
    Cell(Position("bar", 3), Content(OrdinalSchema[Long](), 19)),
    Cell(Position("baz", 1), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", 2), Content(DiscreteSchema[Long](), 19)),
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

  case class PreservingMaxPositionWithValue[P <: Nat]() extends SquasherWithValue[P] {
    type V = String
    type T = squasher.T

    val squasher = PreservingMaximumPosition[P]()
    val tTag = squasher.tTag

    def prepareWithValue[D <: Nat : ToInt](cell: Cell[P], dim: D, ext: V)(implicit ev: LTEq[D, P]): Option[T] =
      squasher.prepare(cell, dim)

    def reduce(lt: T, rt: T): T = squasher.reduce(lt, rt)

    def presentWithValue(t: T, ext: V): Option[Content] = if (ext == "ext") squasher.present(t) else None
  }
}

class TestScaldingMatrixSquash extends TestMatrixSquash {

  "A Matrix.squash" should "return its first squashed data in 2D" in {
    toPipe(data2)
      .squash(_1, PreservingMaximumPosition(), Default())
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its second squashed data in 2D" in {
    toPipe(data2)
      .squash(_2, PreservingMaximumPosition(), Default(12))
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its first squashed data in 3D" in {
    toPipe(data3)
      .squash(_1, PreservingMaximumPosition(), Default())
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its second squashed data in 3D" in {
    toPipe(data3)
      .squash(_2, PreservingMaximumPosition(), Default(12))
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its third squashed data in 3D" in {
    toPipe(data3)
      .squash(_3, PreservingMaximumPosition(), Default())
      .toList.sortBy(_.position) shouldBe result5
  }

  "A Matrix.squashWithValue" should "return its first squashed data in 2D" in {
    toPipe(data2)
      .squashWithValue(_1, ValuePipe(ext), TestMatrixSquash.PreservingMaxPositionWithValue(), Default(12))
      .toList.sortBy(_.position) shouldBe result6
  }

  it should "return its second squashed data in 2D" in {
    toPipe(data2)
      .squashWithValue(_2, ValuePipe(ext), TestMatrixSquash.PreservingMaxPositionWithValue(), Default())
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its first squashed data in 3D" in {
    toPipe(data3)
      .squashWithValue(_1, ValuePipe(ext), TestMatrixSquash.PreservingMaxPositionWithValue(), Default(12))
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its second squashed data in 3D" in {
    toPipe(data3)
      .squashWithValue(_2, ValuePipe(ext), TestMatrixSquash.PreservingMaxPositionWithValue(), Default())
      .toList.sortBy(_.position) shouldBe result9
  }

  it should "return its third squashed data in 3D" in {
    toPipe(data3)
      .squashWithValue(_3, ValuePipe(ext), TestMatrixSquash.PreservingMaxPositionWithValue(), Default(12))
      .toList.sortBy(_.position) shouldBe result10
  }
}

class TestSparkMatrixSquash extends TestMatrixSquash {

  "A Matrix.squash" should "return its first squashed data in 2D" in {
    toRDD(data2)
      .squash(_1, PreservingMaximumPosition(), Default())
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its second squashed data in 2D" in {
    toRDD(data2)
      .squash(_2, PreservingMaximumPosition(), Default(12))
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its first squashed data in 3D" in {
    toRDD(data3)
      .squash(_1, PreservingMaximumPosition(), Default())
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its second squashed data in 3D" in {
    toRDD(data3)
      .squash(_2, PreservingMaximumPosition(), Default(12))
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its third squashed data in 3D" in {
    toRDD(data3)
      .squash(_3, PreservingMaximumPosition(), Default())
      .toList.sortBy(_.position) shouldBe result5
  }

  "A Matrix.squashWithValue" should "return its first squashed data in 2D" in {
    toRDD(data2)
      .squashWithValue(_1, ext, TestMatrixSquash.PreservingMaxPositionWithValue(), Default(12))
      .toList.sortBy(_.position) shouldBe result6
  }

  it should "return its second squashed data in 2D" in {
    toRDD(data2)
      .squashWithValue(_2, ext, TestMatrixSquash.PreservingMaxPositionWithValue(), Default())
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its first squashed data in 3D" in {
    toRDD(data3)
      .squashWithValue(_1, ext, TestMatrixSquash.PreservingMaxPositionWithValue(), Default(12))
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its second squashed data in 3D" in {
    toRDD(data3)
      .squashWithValue(_2, ext, TestMatrixSquash.PreservingMaxPositionWithValue(), Default())
      .toList.sortBy(_.position) shouldBe result9
  }

  it should "return its third squashed data in 3D" in {
    toRDD(data3)
      .squashWithValue(_3, ext, TestMatrixSquash.PreservingMaxPositionWithValue(), Default(12))
      .toList.sortBy(_.position) shouldBe result10
  }
}

trait TestMatrixMelt extends TestMatrix {

  val result1 = List(
    Cell(Position("1.bar"), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("1.baz"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("1.foo"), Content(OrdinalSchema[String](), "3.14")),
    Cell(Position("1.qux"), Content(OrdinalSchema[String](), "12.56")),
    Cell(Position("2.bar"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("2.baz"), Content(DiscreteSchema[Long](), 19)),
    Cell(Position("2.foo"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("3.bar"), Content(OrdinalSchema[Long](), 19)),
    Cell(Position("3.foo"), Content(NominalSchema[String](), "9.42")),
    Cell(
      Position("4.foo"),
      Content(
        DateSchema[java.util.Date](),
        DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
      )
    )
  )

  val result2 = List(
    Cell(Position("bar.1"), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("bar.2"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar.3"), Content(OrdinalSchema[Long](), 19)),
    Cell(Position("baz.1"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz.2"), Content(DiscreteSchema[Long](), 19)),
    Cell(Position("foo.1"), Content(OrdinalSchema[String](), "3.14")),
    Cell(Position("foo.2"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("foo.3"), Content(NominalSchema[String](), "9.42")),
    Cell(
      Position("foo.4"),
      Content(
        DateSchema[java.util.Date](),
        DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
      )
    ),
    Cell(Position("qux.1"), Content(OrdinalSchema[String](), "12.56"))
  )

  val result3 = List(
    Cell(Position(1, "xyz.bar"), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position(1, "xyz.baz"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position(1, "xyz.foo"), Content(OrdinalSchema[String](), "3.14")),
    Cell(Position(1, "xyz.qux"), Content(OrdinalSchema[String](), "12.56")),
    Cell(Position(2, "xyz.bar"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position(2, "xyz.baz"), Content(DiscreteSchema[Long](), 19)),
    Cell(Position(2, "xyz.foo"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position(3, "xyz.bar"), Content(OrdinalSchema[Long](), 19)),
    Cell(Position(3, "xyz.foo"), Content(NominalSchema[String](), "9.42")),
    Cell(
      Position(4, "xyz.foo"),
      Content(
        DateSchema[java.util.Date](),
        DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
      )
    )
  )

  val result4 = List(
    Cell(Position("bar", "xyz.1"), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("bar", "xyz.2"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar", "xyz.3"), Content(OrdinalSchema[Long](), 19)),
    Cell(Position("baz", "xyz.1"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", "xyz.2"), Content(DiscreteSchema[Long](), 19)),
    Cell(Position("foo", "xyz.1"), Content(OrdinalSchema[String](), "3.14")),
    Cell(Position("foo", "xyz.2"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("foo", "xyz.3"), Content(NominalSchema[String](), "9.42")),
    Cell(
      Position("foo", "xyz.4"),
      Content(
        DateSchema[java.util.Date](),
        DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
      )
    ),
    Cell(Position("qux", "xyz.1"), Content(OrdinalSchema[String](), "12.56"))
  )

  val result5 = List(
    Cell(Position("bar.xyz", 1), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("bar.xyz", 2), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("bar.xyz", 3), Content(OrdinalSchema[Long](), 19)),
    Cell(Position("baz.xyz", 1), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz.xyz", 2), Content(DiscreteSchema[Long](), 19)),
    Cell(Position("foo.xyz", 1), Content(OrdinalSchema[String](), "3.14")),
    Cell(Position("foo.xyz", 2), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("foo.xyz", 3), Content(NominalSchema[String](), "9.42")),
    Cell(
      Position("foo.xyz", 4),
      Content(
        DateSchema[java.util.Date](),
        DateValue((new java.text.SimpleDateFormat("yyyy-MM-dd hh:mm:ss")).parse("2000-01-01 12:56:00"))
      )
    ),
    Cell(Position("qux.xyz", 1), Content(OrdinalSchema[String](), "12.56"))
  )
}

object TestMatrixMelt {
  def merge(i: Value, d: Value) = Value.concatenate(".")(i, d)
}

class TestScaldingMatrixMelt extends TestMatrixMelt {

  "A Matrix.melt" should "return its first melted data in 2D" in {
    toPipe(data2)
      .melt(_1, _2, TestMatrixMelt.merge)
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its second melted data in 2D" in {
    toPipe(data2)
      .melt(_2, _1, TestMatrixMelt.merge)
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its first melted data in 3D" in {
    toPipe(data3)
      .melt(_1, _3, TestMatrixMelt.merge)
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its second melted data in 3D" in {
    toPipe(data3)
      .melt(_2, _3, TestMatrixMelt.merge)
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its third melted data in 3D" in {
    toPipe(data3)
      .melt(_3, _1, TestMatrixMelt.merge)
      .toList.sortBy(_.position) shouldBe result5
  }
}

class TestSparkMatrixMelt extends TestMatrixMelt {

  "A Matrix.melt" should "return its first melted data in 2D" in {
    toRDD(data2)
      .melt(_1, _2, TestMatrixMelt.merge)
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its second melted data in 2D" in {
    toRDD(data2)
      .melt(_2, _1, TestMatrixMelt.merge)
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its first melted data in 3D" in {
    toRDD(data3)
      .melt(_1, _3, TestMatrixMelt.merge)
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its second melted data in 3D" in {
    toRDD(data3)
      .melt(_2, _3, TestMatrixMelt.merge)
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its third melted data in 3D" in {
    toRDD(data3)
      .melt(_3, _1, TestMatrixMelt.merge)
      .toList.sortBy(_.position) shouldBe result5
  }
}

trait TestMatrixExpand extends TestMatrix {

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
    Cell(Position("bar", 3, "abc"), Content(OrdinalSchema[Long](), 19)),
    Cell(Position("baz", 1, "abc"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", 2, "abc"), Content(DiscreteSchema[Long](), 19)),
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
    Cell(Position("bar", 3, "abc", "def"), Content(OrdinalSchema[Long](), 19)),
    Cell(Position("baz", 1, "abc", "def"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", 2, "abc", "def"), Content(DiscreteSchema[Long](), 19)),
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
    Cell(Position("bar", 3, "abc", "def", "ghi"), Content(OrdinalSchema[Long](), 19)),
    Cell(Position("baz", 1, "abc", "def", "ghi"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", 2, "abc", "def", "ghi"), Content(DiscreteSchema[Long](), 19)),
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
    Cell(Position("bar", 3, "xyz", "abc"), Content(OrdinalSchema[Long](), 19)),
    Cell(Position("baz", 1, "xyz", "abc"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", 2, "xyz", "abc"), Content(DiscreteSchema[Long](), 19)),
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
    Cell(Position("bar", 3, "xyz", "abc", "def"), Content(OrdinalSchema[Long](), 19)),
    Cell(Position("baz", 1, "xyz", "abc", "def"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", 2, "xyz", "abc", "def"), Content(DiscreteSchema[Long](), 19)),
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
    Cell(Position("bar", 3, "abc"), Content(OrdinalSchema[Long](), 19)),
    Cell(Position("baz", 1, "abc"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", 2, "abc"), Content(DiscreteSchema[Long](), 19)),
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
    Cell(Position("bar", 3, "abc", "def"), Content(OrdinalSchema[Long](), 19)),
    Cell(Position("baz", 1, "abc", "def"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", 2, "abc", "def"), Content(DiscreteSchema[Long](), 19)),
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
    Cell(Position("bar", 3, "abc", "def", "ghi"), Content(OrdinalSchema[Long](), 19)),
    Cell(Position("baz", 1, "abc", "def", "ghi"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", 2, "abc", "def", "ghi"), Content(DiscreteSchema[Long](), 19)),
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
    Cell(Position("bar", 3, "xyz", "abc"), Content(OrdinalSchema[Long](), 19)),
    Cell(Position("baz", 1, "xyz", "abc"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", 2, "xyz", "abc"), Content(DiscreteSchema[Long](), 19)),
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
    Cell(Position("bar", 3, "xyz", "abc", "def"), Content(OrdinalSchema[Long](), 19)),
    Cell(Position("baz", 1, "xyz", "abc", "def"), Content(OrdinalSchema[String](), "9.42")),
    Cell(Position("baz", 2, "xyz", "abc", "def"), Content(DiscreteSchema[Long](), 19)),
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

object TestMatrixExpand {

  def expand1D[P <: Nat](cell: Cell[P]): Option[Position[Succ[P]]] = cell.position.append("abc").toOption
  def expand2D[P <: Nat](cell: Cell[P]): Option[Position[Succ[Succ[P]]]] = cell
    .position
    .append("abc")
    .append("def")
    .toOption
  def expand3D[P <: Nat](cell: Cell[P]): Option[Position[Succ[Succ[Succ[P]]]]] = cell
    .position
    .append("abc")
    .append("def")
    .append("ghi")
    .toOption
  def expand4D[P <: Nat](cell: Cell[P]): Option[Position[Succ[Succ[Succ[Succ[P]]]]]] = cell
    .position
    .append("abc")
    .append("def")
    .append("ghi")
    .append("jkl")
    .toOption

  def expand1DWithValue[P <: Nat](cell: Cell[P], ext: String): Option[Position[Succ[P]]] = cell
    .position
    .append(ext)
    .toOption
  def expand2DWithValue[P <: Nat](cell: Cell[P], ext: String): Option[Position[Succ[Succ[P]]]] = cell
    .position
    .append(ext)
    .append("def")
    .toOption
  def expand3DWithValue[P <: Nat](cell: Cell[P], ext: String): Option[Position[Succ[Succ[Succ[P]]]]] = cell
    .position
    .append(ext)
    .append("def")
    .append("ghi")
    .toOption
  def expand4DWithValue[P <: Nat](cell: Cell[P], ext: String): Option[Position[Succ[Succ[Succ[Succ[P]]]]]] = cell
    .position
    .append(ext)
    .append("def")
    .append("ghi")
    .append("jkl")
    .toOption
}

class TestScaldingMatrixExpand extends TestMatrixExpand {

  "A Matrix.expand" should "return its 1D expanded data in 1D" in {
    toPipe(data1)
      .relocate(TestMatrixExpand.expand1D)
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its 2D expanded data in 1D" in {
    toPipe(data1)
      .relocate(TestMatrixExpand.expand2D)
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its 3D expanded data in 1D" in {
    toPipe(data1)
      .relocate(TestMatrixExpand.expand3D)
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its 4D expanded data in 1D" in {
    toPipe(data1)
      .relocate(TestMatrixExpand.expand4D)
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its 1D expanded data in 2D" in {
    toPipe(data2)
      .relocate(TestMatrixExpand.expand1D)
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its 2D expanded data in 2D" in {
    toPipe(data2)
      .relocate(TestMatrixExpand.expand2D)
      .toList.sortBy(_.position) shouldBe result6
  }

  it should "return its 3D expanded data in 2D" in {
    toPipe(data2)
      .relocate(TestMatrixExpand.expand3D)
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its 1D expanded data in 3D" in {
    toPipe(data3)
      .relocate(TestMatrixExpand.expand1D)
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its 2D expanded data in 3D" in {
    toPipe(data3)
      .relocate(TestMatrixExpand.expand2D)
      .toList.sortBy(_.position) shouldBe result9
  }

  "A Matrix.expandWithValue" should "return its 1D expanded data in 1D" in {
    toPipe(data1)
      .relocateWithValue(ValuePipe(ext), TestMatrixExpand.expand1DWithValue)
      .toList.sortBy(_.position) shouldBe result10
  }

  it should "return its 2D expanded data in 1D" in {
    toPipe(data1)
      .relocateWithValue(ValuePipe(ext), TestMatrixExpand.expand2DWithValue)
      .toList.sortBy(_.position) shouldBe result11
  }

  it should "return its 3D expanded data in 1D" in {
    toPipe(data1)
      .relocateWithValue(ValuePipe(ext), TestMatrixExpand.expand3DWithValue)
      .toList.sortBy(_.position) shouldBe result12
  }

  it should "return its 4D expanded data in 1D" in {
    toPipe(data1)
      .relocateWithValue(ValuePipe(ext), TestMatrixExpand.expand4DWithValue)
      .toList.sortBy(_.position) shouldBe result13
  }

  it should "return its 1D expanded data in 2D" in {
    toPipe(data2)
      .relocateWithValue(ValuePipe(ext), TestMatrixExpand.expand1DWithValue)
      .toList.sortBy(_.position) shouldBe result14
  }

  it should "return its 2D expanded data in 2D" in {
    toPipe(data2)
      .relocateWithValue(ValuePipe(ext), TestMatrixExpand.expand2DWithValue)
      .toList.sortBy(_.position) shouldBe result15
  }

  it should "return its 3D expanded data in 2D" in {
    toPipe(data2)
      .relocateWithValue(ValuePipe(ext), TestMatrixExpand.expand3DWithValue)
      .toList.sortBy(_.position) shouldBe result16
  }

  it should "return its 1D expanded data in 3D" in {
    toPipe(data3)
      .relocateWithValue(ValuePipe(ext), TestMatrixExpand.expand1DWithValue)
      .toList.sortBy(_.position) shouldBe result17
  }

  it should "return its 2D expanded data in 3D" in {
    toPipe(data3)
      .relocateWithValue(ValuePipe(ext), TestMatrixExpand.expand2DWithValue)
      .toList.sortBy(_.position) shouldBe result18
  }
}

class TestSparkMatrixExpand extends TestMatrixExpand {

  "A Matrix.expand" should "return its 1D expanded data in 1D" in {
    toRDD(data1)
      .relocate(TestMatrixExpand.expand1D)
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its 2D expanded data in 1D" in {
    toRDD(data1)
      .relocate(TestMatrixExpand.expand2D)
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its 3D expanded data in 1D" in {
    toRDD(data1)
      .relocate(TestMatrixExpand.expand3D)
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its 4D expanded data in 1D" in {
    toRDD(data1)
      .relocate(TestMatrixExpand.expand4D)
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its expanded 1D data in 2D" in {
    toRDD(data2)
      .relocate(TestMatrixExpand.expand1D)
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its expanded 2D data in 2D" in {
    toRDD(data2)
      .relocate(TestMatrixExpand.expand2D)
      .toList.sortBy(_.position) shouldBe result6
  }

  it should "return its expanded 3D data in 2D" in {
    toRDD(data2)
      .relocate(TestMatrixExpand.expand3D)
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its expanded 1D data in 3D" in {
    toRDD(data3)
      .relocate(TestMatrixExpand.expand1D)
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its expanded 2D data in 3D" in {
    toRDD(data3)
      .relocate(TestMatrixExpand.expand2D)
      .toList.sortBy(_.position) shouldBe result9
  }

  "A Matrix.expandWithValue" should "return its 1D expanded data in 1D" in {
    toRDD(data1)
      .relocateWithValue(ext, TestMatrixExpand.expand1DWithValue)
      .toList.sortBy(_.position) shouldBe result10
  }

  it should "return its 2D expanded data in 1D" in {
    toRDD(data1)
      .relocateWithValue(ext, TestMatrixExpand.expand2DWithValue)
      .toList.sortBy(_.position) shouldBe result11
  }

  it should "return its 3D expanded data in 1D" in {
    toRDD(data1)
      .relocateWithValue(ext, TestMatrixExpand.expand3DWithValue)
      .toList.sortBy(_.position) shouldBe result12
  }

  it should "return its 4D expanded data in 1D" in {
    toRDD(data1)
      .relocateWithValue(ext, TestMatrixExpand.expand4DWithValue)
      .toList.sortBy(_.position) shouldBe result13
  }

  it should "return its 1D expanded data in 2D" in {
    toRDD(data2)
      .relocateWithValue(ext, TestMatrixExpand.expand1DWithValue)
      .toList.sortBy(_.position) shouldBe result14
  }

  it should "return its 2D expanded data in 2D" in {
    toRDD(data2)
      .relocateWithValue(ext, TestMatrixExpand.expand2DWithValue)
      .toList.sortBy(_.position) shouldBe result15
  }

  it should "return its 3D expanded data in 2D" in {
    toRDD(data2)
      .relocateWithValue(ext, TestMatrixExpand.expand3DWithValue)
      .toList.sortBy(_.position) shouldBe result16
  }

  it should "return its 1D expanded data in 3D" in {
    toRDD(data3)
      .relocateWithValue(ext, TestMatrixExpand.expand1DWithValue)
      .toList.sortBy(_.position) shouldBe result17
  }

  it should "return its 2D expanded data in 3D" in {
    toRDD(data3)
      .relocateWithValue(ext, TestMatrixExpand.expand2DWithValue)
      .toList.sortBy(_.position) shouldBe result18
  }
}

trait TestMatrixPermute extends TestMatrix {

  val dataA = List(
    Cell(Position(1, 3), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position(2, 2), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position(3, 1), Content(ContinuousSchema[Double](), 9.42))
  )

  val dataB = List(
    Cell(Position(1, 2, 3), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position(2, 2, 2), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position(3, 2, 1), Content(ContinuousSchema[Double](), 9.42))
  )

  val dataC = List(
    Cell(Position(1, 2, 3, 4), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position(2, 2, 2, 2), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position(1, 1, 4, 4), Content(ContinuousSchema[Double](), 9.42)),
    Cell(Position(4, 1, 3, 2), Content(ContinuousSchema[Double](), 12.56))
  )

  val dataD = List(
    Cell(Position(1, 2, 3, 4, 5), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position(2, 2, 2, 2, 2), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position(1, 1, 3, 5, 5), Content(ContinuousSchema[Double](), 9.42)),
    Cell(Position(4, 4, 4, 1, 1), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position(5, 4, 3, 2, 1), Content(ContinuousSchema[Double](), 18.84))
  )

  val result1 = List(
    Cell(Position(1, 3), Content(ContinuousSchema[Double](), 9.42)),
    Cell(Position(2, 2), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position(3, 1), Content(ContinuousSchema[Double](), 3.14))
  )

  val result2 = List(
    Cell(Position(2, 1, 3), Content(ContinuousSchema[Double](), 9.42)),
    Cell(Position(2, 2, 2), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position(2, 3, 1), Content(ContinuousSchema[Double](), 3.14))
  )

  val result3 = List(
    Cell(Position(2, 2, 2, 2), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position(2, 3, 4, 1), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position(4, 3, 1, 2), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position(4, 4, 1, 1), Content(ContinuousSchema[Double](), 9.42))
  )

  val result4 = List(
    Cell(Position(1, 4, 4, 1, 4), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position(2, 2, 2, 2, 2), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position(2, 4, 5, 1, 3), Content(ContinuousSchema[Double](), 18.84)),
    Cell(Position(4, 2, 1, 5, 3), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position(5, 1, 1, 5, 3), Content(ContinuousSchema[Double](), 9.42))
  )
}

class TestScaldingMatrixPermute extends TestMatrixPermute {

  "A Matrix.permute" should "return its permutation in 2D" in {
    toPipe(dataA)
      .permute(_2, _1)
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its permutation in 3D" in {
    toPipe(dataB)
      .permute(_2, _3, _1)
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its permutation in 4D" in {
    toPipe(dataC)
      .permute(_4, _3, _1, _2)
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its permutation in 5D" in {
    toPipe(dataD)
      .permute(_4, _2, _1, _5, _3)
      .toList.sortBy(_.position) shouldBe result4
  }
}

class TestSparkMatrixPermute extends TestMatrixPermute {

  "A Matrix.permute" should "return its permutation in 2D" in {
    toRDD(dataA)
      .permute(_2, _1)
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its permutation in 3D" in {
    toRDD(dataB)
      .permute(_2, _3, _1)
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its permutation in 4D" in {
    toRDD(dataC)
      .permute(_4, _3, _1, _2)
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its permutation in 5D" in {
    toRDD(dataD)
      .permute(_4, _2, _1, _5, _3)
      .toList.sortBy(_.position) shouldBe result4
  }
}

trait TestMatrixToVector extends TestMatrix {

  val result1 = data2.map { case Cell(Position(f, s), c) =>
      Cell(Position(f.toShortString + TestMatrixToVector.separator + s.toShortString), c)
    }
    .sortBy(_.position)

  val result2 = data3.map { case Cell(Position(f, s, t), c) =>
     Cell(
       Position(
         f.toShortString + TestMatrixToVector.separator +
         s.toShortString + TestMatrixToVector.separator +
         t.toShortString
       ),
       c
     )
    }
    .sortBy(_.position)
}

object TestMatrixToVector {

  val separator = ":"

  def melt(coords: List[Value]): Value = coords.map(_.toShortString).mkString(separator)
}

class TestScaldingMatrixToVector extends TestMatrixToVector {

  "A Matrix.toVector" should "return its vector for 2D" in {
    toPipe(data2)
      .toVector(TestMatrixToVector.melt)
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its permutation vector for 3D" in {
    toPipe(data3)
      .toVector(TestMatrixToVector.melt)
      .toList.sortBy(_.position) shouldBe result2
  }
}

class TestSparkMatrixToVector extends TestMatrixToVector {

  "A Matrix.toVector" should "return its vector for 2D" in {
    toRDD(data2)
      .toVector(TestMatrixToVector.melt)
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its permutation vector for 3D" in {
    toRDD(data3)
      .toVector(TestMatrixToVector.melt)
      .toList.sortBy(_.position) shouldBe result2
  }
}

trait TestMatrixMaterialise extends TestMatrix {

  val data = List(
    ("a", "one", Content(ContinuousSchema[Double](), 3.14)),
    ("a", "two", Content(NominalSchema[String](), "foo")),
    ("a", "three", Content(DiscreteSchema[Long](), 42)),
    ("b", "one", Content(ContinuousSchema[Double](), 6.28)),
    ("b", "two", Content(DiscreteSchema[Long](), 123)),
    ("b", "three", Content(ContinuousSchema[Double](), 9.42)),
    ("c", "two", Content(NominalSchema[String](), "bar")),
    ("c", "three", Content(ContinuousSchema[Double](), 12.56))
  )

  val result = List(
    Cell(Position("a", "one"), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position("a", "two"), Content(NominalSchema[String](), "foo")),
    Cell(Position("a", "three"), Content(DiscreteSchema[Long](), 42)),
    Cell(Position("b", "one"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("b", "two"), Content(DiscreteSchema[Long](), 123)),
    Cell(Position("b", "three"), Content(ContinuousSchema[Double](), 9.42)),
    Cell(Position("c", "two"), Content(NominalSchema[String](), "bar")),
    Cell(Position("c", "three"), Content(ContinuousSchema[Double](), 12.56))
  )
}

class TestScaldingMatrixMaterialise extends TestMatrixMaterialise {

  "A Matrix.materialise" should "return its list" in {
    tupleToPipeMatrix2(data)
      .materialise(Default())
      .sortBy(_.position) shouldBe result.sortBy(_.position)
  }
}

class TestSparkMatrixMaterialise extends TestMatrixMaterialise {

  "A Matrix.materialise" should "return its list" in {
    tupleToRDDMatrix2(data)
      .materialise(Default())
      .sortBy(_.position) shouldBe result.sortBy(_.position)
  }
}

trait TestMatrixToText extends TestMatrix {

  val result1 = data1.map(_.toString()).sorted

  val result2 = data2.map(_.toShortString("|", false)).sorted

  val result3 = data3.map(_.toShortString("/", true)).sorted
}

class TestScaldingMatrixToText extends TestMatrixToText {

  "A Matrix.toText" should "return its strings for 1D" in {
    toPipe(data1)
      .toText(Cell.toString(true))
      .toList.sorted shouldBe result1
  }

  "A Matrix.toText" should "return its strings for 2D" in {
    toPipe(data2)
      .toText(Cell.toString(false, "|", false))
      .toList.sorted shouldBe result2
  }

  "A Matrix.toText" should "return its strings for 3D" in {
    toPipe(data3)
      .toText(Cell.toString(false, "/", true))
      .toList.sorted shouldBe result3
  }
}

class TestSparkMatrixToText extends TestMatrixToText {

  "A Matrix.toText" should "return its strings for 1D" in {
    toRDD(data1)
      .toText(Cell.toString(true))
      .toList.sorted shouldBe result1
  }

  "A Matrix.toText" should "return its strings for 2D" in {
    toRDD(data2)
      .toText(Cell.toString(false, "|", false))
      .toList.sorted shouldBe result2
  }

  "A Matrix.toText" should "return its strings for 3D" in {
    toRDD(data3)
      .toText(Cell.toString(false, "/", true))
      .toList.sorted shouldBe result3
  }
}

trait TestMatrixReshape extends TestMatrix {

  val dataA = List(
    Cell(Position("foo", "letter"), Content(NominalSchema[String](), "a")),
    Cell(Position("foo", "number"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position("bar", "letter"), Content(NominalSchema[String](), "b")),
    Cell(Position("bar", "number"), Content(DiscreteSchema[Long](), 2)),
    Cell(Position("baz", "letter"), Content(NominalSchema[String](), "a")),
    Cell(Position("qux", "number"), Content(DiscreteSchema[Long](), 2))
  )

  val dataB = List(
    Cell(Position("foo", "letter", true), Content(NominalSchema[String](), "a")),
    Cell(Position("foo", "number", true), Content(DiscreteSchema[Long](), 1)),
    Cell(Position("bar", "letter", true), Content(NominalSchema[String](), "b")),
    Cell(Position("bar", "number", true), Content(DiscreteSchema[Long](), 2)),
    Cell(Position("baz", "letter", true), Content(NominalSchema[String](), "a")),
    Cell(Position("qux", "number", true), Content(DiscreteSchema[Long](), 2)),
    Cell(Position("foo", "number", false), Content(DiscreteSchema[Long](), 3)),
    Cell(Position("bar", "letter", false), Content(NominalSchema[String](), "c")),
    Cell(Position("baz", "number", false), Content(DiscreteSchema[Long](), 4)),
    Cell(Position("qux", "letter", false), Content(NominalSchema[String](), "d"))
  )

  val result1 = List(
    Cell(Position("bar", "letter", "a"), Content(NominalSchema[String](), "b")),
    Cell(Position("bar", "number", "NA"), Content(DiscreteSchema[Long](), 2)),
    Cell(Position("foo", "letter", "a"), Content(NominalSchema[String](), "a")),
    Cell(Position("foo", "number", "NA"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position("qux", "number", "NA"), Content(DiscreteSchema[Long](), 2))
  )

  val result2 = List(
    Cell(Position("bar", "number", "b"), Content(DiscreteSchema[Long](), 2)),
    Cell(Position("foo", "number", "a"), Content(DiscreteSchema[Long](), 1)),
    Cell(Position("qux", "number", "NA"), Content(DiscreteSchema[Long](), 2))
  )

  val result3 = List(
    Cell(Position("bar", "letter", false, "d"), Content(NominalSchema[String](), "c")),
    Cell(Position("bar", "letter", true, "NA"), Content(NominalSchema[String](), "b")),
    Cell(Position("bar", "number", true, "2"), Content(DiscreteSchema[Long](), 2)),
    Cell(Position("baz", "letter", true, "NA"), Content(NominalSchema[String](), "a")),
    Cell(Position("baz", "number", false, "NA"), Content(DiscreteSchema[Long](), 4)),
    Cell(Position("foo", "letter", true, "NA"), Content(NominalSchema[String](), "a")),
    Cell(Position("foo", "number", false, "NA"), Content(DiscreteSchema[Long](), 3)),
    Cell(Position("foo", "number", true, "2"), Content(DiscreteSchema[Long](), 1))
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
    Cell(Position("baz", "number", false, "NA"), Content(DiscreteSchema[Long](), 4)),
    Cell(Position("foo", "number", false, "1"), Content(DiscreteSchema[Long](), 3)),
    Cell(Position("qux", "letter", false, "NA"), Content(NominalSchema[String](), "d"))
  )
}

object TestMatrixReshape {
  def cast[P <: Nat](cell: Cell[P], value: Option[Value]): Option[Position[Succ[P]]] = cell
    .position
    .append(value.map(_.toShortString).getOrElse("NA").toString)
    .toOption
}

class TestScaldingMatrixReshape extends TestMatrixReshape {

  "A Matrix.reshape" should "reshape its first dimension in 2D" in {
    toPipe(dataA)
      .reshape(_1, "baz", TestMatrixReshape.cast, InMemory())
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "reshape its second dimension in 2D" in {
    toPipe(dataA)
      .reshape(_2, "letter", TestMatrixReshape.cast, Default())
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "reshape its first dimension in 3D" in {
    toPipe(dataB)
      .reshape(_1, "qux", TestMatrixReshape.cast, Default(12))
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "reshape its second dimension in 3D" in {
    toPipe(dataB)
      .reshape(_2, "number", TestMatrixReshape.cast, Unbalanced(12))
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "reshape its third dimension in 3D" in {
    toPipe(dataB)
      .reshape(_3, true, TestMatrixReshape.cast, InMemory())
      .toList.sortBy(_.position) shouldBe result5
  }
}

class TestSparkMatrixReshape extends TestMatrixReshape {

  "A Matrix.reshape" should "reshape its first dimension in 2D" in {
    toRDD(dataA)
      .reshape(_1, "baz", TestMatrixReshape.cast, InMemory())
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "reshape its second dimension in 2D" in {
    toRDD(dataA)
      .reshape(_2, "letter", TestMatrixReshape.cast, Default())
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "reshape its first dimension in 3D" in {
    toRDD(dataB)
      .reshape(_1, "qux", TestMatrixReshape.cast, Default(12))
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "reshape its second dimension in 3D" in {
    toRDD(dataB)
      .reshape(_2, "number", TestMatrixReshape.cast, InMemory())
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "reshape its third dimension in 3D" in {
    toRDD(dataB)
      .reshape(_3, true, TestMatrixReshape.cast, Default())
      .toList.sortBy(_.position) shouldBe result5
  }
}

