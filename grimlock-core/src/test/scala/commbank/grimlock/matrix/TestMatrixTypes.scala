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
import commbank.grimlock.framework.environment.implicits._
import commbank.grimlock.framework.environment.tuner._
import commbank.grimlock.framework.metadata._
import commbank.grimlock.framework.position._

import shapeless.nat.{ _0, _1, _2 }

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

class TestScalaMatrixTypes extends TestMatrixTypes with TestScala {
  import commbank.grimlock.scala.environment.implicits._

  "A Matrix.types" should "return its first over types in 1D" in {
    toU(data1)
      .types(Over(_0), Default())(false)
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its first over specific types in 1D" in {
    toU(data1)
      .types(Over(_0), Default())(true)
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its first over types in 2D" in {
    toU(data2)
      .types(Over(_0), Default())(false)
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its first over specific types in 2D" in {
    toU(data2)
      .types(Over(_0), Default())(true)
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its first along types in 2D" in {
    toU(data2)
      .types(Along(_0), Default())(false)
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its first along specific types in 2D" in {
    toU(data2)
      .types(Along(_0), Default())(true)
      .toList.sortBy(_.position) shouldBe result6
  }

  it should "return its second over types in 2D" in {
    toU(data2)
      .types(Over(_1), Default())(false)
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its second over specific types in 2D" in {
    toU(data2)
      .types(Over(_1), Default())(true)
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its second along types in 2D" in {
    toU(data2)
      .types(Along(_1), Default())(false)
      .toList.sortBy(_.position) shouldBe result9
  }

  it should "return its second along specific types in 2D" in {
    toU(data2)
      .types(Along(_1), Default())(true)
      .toList.sortBy(_.position) shouldBe result10
  }

  it should "return its first over types in 3D" in {
    toU(data3)
      .types(Over(_0), Default())(false)
      .toList.sortBy(_.position) shouldBe result11
  }

  it should "return its first over specific types in 3D" in {
    toU(data3)
      .types(Over(_0), Default())(true)
      .toList.sortBy(_.position) shouldBe result12
  }

  it should "return its first along types in 3D" in {
    toU(data3)
      .types(Along(_0), Default())(false)
      .toList.sortBy(_.position) shouldBe result13
  }

  it should "return its first along specific types in 3D" in {
    toU(data3)
      .types(Along(_0), Default())(true)
      .toList.sortBy(_.position) shouldBe result14
  }

  it should "return its second over types in 3D" in {
    toU(data3)
      .types(Over(_1), Default())(false)
      .toList.sortBy(_.position) shouldBe result15
  }

  it should "return its second over specific types in 3D" in {
    toU(data3)
      .types(Over(_1), Default())(true)
      .toList.sortBy(_.position) shouldBe result16
  }

  it should "return its second along types in 3D" in {
    toU(data3)
      .types(Along(_1), Default())(false)
      .toList.sortBy(_.position) shouldBe result17
  }

  it should "return its second along specific types in 3D" in {
    toU(data3)
      .types(Along(_1), Default())(true)
      .toList.sortBy(_.position) shouldBe result18
  }

  it should "return its third over types in 3D" in {
    toU(data3)
      .types(Over(_2), Default())(false)
      .toList.sortBy(_.position) shouldBe result19
  }

  it should "return its third over specific types in 3D" in {
    toU(data3)
      .types(Over(_2), Default())(true)
      .toList.sortBy(_.position) shouldBe result20
  }

  it should "return its third along types in 3D" in {
    toU(data3)
      .types(Along(_2), Default())(false)
      .toList.sortBy(_.position) shouldBe result21
  }

  it should "return its third along specific types in 3D" in {
    toU(data3)
      .types(Along(_2), Default())(true)
      .toList.sortBy(_.position) shouldBe result22
  }
}

class TestScaldingMatrixTypes extends TestMatrixTypes with TestScalding {
  import commbank.grimlock.scalding.environment.implicits._

  "A Matrix.types" should "return its first over types in 1D" in {
    toU(data1)
      .types(Over(_0), Default())(false)
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its first over specific types in 1D" in {
    toU(data1)
      .types(Over(_0), Default(12))(true)
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its first over types in 2D" in {
    toU(data2)
      .types(Over(_0), Default())(false)
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its first over specific types in 2D" in {
    toU(data2)
      .types(Over(_0), Default(12))(true)
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its first along types in 2D" in {
    toU(data2)
      .types(Along(_0), Default())(false)
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its first along specific types in 2D" in {
    toU(data2)
      .types(Along(_0), Default(12))(true)
      .toList.sortBy(_.position) shouldBe result6
  }

  it should "return its second over types in 2D" in {
    toU(data2)
      .types(Over(_1), Default())(false)
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its second over specific types in 2D" in {
    toU(data2)
      .types(Over(_1), Default(12))(true)
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its second along types in 2D" in {
    toU(data2)
      .types(Along(_1), Default())(false)
      .toList.sortBy(_.position) shouldBe result9
  }

  it should "return its second along specific types in 2D" in {
    toU(data2)
      .types(Along(_1), Default(12))(true)
      .toList.sortBy(_.position) shouldBe result10
  }

  it should "return its first over types in 3D" in {
    toU(data3)
      .types(Over(_0), Default())(false)
      .toList.sortBy(_.position) shouldBe result11
  }

  it should "return its first over specific types in 3D" in {
    toU(data3)
      .types(Over(_0), Default(12))(true)
      .toList.sortBy(_.position) shouldBe result12
  }

  it should "return its first along types in 3D" in {
    toU(data3)
      .types(Along(_0), Default())(false)
      .toList.sortBy(_.position) shouldBe result13
  }

  it should "return its first along specific types in 3D" in {
    toU(data3)
      .types(Along(_0), Default(12))(true)
      .toList.sortBy(_.position) shouldBe result14
  }

  it should "return its second over types in 3D" in {
    toU(data3)
      .types(Over(_1), Default())(false)
      .toList.sortBy(_.position) shouldBe result15
  }

  it should "return its second over specific types in 3D" in {
    toU(data3)
      .types(Over(_1), Default(12))(true)
      .toList.sortBy(_.position) shouldBe result16
  }

  it should "return its second along types in 3D" in {
    toU(data3)
      .types(Along(_1), Default())(false)
      .toList.sortBy(_.position) shouldBe result17
  }

  it should "return its second along specific types in 3D" in {
    toU(data3)
      .types(Along(_1), Default(12))(true)
      .toList.sortBy(_.position) shouldBe result18
  }

  it should "return its third over types in 3D" in {
    toU(data3)
      .types(Over(_2), Default())(false)
      .toList.sortBy(_.position) shouldBe result19
  }

  it should "return its third over specific types in 3D" in {
    toU(data3)
      .types(Over(_2), Default(12))(true)
      .toList.sortBy(_.position) shouldBe result20
  }

  it should "return its third along types in 3D" in {
    toU(data3)
      .types(Along(_2), Default())(false)
      .toList.sortBy(_.position) shouldBe result21
  }

  it should "return its third along specific types in 3D" in {
    toU(data3)
      .types(Along(_2), Default(12))(true)
      .toList.sortBy(_.position) shouldBe result22
  }
}

class TestSparkMatrixTypes extends TestMatrixTypes with TestSpark {
  import commbank.grimlock.spark.environment.implicits._

  "A Matrix.types" should "return its first over types in 1D" in {
    toU(data1)
      .types(Over(_0), Default())(false)
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its first over specific types in 1D" in {
    toU(data1)
      .types(Over(_0), Default(12))(true)
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its first over types in 2D" in {
    toU(data2)
      .types(Over(_0), Default())(false)
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its first over specific types in 2D" in {
    toU(data2)
      .types(Over(_0), Default(12))(true)
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its first along types in 2D" in {
    toU(data2)
      .types(Along(_0), Default())(false)
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its first along specific types in 2D" in {
    toU(data2)
      .types(Along(_0), Default(12))(true)
      .toList.sortBy(_.position) shouldBe result6
  }

  it should "return its second over types in 2D" in {
    toU(data2)
      .types(Over(_1), Default())(false)
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its second over specific types in 2D" in {
    toU(data2)
      .types(Over(_1), Default(12))(true)
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its second along types in 2D" in {
    toU(data2)
      .types(Along(_1), Default())(false)
      .toList.sortBy(_.position) shouldBe result9
  }

  it should "return its second along specific types in 2D" in {
    toU(data2)
      .types(Along(_1), Default(12))(true)
      .toList.sortBy(_.position) shouldBe result10
  }

  it should "return its first over types in 3D" in {
    toU(data3)
      .types(Over(_0), Default())(false)
      .toList.sortBy(_.position) shouldBe result11
  }

  it should "return its first over specific types in 3D" in {
    toU(data3)
      .types(Over(_0), Default(12))(true)
      .toList.sortBy(_.position) shouldBe result12
  }

  it should "return its first along types in 3D" in {
    toU(data3)
      .types(Along(_0), Default())(false)
      .toList.sortBy(_.position) shouldBe result13
  }

  it should "return its first along specific types in 3D" in {
    toU(data3)
      .types(Along(_0), Default(12))(true)
      .toList.sortBy(_.position) shouldBe result14
  }

  it should "return its second over types in 3D" in {
    toU(data3)
      .types(Over(_1), Default())(false)
      .toList.sortBy(_.position) shouldBe result15
  }

  it should "return its second over specific types in 3D" in {
    toU(data3)
      .types(Over(_1), Default(12))(true)
      .toList.sortBy(_.position) shouldBe result16
  }

  it should "return its second along types in 3D" in {
    toU(data3)
      .types(Along(_1), Default())(false)
      .toList.sortBy(_.position) shouldBe result17
  }

  it should "return its second along specific types in 3D" in {
    toU(data3)
      .types(Along(_1), Default(12))(true)
      .toList.sortBy(_.position) shouldBe result18
  }

  it should "return its third over types in 3D" in {
    toU(data3)
      .types(Over(_2), Default())(false)
      .toList.sortBy(_.position) shouldBe result19
  }

  it should "return its third over specific types in 3D" in {
    toU(data3)
      .types(Over(_2), Default(12))(true)
      .toList.sortBy(_.position) shouldBe result20
  }

  it should "return its third along types in 3D" in {
    toU(data3)
      .types(Along(_2), Default())(false)
      .toList.sortBy(_.position) shouldBe result21
  }

  it should "return its third along specific types in 3D" in {
    toU(data3)
      .types(Along(_2), Default(12))(true)
      .toList.sortBy(_.position) shouldBe result22
  }
}

