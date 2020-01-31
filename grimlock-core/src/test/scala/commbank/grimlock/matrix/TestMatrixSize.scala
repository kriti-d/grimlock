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
import commbank.grimlock.framework.environment.implicits._
import commbank.grimlock.framework.environment.tuner._
import commbank.grimlock.framework.metadata._
import commbank.grimlock.framework.position._

import shapeless.nat.{ _0, _1, _2 }

trait TestMatrixSize extends TestMatrix {
  val dataA = List(
    Cell(Position(1, 1), Content(OrdinalSchema[String](), "a")),
    Cell(Position(2, 2), Content(OrdinalSchema[String](), "b")),
    Cell(Position(3, 3), Content(OrdinalSchema[String](), "c"))
  )

  val result1 = List(Cell(Position(0L), Content(DiscreteSchema[Long](), 4L)))

  val result2 = List(Cell(Position(0L), Content(DiscreteSchema[Long](), 4L)))

  val result3 = List(Cell(Position(0L), Content(DiscreteSchema[Long](), 4L)))

  val result4 = List(Cell(Position(0L), Content(DiscreteSchema[Long](), data2.length.toLong)))

  val result5 = List(Cell(Position(1L), Content(DiscreteSchema[Long](), 4L)))

  val result6 = List(Cell(Position(1L), Content(DiscreteSchema[Long](), data2.length.toLong)))

  val result7 = List(Cell(Position(0L), Content(DiscreteSchema[Long](), 4L)))

  val result8 = List(Cell(Position(0L), Content(DiscreteSchema[Long](), data3.length.toLong)))

  val result9 = List(Cell(Position(1L), Content(DiscreteSchema[Long](), 4L)))

  val result10 = List(Cell(Position(1L), Content(DiscreteSchema[Long](), data3.length.toLong)))

  val result11 = List(Cell(Position(2L), Content(DiscreteSchema[Long](), 1L)))

  val result12 = List(Cell(Position(2L), Content(DiscreteSchema[Long](), data3.length.toLong)))

  val result13 = List(Cell(Position(1L), Content(DiscreteSchema[Long](), 3L)))
}

class TestScalaMatrixSize extends TestMatrixSize with TestScala {
  import commbank.grimlock.scala.environment.implicits._

  "A Matrix.size" should "return its first size in 1D" in {
    toU(data1)
      .measure(_0, false, Default())
      .toList shouldBe result1
  }

  it should "return its first distinct size in 1D" in {
    toU(data1)
      .measure(_0, true, Default())
      .toList shouldBe result2
  }

  it should "return its first size in 2D" in {
    toU(data2)
      .measure(_0, false, Default())
      .toList shouldBe result3
  }

  it should "return its first distinct size in 2D" in {
    toU(data2)
      .measure(_0, true, Default())
      .toList shouldBe result4
  }

  it should "return its second size in 2D" in {
    toU(data2)
      .measure(_1, false, Default())
      .toList shouldBe result5
  }

  it should "return its second distinct size in 2D" in {
    toU(data2)
      .measure(_1, true, Default())
      .toList shouldBe result6
  }

  it should "return its first size in 3D" in {
    toU(data3)
      .measure(_0, false, Default())
      .toList shouldBe result7
  }

  it should "return its first distinct size in 3D" in {
    toU(data3)
      .measure(_0, true, Default())
      .toList shouldBe result8
  }

  it should "return its second size in 3D" in {
    toU(data3)
      .measure(_1, false, Default())
      .toList shouldBe result9
  }

  it should "return its second distinct size in 3D" in {
    toU(data3)
      .measure(_1, true, Default())
      .toList shouldBe result10
  }

  it should "return its third size in 3D" in {
    toU(data3)
      .measure(_2, false, Default())
      .toList shouldBe result11
  }

  it should "return its third distinct size in 3D" in {
    toU(data3)
      .measure(_2, true, Default())
      .toList shouldBe result12
  }

  it should "return its distinct size" in {
    toU(dataA)
      .measure(_1, true, Default())
      .toList shouldBe result13
  }
}

class TestScaldingMatrixSize extends TestMatrixSize with TestScalding {
  import commbank.grimlock.scalding.environment.implicits._

  "A Matrix.size" should "return its first size in 1D" in {
    toU(data1)
      .measure(_0, false, Default())
      .toList shouldBe result1
  }

  it should "return its first distinct size in 1D" in {
    toU(data1)
      .measure(_0, true, Default(12))
      .toList shouldBe result2
  }

  it should "return its first size in 2D" in {
    toU(data2)
      .measure(_0, false, Default())
      .toList shouldBe result3
  }

  it should "return its first distinct size in 2D" in {
    toU(data2)
      .measure(_0, true, Default(12))
      .toList shouldBe result4
  }

  it should "return its second size in 2D" in {
    toU(data2)
      .measure(_1, false, Default())
      .toList shouldBe result5
  }

  it should "return its second distinct size in 2D" in {
    toU(data2)
      .measure(_1, true, Default(12))
      .toList shouldBe result6
  }

  it should "return its first size in 3D" in {
    toU(data3)
      .measure(_0, false, Default())
      .toList shouldBe result7
  }

  it should "return its first distinct size in 3D" in {
    toU(data3)
      .measure(_0, true, Default(12))
      .toList shouldBe result8
  }

  it should "return its second size in 3D" in {
    toU(data3)
      .measure(_1, false, Default())
      .toList shouldBe result9
  }

  it should "return its second distinct size in 3D" in {
    toU(data3)
      .measure(_1, true, Default(12))
      .toList shouldBe result10
  }

  it should "return its third size in 3D" in {
    toU(data3)
      .measure(_2, false, Default())
      .toList shouldBe result11
  }

  it should "return its third distinct size in 3D" in {
    toU(data3)
      .measure(_2, true, Default(12))
      .toList shouldBe result12
  }

  it should "return its distinct size" in {
    toU(dataA)
      .measure(_1, true, Default())
      .toList shouldBe result13
  }
}

class TestSparkMatrixSize extends TestMatrixSize with TestSpark {
  import commbank.grimlock.spark.environment.implicits._

  "A Matrix.size" should "return its first size in 1D" in {
    toU(data1)
      .measure(_0, false, Default())
      .toList shouldBe result1
  }

  it should "return its first distinct size in 1D" in {
    toU(data1)
      .measure(_0, true, Default(12))
      .toList shouldBe result2
  }

  it should "return its first size in 2D" in {
    toU(data2)
      .measure(_0, false, Default())
      .toList shouldBe result3
  }

  it should "return its first distinct size in 2D" in {
    toU(data2)
      .measure(_0, true, Default(12))
      .toList shouldBe result4
  }

  it should "return its second size in 2D" in {
    toU(data2)
      .measure(_1, false, Default())
      .toList shouldBe result5
  }

  it should "return its second distinct size in 2D" in {
    toU(data2)
      .measure(_1, true, Default(12))
      .toList shouldBe result6
  }

  it should "return its first size in 3D" in {
    toU(data3)
      .measure(_0, false, Default())
      .toList shouldBe result7
  }

  it should "return its first distinct size in 3D" in {
    toU(data3)
      .measure(_0, true, Default(12))
      .toList shouldBe result8
  }

  it should "return its second size in 3D" in {
    toU(data3)
      .measure(_1, false, Default())
      .toList shouldBe result9
  }

  it should "return its second distinct size in 3D" in {
    toU(data3)
      .measure(_1, true, Default(12))
      .toList shouldBe result10
  }

  it should "return its third size in 3D" in {
    toU(data3)
      .measure(_2, false, Default())
      .toList shouldBe result11
  }

  it should "return its third distinct size in 3D" in {
    toU(data3)
      .measure(_2, true, Default(12))
      .toList shouldBe result12
  }

  it should "return its distinct size" in {
    toU(dataA)
      .measure(_1, true, Default())
      .toList shouldBe result13
  }
}

