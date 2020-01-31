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

trait TestMatrixShape extends TestMatrix {
  val result1 = List(Cell(Position(0L), Content(DiscreteSchema[Long](), 4L)))

  val result2 = List(
    Cell(Position(0L), Content(DiscreteSchema[Long](), 4L)),
    Cell(Position(1L), Content(DiscreteSchema[Long](), 4L))
  )

  val result3 = List(
    Cell(Position(0L), Content(DiscreteSchema[Long](), 4L)),
    Cell(Position(1L), Content(DiscreteSchema[Long](), 4L)),
    Cell(Position(2L), Content(DiscreteSchema[Long](), 1L))
  )
}

class TestScalaMatrixShape extends TestMatrixShape with TestScala {
  import commbank.grimlock.scala.environment.implicits._

  "A Matrix.shape" should "return its shape in 1D" in {
    toU(data1)
      .shape(Default())
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its shape in 2D" in {
    toU(data2)
      .shape(Default())
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its shape in 3D" in {
    toU(data3)
      .shape(Default())
      .toList.sortBy(_.position) shouldBe result3
  }
}

class TestScaldingMatrixShape extends TestMatrixShape with TestScalding {
  import commbank.grimlock.scalding.environment.implicits._

  "A Matrix.shape" should "return its shape in 1D" in {
    toU(data1)
      .shape(Default())
      .toList shouldBe result1
  }

  it should "return its shape in 2D" in {
    toU(data2)
      .shape(Default(12))
      .toList shouldBe result2
  }

  it should "return its shape in 3D" in {
    toU(data3)
      .shape(Default())
      .toList shouldBe result3
  }
}

class TestSparkMatrixShape extends TestMatrixShape with TestSpark {
  import commbank.grimlock.spark.environment.implicits._

  "A Matrix.shape" should "return its shape in 1D" in {
    toU(data1)
      .shape(Default())
      .toList shouldBe result1
  }

  it should "return its shape in 2D" in {
    toU(data2)
      .shape(Default(12))
      .toList shouldBe result2
  }

  it should "return its shape in 3D" in {
    toU(data3)
      .shape(Default())
      .toList shouldBe result3
  }
}

