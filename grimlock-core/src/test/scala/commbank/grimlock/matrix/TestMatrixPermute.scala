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
import commbank.grimlock.framework.metadata._
import commbank.grimlock.framework.position._

import shapeless.nat.{ _0, _1, _2, _3, _4 }

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

class TestScalaMatrixPermute extends TestMatrixPermute with TestScala {
  import commbank.grimlock.scala.environment.implicits._

  "A Matrix.permute" should "return its permutation in 2D" in {
    toU(dataA)
      .permute(_1, _0)
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its permutation in 3D" in {
    toU(dataB)
      .permute(_1, _2, _0)
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its permutation in 4D" in {
    toU(dataC)
      .permute(_3, _2, _0, _1)
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its permutation in 5D" in {
    toU(dataD)
      .permute(_3, _1, _0, _4, _2)
      .toList.sortBy(_.position) shouldBe result4
  }
}

class TestScaldingMatrixPermute extends TestMatrixPermute with TestScalding {
  import commbank.grimlock.scalding.environment.implicits._

  "A Matrix.permute" should "return its permutation in 2D" in {
    toU(dataA)
      .permute(_1, _0)
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its permutation in 3D" in {
    toU(dataB)
      .permute(_1, _2, _0)
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its permutation in 4D" in {
    toU(dataC)
      .permute(_3, _2, _0, _1)
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its permutation in 5D" in {
    toU(dataD)
      .permute(_3, _1, _0, _4, _2)
      .toList.sortBy(_.position) shouldBe result4
  }
}

class TestSparkMatrixPermute extends TestMatrixPermute with TestSpark {
  import commbank.grimlock.spark.environment.implicits._

  "A Matrix.permute" should "return its permutation in 2D" in {
    toU(dataA)
      .permute(_1, _0)
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its permutation in 3D" in {
    toU(dataB)
      .permute(_1, _2, _0)
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its permutation in 4D" in {
    toU(dataC)
      .permute(_3, _2, _0, _1)
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its permutation in 5D" in {
    toU(dataD)
      .permute(_3, _1, _0, _4, _2)
      .toList.sortBy(_.position) shouldBe result4
  }
}

