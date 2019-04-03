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

trait TestMatrixDomain extends TestMatrix {
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

class TestScalaMatrixDomain extends TestMatrixDomain with TestScala {
  import commbank.grimlock.scala.environment.implicits._

  "A Matrix.domain" should "return its domain in 1D" in {
    toU(dataA)
      .domain(Default())
      .toList.sorted shouldBe result1
  }

  it should "return its domain in 2D" in {
    toU(dataB)
      .domain(Default())
      .toList.sorted shouldBe result2
  }

  it should "return its domain in 3D" in {
    toU(dataC)
      .domain(Default())
      .toList.sorted shouldBe result3
  }

  it should "return its domain in 4D" in {
    toU(dataD)
      .domain(Default())
      .toList.sorted shouldBe result4
  }

  it should "return its domain in 5D" in {
    toU(dataE)
      .domain(Default())
      .toList.sorted shouldBe result5
  }
}

class TestScaldingMatrixDomain extends TestMatrixDomain with TestScalding {
  import commbank.grimlock.scalding.environment.implicits._

  "A Matrix.domain" should "return its domain in 1D" in {
    toU(dataA)
      .domain(InMemory())
      .toList.sorted shouldBe result1
  }

  it should "return its domain in 2D" in {
    toU(dataB)
      .domain(InMemory(12))
      .toList.sorted shouldBe result2
  }

  it should "return its domain in 3D" in {
    toU(dataC)
      .domain(Default())
      .toList.sorted shouldBe result3
  }

  it should "return its domain in 4D" in {
    toU(dataD)
      .domain(Default(12))
      .toList.sorted shouldBe result4
  }

  it should "return its domain in 5D" in {
    toU(dataE)
      .domain(InMemory())
      .toList.sorted shouldBe result5
  }
}

class TestSparkMatrixDomain extends TestMatrixDomain with TestSpark {
  import commbank.grimlock.spark.environment.implicits._

  "A Matrix.domain" should "return its domain in 1D" in {
    toU(dataA)
      .domain(Default())
      .toList.sorted shouldBe result1
  }

  it should "return its domain in 2D" in {
    toU(dataB)
      .domain(Default(12))
      .toList.sorted shouldBe result2
  }

  it should "return its domain in 3D" in {
    toU(dataC)
      .domain(Default())
      .toList.sorted shouldBe result3
  }

  it should "return its domain in 4D" in {
    toU(dataD)
      .domain(Default(12))
      .toList.sorted shouldBe result4
  }

  it should "return its domain in 5D" in {
    toU(dataE)
      .domain(Default())
      .toList.sorted shouldBe result5
  }
}

