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

import commbank.grimlock.library.aggregate._

import com.twitter.scalding.typed.TypedPipe

import shapeless.nat.{ _0, _1, _2 }

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

class TestScalaMatrixFill extends TestMatrixFill with TestScala {
  import commbank.grimlock.scala.environment.implicits._

  "A Matrix.fill" should "return its filled data in 1D" in {
    toU(num1)
      .fillHomogeneous(Content(ContinuousSchema[Double](), 0.0), Default())
      .toList.sortBy(_.position) shouldBe result0
  }

  it should "return its filled data in 2D" in {
    toU(num2)
      .fillHomogeneous(Content(ContinuousSchema[Double](), 0.0), Default())
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its filled data in 3D" in {
    toU(num3)
      .fillHomogeneous(Content(ContinuousSchema[Double](), 0.0), Default())
      .toList.sortBy(_.position) shouldBe result2
  }

  "A Matrix.fill" should "return its first over filled data in 1D" in {
    val cells = toU(num1)

    cells
      .fillHeterogeneous(Over(_0), Default())(cells.summarise(Over(_0))(Mean()))
      .toList.sortBy(_.position) shouldBe result0
  }

  it should "return its first along filled data in 1D" in {
    val cells = toU(num1)

    cells
      .fillHeterogeneous(Along(_0), Default())(cells.summarise(Along(_0))(Mean()))
      .toList.sortBy(_.position) shouldBe result0
  }

  it should "return its first over filled data in 2D" in {
    val cells = toU(num2)

    cells
      .fillHeterogeneous(Over(_0), Default())(cells.summarise(Over(_0))(Mean()))
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its first along filled data in 2D" in {
    val cells = toU(num2)

    cells
      .fillHeterogeneous(Along(_0), Default())(cells.summarise(Along(_0))(Mean()))
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its second over filled data in 2D" in {
    val cells = toU(num2)

    cells
      .fillHeterogeneous(Over(_1), Default())(cells.summarise(Over(_1))(Mean()))
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its second along filled data in 2D" in {
    val cells = toU(num2)

    cells
      .fillHeterogeneous(Along(_1), Default())(cells.summarise(Along(_1))(Mean()))
      .toList.sortBy(_.position) shouldBe result6
  }

  it should "return its first over filled data in 3D" in {
    val cells = toU(num3)

    cells
      .fillHeterogeneous(Over(_0), Default())(cells.summarise(Over(_0))(Mean()))
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its first along filled data in 3D" in {
    val cells = toU(num3)

    cells
      .fillHeterogeneous(Along(_0), Default())(cells.summarise(Along(_0))(Mean()))
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its second over filled data in 3D" in {
    val cells = toU(num3)

    cells
      .fillHeterogeneous(Over(_1), Default())(cells.summarise(Over(_1))(Mean()))
      .toList.sortBy(_.position) shouldBe result9
  }

  it should "return its second along filled data in 3D" in {
    val cells = toU(num3)

    cells
      .fillHeterogeneous(Along(_1), Default())(cells.summarise(Along(_1))(Mean()))
      .toList.sortBy(_.position) shouldBe result10
  }

  it should "return its third over filled data in 3D" in {
    val cells = toU(num3)

    cells
      .fillHeterogeneous(Over(_2), Default())(cells.summarise(Over(_2))(Mean()))
      .toList.sortBy(_.position) shouldBe result11
  }

  it should "return its third along filled data in 3D" in {
    val cells = toU(num3)

    cells
      .fillHeterogeneous(Along(_2), Default())(cells.summarise(Along(_2))(Mean()))
      .toList.sortBy(_.position) shouldBe result12
  }

  it should "return empty data - Default" in {
    toU(num3)
      .fillHeterogeneous(Along(_2), Default())(List.empty)
      .toList.sortBy(_.position) shouldBe List()
  }
}

class TestScaldingMatrixFill extends TestMatrixFill with TestScalding {
  import commbank.grimlock.scalding.environment.implicits._

  "A Matrix.fill" should "return its filled data in 1D" in {
    toU(num1)
      .fillHomogeneous(Content(ContinuousSchema[Double](), 0.0), Default())
      .toList.sortBy(_.position) shouldBe result0
  }

  it should "return its filled data in 2D" in {
    toU(num2)
      .fillHomogeneous(Content(ContinuousSchema[Double](), 0.0), Binary(InMemory(12), Default(12)))
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its filled data in 3D" in {
    toU(num3)
      .fillHomogeneous(Content(ContinuousSchema[Double](), 0.0), Binary(Default(12), Default(12)))
      .toList.sortBy(_.position) shouldBe result2
  }

  "A Matrix.fill" should "return its first over filled data in 1D" in {
    val cells = toU(num1)

    cells
      .fillHeterogeneous(Over(_0), Default())(cells.summarise(Over(_0))(Mean()))
      .toList.sortBy(_.position) shouldBe result0
  }

  it should "return its first along filled data in 1D" in {
    val cells = toU(num1)

    cells
      .fillHeterogeneous(Along(_0), Ternary(InMemory(), InMemory(), Default()))(cells.summarise(Along(_0))(Mean()))
      .toList.sortBy(_.position) shouldBe result0
  }

  it should "return its first over filled data in 2D" in {
    val cells = toU(num2)

    cells
      .fillHeterogeneous(Over(_0), Ternary(InMemory(), Default(), Default(12)))(cells.summarise(Over(_0))(Mean()))
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its first along filled data in 2D" in {
    val cells = toU(num2)

    cells
      .fillHeterogeneous(Along(_0), Ternary(InMemory(), InMemory(), Default(12)))(cells.summarise(Along(_0))(Mean()))
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its second over filled data in 2D" in {
    val cells = toU(num2)

    cells
      .fillHeterogeneous(Over(_1), Ternary(InMemory(), Default(), Default(12)))(cells.summarise(Over(_1))(Mean()))
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its second along filled data in 2D" in {
    val cells = toU(num2)

    cells
      .fillHeterogeneous(Along(_1), Ternary(InMemory(), Default(12), Default(12)))(cells.summarise(Along(_1))(Mean()))
      .toList.sortBy(_.position) shouldBe result6
  }

  it should "return its first over filled data in 3D" in {
    val cells = toU(num3)

    cells
      .fillHeterogeneous(Over(_0), Ternary(Default(), Default(), Default(12)))(cells.summarise(Over(_0))(Mean()))
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its first along filled data in 3D" in {
    val cells = toU(num3)

    cells
      .fillHeterogeneous(Along(_0), Ternary(Default(), Default(12), Default(12)))(cells.summarise(Along(_0))(Mean()))
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its second over filled data in 3D" in {
    val cells = toU(num3)

    cells
      .fillHeterogeneous(Over(_1), Ternary(Default(12), Default(12), Default(12)))(cells.summarise(Over(_1))(Mean()))
      .toList.sortBy(_.position) shouldBe result9
  }

  it should "return its second along filled data in 3D" in {
    val cells = toU(num3)

    cells
      .fillHeterogeneous(Along(_1), Default())(cells.summarise(Along(_1))(Mean()))
      .toList.sortBy(_.position) shouldBe result10
  }

  it should "return its third over filled data in 3D" in {
    val cells = toU(num3)

    cells
      .fillHeterogeneous(Over(_2), Ternary(InMemory(), InMemory(), Default()))(cells.summarise(Over(_2))(Mean()))
      .toList.sortBy(_.position) shouldBe result11
  }

  it should "return its third along filled data in 3D" in {
    val cells = toU(num3)

    cells
      .fillHeterogeneous(Along(_2), Ternary(InMemory(), Default(), Default()))(cells.summarise(Along(_2))(Mean()))
      .toList.sortBy(_.position) shouldBe result12
  }

  it should "return empty data - InMemory" in {
    toU(num3)
      .fillHeterogeneous(Along(_2), Ternary(InMemory(), InMemory(), Default()))(TypedPipe.empty)
      .toList.sortBy(_.position) shouldBe List()
  }

  it should "return empty data - Default" in {
    toU(num3)
      .fillHeterogeneous(Along(_2), Default())(TypedPipe.empty)
      .toList.sortBy(_.position) shouldBe List()
  }
}

class TestSparkMatrixFill extends TestMatrixFill with TestSpark {
  import commbank.grimlock.spark.environment.implicits._

  "A Matrix.fill" should "return its filled data in 1D" in {
    toU(num1)
      .fillHomogeneous(Content(ContinuousSchema[Double](), 0.0), Default())
      .toList.sortBy(_.position) shouldBe result0
  }

  it should "return its filled data in 2D" in {
    toU(num2)
      .fillHomogeneous(Content(ContinuousSchema[Double](), 0.0), Binary(InMemory(12), Default(12)))
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its filled data in 3D" in {
    toU(num3)
      .fillHomogeneous(Content(ContinuousSchema[Double](), 0.0), Binary(Default(12), Default(12)))
      .toList.sortBy(_.position) shouldBe result2
  }

  "A Matrix.fill" should "return its first over filled data in 1D" in {
    val cells = toU(num1)

    cells
      .fillHeterogeneous(Over(_0), Default())(cells.summarise(Over(_0))(Mean()))
      .toList.sortBy(_.position) shouldBe result0
  }

  it should "return its first along filled data in 1D" in {
    val cells = toU(num1)

    cells
      .fillHeterogeneous(Along(_0), Ternary(InMemory(), InMemory(), Default()))(cells.summarise(Along(_0))(Mean()))
      .toList.sortBy(_.position) shouldBe result0
  }

  it should "return its first over filled data in 2D" in {
    val cells = toU(num2)

    cells
      .fillHeterogeneous(Over(_0), Ternary(InMemory(), Default(), Default()))(cells.summarise(Over(_0))(Mean()))
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its first along filled data in 2D" in {
    val cells = toU(num2)

    cells
      .fillHeterogeneous(Along(_0), Ternary(InMemory(), InMemory(), Default(12)))(cells.summarise(Along(_0))(Mean()))
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its second over filled data in 2D" in {
    val cells = toU(num2)

    cells
      .fillHeterogeneous(Over(_1), Ternary(InMemory(), Default(), Default(12)))(cells.summarise(Over(_1))(Mean()))
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its second along filled data in 2D" in {
    val cells = toU(num2)

    cells
      .fillHeterogeneous(Along(_1), Ternary(InMemory(), Default(12), Default(12)))(cells.summarise(Along(_1))(Mean()))
      .toList.sortBy(_.position) shouldBe result6
  }

  it should "return its first over filled data in 3D" in {
    val cells = toU(num3)

    cells
      .fillHeterogeneous(Over(_0), Ternary(Default(), Default(), Default(12)))(cells.summarise(Over(_0))(Mean()))
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its first along filled data in 3D" in {
    val cells = toU(num3)

    cells
      .fillHeterogeneous(Along(_0), Ternary(Default(), Default(12), Default(12)))(cells.summarise(Along(_0))(Mean()))
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its second over filled data in 3D" in {
    val cells = toU(num3)

    cells
      .fillHeterogeneous(Over(_1), Ternary(Default(12), Default(12), Default(12)))(cells.summarise(Over(_1))(Mean()))
      .toList.sortBy(_.position) shouldBe result9
  }

  it should "return its second along filled data in 3D" in {
    val cells = toU(num3)

    cells
      .fillHeterogeneous(Along(_1), Default())(cells.summarise(Along(_1))(Mean()))
      .toList.sortBy(_.position) shouldBe result10
  }

  it should "return its third over filled data in 3D" in {
    val cells = toU(num3)

    cells
      .fillHeterogeneous(Over(_2), Ternary(InMemory(), InMemory(), Default()))(cells.summarise(Over(_2))(Mean()))
      .toList.sortBy(_.position) shouldBe result11
  }

  it should "return its third along filled data in 3D" in {
    val cells = toU(num3)

    cells
      .fillHeterogeneous(Along(_2), Ternary(InMemory(), Default(), Default()))(cells.summarise(Along(_2))(Mean()))
      .toList.sortBy(_.position) shouldBe result12
  }

  it should "return empty data - InMemory" in {
    toU(num3)
      .fillHeterogeneous(Along(_2), Ternary(InMemory(), InMemory(), Default()))(toU(List()))
      .toList.sortBy(_.position) shouldBe List()
  }

  it should "return empty data - Default" in {
    toU(num3)
      .fillHeterogeneous(Along(_2), Default())(toU(List()))
      .toList.sortBy(_.position) shouldBe List()
  }
}

