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
import commbank.grimlock.framework.window._

import com.twitter.scalding.typed.ValuePipe

import shapeless.{ ::, HList, HNil }
import shapeless.nat.{ _0, _1, _2 }

trait TestMatrixSlide extends TestMatrix {
  type P = Value[String] :: HNil
  type S = HNil
  type R = Value[String] :: HNil
  type Q = Value[String] :: HNil

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
  case class Delta[
    P <: HList,
    S <: HList,
    R <: HList,
    Q <: HList
  ](
    times: Int
  )(implicit
    ev: Position.AppendConstraints.Aux[S, Value[String], Q]
  ) extends Window[P, S, R, Q] {
    type I = Option[Double]
    type T = (Option[Double], Position[R])
    type O = (Double, Position[R], Position[R])

    def prepare(cell: Cell[P]): I = cell.content.value.as[Double]

    def initialise(rem: Position[R], in: I): (T, TraversableOnce[O]) = ((in, rem), List())

    def update(rem: Position[R], in: I, t: T): (T, TraversableOnce[O]) = ((in, rem), (in, t._1) match {
     case (Some(dc), Some(dt)) => List((dc - dt, rem, t._2))
     case _ => List()
   })

    def present(pos: Position[S], out: O): TraversableOnce[Cell[Q]] = List(
      Cell(
        pos.append(times + "*(" + out._2.toShortString("|") + "-" + out._3.toShortString("|") + ")"),
        Content(ContinuousSchema[Double](), times * out._1)
      )
    )
  }

  case class DeltaWithValue[
    P <: HList,
    S <: HList,
    R <: HList,
    Q <: HList
  ](
    key: String
  )(implicit
    ev: Position.AppendConstraints.Aux[S, Value[String], Q]
  ) extends WindowWithValue[P, S, R, Q] {
    type V = Map[String, Int]
    type I = Option[Double]
    type T = (Option[Double], Position[R])
    type O = (Double, Position[R], Position[R])

    def prepareWithValue(cell: Cell[P], ext: V): I = cell.content.value.as[Double]

    def initialise(rem: Position[R], in: I): (T, TraversableOnce[O]) = ((in, rem), List())

    def update(rem: Position[R], in: I, t: T): (T, TraversableOnce[O]) = ((in, rem), (in, t._1) match {
     case (Some(dc), Some(dt)) => List((dc - dt, rem, t._2))
     case _ => List()
   })

    def presentWithValue(pos: Position[S], out: O, ext: V): TraversableOnce[Cell[Q]] = List(
      Cell(
        pos.append(ext(key) + "*(" + out._2.toShortString("|") + "-" + out._3.toShortString("|") + ")"),
        Content(ContinuousSchema[Double](), ext(key) * out._1)
      )
    )
  }
}

class TestScalaMatrixSlide extends TestMatrixSlide with TestScala {
  import commbank.grimlock.scala.environment.implicits._

  "A Matrix.slide" should "return its first along derived data in 1D" in {
    toU(num1)
      .slide(Along(_0), Default())(
        false,
        List(TestMatrixSlide.Delta[P, S, R, Q](1), TestMatrixSlide.Delta[P, S, R, Q](2))
      )
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its first over derived data in 2D" in {
    toU(num2)
      .slide(Over(_0), Default())(false, TestMatrixSlide.Delta(1))
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its first along derived data in 2D" in {
    toU(num2)
      .slide(Along(_0), Default())(true, TestMatrixSlide.Delta(1))
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its second over derived data in 2D" in {
    toU(num2)
      .slide(Over(_1), Default())(true, TestMatrixSlide.Delta(1))
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its second along derived data in 2D" in {
    toU(num2)
      .slide(Along(_1), Default())(false, TestMatrixSlide.Delta(1))
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its first over derived data in 3D" in {
    toU(num3)
      .slide(Over(_0), Default())(false, TestMatrixSlide.Delta(1))
      .toList.sortBy(_.position) shouldBe result6
  }

  it should "return its first along derived data in 3D" in {
    toU(num3)
      .slide(Along(_0), Default())(true, TestMatrixSlide.Delta(1))
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its second over derived data in 3D" in {
    toU(num3)
      .slide(Over(_1), Default())(true, TestMatrixSlide.Delta(1))
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its second along derived data in 3D" in {
    toU(num3)
      .slide(Along(_1), Default())(false, TestMatrixSlide.Delta(1))
      .toList.sortBy(_.position) shouldBe result9
  }

  it should "return its third over derived data in 3D" in {
    toU(num3)
      .slide(Over(_2), Default())(false, TestMatrixSlide.Delta(1))
      .toList.sortBy(_.position) shouldBe result10
  }

  it should "return its third along derived data in 3D" in {
    toU(num3)
      .slide(Along(_2), Default())(true, TestMatrixSlide.Delta(1))
      .toList.sortBy(_.position) shouldBe result11
  }

  "A Matrix.slideWithValue" should "return its first along derived data in 1D" in {
    toU(num1)
      .slideWithValue(Along(_0), Default())(
        true,
        ext,
        TestMatrixSlide.DeltaWithValue[P, S, R, Q]("one"),
        TestMatrixSlide.DeltaWithValue[P, S, R, Q]("two")
      )
      .toList.sortBy(_.position) shouldBe result12
  }

  it should "return its first over derived data in 2D" in {
    toU(num2)
      .slideWithValue(Over(_0), Default())(false, ext, TestMatrixSlide.DeltaWithValue("one"))
      .toList.sortBy(_.position) shouldBe result13
  }

  it should "return its first along derived data in 2D" in {
    toU(num2)
      .slideWithValue(Along(_0), Default())(false, ext, TestMatrixSlide.DeltaWithValue("one"))
      .toList.sortBy(_.position) shouldBe result14
  }

  it should "return its second over derived data in 2D" in {
    toU(num2)
      .slideWithValue(Over(_1), Default())(true, ext, TestMatrixSlide.DeltaWithValue("one"))
      .toList.sortBy(_.position) shouldBe result15
  }

  it should "return its second along derived data in 2D" in {
    toU(num2)
      .slideWithValue(Along(_1), Default())(true, ext, TestMatrixSlide.DeltaWithValue("one"))
      .toList.sortBy(_.position) shouldBe result16
  }

  it should "return its first over derived data in 3D" in {
    toU(num3)
      .slideWithValue(Over(_0), Default())(false, ext, TestMatrixSlide.DeltaWithValue("one"))
      .toList.sortBy(_.position) shouldBe result17
  }

  it should "return its first along derived data in 3D" in {
    toU(num3)
      .slideWithValue(Along(_0), Default())(false, ext, TestMatrixSlide.DeltaWithValue("one"))
      .toList.sortBy(_.position) shouldBe result18
  }

  it should "return its second over derived data in 3D" in {
    toU(num3)
      .slideWithValue(Over(_1), Default())(true, ext, TestMatrixSlide.DeltaWithValue("one"))
      .toList.sortBy(_.position) shouldBe result19
  }

  it should "return its second along derived data in 3D" in {
    toU(num3)
      .slideWithValue(Along(_1), Default())(true, ext, TestMatrixSlide.DeltaWithValue("one"))
      .toList.sortBy(_.position) shouldBe result20
  }

  it should "return its third over derived data in 3D" in {
    toU(num3)
      .slideWithValue(Over(_2), Default())(false, ext, TestMatrixSlide.DeltaWithValue("one"))
      .toList.sortBy(_.position) shouldBe result21
  }

  it should "return its third along derived data in 3D" in {
    toU(num3)
      .slideWithValue(Along(_2), Default())(false, ext, TestMatrixSlide.DeltaWithValue("one"))
      .toList.sortBy(_.position) shouldBe result22
  }
}

class TestScaldingMatrixSlide extends TestMatrixSlide with TestScalding {
  import commbank.grimlock.scalding.environment.implicits._

  "A Matrix.slide" should "return its first along derived data in 1D" in {
    toU(num1)
      .slide(Along(_0), Default())(
        false,
        List(TestMatrixSlide.Delta[P, S, R, Q](1), TestMatrixSlide.Delta[P, S, R, Q](2))
      )
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its first over derived data in 2D" in {
    toU(num2)
      .slide(Over(_0), Default(12))(false, TestMatrixSlide.Delta(1))
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its first along derived data in 2D" in {
    toU(num2)
      .slide(Along(_0), Redistribute(12))(true, TestMatrixSlide.Delta(1))
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its second over derived data in 2D" in {
    toU(num2)
      .slide(Over(_1), Default())(true, TestMatrixSlide.Delta(1))
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its second along derived data in 2D" in {
    toU(num2)
      .slide(Along(_1), Default(12))(false, TestMatrixSlide.Delta(1))
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its first over derived data in 3D" in {
    toU(num3)
      .slide(Over(_0), Redistribute(12))(false, TestMatrixSlide.Delta(1))
      .toList.sortBy(_.position) shouldBe result6
  }

  it should "return its first along derived data in 3D" in {
    toU(num3)
      .slide(Along(_0), Default())(true, TestMatrixSlide.Delta(1))
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its second over derived data in 3D" in {
    toU(num3)
      .slide(Over(_1), Default(12))(true, TestMatrixSlide.Delta(1))
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its second along derived data in 3D" in {
    toU(num3)
      .slide(Along(_1), Redistribute(12))(false, TestMatrixSlide.Delta(1))
      .toList.sortBy(_.position) shouldBe result9
  }

  it should "return its third over derived data in 3D" in {
    toU(num3)
      .slide(Over(_2), Default())(false, TestMatrixSlide.Delta(1))
      .toList.sortBy(_.position) shouldBe result10
  }

  it should "return its third along derived data in 3D" in {
    toU(num3)
      .slide(Along(_2), Default(12))(true, TestMatrixSlide.Delta(1))
      .toList.sortBy(_.position) shouldBe result11
  }

  "A Matrix.slideWithValue" should "return its first along derived data in 1D" in {
    toU(num1)
      .slideWithValue(Along(_0), Redistribute(12))(
        true,
        ValuePipe(ext),
        TestMatrixSlide.DeltaWithValue[P, S, R, Q]("one"),
        TestMatrixSlide.DeltaWithValue[P, S, R, Q]("two")
      )
      .toList.sortBy(_.position) shouldBe result12
  }

  it should "return its first over derived data in 2D" in {
    toU(num2)
      .slideWithValue(Over(_0), Default())(false, ValuePipe(ext), TestMatrixSlide.DeltaWithValue("one"))
      .toList.sortBy(_.position) shouldBe result13
  }

  it should "return its first along derived data in 2D" in {
    toU(num2)
      .slideWithValue(Along(_0), Default(12))(false, ValuePipe(ext), TestMatrixSlide.DeltaWithValue("one"))
      .toList.sortBy(_.position) shouldBe result14
  }

  it should "return its second over derived data in 2D" in {
    toU(num2)
      .slideWithValue(Over(_1), Redistribute(12))(true, ValuePipe(ext), TestMatrixSlide.DeltaWithValue("one"))
      .toList.sortBy(_.position) shouldBe result15
  }

  it should "return its second along derived data in 2D" in {
    toU(num2)
      .slideWithValue(Along(_1), Default())(true, ValuePipe(ext), TestMatrixSlide.DeltaWithValue("one"))
      .toList.sortBy(_.position) shouldBe result16
  }

  it should "return its first over derived data in 3D" in {
    toU(num3)
      .slideWithValue(Over(_0), Default(12))(false, ValuePipe(ext), TestMatrixSlide.DeltaWithValue("one"))
      .toList.sortBy(_.position) shouldBe result17
  }

  it should "return its first along derived data in 3D" in {
    toU(num3)
      .slideWithValue(Along(_0), Redistribute(12))(false, ValuePipe(ext), TestMatrixSlide.DeltaWithValue("one"))
      .toList.sortBy(_.position) shouldBe result18
  }

  it should "return its second over derived data in 3D" in {
    toU(num3)
      .slideWithValue(Over(_1), Default())(true, ValuePipe(ext), TestMatrixSlide.DeltaWithValue("one"))
      .toList.sortBy(_.position) shouldBe result19
  }

  it should "return its second along derived data in 3D" in {
    toU(num3)
      .slideWithValue(Along(_1), Default(12))(true, ValuePipe(ext), TestMatrixSlide.DeltaWithValue("one"))
      .toList.sortBy(_.position) shouldBe result20
  }

  it should "return its third over derived data in 3D" in {
    toU(num3)
      .slideWithValue(Over(_2), Redistribute(12))(false, ValuePipe(ext), TestMatrixSlide.DeltaWithValue("one"))
      .toList.sortBy(_.position) shouldBe result21
  }

  it should "return its third along derived data in 3D" in {
    toU(num3)
      .slideWithValue(Along(_2), Default())(false, ValuePipe(ext), TestMatrixSlide.DeltaWithValue("one"))
      .toList.sortBy(_.position) shouldBe result22
  }
}

class TestSparkMatrixSlide extends TestMatrixSlide with TestSpark {
  import commbank.grimlock.spark.environment.implicits._

  "A Matrix.slide" should "return its first along derived data in 1D" in {
    toU(num1)
      .slide(Along(_0), Default())(
        false,
        List(TestMatrixSlide.Delta[P, S, R, Q](1), TestMatrixSlide.Delta[P, S, R, Q](2))
      )
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its first over derived data in 2D" in {
    toU(num2)
      .slide(Over(_0), Default(12))(false, TestMatrixSlide.Delta(1))
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its first along derived data in 2D" in {
    toU(num2)
      .slide(Along(_0), Default())(true, TestMatrixSlide.Delta(1))
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its second over derived data in 2D" in {
    toU(num2)
      .slide(Over(_1), Default())(true, TestMatrixSlide.Delta(1))
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its second along derived data in 2D" in {
    toU(num2)
      .slide(Along(_1), Default(12))(false, TestMatrixSlide.Delta(1))
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its first over derived data in 3D" in {
    toU(num3)
      .slide(Over(_0), Default())(false, TestMatrixSlide.Delta(1))
      .toList.sortBy(_.position) shouldBe result6
  }

  it should "return its first along derived data in 3D" in {
    toU(num3)
      .slide(Along(_0), Default(12))(true, TestMatrixSlide.Delta(1))
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its second over derived data in 3D" in {
    toU(num3)
      .slide(Over(_1), Default())(true, TestMatrixSlide.Delta(1))
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its second along derived data in 3D" in {
    toU(num3)
      .slide(Along(_1), Default(12))(false, TestMatrixSlide.Delta(1))
      .toList.sortBy(_.position) shouldBe result9
  }

  it should "return its third over derived data in 3D" in {
    toU(num3)
      .slide(Over(_2), Default())(false, TestMatrixSlide.Delta(1))
      .toList.sortBy(_.position) shouldBe result10
  }

  it should "return its third along derived data in 3D" in {
    toU(num3)
      .slide(Along(_2), Default(12))(true, TestMatrixSlide.Delta(1))
      .toList.sortBy(_.position) shouldBe result11
  }

  "A Matrix.slideWithValue" should "return its first along derived data in 1D" in {
    toU(num1)
      .slideWithValue(Along(_0), Default())(
        true,
        ext,
        TestMatrixSlide.DeltaWithValue[P, S, R, Q]("one"),
        TestMatrixSlide.DeltaWithValue[P, S, R, Q]("two")
      )
      .toList.sortBy(_.position) shouldBe result12
  }

  it should "return its first over derived data in 2D" in {
    toU(num2)
      .slideWithValue(Over(_0), Default(12))(false, ext, TestMatrixSlide.DeltaWithValue("one"))
      .toList.sortBy(_.position) shouldBe result13
  }

  it should "return its first along derived data in 2D" in {
    toU(num2)
      .slideWithValue(Along(_0), Default())(false, ext, TestMatrixSlide.DeltaWithValue("one"))
      .toList.sortBy(_.position) shouldBe result14
  }

  it should "return its second over derived data in 2D" in {
    toU(num2)
      .slideWithValue(Over(_1), Default(12))(true, ext, TestMatrixSlide.DeltaWithValue("one"))
      .toList.sortBy(_.position) shouldBe result15
  }

  it should "return its second along derived data in 2D" in {
    toU(num2)
      .slideWithValue(Along(_1), Default())(true, ext, TestMatrixSlide.DeltaWithValue("one"))
      .toList.sortBy(_.position) shouldBe result16
  }

  it should "return its first over derived data in 3D" in {
    toU(num3)
      .slideWithValue(Over(_0), Default(12))(false, ext, TestMatrixSlide.DeltaWithValue("one"))
      .toList.sortBy(_.position) shouldBe result17
  }

  it should "return its first along derived data in 3D" in {
    toU(num3)
      .slideWithValue(Along(_0), Default())(false, ext, TestMatrixSlide.DeltaWithValue("one"))
      .toList.sortBy(_.position) shouldBe result18
  }

  it should "return its second over derived data in 3D" in {
    toU(num3)
      .slideWithValue(Over(_1), Default(12))(true, ext, TestMatrixSlide.DeltaWithValue("one"))
      .toList.sortBy(_.position) shouldBe result19
  }

  it should "return its second along derived data in 3D" in {
    toU(num3)
      .slideWithValue(Along(_1), Default())(true, ext, TestMatrixSlide.DeltaWithValue("one"))
      .toList.sortBy(_.position) shouldBe result20
  }

  it should "return its third over derived data in 3D" in {
    toU(num3)
      .slideWithValue(Over(_2), Default(12))(false, ext, TestMatrixSlide.DeltaWithValue("one"))
      .toList.sortBy(_.position) shouldBe result21
  }

  it should "return its third along derived data in 3D" in {
    toU(num3)
      .slideWithValue(Along(_2), Default())(false, ext, TestMatrixSlide.DeltaWithValue("one"))
      .toList.sortBy(_.position) shouldBe result22
  }
}

