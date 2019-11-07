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
import commbank.grimlock.framework.aggregate._
import commbank.grimlock.framework.content._
import commbank.grimlock.framework.encoding._
import commbank.grimlock.framework.environment.implicits._
import commbank.grimlock.framework.environment.tuner._
import commbank.grimlock.framework.extract._
import commbank.grimlock.framework.metadata._
import commbank.grimlock.framework.position._

import commbank.grimlock.library.aggregate._

import com.twitter.scalding.typed.ValuePipe

import shapeless.{ ::, HList, HNil, Nat }
import shapeless.nat.{ _0, _1, _2, _3 }

trait TestMatrixSummarise extends TestMatrix {
  val ext1 = Map(
    Position("foo") -> 1.0 / 1,
    Position("bar") -> 1.0 / 2,
    Position("baz") -> 1.0 / 3,
    Position("qux") -> 1.0 / 4,
    Position("foo.2") -> 1.0,
    Position("bar.2") -> 1.0,
    Position("baz.2") -> 1.0,
    Position("qux.2") -> 1.0,
    Position("1") -> 1.0 / 2,
    Position("2") -> 1.0 / 4,
    Position("3") -> 1.0 / 6,
    Position("4") -> 1.0 / 8,
    Position("1.2") -> 1.0,
    Position("2.2") -> 1.0,
    Position("3.2") -> 1.0,
    Position("4.2") -> 1.0,
    Position("xyz") -> 1 / 3.14,
    Position("xyz.2") -> 1 / 6.28
  )

  val ext2 = Map(
    Position(1) -> 1.0 / 2,
    Position(2) -> 1.0 / 4,
    Position(3) -> 1.0 / 6,
    Position(4) -> 1.0 / 8
  )

  type W1 = Map[Position[Value[String] :: HNil], Double]
  type W2 = Map[Position[Value[Int] :: HNil], Double]

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

  val result45 = List(
    Cell(Position(1, "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position(2, "xyz"), Content(ContinuousSchema[Double](), 18.84)),
    Cell(Position(3, "xyz"), Content(ContinuousSchema[Double](), 18.84)),
    Cell(Position(4, "xyz"), Content(ContinuousSchema[Double](), 12.56))
  )

  val result46 = List(
    Cell(Position("bar", "xyz"), Content(ContinuousSchema[Double](), 18.84)),
    Cell(Position("baz", "xyz"), Content(ContinuousSchema[Double](), 18.84)),
    Cell(Position("foo", "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("qux", "xyz"), Content(ContinuousSchema[Double](), 12.56))
  )
}

object TestMatrixSummarise {
  case class ExtractWithName[
    P <: HList,
    D <: Nat
  ](
    dim: D,
    name: String
  )(implicit
    ev: Position.IndexConstraints[P, D] { type V <: Value[_] }
  ) extends Extract[P, Map[Position[Value[String] :: HNil], Double], Double] {
    def extract(cell: Cell[P], ext: Map[Position[Value[String] :: HNil], Double]): Option[Double] = ext
      .get(Position(name.format(cell.position(dim).toShortString)))
  }

  case class BadCount[P <: HList, S <: HList]() extends Aggregator[P, S, S] {
    type T = Long
    type O[A] = Multiple[A]

    val tTag = scala.reflect.classTag[T]
    val oTag = scala.reflect.classTag[O[_]]

    def prepare(cell: Cell[P]): Option[T] = Option(1)
    def reduce(lt: T, rt: T): T = lt + rt
    def present(pos: Position[S], t: T): O[Cell[S]] = Multiple(List(Cell(pos, Content(DiscreteSchema[Long](), t))))
  }
}

class TestScalaMatrixSummarise extends TestMatrixSummarise with TestScala {
  import commbank.grimlock.scala.environment.implicits._

  "A Matrix.summarise" should "return its first over aggregates in 2D" in {
    toU(num2)
      .summarise(Over(_0), Default())(Minimum())
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its first along aggregates in 2D" in {
    toU(num2)
      .summarise(Along(_0), Default())(Maximum())
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its second over aggregates in 2D" in {
    toU(num2)
      .summarise(Over(_1), Default())(Maximum())
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its second along aggregates in 2D" in {
    toU(num2)
      .summarise(Along(_1), Default())(Minimum())
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its first over aggregates in 3D" in {
    toU(num3)
      .summarise(Over(_0), Default())(Minimum())
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its first along aggregates in 3D" in {
    toU(num3)
      .summarise(Along(_0), Default())(Maximum())
      .toList.sortBy(_.position) shouldBe result6
  }

  it should "return its second over aggregates in 3D" in {
    toU(num3)
      .summarise(Over(_1), Default())(Maximum())
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its second along aggregates in 3D" in {
    toU(num3)
      .summarise(Along(_1), Default())(Minimum())
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its third over aggregates in 3D" in {
    toU(num3)
      .summarise(Over(_2), Default())(Maximum())
      .toList.sortBy(_.position) shouldBe result9
  }

  it should "return its third along aggregates in 3D" in {
    toU(num3)
      .summarise(Along(_2), Default())(Minimum())
      .toList.sortBy(_.position) shouldBe result10
  }

  it should "return its third over 2D aggregates in 3D" in {
    toU(num3)
      .summarise(Over(_0, _1), Default())(Minimum())
      .toList.sortBy(_.position) shouldBe result10
  }

  it should "return its second along 2D aggregates in 3D" in {
    toU(num3)
      .summarise(Along(_0, _1), Default())(Maximum())
      .toList.sortBy(_.position) shouldBe result9
  }

  it should "return its max over 2D aggregates in 4D" in {
    toU(num4)
      .summarise(Over(_1, _2), Default())(Maximum())
      .toList.sortBy(_.position) shouldBe result45
  }

  it should "returns its max along 2D aggregates in 4D" in {
    toU(num4)
      .summarise(Along(_1, _3), Default())(Maximum())
      .toList.sortBy(_.position) shouldBe result46
  }

  it should "throw an exception for too many single" in {
    a [Exception] shouldBe thrownBy { toU(num2).summarise(Over(_0))(Minimum(), Maximum()) }
  }

  it should "throw an exception for a multiple" in {
    a [Exception] shouldBe thrownBy { toU(num2).summarise(Over(_0))(TestMatrixSummarise.BadCount()) }
  }

  "A Matrix.summariseWithValue" should "return its first over aggregates in 2D" in {
    toU(num2)
      .summariseWithValue(Over(_0), Default())(ext1, WeightedSums(ExtractWithDimension(_0)))
      .toList.sortBy(_.position) shouldBe result11
  }

  it should "return its first along aggregates in 2D" in {
    toU(num2)
      .summariseWithValue(Along(_0), Default())(ext2, WeightedSums(ExtractWithDimension(_1)))
      .toList.sortBy(_.position) shouldBe result12
  }

  it should "return its second over aggregates in 2D" in {
    toU(num2)
      .summariseWithValue(Over(_1), Default())(ext2, WeightedSums(ExtractWithDimension(_1)))
      .toList.sortBy(_.position) shouldBe result13
  }

  it should "return its second along aggregates in 2D" in {
    toU(num2)
      .summariseWithValue(Along(_1), Default())(ext1, WeightedSums(ExtractWithDimension(_0)))
      .toList.sortBy(_.position) shouldBe result14
  }

  it should "return its first over aggregates in 3D" in {
    toU(num3)
      .summariseWithValue(Over(_0), Default())(ext1, WeightedSums(ExtractWithDimension(_0)))
      .toList.sortBy(_.position) shouldBe result15
  }

  it should "return its first along aggregates in 3D" in {
    toU(num3)
      .summariseWithValue(Along(_0), Default())(ext2, WeightedSums(ExtractWithDimension(_1)))
      .toList.sortBy(_.position) shouldBe result16
  }

  it should "return its second over aggregates in 3D" in {
    toU(num3)
      .summariseWithValue(Over(_1), Default())(ext2, WeightedSums(ExtractWithDimension(_1)))
      .toList.sortBy(_.position) shouldBe result17
  }

  it should "return its second along aggregates in 3D" in {
    toU(num3)
      .summariseWithValue(Along(_1), Default())(ext1, WeightedSums(ExtractWithDimension(_0)))
      .toList.sortBy(_.position) shouldBe result18
  }

  it should "return its third over aggregates in 3D" in {
    toU(num3)
      .summariseWithValue(Over(_2), Default())(ext1, WeightedSums(ExtractWithDimension(_2)))
      .toList.sortBy(_.position) shouldBe result19
  }

  it should "return its third along aggregates in 3D" in {
    toU(num3)
      .summariseWithValue(Along(_2), Default())(ext1, WeightedSums(ExtractWithDimension(_2)))
      .toList.sortBy(_.position) shouldBe result20
  }

  it should "throw an exception for too many single" in {
    a [Exception] shouldBe thrownBy { toU(num2).summariseWithValue(Over(_0))(ext1, Minimum(), Maximum()) }
  }

  it should "throw an exception for a multiple" in {
    a [Exception] shouldBe thrownBy {
      toU(num2).summariseWithValue(Over(_0))(ext1, TestMatrixSummarise.BadCount())
    }
  }

  "A Matrix.summariseAndExpand" should "return its first over aggregates in 1D" in {
    toU(num1)
      .summarise(Over(_0), Default())(Minimum().andThenRelocate(_.position.append("min").toOption))
      .toList.sortBy(_.position) shouldBe result21
  }

  it should "return its first along aggregates in 1D" in {
    toU(num1)
      .summarise(Along(_0), Default())(
        List(
          Minimum[P1, S0]().andThenRelocate(_.position.append("min").toOption),
          Maximum[P1, S0]().andThenRelocate(_.position.append("max").toOption)
        )
      )
      .toList.sortBy(_.position) shouldBe result22
  }

  it should "return its first over aggregates in 2D" in {
    toU(num2)
      .summarise(Over(_0), Default())(Minimum().andThenRelocate(_.position.append("min").toOption))
      .toList.sortBy(_.position) shouldBe result23
  }

  it should "return its first along aggregates in 2D" in {
    toU(num2)
      .summarise(Along(_0), Default())(
        Minimum[P2, S22]().andThenRelocate(_.position.append("min").toOption),
        Maximum[P2, S22]().andThenRelocate(_.position.append("max").toOption)
      )
      .toList.sortBy(_.position) shouldBe result24
  }

  it should "return its second over aggregates in 2D" in {
    toU(num2)
      .summarise(Over(_1), Default())(
        List(
          Minimum[P2, S22]().andThenRelocate(_.position.append("min").toOption),
          Maximum[P2, S22]().andThenRelocate(_.position.append("max").toOption)
        )
      )
      .toList.sortBy(_.position) shouldBe result25
  }

  it should "return its second along aggregates in 2D" in {
    toU(num2)
      .summarise(Along(_1), Default())(Minimum().andThenRelocate(_.position.append("min").toOption))
      .toList.sortBy(_.position) shouldBe result26
  }

  it should "return its first over aggregates in 3D" in {
    toU(num3)
      .summarise(Over(_0), Default())(Minimum().andThenRelocate(_.position.append("min").toOption))
      .toList.sortBy(_.position) shouldBe result27
  }

  it should "return its first along aggregates in 3D" in {
    toU(num3)
      .summarise(Along(_0), Default())(
        Minimum[P3, S323]().andThenRelocate(_.position.append("min").toOption),
        Maximum[P3, S323]().andThenRelocate(_.position.append("max").toOption)
      )
      .toList.sortBy(_.position) shouldBe result28
  }

  it should "return its second over aggregates in 3D" in {
    toU(num3)
      .summarise(Over(_1), Default())(
        List(
          Minimum[P3, S32]().andThenRelocate(_.position.append("min").toOption),
          Maximum[P3, S32]().andThenRelocate(_.position.append("max").toOption)
        )
      )
      .toList.sortBy(_.position) shouldBe result29
  }

  it should "return its second along aggregates in 3D" in {
    toU(num3)
      .summarise(Along(_1), Default())(Minimum().andThenRelocate(_.position.append("min").toOption))
      .toList.sortBy(_.position) shouldBe result30
  }

  it should "return its third over aggregates in 3D" in {
    toU(num3)
      .summarise(Over(_2), Default())(
        Minimum[P3, S33]().andThenRelocate(_.position.append("min").toOption),
        Maximum[P3, S33]().andThenRelocate(_.position.append("max").toOption)
      )
      .toList.sortBy(_.position) shouldBe result31
  }

  it should "return its third along aggregates in 3D" in {
    toU(num3)
      .summarise(Along(_2), Default())(Minimum().andThenRelocate(_.position.append("min").toOption))
      .toList.sortBy(_.position) shouldBe result32
  }

  "A Matrix.summariseAndExpandWithValue" should "return its first over aggregates in 1D" in {
    toU(num1)
      .summariseWithValue(Over(_0), Default())(
        ext1,
        WeightedSums(
          ExtractWithDimension[P1, _0, Double]
        ).andThenRelocateWithValue((c: Cell[S11], e: W1) => c.position.append("sum").toOption)
      )
      .toList.sortBy(_.position) shouldBe result33
  }

  it should "return its first along aggregates in 1D" in {
    toU(num1)
      .summariseWithValue(Along(_0), Default())(
        ext1,
        List(
          WeightedSums[P1, S0, W1](
            ExtractWithDimension(_0)
          ).andThenRelocateWithValue((c: Cell[S0], e: W1) => c.position.append("sum.1").toOption),
          WeightedSums[P1, S0, W1](
            TestMatrixSummarise.ExtractWithName(_0, "%1$s.2")
          ).andThenRelocateWithValue((c: Cell[S0], e: W1) => c.position.append("sum.2").toOption)
        )
      )
      .toList.sortBy(_.position) shouldBe result34
  }

  it should "return its first over aggregates in 2D" in {
    toU(num2)
      .summariseWithValue(Over(_0), Default())(
        ext1,
        WeightedSums(
          ExtractWithDimension[P2, _0, Double]
        ).andThenRelocateWithValue((c: Cell[S21], e: W1) => c.position.append("sum").toOption)
      )
      .toList.sortBy(_.position) shouldBe result35
  }

  it should "return its first along aggregates in 2D" in {
    toU(num2)
      .summariseWithValue(Along(_0), Default())(
        ext1,
        WeightedSums[P2, S22, W1](
          TestMatrixSummarise.ExtractWithName(_1, "%1$s")
        ).andThenRelocateWithValue((c: Cell[S22], e: W1) => c.position.append("sum.1").toOption),
        WeightedSums[P2, S22, W1](
          TestMatrixSummarise.ExtractWithName(_0, "%1$s.2")
        ).andThenRelocateWithValue((c: Cell[S22], e: W1) => c.position.append("sum.2").toOption)
      )
      .toList.sortBy(_.position) shouldBe result36
  }

  it should "return its second over aggregates in 2D" in {
    toU(num2)
      .summariseWithValue(Over(_1), Default())(
        ext1,
        List(
          WeightedSums[P2, S22, W1](
            TestMatrixSummarise.ExtractWithName(_1, "%1$s")
          ).andThenRelocateWithValue((c: Cell[S22], e: W1) => c.position.append("sum.1").toOption),
          WeightedSums[P2, S22, W1](
            TestMatrixSummarise.ExtractWithName(_1, "%1$s.2")
          ).andThenRelocateWithValue((c: Cell[S22], e: W1) => c.position.append("sum.2").toOption)
        )
      )
      .toList.sortBy(_.position) shouldBe result37
  }

  it should "return its second along aggregates in 2D" in {
    toU(num2)
      .summariseWithValue(Along(_1), Default())(
        ext1,
        WeightedSums(
          ExtractWithDimension[P2, _0, Double]
        ).andThenRelocateWithValue((c: Cell[S21], e: W1) => c.position.append("sum").toOption)
      )
      .toList.sortBy(_.position) shouldBe result38
  }

  it should "return its first over aggregates in 3D" in {
    toU(num3)
      .summariseWithValue(Over(_0), Default())(
        ext1,
        WeightedSums(
          ExtractWithDimension[P3, _0, Double]
        ).andThenRelocateWithValue((c: Cell[S31], e: W1) => c.position.append("sum").toOption)
      )
      .toList.sortBy(_.position) shouldBe result39
  }

  it should "return its first along aggregates in 3D" in {
    toU(num3)
      .summariseWithValue(Along(_0), Default())(
        ext1,
        WeightedSums[P3, S323, W1](
          TestMatrixSummarise.ExtractWithName(_1, "%1$s")
        ).andThenRelocateWithValue((c: Cell[S323], e: W1) => c.position.append("sum.1").toOption),
        WeightedSums[P3, S323, W1](
          TestMatrixSummarise.ExtractWithName(_1, "%1$s.2")
        ).andThenRelocateWithValue((c: Cell[S323], e: W1) => c.position.append("sum.2").toOption)
      )
      .toList.sortBy(_.position) shouldBe result40
  }

  it should "return its second over aggregates in 3D" in {
    toU(num3)
      .summariseWithValue(Over(_1), Default())(
        ext1,
        List(
          WeightedSums[P3, S32, W1](
            TestMatrixSummarise.ExtractWithName(_1, "%1$s")
          ).andThenRelocateWithValue((c: Cell[S32], e: W1) => c.position.append("sum.1").toOption),
          WeightedSums[P3, S32, W1](
            TestMatrixSummarise.ExtractWithName(_1, "%1$s.2")
          ).andThenRelocateWithValue((c: Cell[S32], e: W1) => c.position.append("sum.2").toOption)
        )
      )
      .toList.sortBy(_.position) shouldBe result41
  }

  it should "return its second along aggregates in 3D" in {
    toU(num3)
      .summariseWithValue(Along(_1), Default())(
        ext1,
        WeightedSums[P3, S313, W1](
          ExtractWithDimension(_0)
        ).andThenRelocateWithValue((c: Cell[S313], e: W1) => c.position.append("sum").toOption)
      )
      .toList.sortBy(_.position) shouldBe result42
  }

  it should "return its third over aggregates in 3D" in {
    toU(num3)
      .summariseWithValue(Over(_2), Default())(
        ext1,
        WeightedSums[P3, S33, W1](
          ExtractWithDimension(_2)
        ).andThenRelocateWithValue((c: Cell[S33], e: W1) => c.position.append("sum.1").toOption),
        WeightedSums[P3, S33, W1](
          TestMatrixSummarise.ExtractWithName(_2, "%1$s.2")
        ).andThenRelocateWithValue((c: Cell[S33], e: W1) => c.position.append("sum.2").toOption)
      )
      .toList.sortBy(_.position) shouldBe result43
  }

  it should "return its third along aggregates in 3D" in {
    toU(num3)
      .summariseWithValue(Along(_2), Default())(
        ext1,
        WeightedSums[P3, S312, W1](
          ExtractWithDimension(_2)
        ).andThenRelocateWithValue((c: Cell[S312], e: W1) => c.position.append("sum").toOption)
      )
      .toList.sortBy(_.position) shouldBe result44
  }
}

class TestScaldingMatrixSummarise extends TestMatrixSummarise with TestScalding {
  import commbank.grimlock.scalding.environment.implicits._

  "A Matrix.summarise" should "return its first over aggregates in 2D" in {
    toU(num2)
      .summarise(Over(_0), Default())(Minimum())
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its first along aggregates in 2D" in {
    toU(num2)
      .summarise(Along(_0), Default(12))(Maximum())
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its second over aggregates in 2D" in {
    toU(num2)
      .summarise(Over(_1), Default())(Maximum())
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its second along aggregates in 2D" in {
    toU(num2)
      .summarise(Along(_1), Default(12))(Minimum())
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its first over aggregates in 3D" in {
    toU(num3)
      .summarise(Over(_0), Default())(Minimum())
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its first along aggregates in 3D" in {
    toU(num3)
      .summarise(Along(_0), Default(12))(Maximum())
      .toList.sortBy(_.position) shouldBe result6
  }

  it should "return its second over aggregates in 3D" in {
    toU(num3)
      .summarise(Over(_1), Default())(Maximum())
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its second along aggregates in 3D" in {
    toU(num3)
      .summarise(Along(_1), Default(12))(Minimum())
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its third over aggregates in 3D" in {
    toU(num3)
      .summarise(Over(_2), Default())(Maximum())
      .toList.sortBy(_.position) shouldBe result9
  }

  it should "return its third along aggregates in 3D" in {
    toU(num3)
      .summarise(Along(_2), Default(12))(Minimum())
      .toList.sortBy(_.position) shouldBe result10
  }

  it should "return its third over 2D aggregates in 3D" in {
    toU(num3)
      .summarise(Over(_0, _1), Default())(Minimum())
      .toList.sortBy(_.position) shouldBe result10
  }

  it should "return its second along 2D aggregates in 3D" in {
    toU(num3)
      .summarise(Along(_0, _1), Default(12))(Maximum())
      .toList.sortBy(_.position) shouldBe result9
  }

  it should "returns its max along 2D aggregates in 4D" in {
    toU(num4)
      .summarise(Along(_1, _3), Default(12))(Maximum())
      .toList.sortBy(_.position) shouldBe result46
  }

  it should "throw an exception for too many single" in {
    a [Exception] shouldBe thrownBy { toU(num2).summarise(Over(_0))(Minimum(), Maximum()) }
  }

  it should "throw an exception for a multiple" in {
    a [Exception] shouldBe thrownBy { toU(num2).summarise(Over(_0))(TestMatrixSummarise.BadCount()) }
  }

  "A Matrix.summariseWithValue" should "return its first over aggregates in 2D" in {
    toU(num2)
      .summariseWithValue(Over(_0), Default())(ValuePipe(ext1), WeightedSums(ExtractWithDimension(_0)))
      .toList.sortBy(_.position) shouldBe result11
  }

  it should "return its first along aggregates in 2D" in {
    toU(num2)
      .summariseWithValue(Along(_0), Default(12))(ValuePipe(ext2), WeightedSums(ExtractWithDimension(_1)))
      .toList.sortBy(_.position) shouldBe result12
  }

  it should "return its second over aggregates in 2D" in {
    toU(num2)
      .summariseWithValue(Over(_1), Default())(ValuePipe(ext2), WeightedSums(ExtractWithDimension(_1)))
      .toList.sortBy(_.position) shouldBe result13
  }

  it should "return its second along aggregates in 2D" in {
    toU(num2)
      .summariseWithValue(Along(_1), Default(12))(ValuePipe(ext1), WeightedSums(ExtractWithDimension(_0)))
      .toList.sortBy(_.position) shouldBe result14
  }

  it should "return its first over aggregates in 3D" in {
    toU(num3)
      .summariseWithValue(Over(_0), Default())(ValuePipe(ext1), WeightedSums(ExtractWithDimension(_0)))
      .toList.sortBy(_.position) shouldBe result15
  }

  it should "return its first along aggregates in 3D" in {
    toU(num3)
      .summariseWithValue(Along(_0), Default(12))(ValuePipe(ext2), WeightedSums(ExtractWithDimension(_1)))
      .toList.sortBy(_.position) shouldBe result16
  }

  it should "return its second over aggregates in 3D" in {
    toU(num3)
      .summariseWithValue(Over(_1), Default())(ValuePipe(ext2), WeightedSums(ExtractWithDimension(_1)))
      .toList.sortBy(_.position) shouldBe result17
  }

  it should "return its second along aggregates in 3D" in {
    toU(num3)
      .summariseWithValue(Along(_1), Default(12))(ValuePipe(ext1), WeightedSums(ExtractWithDimension(_0)))
      .toList.sortBy(_.position) shouldBe result18
  }

  it should "return its third over aggregates in 3D" in {
    toU(num3)
      .summariseWithValue(Over(_2), Default())(ValuePipe(ext1), WeightedSums(ExtractWithDimension(_2)))
      .toList.sortBy(_.position) shouldBe result19
  }

  it should "return its third along aggregates in 3D" in {
    toU(num3)
      .summariseWithValue(Along(_2), Default(12))(ValuePipe(ext1), WeightedSums(ExtractWithDimension(_2)))
      .toList.sortBy(_.position) shouldBe result20
  }

  it should "throw an exception for too many single" in {
    a [Exception] shouldBe thrownBy { toU(num2).summariseWithValue(Over(_0))(ValuePipe(ext1), Minimum(), Maximum()) }
  }

  it should "throw an exception for a multiple" in {
    a [Exception] shouldBe thrownBy {
      toU(num2).summariseWithValue(Over(_0))(ValuePipe(ext1), TestMatrixSummarise.BadCount())
    }
  }

  "A Matrix.summariseAndExpand" should "return its first over aggregates in 1D" in {
    toU(num1)
      .summarise(Over(_0), Default())(Minimum().andThenRelocate(_.position.append("min").toOption))
      .toList.sortBy(_.position) shouldBe result21
  }

  it should "return its first along aggregates in 1D" in {
    toU(num1)
      .summarise(Along(_0), Default(12))(
        List(
          Minimum[P1, S0]().andThenRelocate(_.position.append("min").toOption),
          Maximum[P1, S0]().andThenRelocate(_.position.append("max").toOption)
        )
      )
      .toList.sortBy(_.position) shouldBe result22
  }

  it should "return its first over aggregates in 2D" in {
    toU(num2)
      .summarise(Over(_0), Default())(Minimum().andThenRelocate(_.position.append("min").toOption))
      .toList.sortBy(_.position) shouldBe result23
  }

  it should "return its first along aggregates in 2D" in {
    toU(num2)
      .summarise(Along(_0), Default(12))(
        Minimum[P2, S22]().andThenRelocate(_.position.append("min").toOption),
        Maximum[P2, S22]().andThenRelocate(_.position.append("max").toOption)
      )
      .toList.sortBy(_.position) shouldBe result24
  }

  it should "return its second over aggregates in 2D" in {
    toU(num2)
      .summarise(Over(_1), Default())(
        List(
          Minimum[P2, S22]().andThenRelocate(_.position.append("min").toOption),
          Maximum[P2, S22]().andThenRelocate(_.position.append("max").toOption)
        )
      )
      .toList.sortBy(_.position) shouldBe result25
  }

  it should "return its second along aggregates in 2D" in {
    toU(num2)
      .summarise(Along(_1), Default(12))(Minimum().andThenRelocate(_.position.append("min").toOption))
      .toList.sortBy(_.position) shouldBe result26
  }

  it should "return its first over aggregates in 3D" in {
    toU(num3)
      .summarise(Over(_0), Default())(Minimum().andThenRelocate(_.position.append("min").toOption))
      .toList.sortBy(_.position) shouldBe result27
  }

  it should "return its first along aggregates in 3D" in {
    toU(num3)
      .summarise(Along(_0), Default(12))(
        Minimum[P3, S323]().andThenRelocate(_.position.append("min").toOption),
        Maximum[P3, S323]().andThenRelocate(_.position.append("max").toOption)
      )
      .toList.sortBy(_.position) shouldBe result28
  }

  it should "return its second over aggregates in 3D" in {
    toU(num3)
      .summarise(Over(_1), Default())(
        List(
          Minimum[P3, S32]().andThenRelocate(_.position.append("min").toOption),
          Maximum[P3, S32]().andThenRelocate(_.position.append("max").toOption)
        )
      )
      .toList.sortBy(_.position) shouldBe result29
  }

  it should "return its second along aggregates in 3D" in {
    toU(num3)
      .summarise(Along(_1), Default(12))(Minimum().andThenRelocate(_.position.append("min").toOption))
      .toList.sortBy(_.position) shouldBe result30
  }

  it should "return its third over aggregates in 3D" in {
    toU(num3)
      .summarise(Over(_2), Default())(
        Minimum[P3, S33]().andThenRelocate(_.position.append("min").toOption),
        Maximum[P3, S33]().andThenRelocate(_.position.append("max").toOption)
      )
      .toList.sortBy(_.position) shouldBe result31
  }

  it should "return its third along aggregates in 3D" in {
    toU(num3)
      .summarise(Along(_2), Default(12))(Minimum().andThenRelocate(_.position.append("min").toOption))
      .toList.sortBy(_.position) shouldBe result32
  }

  "A Matrix.summariseAndExpandWithValue" should "return its first over aggregates in 1D" in {
    toU(num1)
      .summariseWithValue(Over(_0), Default())(
        ValuePipe(ext1),
        WeightedSums(
          ExtractWithDimension[P1, _0, Double]
        ).andThenRelocateWithValue((c: Cell[S11], e: W1) => c.position.append("sum").toOption)
      )
      .toList.sortBy(_.position) shouldBe result33
  }

  it should "return its first along aggregates in 1D" in {
    toU(num1)
      .summariseWithValue(Along(_0), Default(12))(
        ValuePipe(ext1),
        List(
          WeightedSums[P1, S0, W1](
            ExtractWithDimension(_0)
          ).andThenRelocateWithValue((c: Cell[S0], e: W1) => c.position.append("sum.1").toOption),
          WeightedSums[P1, S0, W1](
            TestMatrixSummarise.ExtractWithName(_0, "%1$s.2")
          ).andThenRelocateWithValue((c: Cell[S0], e: W1) => c.position.append("sum.2").toOption)
        )
      )
      .toList.sortBy(_.position) shouldBe result34
  }

  it should "return its first over aggregates in 2D" in {
    toU(num2)
      .summariseWithValue(Over(_0), Default())(
        ValuePipe(ext1),
        WeightedSums(
          ExtractWithDimension[P2, _0, Double]
        ).andThenRelocateWithValue((c: Cell[S21], e: W1) => c.position.append("sum").toOption)
      )
      .toList.sortBy(_.position) shouldBe result35
  }

  it should "return its first along aggregates in 2D" in {
    toU(num2)
      .summariseWithValue(Along(_0), Default(12))(
        ValuePipe(ext1),
        WeightedSums[P2, S22, W1](
          TestMatrixSummarise.ExtractWithName(_1, "%1$s")
        ).andThenRelocateWithValue((c: Cell[S22], e: W1) => c.position.append("sum.1").toOption),
        WeightedSums[P2, S22, W1](
          TestMatrixSummarise.ExtractWithName(_0, "%1$s.2")
        ).andThenRelocateWithValue((c: Cell[S22], e: W1) => c.position.append("sum.2").toOption)
      )
      .toList.sortBy(_.position) shouldBe result36
  }

  it should "return its second over aggregates in 2D" in {
    toU(num2)
      .summariseWithValue(Over(_1), Default())(
        ValuePipe(ext1),
        List(
          WeightedSums[P2, S22, W1](
            TestMatrixSummarise.ExtractWithName(_1, "%1$s")
          ).andThenRelocateWithValue((c: Cell[S22], e: W1) => c.position.append("sum.1").toOption),
          WeightedSums[P2, S22, W1](
            TestMatrixSummarise.ExtractWithName(_1, "%1$s.2")
          ).andThenRelocateWithValue((c: Cell[S22], e: W1) => c.position.append("sum.2").toOption)
        )
      )
      .toList.sortBy(_.position) shouldBe result37
  }

  it should "return its second along aggregates in 2D" in {
    toU(num2)
      .summariseWithValue(Along(_1), Default(12))(
        ValuePipe(ext1),
        WeightedSums(
          ExtractWithDimension[P2, _0, Double]
        ).andThenRelocateWithValue((c: Cell[S21], e: W1) => c.position.append("sum").toOption)
      )
      .toList.sortBy(_.position) shouldBe result38
  }

  it should "return its first over aggregates in 3D" in {
    toU(num3)
      .summariseWithValue(Over(_0), Default())(
        ValuePipe(ext1),
        WeightedSums(
          ExtractWithDimension[P3, _0, Double]
        ).andThenRelocateWithValue((c: Cell[S31], e: W1) => c.position.append("sum").toOption)
      )
      .toList.sortBy(_.position) shouldBe result39
  }

  it should "return its first along aggregates in 3D" in {
    toU(num3)
      .summariseWithValue(Along(_0), Default(12))(
        ValuePipe(ext1),
        WeightedSums[P3, S323, W1](
          TestMatrixSummarise.ExtractWithName(_1, "%1$s")
        ).andThenRelocateWithValue((c: Cell[S323], e: W1) => c.position.append("sum.1").toOption),
        WeightedSums[P3, S323, W1](
          TestMatrixSummarise.ExtractWithName(_1, "%1$s.2")
        ).andThenRelocateWithValue((c: Cell[S323], e: W1) => c.position.append("sum.2").toOption)
      )
      .toList.sortBy(_.position) shouldBe result40
  }

  it should "return its second over aggregates in 3D" in {
    toU(num3)
      .summariseWithValue(Over(_1), Default())(
        ValuePipe(ext1),
        List(
          WeightedSums[P3, S32, W1](
            TestMatrixSummarise.ExtractWithName(_1, "%1$s")
          ).andThenRelocateWithValue((c: Cell[S32], e: W1) => c.position.append("sum.1").toOption),
          WeightedSums[P3, S32, W1](
            TestMatrixSummarise.ExtractWithName(_1, "%1$s.2")
          ).andThenRelocateWithValue((c: Cell[S32], e: W1) => c.position.append("sum.2").toOption)
        )
      )
      .toList.sortBy(_.position) shouldBe result41
  }

  it should "return its second along aggregates in 3D" in {
    toU(num3)
      .summariseWithValue(Along(_1), Default(12))(
        ValuePipe(ext1),
        WeightedSums[P3, S313, W1](
          ExtractWithDimension(_0)
        ).andThenRelocateWithValue((c: Cell[S313], e: W1) => c.position.append("sum").toOption)
      )
      .toList.sortBy(_.position) shouldBe result42
  }

  it should "return its third over aggregates in 3D" in {
    toU(num3)
      .summariseWithValue(Over(_2), Default())(
        ValuePipe(ext1),
        WeightedSums[P3, S33, W1](
          ExtractWithDimension(_2)
        ).andThenRelocateWithValue((c: Cell[S33], e: W1) => c.position.append("sum.1").toOption),
        WeightedSums[P3, S33, W1](
          TestMatrixSummarise.ExtractWithName(_2, "%1$s.2")
        ).andThenRelocateWithValue((c: Cell[S33], e: W1) => c.position.append("sum.2").toOption)
      )
      .toList.sortBy(_.position) shouldBe result43
  }

  it should "return its third along aggregates in 3D" in {
    toU(num3)
      .summariseWithValue(Along(_2), Default(12))(
        ValuePipe(ext1),
        WeightedSums[P3, S312, W1](
          ExtractWithDimension(_2)
        ).andThenRelocateWithValue((c: Cell[S312], e: W1) => c.position.append("sum").toOption)
      )
      .toList.sortBy(_.position) shouldBe result44
  }
}

class TestSparkMatrixSummarise extends TestMatrixSummarise with TestSpark {
  import commbank.grimlock.spark.environment.implicits._

  "A Matrix.summarise" should "return its first over aggregates in 2D" in {
    toU(num2)
      .summarise(Over(_0), Default())(Minimum())
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its first along aggregates in 2D" in {
    toU(num2)
      .summarise(Along(_0), Default(12))(Maximum())
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its second over aggregates in 2D" in {
    toU(num2)
      .summarise(Over(_1), Default())(Maximum())
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its second along aggregates in 2D" in {
    toU(num2)
      .summarise(Along(_1), Default(12))(Minimum())
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its first over aggregates in 3D" in {
    toU(num3)
      .summarise(Over(_0), Default())(Minimum())
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its first along aggregates in 3D" in {
    toU(num3)
      .summarise(Along(_0), Default(12))(Maximum())
      .toList.sortBy(_.position) shouldBe result6
  }

  it should "return its second over aggregates in 3D" in {
    toU(num3)
      .summarise(Over(_1), Default())(Maximum())
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its second along aggregates in 3D" in {
    toU(num3)
      .summarise(Along(_1), Default(12))(Minimum())
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its third over aggregates in 3D" in {
    toU(num3)
      .summarise(Over(_2), Default())(Maximum())
      .toList.sortBy(_.position) shouldBe result9
  }

  it should "return its third along aggregates in 3D" in {
    toU(num3)
      .summarise(Along(_2), Default(12))(Minimum())
      .toList.sortBy(_.position) shouldBe result10
  }

  it should "return its third over 2D aggregates in 3D" in {
    toU(num3)
      .summarise(Over(_0, _1), Default())(Minimum())
      .toList.sortBy(_.position) shouldBe result10
  }

  it should "return its second along 2D aggregates in 3D" in {
    toU(num3)
      .summarise(Along(_0, _1), Default(12))(Maximum())
      .toList.sortBy(_.position) shouldBe result9
  }

  it should "returns its max along 2D aggregates in 4D" in {
    toU(num4)
      .summarise(Along(_1, _3), Default(12))(Maximum())
      .toList.sortBy(_.position) shouldBe result46
  }

  it should "throw an exception for too many single" in {
    a [Exception] shouldBe thrownBy { toU(num2).summarise(Over(_0))(Minimum(), Maximum()) }
  }

  it should "throw an exception for a multiple" in {
    a [Exception] shouldBe thrownBy { toU(num2).summarise(Over(_0))(TestMatrixSummarise.BadCount()) }
  }

  "A Matrix.summariseWithValue" should "return its first over aggregates in 2D" in {
    toU(num2)
      .summariseWithValue(Over(_0), Default())(ext1, WeightedSums(ExtractWithDimension(_0)))
      .toList.sortBy(_.position) shouldBe result11
  }

  it should "return its first along aggregates in 2D" in {
    toU(num2)
      .summariseWithValue(Along(_0), Default(12))(ext2, WeightedSums(ExtractWithDimension(_1)))
      .toList.sortBy(_.position) shouldBe result12
  }

  it should "return its second over aggregates in 2D" in {
    toU(num2)
      .summariseWithValue(Over(_1), Default())(ext2, WeightedSums(ExtractWithDimension(_1)))
      .toList.sortBy(_.position) shouldBe result13
  }

  it should "return its second along aggregates in 2D" in {
    toU(num2)
      .summariseWithValue(Along(_1), Default(12))(ext1, WeightedSums(ExtractWithDimension(_0)))
      .toList.sortBy(_.position) shouldBe result14
  }

  it should "return its first over aggregates in 3D" in {
    toU(num3)
      .summariseWithValue(Over(_0), Default())(ext1, WeightedSums(ExtractWithDimension(_0)))
      .toList.sortBy(_.position) shouldBe result15
  }

  it should "return its first along aggregates in 3D" in {
    toU(num3)
      .summariseWithValue(Along(_0), Default(12))(ext2, WeightedSums(ExtractWithDimension(_1)))
      .toList.sortBy(_.position) shouldBe result16
  }

  it should "return its second over aggregates in 3D" in {
    toU(num3)
      .summariseWithValue(Over(_1), Default())(ext2, WeightedSums(ExtractWithDimension(_1)))
      .toList.sortBy(_.position) shouldBe result17
  }

  it should "return its second along aggregates in 3D" in {
    toU(num3)
      .summariseWithValue(Along(_1), Default(12))(ext1, WeightedSums(ExtractWithDimension(_0)))
      .toList.sortBy(_.position) shouldBe result18
  }

  it should "return its third over aggregates in 3D" in {
    toU(num3)
      .summariseWithValue(Over(_2), Default())(ext1, WeightedSums(ExtractWithDimension(_2)))
      .toList.sortBy(_.position) shouldBe result19
  }

  it should "return its third along aggregates in 3D" in {
    toU(num3)
      .summariseWithValue(Along(_2), Default(12))(ext1, WeightedSums(ExtractWithDimension(_2)))
      .toList.sortBy(_.position) shouldBe result20
  }

  it should "throw an exception for too many single" in {
    a [Exception] shouldBe thrownBy { toU(num2).summariseWithValue(Over(_0))(ext1, Minimum(), Maximum()) }
  }

  it should "throw an exception for a multiple" in {
    a [Exception] shouldBe thrownBy { toU(num2).summariseWithValue(Over(_0))(ext1, TestMatrixSummarise.BadCount()) }
  }

  "A Matrix.summariseAndExpand" should "return its first over aggregates in 1D" in {
    toU(num1)
      .summarise(Over(_0), Default())(Minimum().andThenRelocate(_.position.append("min").toOption))
      .toList.sortBy(_.position) shouldBe result21
  }

  it should "return its first along aggregates in 1D" in {
    toU(num1)
      .summarise(Along(_0), Default(12))(
        List(
          Minimum[P1, S0]().andThenRelocate(_.position.append("min").toOption),
          Maximum[P1, S0]().andThenRelocate(_.position.append("max").toOption)
        )
      )
      .toList.sortBy(_.position) shouldBe result22
  }

  it should "return its first over aggregates in 2D" in {
    toU(num2)
      .summarise(Over(_0), Default())(Minimum().andThenRelocate(_.position.append("min").toOption))
      .toList.sortBy(_.position) shouldBe result23
  }

  it should "return its first along aggregates in 2D" in {
    toU(num2)
      .summarise(Along(_0), Default(12))(
        Minimum[P2, S22]().andThenRelocate(_.position.append("min").toOption),
        Maximum[P2, S22]().andThenRelocate(_.position.append("max").toOption)
      )
      .toList.sortBy(_.position) shouldBe result24
  }

  it should "return its second over aggregates in 2D" in {
    toU(num2)
      .summarise(Over(_1), Default())(
        List(
          Minimum[P2, S22]().andThenRelocate(_.position.append("min").toOption),
          Maximum[P2, S22]().andThenRelocate(_.position.append("max").toOption)
        )
      )
      .toList.sortBy(_.position) shouldBe result25
  }

  it should "return its second along aggregates in 2D" in {
    toU(num2)
      .summarise(Along(_1), Default(12))(Minimum().andThenRelocate(_.position.append("min").toOption))
      .toList.sortBy(_.position) shouldBe result26
  }

  it should "return its first over aggregates in 3D" in {
    toU(num3)
      .summarise(Over(_0), Default())(Minimum().andThenRelocate(_.position.append("min").toOption))
      .toList.sortBy(_.position) shouldBe result27
  }

  it should "return its first along aggregates in 3D" in {
    toU(num3)
      .summarise(Along(_0), Default(12))(
        Minimum[P3, S323]().andThenRelocate(_.position.append("min").toOption),
        Maximum[P3, S323]().andThenRelocate(_.position.append("max").toOption)
      )
      .toList.sortBy(_.position) shouldBe result28
  }

  it should "return its second over aggregates in 3D" in {
    toU(num3)
      .summarise(Over(_1), Default())(
        List(
          Minimum[P3, S32]().andThenRelocate(_.position.append("min").toOption),
          Maximum[P3, S32]().andThenRelocate(_.position.append("max").toOption)
        )
      )
      .toList.sortBy(_.position) shouldBe result29
  }

  it should "return its second along aggregates in 3D" in {
    toU(num3)
      .summarise(Along(_1), Default(12))(Minimum().andThenRelocate(_.position.append("min").toOption))
      .toList.sortBy(_.position) shouldBe result30
  }

  it should "return its third over aggregates in 3D" in {
    toU(num3)
      .summarise(Over(_2), Default())(
        Minimum[P3, S33]().andThenRelocate(_.position.append("min").toOption),
        Maximum[P3, S33]().andThenRelocate(_.position.append("max").toOption)
      )
      .toList.sortBy(_.position) shouldBe result31
  }

  it should "return its third along aggregates in 3D" in {
    toU(num3)
      .summarise(Along(_2), Default(12))(Minimum().andThenRelocate(_.position.append("min").toOption))
      .toList.sortBy(_.position) shouldBe result32
  }

  "A Matrix.summariseAndExpandWithValue" should "return its first over aggregates in 1D" in {
    toU(num1)
      .summariseWithValue(Over(_0), Default())(
        ext1,
        WeightedSums(
          ExtractWithDimension[P1, _0, Double]
        ).andThenRelocateWithValue((c: Cell[S11], e: W1) => c.position.append("sum").toOption)
      )
      .toList.sortBy(_.position) shouldBe result33
  }

  it should "return its first along aggregates in 1D" in {
    toU(num1)
      .summariseWithValue(Along(_0), Default(12))(
        ext1,
        List(
          WeightedSums[P1, S0, W1](
            ExtractWithDimension(_0)
          ).andThenRelocateWithValue((c: Cell[S0], e: W1) => c.position.append("sum.1").toOption),
          WeightedSums[P1, S0, W1](
            TestMatrixSummarise.ExtractWithName(_0, "%1$s.2")
          ).andThenRelocateWithValue((c: Cell[S0], e: W1) => c.position.append("sum.2").toOption)
        )
      )
      .toList.sortBy(_.position) shouldBe result34
  }

  it should "return its first over aggregates in 2D" in {
    toU(num2)
      .summariseWithValue(Over(_0), Default())(
        ext1,
        WeightedSums(
          ExtractWithDimension[P2, _0, Double]
        ).andThenRelocateWithValue((c: Cell[S21], e: W1) => c.position.append("sum").toOption)
      )
      .toList.sortBy(_.position) shouldBe result35
  }

  it should "return its first along aggregates in 2D" in {
    toU(num2)
      .summariseWithValue(Along(_0), Default(12))(
        ext1,
        WeightedSums[P2, S22, W1](
          TestMatrixSummarise.ExtractWithName(_1, "%1$s")
        ).andThenRelocateWithValue((c: Cell[S22], e: W1) => c.position.append("sum.1").toOption),
        WeightedSums[P2, S22, W1](
          TestMatrixSummarise.ExtractWithName(_0, "%1$s.2")
        ).andThenRelocateWithValue((c: Cell[S22], e: W1) => c.position.append("sum.2").toOption)
      )
      .toList.sortBy(_.position) shouldBe result36
  }

  it should "return its second over aggregates in 2D" in {
    toU(num2)
      .summariseWithValue(Over(_1), Default())(
        ext1,
        List(
          WeightedSums[P2, S22, W1](
            TestMatrixSummarise.ExtractWithName(_1, "%1$s")
          ).andThenRelocateWithValue((c: Cell[S22], e: W1) => c.position.append("sum.1").toOption),
          WeightedSums[P2, S22, W1](
            TestMatrixSummarise.ExtractWithName(_1, "%1$s.2")
          ).andThenRelocateWithValue((c: Cell[S22], e: W1) => c.position.append("sum.2").toOption)
        )
      )
      .toList.sortBy(_.position) shouldBe result37
  }

  it should "return its second along aggregates in 2D" in {
    toU(num2)
      .summariseWithValue(Along(_1), Default(12))(
        ext1,
        WeightedSums(
          ExtractWithDimension[P2, _0, Double]
        ).andThenRelocateWithValue((c: Cell[S21], e: W1) => c.position.append("sum").toOption)
      )
      .toList.sortBy(_.position) shouldBe result38
  }

  it should "return its first over aggregates in 3D" in {
    toU(num3)
      .summariseWithValue(Over(_0), Default())(
        ext1,
        WeightedSums(
          ExtractWithDimension[P3, _0, Double]
        ).andThenRelocateWithValue((c: Cell[S31], e: W1) => c.position.append("sum").toOption)
      )
      .toList.sortBy(_.position) shouldBe result39
  }

  it should "return its first along aggregates in 3D" in {
    toU(num3)
      .summariseWithValue(Along(_0), Default(12))(
        ext1,
        WeightedSums[P3, S323, W1](
          TestMatrixSummarise.ExtractWithName(_1, "%1$s")
        ).andThenRelocateWithValue((c: Cell[S323], e: W1) => c.position.append("sum.1").toOption),
        WeightedSums[P3, S323, W1](
          TestMatrixSummarise.ExtractWithName(_1, "%1$s.2")
        ).andThenRelocateWithValue((c: Cell[S323], e: W1) => c.position.append("sum.2").toOption)
      )
      .toList.sortBy(_.position) shouldBe result40
  }

  it should "return its second over aggregates in 3D" in {
    toU(num3)
      .summariseWithValue(Over(_1), Default())(
        ext1,
        List(
          WeightedSums[P3, S32, W1](
            TestMatrixSummarise.ExtractWithName(_1, "%1$s")
          ).andThenRelocateWithValue((c: Cell[S32], e: W1) => c.position.append("sum.1").toOption),
          WeightedSums[P3, S32, W1](
            TestMatrixSummarise.ExtractWithName(_1, "%1$s.2")
          ).andThenRelocateWithValue((c: Cell[S32], e: W1) => c.position.append("sum.2").toOption)
        )
      )
      .toList.sortBy(_.position) shouldBe result41
  }

  it should "return its second along aggregates in 3D" in {
    toU(num3)
      .summariseWithValue(Along(_1), Default(12))(
        ext1,
        WeightedSums[P3, S313, W1](
          ExtractWithDimension(_0)
        ).andThenRelocateWithValue((c: Cell[S313], e: W1) => c.position.append("sum").toOption)
      )
      .toList.sortBy(_.position) shouldBe result42
  }

  it should "return its third over aggregates in 3D" in {
    toU(num3)
      .summariseWithValue(Over(_2), Default())(
        ext1,
        WeightedSums[P3, S33, W1](
          ExtractWithDimension(_2)
        ).andThenRelocateWithValue((c: Cell[S33], e: W1) => c.position.append("sum.1").toOption),
        WeightedSums[P3, S33, W1](
          TestMatrixSummarise.ExtractWithName(_2, "%1$s.2")
        ).andThenRelocateWithValue((c: Cell[S33], e: W1) => c.position.append("sum.2").toOption)
      )
      .toList.sortBy(_.position) shouldBe result43
  }

  it should "return its third along aggregates in 3D" in {
    toU(num3)
      .summariseWithValue(Along(_2), Default(12))(
        ext1,
        WeightedSums[P3, S312, W1](
          ExtractWithDimension(_2)
        ).andThenRelocateWithValue((c: Cell[S312], e: W1) => c.position.append("sum").toOption)
      )
      .toList.sortBy(_.position) shouldBe result44
  }
}

