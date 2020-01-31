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
import commbank.grimlock.framework.encoding._
import commbank.grimlock.framework.environment.implicits._
import commbank.grimlock.framework.environment.tuner._
import commbank.grimlock.framework.metadata._
import commbank.grimlock.framework.pairwise._
import commbank.grimlock.framework.position._

import commbank.grimlock.library.pairwise._

import com.twitter.scalding.typed.{ TypedPipe, ValuePipe }

import shapeless.HList
import shapeless.nat.{ _0, _1, _2 }

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

  def plus[
    P <: HList,
    S <: HList,
    R <: HList
  ](
    slice: Slice[P, S, R]
  )(implicit
    ev1: Value.Box[String],
    ev2: Position.PrependConstraints[R, Value[String]]
  ) = Locate.PrependPairwiseSelectedStringToRemainder[P, S, R](slice, "(%1$s+%2$s)", false, "|")

  def minus[
    P <: HList,
    S <: HList,
    R <: HList
  ](
    slice: Slice[P, S, R]
  )(implicit
    ev1: Value.Box[String],
    ev2: Position.PrependConstraints[R, Value[String]]
  ) = Locate.PrependPairwiseSelectedStringToRemainder[P, S, R](slice, "(%1$s-%2$s)", false, "|")
}

object TestMatrixPairwise {
  case class PlusX[P <: HList, Q <: HList](pos: Locate.FromPairwiseCells[P, Q]) extends OperatorWithValue[P, Q] {
    type V = Double

    val plus = Plus(pos)

    def computeWithValue(left: Cell[P], right: Cell[P], ext: V): TraversableOnce[Cell[Q]] = plus
      .compute(left, right)
      .map { case Cell(pos, Content(_, DoubleValue(d))) => Cell(pos, Content(ContinuousSchema[Double](), d + ext)) }
  }

  case class MinusX[P <: HList, Q <: HList](pos: Locate.FromPairwiseCells[P, Q]) extends OperatorWithValue[P, Q] {
    type V = Double

    val minus = Minus(pos)

    def computeWithValue(left: Cell[P], right: Cell[P], ext: V): TraversableOnce[Cell[Q]] = minus
      .compute(left, right)
      .map { case Cell(pos, Content(_, DoubleValue(d))) => Cell(pos, Content(ContinuousSchema[Double](), d - ext)) }
  }
}

class TestScalaMatrixPairwise extends TestMatrixPairwise with TestScala {
  import commbank.grimlock.scala.environment.implicits._

  "A Matrix.pairwise" should "return its first over pairwise in 1D" in {
    toU(num1)
      .pair(Over(_0), Default())(Lower, Plus(plus(Over(_0))))
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its first over pairwise in 2D" in {
    toU(num2)
      .pair(Over(_0), Default())(Lower, Plus(plus(Over(_0))))
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its first along pairwise in 2D" in {
    toU(num2)
      .pair(Along(_0), Default())(Lower, List(Plus(plus(Along[P2, _0])), Minus(minus(Along[P2, _0]))))
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its second over pairwise in 2D" in {
    toU(num2)
      .pair(Over(_1), Default())(Lower, Plus(plus(Over(_1))), Minus(minus(Over(_1))))
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its second along pairwise in 2D" in {
    toU(num2)
      .pair(Along(_1), Default())(Lower, Plus(plus(Along(_1))))
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its first over pairwise in 3D" in {
    toU(num3)
      .pair(Over(_0), Default())(Lower, Plus(plus(Over(_0))))
      .toList.sortBy(_.position) shouldBe result6
  }

  it should "return its first along pairwise in 3D" in {
    toU(num3)
      .pair(Along(_0), Default())(Lower, List(Plus(plus(Along[P3, _0])), Minus(minus(Along[P3, _0]))))
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its second over pairwise in 3D" in {
    toU(num3)
      .pair(Over(_1), Default())(Lower, Plus(plus(Over(_1))), Minus(minus(Over(_1))))
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its second along pairwise in 3D" in {
    toU(num3)
      .pair(Along(_1), Default())(Lower, Plus(plus(Along(_1))))
      .toList.sortBy(_.position) shouldBe result9
  }

  it should "return its third over pairwise in 3D" in {
    toU(num3)
      .pair(Over(_2), Default())(Lower, List(Plus(plus(Over[P3, _2])), Minus(minus(Over[P3, _2]))))
      .toList.sortBy(_.position) shouldBe result10
  }

  it should "return its third along pairwise in 3D" in {
    toU(num3)
      .pair(Along(_2), Default())(Lower, Plus(plus(Along(_2))))
      .toList.sortBy(_.position) shouldBe result11
  }

  "A Matrix.pairwiseWithValue" should "return its first over pairwise in 1D" in {
    toU(num1)
      .pairWithValue(Over(_0), Default())(Lower, ext, TestMatrixPairwise.PlusX(plus(Over(_0))))
      .toList.sortBy(_.position) shouldBe result12
  }

  it should "return its first over pairwise in 2D" in {
    toU(num2)
      .pairWithValue(Over(_0), Default())(Lower, ext, TestMatrixPairwise.PlusX(plus(Over(_0))))
      .toList.sortBy(_.position) shouldBe result13
  }

  it should "return its first along pairwise in 2D" in {
    toU(num2)
      .pairWithValue(Along(_0), Default())(
        Lower,
        ext,
        TestMatrixPairwise.PlusX(plus(Along(_0))),
        TestMatrixPairwise.MinusX(minus(Along(_0)))
      )
      .toList.sortBy(_.position) shouldBe result14
  }

  it should "return its second over pairwise in 2D" in {
    toU(num2)
      .pairWithValue(Over(_1), Default())(
        Lower,
        ext,
        List(TestMatrixPairwise.PlusX(plus(Over[P2, _1])), TestMatrixPairwise.MinusX(minus(Over[P2, _1])))
      )
      .toList.sortBy(_.position) shouldBe result15
  }

  it should "return its second along pairwise in 2D" in {
    toU(num2)
      .pairWithValue(Along(_1), Default())(Lower, ext, TestMatrixPairwise.PlusX(plus(Along(_1))))
      .toList.sortBy(_.position) shouldBe result16
  }

  it should "return its first over pairwise in 3D" in {
    toU(num3)
      .pairWithValue(Over(_0), Default())(Lower, ext, TestMatrixPairwise.PlusX(plus(Over(_0))))
      .toList.sortBy(_.position) shouldBe result17
  }

  it should "return its first along pairwise in 3D" in {
    toU(num3)
      .pairWithValue(Along(_0), Default())(
        Lower,
        ext,
        TestMatrixPairwise.PlusX(plus(Along(_0))),
        TestMatrixPairwise.MinusX(minus(Along(_0)))
      )
      .toList.sortBy(_.position) shouldBe result18
  }

  it should "return its second over pairwise in 3D" in {
    toU(num3)
      .pairWithValue(Over(_1), Default())(
        Lower,
        ext,
        List(TestMatrixPairwise.PlusX(plus(Over[P3, _1])), TestMatrixPairwise.MinusX(minus(Over[P3, _1])))
      )
      .toList.sortBy(_.position) shouldBe result19
  }

  it should "return its second along pairwise in 3D" in {
    toU(num3)
      .pairWithValue(Along(_1), Default())(Lower, ext, TestMatrixPairwise.PlusX(plus(Along(_1))))
      .toList.sortBy(_.position) shouldBe result20
  }

  it should "return its third over pairwise in 3D" in {
    toU(num3)
      .pairWithValue(Over(_2), Default())(
        Lower,
        ext,
        TestMatrixPairwise.PlusX(plus(Over(_2))),
        TestMatrixPairwise.MinusX(minus(Over(_2)))
      )
      .toList.sortBy(_.position) shouldBe result21
  }

  it should "return its third along pairwise in 3D" in {
    toU(num3)
      .pairWithValue(Along(_2), Default())(Lower, ext, TestMatrixPairwise.PlusX(plus(Along(_2))))
      .toList.sortBy(_.position) shouldBe result22
  }

  "A Matrix.pairwiseBetween" should "return its first over pairwise in 1D" in {
    toU(num1)
      .pairBetween(Over(_0), Default())(Lower, toU(dataA), Plus(plus(Over(_0))))
      .toList.sortBy(_.position) shouldBe result23
  }

  it should "return its first over pairwise in 2D" in {
    toU(num2)
      .pairBetween(Over(_0), Default())(Lower, toU(dataB), Plus(plus(Over(_0))))
      .toList.sortBy(_.position) shouldBe result24
  }

  it should "return its first along pairwise in 2D" in {
    toU(num2)
      .pairBetween(Along(_0), Default())(
        Lower,
        toU(dataC),
        List(Plus(plus(Along[P2, _0])), Minus(minus(Along[P2, _0])))
      )
      .toList.sortBy(_.position) shouldBe result25
  }

  it should "return its second over pairwise in 2D" in {
    toU(num2)
      .pairBetween(Over(_1), Default())(Lower, toU(dataD), Plus(plus(Over(_1))), Minus(minus(Over(_1))))
      .toList.sortBy(_.position) shouldBe result26
  }

  it should "return its second along pairwise in 2D" in {
    toU(num2)
      .pairBetween(Along(_1), Default())(Lower, toU(dataE), Plus(plus(Along(_1))))
      .toList.sortBy(_.position) shouldBe result27
  }

  it should "return its first over pairwise in 3D" in {
    toU(num3)
      .pairBetween(Over(_0), Default())(Lower, toU(dataF), Plus(plus(Over(_0))))
      .toList.sortBy(_.position) shouldBe result28
  }

  it should "return its first along pairwise in 3D" in {
    toU(num3)
      .pairBetween(Along(_0), Default())(
        Lower,
        toU(dataG),
        List(Plus(plus(Along[P3, _0])), Minus(minus(Along[P3, _0])))
      )
      .toList.sortBy(_.position) shouldBe result29
  }

  it should "return its second over pairwise in 3D" in {
    toU(num3)
      .pairBetween(Over(_1), Default())(Lower, toU(dataH), Plus(plus(Over(_1))), Minus(minus(Over(_1))))
      .toList.sortBy(_.position) shouldBe result30
  }

  it should "return its second along pairwise in 3D" in {
    toU(num3)
      .pairBetween(Along(_1), Default())(Lower, toU(dataI), Plus(plus(Along(_1))))
      .toList.sortBy(_.position) shouldBe result31
  }

  it should "return its third over pairwise in 3D" in {
    toU(num3)
      .pairBetween(Over(_2), Default())(Lower, toU(dataJ), List(Plus(plus(Over[P3, _2])), Minus(minus(Over[P3, _2]))))
      .toList.sortBy(_.position) shouldBe result32
  }

  it should "return its third along pairwise in 3D" in {
    toU(num3)
      .pairBetween(Along(_2), Default())(Lower, toU(dataK), Plus(plus(Along(_2))))
      .toList.sortBy(_.position) shouldBe result33
  }

  "A Matrix.pairwiseBetweenWithValue" should "return its first over pairwise in 1D" in {
    toU(num1)
      .pairBetweenWithValue(Over(_0), Default())(Lower, toU(dataL), ext, TestMatrixPairwise.PlusX(plus(Over(_0))))
      .toList.sortBy(_.position) shouldBe result34
  }

  it should "return its first over pairwise in 2D" in {
    toU(num2)
      .pairBetweenWithValue(Over(_0), Default())(Lower, toU(dataM), ext, TestMatrixPairwise.PlusX(plus(Over(_0))))
      .toList.sortBy(_.position) shouldBe result35
  }

  it should "return its first along pairwise in 2D" in {
    toU(num2)
      .pairBetweenWithValue(Along(_0), Default())(
        Lower,
        toU(dataN),
        ext,
        TestMatrixPairwise.PlusX(plus(Along(_0))),
        TestMatrixPairwise.MinusX(minus(Along(_0)))
      )
      .toList.sortBy(_.position) shouldBe result36
  }

  it should "return its second over pairwise in 2D" in {
    toU(num2)
      .pairBetweenWithValue(Over(_1), Default())(
        Lower,
        toU(dataO),
        ext,
        List(TestMatrixPairwise.PlusX(plus(Over[P2, _1])), TestMatrixPairwise.MinusX(minus(Over[P2, _1])))
      )
      .toList.sortBy(_.position) shouldBe result37
  }

  it should "return its second along pairwise in 2D" in {
    toU(num2)
      .pairBetweenWithValue(Along(_1), Default())(Lower, toU(dataP), ext, TestMatrixPairwise.PlusX(plus(Along(_1))))
      .toList.sortBy(_.position) shouldBe result38
  }

  it should "return its first over pairwise in 3D" in {
    toU(num3)
      .pairBetweenWithValue(Over(_0), Default())(Lower, toU(dataQ), ext, TestMatrixPairwise.PlusX(plus(Over(_0))))
      .toList.sortBy(_.position) shouldBe result39
  }

  it should "return its first along pairwise in 3D" in {
    toU(num3)
      .pairBetweenWithValue(Along(_0), Default())(
        Lower,
        toU(dataR),
        ext,
        TestMatrixPairwise.PlusX(plus(Along(_0))),
        TestMatrixPairwise.MinusX(minus(Along(_0)))
      )
      .toList.sortBy(_.position) shouldBe result40
  }

  it should "return its second over pairwise in 3D" in {
    toU(num3)
      .pairBetweenWithValue(Over(_1), Default())(
        Lower,
        toU(dataS),
        ext,
        List(TestMatrixPairwise.PlusX(plus(Over[P3, _1])), TestMatrixPairwise.MinusX(minus(Over[P3, _1])))
      )
      .toList.sortBy(_.position) shouldBe result41
  }

  it should "return its second along pairwise in 3D" in {
    toU(num3)
      .pairBetweenWithValue(Along(_1), Default())(Lower, toU(dataT), ext, TestMatrixPairwise.PlusX(plus(Along(_1))))
      .toList.sortBy(_.position) shouldBe result42
  }

  it should "return its third over pairwise in 3D" in {
    toU(num3)
      .pairBetweenWithValue(Over(_2), Default())(
        Lower,
        toU(dataU),
        ext,
        TestMatrixPairwise.PlusX(plus(Over(_2))),
        TestMatrixPairwise.MinusX(minus(Over(_2)))
      )
      .toList.sortBy(_.position) shouldBe result43
  }

  it should "return its third along pairwise in 3D" in {
    toU(num3)
      .pairBetweenWithValue(Along(_2), Default())(Lower, toU(dataV), ext, TestMatrixPairwise.PlusX(plus(Along(_2))))
      .toList.sortBy(_.position) shouldBe result44
  }

  it should "return empty data - Default" in {
    toU(num3)
      .pairBetween(Along(_2), Default())(Lower, List.empty, Plus(plus(Along(_2))))
      .toList.sortBy(_.position) shouldBe List()
  }
}

class TestScaldingMatrixPairwise extends TestMatrixPairwise with TestScalding {
  import commbank.grimlock.scalding.environment.implicits._

  "A Matrix.pairwise" should "return its first over pairwise in 1D" in {
    toU(num1)
      .pair(Over(_0), InMemory())(Lower, Plus(plus(Over(_0))))
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its first over pairwise in 2D" in {
    toU(num2)
      .pair(Over(_0), Default())(Lower, Plus(plus(Over(_0))))
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its first along pairwise in 2D" in {
    toU(num2)
      .pair(Along(_0), Ternary(InMemory(), Default(12), Default(12)))(
        Lower,
        List(Plus(plus(Along[P2, _0])), Minus(minus(Along[P2, _0])))
      )
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its second over pairwise in 2D" in {
    toU(num2)
      .pair(Over(_1), Ternary(InMemory(), Default(12), Unbalanced(12)))(
        Lower,
        Plus(plus(Over(_1))),
        Minus(minus(Over(_1)))
      )
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its second along pairwise in 2D" in {
    toU(num2)
      .pair(Along(_1), Ternary(InMemory(), Unbalanced(12), Default(12)))(Lower, Plus(plus(Along(_1))))
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its first over pairwise in 3D" in {
    toU(num3)
      .pair(Over(_0), Ternary(InMemory(), Unbalanced(12), Unbalanced(12)))(Lower, Plus(plus(Over(_0))))
      .toList.sortBy(_.position) shouldBe result6
  }

  it should "return its first along pairwise in 3D" in {
    toU(num3)
      .pair(Along(_0), Ternary(Default(12), Default(12), Default(12)))(
        Lower,
        List(Plus(plus(Along[P3, _0])), Minus(minus(Along[P3, _0])))
      )
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its second over pairwise in 3D" in {
    toU(num3)
      .pair(Over(_1), Ternary(Default(12), Default(12), Unbalanced(12)))(
        Lower,
        Plus(plus(Over(_1))),
        Minus(minus(Over(_1)))
      )
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its second along pairwise in 3D" in {
    toU(num3)
      .pair(Along(_1), Ternary(Default(12), Unbalanced(12), Default(12)))(Lower, Plus(plus(Along(_1))))
      .toList.sortBy(_.position) shouldBe result9
  }

  it should "return its third over pairwise in 3D" in {
    toU(num3)
      .pair(Over(_2), Ternary(Default(12), Unbalanced(12), Unbalanced(12)))(
        Lower,
        List(Plus(plus(Over[P3, _2])), Minus(minus(Over[P3, _2])))
      )
      .toList.sortBy(_.position) shouldBe result10
  }

  it should "return its third along pairwise in 3D" in {
    toU(num3)
      .pair(Along(_2), InMemory())(Lower, Plus(plus(Along(_2))))
      .toList.sortBy(_.position) shouldBe result11
  }

  "A Matrix.pairwiseWithValue" should "return its first over pairwise in 1D" in {
    toU(num1)
      .pairWithValue(Over(_0), Default())(
        Lower,
        ValuePipe(ext),
        TestMatrixPairwise.PlusX(plus(Over(_0)))
      )
      .toList.sortBy(_.position) shouldBe result12
  }

  it should "return its first over pairwise in 2D" in {
    toU(num2)
      .pairWithValue(Over(_0), Ternary(InMemory(), Default(12), Default(12)))(
        Lower,
        ValuePipe(ext),
        TestMatrixPairwise.PlusX(plus(Over(_0)))
      )
      .toList.sortBy(_.position) shouldBe result13
  }

  it should "return its first along pairwise in 2D" in {
    toU(num2)
      .pairWithValue(Along(_0), Ternary(InMemory(), Default(12), Unbalanced(12)))(
        Lower,
        ValuePipe(ext),
        TestMatrixPairwise.PlusX(plus(Along(_0))),
        TestMatrixPairwise.MinusX(minus(Along(_0)))
      )
      .toList.sortBy(_.position) shouldBe result14
  }

  it should "return its second over pairwise in 2D" in {
    toU(num2)
      .pairWithValue(Over(_1), Ternary(InMemory(), Unbalanced(12), Default(12)))(
        Lower,
        ValuePipe(ext),
        List(TestMatrixPairwise.PlusX(plus(Over[P2, _1])), TestMatrixPairwise.MinusX(minus(Over[P2, _1])))
      )
      .toList.sortBy(_.position) shouldBe result15
  }

  it should "return its second along pairwise in 2D" in {
    toU(num2)
      .pairWithValue(Along(_1), Ternary(InMemory(), Unbalanced(12), Unbalanced(12)))(
        Lower,
        ValuePipe(ext),
        TestMatrixPairwise.PlusX(plus(Along(_1)))
      )
      .toList.sortBy(_.position) shouldBe result16
  }

  it should "return its first over pairwise in 3D" in {
    toU(num3)
      .pairWithValue(Over(_0), Ternary(Default(12), Default(12), Default(12)))(
        Lower,
        ValuePipe(ext),
        TestMatrixPairwise.PlusX(plus(Over(_0)))
      )
      .toList.sortBy(_.position) shouldBe result17
  }

  it should "return its first along pairwise in 3D" in {
    toU(num3)
      .pairWithValue(Along(_0), Ternary(Default(12), Default(12), Unbalanced(12)))(
        Lower,
        ValuePipe(ext),
        TestMatrixPairwise.PlusX(plus(Along(_0))),
        TestMatrixPairwise.MinusX(minus(Along(_0)))
      )
      .toList.sortBy(_.position) shouldBe result18
  }

  it should "return its second over pairwise in 3D" in {
    toU(num3)
      .pairWithValue(Over(_1), Ternary(Default(12), Unbalanced(12), Default(12)))(
        Lower,
        ValuePipe(ext),
        List(TestMatrixPairwise.PlusX(plus(Over[P3, _1])), TestMatrixPairwise.MinusX(minus(Over[P3, _1])))
      )
      .toList.sortBy(_.position) shouldBe result19
  }

  it should "return its second along pairwise in 3D" in {
    toU(num3)
      .pairWithValue(Along(_1), Ternary(Default(12), Unbalanced(12), Unbalanced(12)))(
        Lower,
        ValuePipe(ext),
        TestMatrixPairwise.PlusX(plus(Along(_1)))
      )
      .toList.sortBy(_.position) shouldBe result20
  }

  it should "return its third over pairwise in 3D" in {
    toU(num3)
      .pairWithValue(Over(_2), InMemory())(
        Lower,
        ValuePipe(ext),
        TestMatrixPairwise.PlusX(plus(Over(_2))),
        TestMatrixPairwise.MinusX(minus(Over(_2)))
      )
      .toList.sortBy(_.position) shouldBe result21
  }

  it should "return its third along pairwise in 3D" in {
    toU(num3)
      .pairWithValue(Along(_2), Default())(
        Lower,
        ValuePipe(ext),
        TestMatrixPairwise.PlusX(plus(Along(_2)))
      )
      .toList.sortBy(_.position) shouldBe result22
  }

  "A Matrix.pairwiseBetween" should "return its first over pairwise in 1D" in {
    toU(num1)
      .pairBetween(Over(_0), Ternary(InMemory(), Default(12), Default(12)))(
        Lower,
        toU(dataA),
        Plus(plus(Over(_0)))
      )
      .toList.sortBy(_.position) shouldBe result23
  }

  it should "return its first over pairwise in 2D" in {
    toU(num2)
      .pairBetween(Over(_0), Ternary(InMemory(), Default(12), Unbalanced(12)))(
        Lower,
        toU(dataB),
        Plus(plus(Over(_0)))
      )
      .toList.sortBy(_.position) shouldBe result24
  }

  it should "return its first along pairwise in 2D" in {
    toU(num2)
      .pairBetween(Along(_0), Ternary(InMemory(), Unbalanced(12), Default(12)))(
        Lower,
        toU(dataC),
        List(Plus(plus(Along[P2, _0])), Minus(minus(Along[P2, _0])))
      )
      .toList.sortBy(_.position) shouldBe result25
  }

  it should "return its second over pairwise in 2D" in {
    toU(num2)
      .pairBetween(Over(_1), Ternary(InMemory(), Unbalanced(12), Unbalanced(12)))(
        Lower,
        toU(dataD),
        Plus(plus(Over(_1))),
        Minus(minus(Over(_1)))
      )
      .toList.sortBy(_.position) shouldBe result26
  }

  it should "return its second along pairwise in 2D" in {
    toU(num2)
      .pairBetween(Along(_1), Ternary(Default(12), Default(12), Default(12)))(
        Lower,
        toU(dataE),
        Plus(plus(Along(_1)))
      )
      .toList.sortBy(_.position) shouldBe result27
  }

  it should "return its first over pairwise in 3D" in {
    toU(num3)
      .pairBetween(Over(_0), Ternary(Default(12), Default(12), Unbalanced(12)))(
        Lower,
        toU(dataF),
        Plus(plus(Over(_0)))
      )
      .toList.sortBy(_.position) shouldBe result28
  }

  it should "return its first along pairwise in 3D" in {
    toU(num3)
      .pairBetween(Along(_0), Ternary(Default(12), Unbalanced(12), Default(12)))(
        Lower,
        toU(dataG),
        List(Plus(plus(Along[P3, _0])), Minus(minus(Along[P3, _0])))
      )
      .toList.sortBy(_.position) shouldBe result29
  }

  it should "return its second over pairwise in 3D" in {
    toU(num3)
      .pairBetween(Over(_1), Ternary(Default(12), Unbalanced(12), Unbalanced(12)))(
        Lower,
        toU(dataH),
        Plus(plus(Over(_1))),
        Minus(minus(Over(_1)))
      )
      .toList.sortBy(_.position) shouldBe result30
  }

  it should "return its second along pairwise in 3D" in {
    toU(num3)
      .pairBetween(Along(_1), InMemory())(Lower, toU(dataI), Plus(plus(Along(_1))))
      .toList.sortBy(_.position) shouldBe result31
  }

  it should "return its third over pairwise in 3D" in {
    toU(num3)
      .pairBetween(Over(_2), Default())(
        Lower,
        toU(dataJ),
        List(Plus(plus(Over[P3, _2])), Minus(minus(Over[P3, _2])))
      )
      .toList.sortBy(_.position) shouldBe result32
  }

  it should "return its third along pairwise in 3D" in {
    toU(num3)
      .pairBetween(Along(_2), Ternary(InMemory(), Default(12), Default(12)))(
        Lower,
        toU(dataK),
        Plus(plus(Along(_2)))
      )
      .toList.sortBy(_.position) shouldBe result33
  }

  "A Matrix.pairwiseBetweenWithValue" should "return its first over pairwise in 1D" in {
    toU(num1)
      .pairBetweenWithValue(Over(_0), Ternary(InMemory(), Default(12), Unbalanced(12)))(
        Lower,
        toU(dataL),
        ValuePipe(ext),
        TestMatrixPairwise.PlusX(plus(Over(_0)))
      )
      .toList.sortBy(_.position) shouldBe result34
  }

  it should "return its first over pairwise in 2D" in {
    toU(num2)
      .pairBetweenWithValue(Over(_0), Ternary(InMemory(), Unbalanced(12), Default(12)))(
        Lower,
        toU(dataM),
        ValuePipe(ext),
        TestMatrixPairwise.PlusX(plus(Over(_0)))
      )
      .toList.sortBy(_.position) shouldBe result35
  }

  it should "return its first along pairwise in 2D" in {
    toU(num2)
      .pairBetweenWithValue(Along(_0), Ternary(InMemory(), Unbalanced(12), Unbalanced(12)))(
        Lower,
        toU(dataN),
        ValuePipe(ext),
        TestMatrixPairwise.PlusX(plus(Along(_0))),
        TestMatrixPairwise.MinusX(minus(Along(_0)))
      )
      .toList.sortBy(_.position) shouldBe result36
  }

  it should "return its second over pairwise in 2D" in {
    toU(num2)
      .pairBetweenWithValue(Over(_1), Ternary(Default(12), Default(12), Default(12)))(
        Lower,
        toU(dataO),
        ValuePipe(ext),
        List(TestMatrixPairwise.PlusX(plus(Over[P2, _1])), TestMatrixPairwise.MinusX(minus(Over[P2, _1])))
      )
      .toList.sortBy(_.position) shouldBe result37
  }

  it should "return its second along pairwise in 2D" in {
    toU(num2)
      .pairBetweenWithValue(Along(_1), Ternary(Default(12), Default(12), Unbalanced(12)))(
        Lower,
        toU(dataP),
        ValuePipe(ext),
        TestMatrixPairwise.PlusX(plus(Along(_1)))
      )
      .toList.sortBy(_.position) shouldBe result38
  }

  it should "return its first over pairwise in 3D" in {
    toU(num3)
      .pairBetweenWithValue(Over(_0), Ternary(Default(12), Unbalanced(12), Default(12)))(
        Lower,
        toU(dataQ),
        ValuePipe(ext),
        TestMatrixPairwise.PlusX(plus(Over(_0)))
      )
      .toList.sortBy(_.position) shouldBe result39
  }

  it should "return its first along pairwise in 3D" in {
    toU(num3)
      .pairBetweenWithValue(Along(_0), Ternary(Default(12), Unbalanced(12), Unbalanced(12)))(
        Lower,
        toU(dataR),
        ValuePipe(ext),
        TestMatrixPairwise.PlusX(plus(Along(_0))),
        TestMatrixPairwise.MinusX(minus(Along(_0)))
      )
      .toList.sortBy(_.position) shouldBe result40
  }

  it should "return its second over pairwise in 3D" in {
    toU(num3)
      .pairBetweenWithValue(Over(_1), InMemory())(
        Lower,
        toU(dataS),
        ValuePipe(ext),
        List(TestMatrixPairwise.PlusX(plus(Over[P3, _1])), TestMatrixPairwise.MinusX(minus(Over[P3, _1])))
      )
      .toList.sortBy(_.position) shouldBe result41
  }

  it should "return its second along pairwise in 3D" in {
    toU(num3)
      .pairBetweenWithValue(Along(_1), Default())(
        Lower,
        toU(dataT),
        ValuePipe(ext),
        TestMatrixPairwise.PlusX(plus(Along(_1)))
      )
      .toList.sortBy(_.position) shouldBe result42
  }

  it should "return its third over pairwise in 3D" in {
    toU(num3)
      .pairBetweenWithValue(Over(_2), Ternary(InMemory(), Default(12), Default(12)))(
        Lower,
        toU(dataU),
        ValuePipe(ext),
        TestMatrixPairwise.PlusX(plus(Over(_2))),
        TestMatrixPairwise.MinusX(minus(Over(_2)))
      )
      .toList.sortBy(_.position) shouldBe result43
  }

  it should "return its third along pairwise in 3D" in {
    toU(num3)
      .pairBetweenWithValue(Along(_2), Ternary(InMemory(), Default(12), Unbalanced(12)))(
        Lower,
        toU(dataV),
        ValuePipe(ext),
        TestMatrixPairwise.PlusX(plus(Along(_2)))
      )
      .toList.sortBy(_.position) shouldBe result44
  }

  it should "return empty data - InMemory" in {
    toU(num3)
      .pairBetween(Along(_2), InMemory())(Lower, TypedPipe.empty, Plus(plus(Along(_2))))
      .toList.sortBy(_.position) shouldBe List()
  }

  it should "return empty data - Default" in {
    toU(num3)
      .pairBetween(Along(_2), Default())(Lower, TypedPipe.empty, Plus(plus(Along(_2))))
      .toList.sortBy(_.position) shouldBe List()
  }
}

class TestSparkMatrixPairwise extends TestMatrixPairwise with TestSpark {
  import commbank.grimlock.spark.environment.implicits._

  "A Matrix.pairwise" should "return its first over pairwise in 1D" in {
    toU(num1)
      .pair(Over(_0), InMemory())(Lower, Plus(plus(Over(_0))))
      .toList.sortBy(_.position) shouldBe result1
  }

  it should "return its first over pairwise in 2D" in {
    toU(num2)
      .pair(Over(_0), Default())(Lower, Plus(plus(Over(_0))))
      .toList.sortBy(_.position) shouldBe result2
  }

  it should "return its first along pairwise in 2D" in {
    toU(num2)
      .pair(Along(_0), Ternary(InMemory(), Default(12), Default(12)))(
        Lower,
        List(Plus(plus(Along[P2, _0])), Minus(minus(Along[P2, _0])))
      )
      .toList.sortBy(_.position) shouldBe result3
  }

  it should "return its second over pairwise in 2D" in {
    toU(num2)
      .pair(Over(_1), Ternary(Default(12), Default(12), Default(12)))(
        Lower,
        List(Plus(plus(Over[P2, _1])), Minus(minus(Over[P2, _1])))
      )
      .toList.sortBy(_.position) shouldBe result4
  }

  it should "return its second along pairwise in 2D" in {
    toU(num2)
      .pair(Along(_1), InMemory())(Lower, Plus(plus(Along(_1))))
      .toList.sortBy(_.position) shouldBe result5
  }

  it should "return its first over pairwise in 3D" in {
    toU(num3)
      .pair(Over(_0), Default())(Lower, Plus(plus(Over(_0))))
      .toList.sortBy(_.position) shouldBe result6
  }

  it should "return its first along pairwise in 3D" in {
    toU(num3)
      .pair(Along(_0), Ternary(InMemory(), Default(12), Default(12)))(
        Lower,
        Plus(plus(Along(_0))),
        Minus(minus(Along(_0)))
      )
      .toList.sortBy(_.position) shouldBe result7
  }

  it should "return its second over pairwise in 3D" in {
    toU(num3)
      .pair(Over(_1), Ternary(Default(12), Default(12), Default(12)))(
        Lower,
        List(Plus(plus(Over[P3, _1])), Minus(minus(Over[P3, _1])))
      )
      .toList.sortBy(_.position) shouldBe result8
  }

  it should "return its second along pairwise in 3D" in {
    toU(num3)
      .pair(Along(_1), InMemory())(Lower, Plus(plus(Along(_1))))
      .toList.sortBy(_.position) shouldBe result9
  }

  it should "return its third over pairwise in 3D" in {
    toU(num3)
      .pair(Over(_2), Default())(Lower, Plus(plus(Over(_2))), Minus(minus(Over(_2))))
      .toList.sortBy(_.position) shouldBe result10
  }

  it should "return its third along pairwise in 3D" in {
    toU(num3)
      .pair(Along(_2), Ternary(InMemory(), Default(12), Default(12)))(Lower, Plus(plus(Along(_2))))
      .toList.sortBy(_.position) shouldBe result11
  }

  "A Matrix.pairwiseWithValue" should "return its first over pairwise in 1D" in {
    toU(num1)
      .pairWithValue(Over(_0), Ternary(Default(12), Default(12), Default(12)))(
        Lower,
        ext,
        TestMatrixPairwise.PlusX(plus(Over(_0)))
      )
      .toList.sortBy(_.position) shouldBe result12
  }

  it should "return its first over pairwise in 2D" in {
    toU(num2)
      .pairWithValue(Over(_0), InMemory())(Lower, ext, TestMatrixPairwise.PlusX(plus(Over(_0))))
      .toList.sortBy(_.position) shouldBe result13
  }

  it should "return its first along pairwise in 2D" in {
    toU(num2)
      .pairWithValue(Along(_0), Default())(
        Lower,
        ext,
        List(TestMatrixPairwise.PlusX(plus(Along[P2, _0])), TestMatrixPairwise.MinusX(minus(Along[P2, _0])))
      )
      .toList.sortBy(_.position) shouldBe result14
  }

  it should "return its second over pairwise in 2D" in {
    toU(num2)
      .pairWithValue(Over(_1), Ternary(InMemory(), Default(12), Default(12)))(
        Lower,
        ext,
        TestMatrixPairwise.PlusX(plus(Over(_1))),
        TestMatrixPairwise.MinusX(minus(Over(_1)))
      )
      .toList.sortBy(_.position) shouldBe result15
  }

  it should "return its second along pairwise in 2D" in {
    toU(num2)
      .pairWithValue(Along(_1), Ternary(Default(12), Default(12), Default(12)))(
        Lower,
        ext,
        TestMatrixPairwise.PlusX(plus(Along(_1)))
      )
      .toList.sortBy(_.position) shouldBe result16
  }

  it should "return its first over pairwise in 3D" in {
    toU(num3)
      .pairWithValue(Over(_0), InMemory())(Lower, ext, TestMatrixPairwise.PlusX(plus(Over(_0))))
      .toList.sortBy(_.position) shouldBe result17
  }

  it should "return its first along pairwise in 3D" in {
    toU(num3)
      .pairWithValue(Along(_0), Default())(
        Lower,
        ext,
        List(TestMatrixPairwise.PlusX(plus(Along[P3, _0])), TestMatrixPairwise.MinusX(minus(Along[P3, _0])))
      )
      .toList.sortBy(_.position) shouldBe result18
  }

  it should "return its second over pairwise in 3D" in {
    toU(num3)
      .pairWithValue(Over(_1), Ternary(InMemory(), Default(12), Default(12)))(
        Lower,
        ext,
        TestMatrixPairwise.PlusX(plus(Over(_1))),
        TestMatrixPairwise.MinusX(minus(Over(_1)))
      )
      .toList.sortBy(_.position) shouldBe result19
  }

  it should "return its second along pairwise in 3D" in {
    toU(num3)
      .pairWithValue(Along(_1), Ternary(Default(12), Default(12), Default(12)))(
        Lower,
        ext,
        TestMatrixPairwise.PlusX(plus(Along(_1)))
      )
      .toList.sortBy(_.position) shouldBe result20
  }

  it should "return its third over pairwise in 3D" in {
    toU(num3)
      .pairWithValue(Over(_2), InMemory())(
        Lower,
        ext,
        List(TestMatrixPairwise.PlusX(plus(Over[P3, _2])), TestMatrixPairwise.MinusX(minus(Over[P3, _2])))
      )
      .toList.sortBy(_.position) shouldBe result21
  }

  it should "return its third along pairwise in 3D" in {
    toU(num3)
      .pairWithValue(Along(_2), Default())(Lower, ext, TestMatrixPairwise.PlusX(plus(Along(_2))))
      .toList.sortBy(_.position) shouldBe result22
  }

  "A Matrix.pairwiseBetween" should "return its first over pairwise in 1D" in {
    toU(num1)
      .pairBetween(Over(_0), Ternary(InMemory(), Default(12), Default(12)))(
        Lower,
        toU(dataA),
        Plus(plus(Over(_0)))
      )
      .toList.sortBy(_.position) shouldBe result23
  }

  it should "return its first over pairwise in 2D" in {
    toU(num2)
      .pairBetween(Over(_0), Ternary(Default(12), Default(12), Default(12)))(
        Lower,
        toU(dataB),
        Plus(plus(Over(_0)))
      )
      .toList.sortBy(_.position) shouldBe result24
  }

  it should "return its first along pairwise in 2D" in {
    toU(num2)
      .pairBetween(Along(_0), InMemory())(Lower, toU(dataC), Plus(plus(Along(_0))), Minus(minus(Along(_0))))
      .toList.sortBy(_.position) shouldBe result25
  }

  it should "return its second over pairwise in 2D" in {
    toU(num2)
      .pairBetween(Over(_1), Default())(
        Lower,
        toU(dataD),
        List(Plus(plus(Over[P2, _1])), Minus(minus(Over[P2, _1])))
      )
      .toList.sortBy(_.position) shouldBe result26
  }

  it should "return its second along pairwise in 2D" in {
    toU(num2)
      .pairBetween(Along(_1), Ternary(InMemory(), Default(12), Default(12)))(
        Lower,
        toU(dataE),
        Plus(plus(Along(_1)))
      )
      .toList.sortBy(_.position) shouldBe result27
  }

  it should "return its first over pairwise in 3D" in {
    toU(num3)
      .pairBetween(Over(_0), Ternary(Default(12), Default(12), Default(12)))(
        Lower,
        toU(dataF),
        Plus(plus(Over(_0)))
      )
      .toList.sortBy(_.position) shouldBe result28
  }

  it should "return its first along pairwise in 3D" in {
    toU(num3)
      .pairBetween(Along(_0), InMemory())(Lower, toU(dataG), Plus(plus(Along(_0))), Minus(minus(Along(_0))))
      .toList.sortBy(_.position) shouldBe result29
  }

  it should "return its second over pairwise in 3D" in {
    toU(num3)
      .pairBetween(Over(_1), Default())(
        Lower,
        toU(dataH),
        List(Plus(plus(Over[P3, _1])), Minus(minus(Over[P3, _1])))
      )
      .toList.sortBy(_.position) shouldBe result30
  }

  it should "return its second along pairwise in 3D" in {
    toU(num3)
      .pairBetween(Along(_1), Ternary(InMemory(), Default(12), Default(12)))(
        Lower,
        toU(dataI),
        Plus(plus(Along(_1)))
      )
      .toList.sortBy(_.position) shouldBe result31
  }

  it should "return its third over pairwise in 3D" in {
    toU(num3)
      .pairBetween(Over(_2), Ternary(Default(12), Default(12), Default(12)))(
        Lower,
        toU(dataJ),
        Plus(plus(Over(_2))),
        Minus(minus(Over(_2)))
      )
      .toList.sortBy(_.position) shouldBe result32
  }

  it should "return its third along pairwise in 3D" in {
    toU(num3)
      .pairBetween(Along(_2), InMemory())(Lower, toU(dataK), Plus(plus(Along(_2))))
      .toList.sortBy(_.position) shouldBe result33
  }

  "A Matrix.pairwiseBetweenWithValue" should "return its first over pairwise in 1D" in {
    toU(num1)
      .pairBetweenWithValue(Over(_0), Default())(
        Lower,
        toU(dataL),
        ext,
        TestMatrixPairwise.PlusX(plus(Over(_0)))
      )
      .toList.sortBy(_.position) shouldBe result34
  }

  it should "return its first over pairwise in 2D" in {
    toU(num2)
      .pairBetweenWithValue(Over(_0), Ternary(InMemory(), Default(12), Default(12)))(
        Lower,
        toU(dataM),
        ext,
        TestMatrixPairwise.PlusX(plus(Over(_0)))
      )
      .toList.sortBy(_.position) shouldBe result35
  }

  it should "return its first along pairwise in 2D" in {
    toU(num2)
      .pairBetweenWithValue(Along(_0), Ternary(Default(12), Default(12), Default(12)))(
        Lower,
        toU(dataN),
        ext,
        List(TestMatrixPairwise.PlusX(plus(Along[P2, _0])), TestMatrixPairwise.MinusX(minus(Along[P2, _0])))
      )
      .toList.sortBy(_.position) shouldBe result36
  }

  it should "return its second over pairwise in 2D" in {
    toU(num2)
      .pairBetweenWithValue(Over(_1), InMemory())(
        Lower,
        toU(dataO),
        ext,
        TestMatrixPairwise.PlusX(plus(Over(_1))),
        TestMatrixPairwise.MinusX(minus(Over(_1)))
      )
      .toList.sortBy(_.position) shouldBe result37
  }

  it should "return its second along pairwise in 2D" in {
    toU(num2)
      .pairBetweenWithValue(Along(_1), Default())(
        Lower,
        toU(dataP),
        ext,
        TestMatrixPairwise.PlusX(plus(Along(_1)))
      )
      .toList.sortBy(_.position) shouldBe result38
  }

  it should "return its first over pairwise in 3D" in {
    toU(num3)
      .pairBetweenWithValue(Over(_0), Ternary(InMemory(), Default(12), Default(12)))(
        Lower,
        toU(dataQ),
        ext,
        TestMatrixPairwise.PlusX(plus(Over(_0)))
      )
      .toList.sortBy(_.position) shouldBe result39
  }

  it should "return its first along pairwise in 3D" in {
    toU(num3)
      .pairBetweenWithValue(Along(_0), Ternary(Default(12), Default(12), Default(12)))(
        Lower,
        toU(dataR),
        ext,
        List(TestMatrixPairwise.PlusX(plus(Along[P3, _0])), TestMatrixPairwise.MinusX(minus(Along[P3, _0])))
      )
      .toList.sortBy(_.position) shouldBe result40
  }

  it should "return its second over pairwise in 3D" in {
    toU(num3)
      .pairBetweenWithValue(Over(_1), InMemory())(
        Lower,
        toU(dataS),
        ext,
        TestMatrixPairwise.PlusX(plus(Over(_1))),
        TestMatrixPairwise.MinusX(minus(Over(_1)))
      )
      .toList.sortBy(_.position) shouldBe result41
  }

  it should "return its second along pairwise in 3D" in {
    toU(num3)
      .pairBetweenWithValue(Along(_1), Default())(
        Lower,
        toU(dataT),
        ext,
        TestMatrixPairwise.PlusX(plus(Along(_1)))
      )
      .toList.sortBy(_.position) shouldBe result42
  }

  it should "return its third over pairwise in 3D" in {
    toU(num3)
      .pairBetweenWithValue(Over(_2), Ternary(InMemory(), Default(12), Default(12)))(
        Lower,
        toU(dataU),
        ext,
        List(TestMatrixPairwise.PlusX(plus(Over[P3, _2])), TestMatrixPairwise.MinusX(minus(Over[P3, _2])))
      )
      .toList.sortBy(_.position) shouldBe result43
  }

  it should "return its third along pairwise in 3D" in {
    toU(num3)
      .pairBetweenWithValue(Along(_2), Ternary(Default(12), Default(12), Default(12)))(
        Lower,
        toU(dataV),
        ext,
        TestMatrixPairwise.PlusX(plus(Along(_2)))
      )
      .toList.sortBy(_.position) shouldBe result44
  }

  it should "return empty data - Default" in {
    toU(num3)
      .pairBetween(Along(_2), Default())(Lower, toU(List()), Plus(plus(Along(_2))))
      .toList.sortBy(_.position) shouldBe List()
  }
}

