// Copyright 2016,2017,2018,2019 Commonwealth Bank of Australia
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
import commbank.grimlock.framework.metadata._
import commbank.grimlock.framework.position._

import shapeless.HList
import shapeless.nat.{ _0, _1 }

trait TestDistribution extends TestGrimlock {
  val data1 = List(
    Cell(Position("foo"), Content(OrdinalSchema[String](), "3.14")),
    Cell(Position("bar"), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("bar"), Content(OrdinalSchema[String](), "6.28")),
    Cell(Position("qux"), Content(OrdinalSchema[String](), "3.14"))
  )

  val data2 = List(
    Cell(Position("row1", "col1"), Content(NominalSchema[String](), "a")),
    Cell(Position("row1", "col3"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("row1", "col4"), Content(NominalSchema[String](), "b")),
    Cell(Position("row2", "col1"), Content(NominalSchema[String](), "a")),
    Cell(Position("row2", "col2"), Content(NominalSchema[String](), "b")),
    Cell(Position("row2", "col4"), Content(DiscreteSchema[Long](), 2L)),
    Cell(Position("row3", "col2"), Content(NominalSchema[String](), "b")),
    Cell(Position("row3", "col3"), Content(NominalSchema[String](), "a")),
    Cell(Position("row3", "col4"), Content(DiscreteSchema[Long](), 3L)),
    Cell(Position("row4", "col1"), Content(DiscreteSchema[Long](), 4L)),
    Cell(Position("row4", "col2"), Content(NominalSchema[String](), "a")),
    Cell(Position("row4", "col3"), Content(NominalSchema[String](), "b"))
  )

  val data3 = List(
    Cell(Position("row1", "col1", "dep1"), Content(NominalSchema[String](), "a")),
    Cell(Position("row1", "col3", "dep1"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("row1", "col4", "dep1"), Content(NominalSchema[String](), "b")),
    Cell(Position("row2", "col1", "dep1"), Content(NominalSchema[String](), "a")),
    Cell(Position("row2", "col2", "dep1"), Content(NominalSchema[String](), "b")),
    Cell(Position("row2", "col4", "dep1"), Content(DiscreteSchema[Long](), 2L)),
    Cell(Position("row3", "col2", "dep1"), Content(NominalSchema[String](), "b")),
    Cell(Position("row3", "col3", "dep1"), Content(NominalSchema[String](), "a")),
    Cell(Position("row3", "col4", "dep1"), Content(DiscreteSchema[Long](), 3L)),
    Cell(Position("row4", "col1", "dep1"), Content(DiscreteSchema[Long](), 4L)),
    Cell(Position("row4", "col2", "dep1"), Content(NominalSchema[String](), "a")),
    Cell(Position("row4", "col3", "dep1"), Content(NominalSchema[String](), "b")),
    Cell(Position("row1", "col1", "dep2"), Content(NominalSchema[String](), "a")),
    Cell(Position("row2", "col2", "dep2"), Content(NominalSchema[String](), "b")),
    Cell(Position("row3", "col3", "dep2"), Content(NominalSchema[String](), "b")),
    Cell(Position("row4", "col4", "dep2"), Content(NominalSchema[String](), "a"))
  )

  val result1 = List(
    Cell(Position("bar", "6.28"), Content(DiscreteSchema[Long](), 2L)),
    Cell(Position("foo", "3.14"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("qux", "3.14"), Content(DiscreteSchema[Long](), 1L))
  )

  val result2 = List(
    Cell(Position("3.14"), Content(DiscreteSchema[Long](), 2L)),
    Cell(Position("6.28"), Content(DiscreteSchema[Long](), 2L))
  )

  val result3 = List(
    Cell(Position("row1", "a"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("row1", "b"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("row2", "a"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("row2", "b"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("row3", "a"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("row3", "b"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("row4", "a"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("row4", "b"), Content(DiscreteSchema[Long](), 1L))
  )

  val result4 = List(
    Cell(Position("col1", "a"), Content(DiscreteSchema[Long](), 2L)),
    Cell(Position("col2", "a"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("col2", "b"), Content(DiscreteSchema[Long](), 2L)),
    Cell(Position("col3", "a"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("col3", "b"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("col4", "b"), Content(DiscreteSchema[Long](), 1L))
  )

  val result5 = List(
    Cell(Position("col1", "4"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("col1", "a"), Content(DiscreteSchema[Long](), 2L)),
    Cell(Position("col2", "a"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("col2", "b"), Content(DiscreteSchema[Long](), 2L)),
    Cell(Position("col3", "1"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("col3", "a"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("col3", "b"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("col4", "2"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("col4", "3"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("col4", "b"), Content(DiscreteSchema[Long](), 1L))
  )

  val result6 = List(
    Cell(Position("row1", "1"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("row1", "a"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("row1", "b"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("row2", "2"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("row2", "a"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("row2", "b"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("row3", "3"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("row3", "a"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("row3", "b"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("row4", "4"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("row4", "a"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("row4", "b"), Content(DiscreteSchema[Long](), 1L))
  )

  val result7 = List(
    Cell(Position("row1", "1"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("row1", "a"), Content(DiscreteSchema[Long](), 2L)),
    Cell(Position("row1", "b"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("row2", "2"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("row2", "a"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("row2", "b"), Content(DiscreteSchema[Long](), 2L)),
    Cell(Position("row3", "3"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("row3", "a"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("row3", "b"), Content(DiscreteSchema[Long](), 2L)),
    Cell(Position("row4", "4"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("row4", "a"), Content(DiscreteSchema[Long](), 2L)),
    Cell(Position("row4", "b"), Content(DiscreteSchema[Long](), 1L))
  )

  val result8 = List(
    Cell(Position("col1", "dep1", "4"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("col1", "dep1", "a"), Content(DiscreteSchema[Long](), 2L)),
    Cell(Position("col1", "dep2", "a"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("col2", "dep1", "a"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("col2", "dep1", "b"), Content(DiscreteSchema[Long](), 2L)),
    Cell(Position("col2", "dep2", "b"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("col3", "dep1", "1"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("col3", "dep1", "a"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("col3", "dep1", "b"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("col3", "dep2", "b"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("col4", "dep1", "2"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("col4", "dep1", "3"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("col4", "dep1", "b"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("col4", "dep2", "a"), Content(DiscreteSchema[Long](), 1L))
  )

  val result9 = List(
    Cell(Position("col1", "a"), Content(DiscreteSchema[Long](), 3L)),
    Cell(Position("col2", "a"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("col2", "b"), Content(DiscreteSchema[Long](), 3L)),
    Cell(Position("col3", "a"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("col3", "b"), Content(DiscreteSchema[Long](), 2L)),
    Cell(Position("col4", "a"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("col4", "b"), Content(DiscreteSchema[Long](), 1L))
  )

  val result10 = List(
    Cell(Position("row1", "dep1", "a"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("row1", "dep1", "b"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("row1", "dep2", "a"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("row2", "dep1", "a"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("row2", "dep1", "b"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("row2", "dep2", "b"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("row3", "dep1", "a"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("row3", "dep1", "b"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("row3", "dep2", "b"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("row4", "dep1", "a"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("row4", "dep1", "b"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("row4", "dep2", "a"), Content(DiscreteSchema[Long](), 1L))
  )

  val result11 = List(
    Cell(Position("dep1", "a"), Content(DiscreteSchema[Long](), 4L)),
    Cell(Position("dep1", "b"), Content(DiscreteSchema[Long](), 4L)),
    Cell(Position("dep2", "a"), Content(DiscreteSchema[Long](), 2L)),
    Cell(Position("dep2", "b"), Content(DiscreteSchema[Long](), 2L))
  )

  val result12 = List(
    Cell(Position("row1", "col1", "a"), Content(DiscreteSchema[Long](), 2L)),
    Cell(Position("row1", "col4", "b"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("row2", "col1", "a"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("row2", "col2", "b"), Content(DiscreteSchema[Long](), 2L)),
    Cell(Position("row3", "col2", "b"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("row3", "col3", "a"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("row3", "col3", "b"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("row4", "col2", "a"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("row4", "col3", "b"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("row4", "col4", "a"), Content(DiscreteSchema[Long](), 1L))
  )
}

trait TestQuantile extends TestGrimlock {
  val probs = List(0.2, 0.4, 0.6, 0.8)

  val data1 = List(Cell(Position("foo"), Content(ContinuousSchema[Double](), 3.14)))

  val data2 = List(
    Cell(Position("foo"), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position("bar"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("baz"), Content(ContinuousSchema[Double](), 9.42))
  )

  val data3 = List(
    Cell(Position("foo"), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position("bar"), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position("baz"), Content(ContinuousSchema[Double](), 3.14))
  )

  val data4 = List(
    Cell(Position("row1", "col1"), Content(DiscreteSchema[Long](), 2L)),
    Cell(Position("row2", "col1"), Content(DiscreteSchema[Long](), 3L)),
    Cell(Position("row3", "col1"), Content(DiscreteSchema[Long](), 4L)),
    Cell(Position("row4", "col1"), Content(DiscreteSchema[Long](), 5L)),
    Cell(Position("row5", "col1"), Content(DiscreteSchema[Long](), 4L)),
    Cell(Position("row6", "col1"), Content(DiscreteSchema[Long](), 4L)),
    Cell(Position("row7", "col1"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("row8", "col1"), Content(DiscreteSchema[Long](), 4L)),
    Cell(Position("row1", "col2"), Content(DiscreteSchema[Long](), 42L)),
    Cell(Position("row2", "col2"), Content(DiscreteSchema[Long](), 42L)),
    Cell(Position("row3", "col2"), Content(DiscreteSchema[Long](), 42L)),
    Cell(Position("row4", "col2"), Content(DiscreteSchema[Long](), 42L)),
    Cell(Position("row5", "col2"), Content(DiscreteSchema[Long](), 42L)),
    Cell(Position("row6", "col2"), Content(DiscreteSchema[Long](), 42L)),
    Cell(Position("row7", "col2"), Content(DiscreteSchema[Long](), 42L)),
    Cell(Position("row8", "col2"), Content(DiscreteSchema[Long](), 42L))
  )

  val data5 = data4.map(_.relocate(c => Position(c.position(_1), c.position(_0))))

  val data6 = List(
    Cell(Position("foo"), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position("bar"), Content(NominalSchema[String](), "6.28")),
    Cell(Position("baz"), Content(ContinuousSchema[Double](), 9.42))
  )

  val result1 = List(
    Cell(Position("quantile=0.200000"), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position("quantile=0.400000"), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position("quantile=0.600000"), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position("quantile=0.800000"), Content(ContinuousSchema[Double](), 3.14))
  )

  val result2 = List(
    Cell(Position("quantile=0.200000"), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position("quantile=0.400000"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("quantile=0.600000"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("quantile=0.800000"), Content(ContinuousSchema[Double](), 9.42))
  )

  val result3 = List(
    Cell(Position("quantile=0.200000"), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position("quantile=0.400000"), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position("quantile=0.600000"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("quantile=0.800000"), Content(ContinuousSchema[Double](), 6.28))
  )

  val result4 = List(
    Cell(Position("quantile=0.200000"), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position("quantile=0.400000"), Content(ContinuousSchema[Double](), 3.768)),
    Cell(Position("quantile=0.600000"), Content(ContinuousSchema[Double](), 5.652)),
    Cell(Position("quantile=0.800000"), Content(ContinuousSchema[Double](), 7.536))
  )

  val result5 = List(
    Cell(Position("quantile=0.200000"), Content(ContinuousSchema[Double](), 3.454)),
    Cell(Position("quantile=0.400000"), Content(ContinuousSchema[Double](), 5.338)),
    Cell(Position("quantile=0.600000"), Content(ContinuousSchema[Double](), 7.222)),
    Cell(Position("quantile=0.800000"), Content(ContinuousSchema[Double](), 9.106))
  )

  val result6 = List(
    Cell(Position("quantile=0.200000"), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position("quantile=0.400000"), Content(ContinuousSchema[Double](), 5.024)),
    Cell(Position("quantile=0.600000"), Content(ContinuousSchema[Double](), 7.536)),
    Cell(Position("quantile=0.800000"), Content(ContinuousSchema[Double](), 9.42))
  )

  val result7 = List(
    Cell(Position("quantile=0.200000"), Content(ContinuousSchema[Double](), 4.396)),
    Cell(Position("quantile=0.400000"), Content(ContinuousSchema[Double](), 5.652)),
    Cell(Position("quantile=0.600000"), Content(ContinuousSchema[Double](), 6.908)),
    Cell(Position("quantile=0.800000"), Content(ContinuousSchema[Double](), 8.164))
  )

  val result8 = List(
    Cell(Position("quantile=0.200000"), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position("quantile=0.400000"), Content(ContinuousSchema[Double](), 5.233333)),
    Cell(Position("quantile=0.600000"), Content(ContinuousSchema[Double](), 7.326667)),
    Cell(Position("quantile=0.800000"), Content(ContinuousSchema[Double](), 9.42))
  )

  val result9 = List(
    Cell(Position("quantile=0.200000"), Content(ContinuousSchema[Double](), 3.2185)),
    Cell(Position("quantile=0.400000"), Content(ContinuousSchema[Double](), 5.2595)),
    Cell(Position("quantile=0.600000"), Content(ContinuousSchema[Double](), 7.3005)),
    Cell(Position("quantile=0.800000"), Content(ContinuousSchema[Double](), 9.3415))
  )

  val result10 = List(
    Cell(Position("col1", "quantile=0.200000"), Content(ContinuousSchema[Double](), 2.0)),
    Cell(Position("col1", "quantile=0.400000"), Content(ContinuousSchema[Double](), 4.0)),
    Cell(Position("col1", "quantile=0.600000"), Content(ContinuousSchema[Double](), 4.0)),
    Cell(Position("col1", "quantile=0.800000"), Content(ContinuousSchema[Double](), 4.0)),
    Cell(Position("col2", "quantile=0.200000"), Content(ContinuousSchema[Double](), 42.0)),
    Cell(Position("col2", "quantile=0.400000"), Content(ContinuousSchema[Double](), 42.0)),
    Cell(Position("col2", "quantile=0.600000"), Content(ContinuousSchema[Double](), 42.0)),
    Cell(Position("col2", "quantile=0.800000"), Content(ContinuousSchema[Double](), 42.0))
  )

  val result11 = List(
    Cell(Position("col1", "quantile=0.200000"), Content(ContinuousSchema[Double](), 2.0)),
    Cell(Position("col1", "quantile=0.400000"), Content(ContinuousSchema[Double](), 3.0)),
    Cell(Position("col1", "quantile=0.600000"), Content(ContinuousSchema[Double](), 4.0)),
    Cell(Position("col1", "quantile=0.800000"), Content(ContinuousSchema[Double](), 4.0)),
    Cell(Position("col2", "quantile=0.200000"), Content(ContinuousSchema[Double](), 42.0)),
    Cell(Position("col2", "quantile=0.400000"), Content(ContinuousSchema[Double](), 42.0)),
    Cell(Position("col2", "quantile=0.600000"), Content(ContinuousSchema[Double](), 42.0)),
    Cell(Position("col2", "quantile=0.800000"), Content(ContinuousSchema[Double](), 42.0))
  )

  val result12 = List(
    Cell(Position("col1", "quantile=0.200000"), Content(ContinuousSchema[Double](), 1.6)),
    Cell(Position("col1", "quantile=0.400000"), Content(ContinuousSchema[Double](), 3.2)),
    Cell(Position("col1", "quantile=0.600000"), Content(ContinuousSchema[Double](), 4.0)),
    Cell(Position("col1", "quantile=0.800000"), Content(ContinuousSchema[Double](), 4.0)),
    Cell(Position("col2", "quantile=0.200000"), Content(ContinuousSchema[Double](), 42.0)),
    Cell(Position("col2", "quantile=0.400000"), Content(ContinuousSchema[Double](), 42.0)),
    Cell(Position("col2", "quantile=0.600000"), Content(ContinuousSchema[Double](), 42.0)),
    Cell(Position("col2", "quantile=0.800000"), Content(ContinuousSchema[Double](), 42.0))
  )

  val result13 = List(
    Cell(Position("col1", "quantile=0.200000"), Content(ContinuousSchema[Double](), 2.1)),
    Cell(Position("col1", "quantile=0.400000"), Content(ContinuousSchema[Double](), 3.7)),
    Cell(Position("col1", "quantile=0.600000"), Content(ContinuousSchema[Double](), 4.0)),
    Cell(Position("col1", "quantile=0.800000"), Content(ContinuousSchema[Double](), 4.0)),
    Cell(Position("col2", "quantile=0.200000"), Content(ContinuousSchema[Double](), 42.0)),
    Cell(Position("col2", "quantile=0.400000"), Content(ContinuousSchema[Double](), 42.0)),
    Cell(Position("col2", "quantile=0.600000"), Content(ContinuousSchema[Double](), 42.0)),
    Cell(Position("col2", "quantile=0.800000"), Content(ContinuousSchema[Double](), 42.0))
  )

  val result14 = List(
    Cell(Position("col1", "quantile=0.200000"), Content(ContinuousSchema[Double](), 1.8)),
    Cell(Position("col1", "quantile=0.400000"), Content(ContinuousSchema[Double](), 3.6)),
    Cell(Position("col1", "quantile=0.600000"), Content(ContinuousSchema[Double](), 4.0)),
    Cell(Position("col1", "quantile=0.800000"), Content(ContinuousSchema[Double](), 4.2)),
    Cell(Position("col2", "quantile=0.200000"), Content(ContinuousSchema[Double](), 42.0)),
    Cell(Position("col2", "quantile=0.400000"), Content(ContinuousSchema[Double](), 42.0)),
    Cell(Position("col2", "quantile=0.600000"), Content(ContinuousSchema[Double](), 42.0)),
    Cell(Position("col2", "quantile=0.800000"), Content(ContinuousSchema[Double](), 42.0))
  )

  val result15 = List(
    Cell(Position("col1", "quantile=0.200000"), Content(ContinuousSchema[Double](), 2.4)),
    Cell(Position("col1", "quantile=0.400000"), Content(ContinuousSchema[Double](), 3.8)),
    Cell(Position("col1", "quantile=0.600000"), Content(ContinuousSchema[Double](), 4.0)),
    Cell(Position("col1", "quantile=0.800000"), Content(ContinuousSchema[Double](), 4.0)),
    Cell(Position("col2", "quantile=0.200000"), Content(ContinuousSchema[Double](), 42.0)),
    Cell(Position("col2", "quantile=0.400000"), Content(ContinuousSchema[Double](), 42.0)),
    Cell(Position("col2", "quantile=0.600000"), Content(ContinuousSchema[Double](), 42.0)),
    Cell(Position("col2", "quantile=0.800000"), Content(ContinuousSchema[Double](), 42.0))
  )

  val result16 = List(
    Cell(Position("col1", "quantile=0.200000"), Content(ContinuousSchema[Double](), 2.0)),
    Cell(Position("col1", "quantile=0.400000"), Content(ContinuousSchema[Double](), 3.666667)),
    Cell(Position("col1", "quantile=0.600000"), Content(ContinuousSchema[Double](), 4.0)),
    Cell(Position("col1", "quantile=0.800000"), Content(ContinuousSchema[Double](), 4.0)),
    Cell(Position("col2", "quantile=0.200000"), Content(ContinuousSchema[Double](), 42.0)),
    Cell(Position("col2", "quantile=0.400000"), Content(ContinuousSchema[Double](), 42.0)),
    Cell(Position("col2", "quantile=0.600000"), Content(ContinuousSchema[Double](), 42.0)),
    Cell(Position("col2", "quantile=0.800000"), Content(ContinuousSchema[Double](), 42.0))
  )

  val result17 = List(
    Cell(Position("col1", "quantile=0.200000"), Content(ContinuousSchema[Double](), 2.025)),
    Cell(Position("col1", "quantile=0.400000"), Content(ContinuousSchema[Double](), 3.675)),
    Cell(Position("col1", "quantile=0.600000"), Content(ContinuousSchema[Double](), 4.0)),
    Cell(Position("col1", "quantile=0.800000"), Content(ContinuousSchema[Double](), 4.0)),
    Cell(Position("col2", "quantile=0.200000"), Content(ContinuousSchema[Double](), 42.0)),
    Cell(Position("col2", "quantile=0.400000"), Content(ContinuousSchema[Double](), 42.0)),
    Cell(Position("col2", "quantile=0.600000"), Content(ContinuousSchema[Double](), 42.0)),
    Cell(Position("col2", "quantile=0.800000"), Content(ContinuousSchema[Double](), 42.0))
  )

  val result18 = List(
    Cell(Position("quantile=0.200000"), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position("quantile=0.400000"), Content(ContinuousSchema[Double](), 9.42)),
    Cell(Position("quantile=0.600000"), Content(ContinuousSchema[Double](), 9.42))
  )
}

object TestQuantile {
  def name[
    S <: HList
  ](implicit
    ev: Position.AppendConstraints[S, Value[String]]
  )= (pos: Position[S], value: Double) => pos.append("quantile=%f".format(value)).toOption
}

trait TestApproximateQuantile extends TestGrimlock {
  val rnd = new scala.util.Random(1234)

  val probs = (0.1 to 0.9 by 0.1).toList

  // Simple Gaussian for algorithms that struggle with sparsly populated regions
  val data1 = (1 to 6000)
    .map(_ => rnd.nextGaussian())
    .toList
    .map(d => Cell(Position("not.used"), Content(ContinuousSchema[Double], d)))

  val data2 = (
      (1 to 6000).map(_ => rnd.nextGaussian()    ) ++
      (1 to 4000).map(_ => rnd.nextGaussian() + 2) ++
      (1 to  500).map(_ => rnd.nextGaussian() - 8)
    )
    .toList
    .map(d => Cell(Position("not.used"), Content(ContinuousSchema[Double], d)))
}

object TestApproximateQuantile {
  def name[
    S <: HList
  ](implicit
    ev: Position.AppendConstraints[S, Value[String]]
  )= (pos: Position[S], value: Double) => pos.append("quantile=%f".format(value)).toOption
}

