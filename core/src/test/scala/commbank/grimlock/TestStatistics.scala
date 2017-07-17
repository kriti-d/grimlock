// Copyright 2017 Commonwealth Bank of Australia
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
import commbank.grimlock.framework.environment.tuner._
import commbank.grimlock.framework.metadata._
import commbank.grimlock.framework.position._

import commbank.grimlock.library.aggregate._

import commbank.grimlock.scalding.environment._

import commbank.grimlock.spark.environment._

import com.twitter.scalding.typed.TypedPipe

import org.apache.spark.rdd.RDD

import shapeless.Nat
import shapeless.nat.{ _1, _2, _3 }

trait TestStatistics extends TestGrimlock {

  val num1 = List(
    Cell(Position("foo"), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position("bar"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("baz"), Content(ContinuousSchema[Double](), 9.42)),
    Cell(Position("qux"), Content(ContinuousSchema[Double](), 12.56))
  )

  val num2 = List(
    Cell(Position("foo", 1), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position("bar", 1), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("baz", 1), Content(ContinuousSchema[Double](), 9.42)),
    Cell(Position("qux", 1), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("foo", 2), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("bar", 2), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("baz", 2), Content(ContinuousSchema[Double](), 18.84)),
    Cell(Position("foo", 3), Content(ContinuousSchema[Double](), 9.42)),
    Cell(Position("bar", 3), Content(ContinuousSchema[Double](), 18.84)),
    Cell(Position("foo", 4), Content(ContinuousSchema[Double](), 12.56))
  )

  val num3 = List(
    Cell(Position("foo", 1, "xyz"), Content(ContinuousSchema[Double](), 3.14)),
    Cell(Position("bar", 1, "xyz"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("baz", 1, "xyz"), Content(ContinuousSchema[Double](), 9.42)),
    Cell(Position("qux", 1, "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("foo", 2, "xyz"), Content(ContinuousSchema[Double](), 6.28)),
    Cell(Position("bar", 2, "xyz"), Content(ContinuousSchema[Double](), 12.56)),
    Cell(Position("baz", 2, "xyz"), Content(ContinuousSchema[Double](), 18.84)),
    Cell(Position("foo", 3, "xyz"), Content(ContinuousSchema[Double](), 9.42)),
    Cell(Position("bar", 3, "xyz"), Content(ContinuousSchema[Double](), 18.84)),
    Cell(Position("foo", 4, "xyz"), Content(ContinuousSchema[Double](), 12.56))
  )
}

class TestScaldingStatistics extends TestStatistics {

  val data1 = toPipe(num1)
  val data2 = toPipe(num2)
  val data3 = toPipe(num3)

  val predicate = (con: Content) => con.value gtr 7.0

  implicit class MapNaN[P <: Nat](pipe: TypedPipe[Cell[P]]) {
    def mapNaN() = pipe
      .map {
        case c if c.content.value.asDouble.map(_.isNaN) == Some(true) =>
          c.mutate(_ => Content(NominalSchema[Boolean](), true))
        case c => c
      }
  }

  "A counts" should "return its correct statistic" in {
    data1
      .counts(Over(_1), Default())
      .toList.sortBy(_.position) shouldBe data1.summarise(Over(_1))(Counts()).toList.sortBy(_.position)

    data1
      .counts(Along(_1), Default(12))
      .toList.sortBy(_.position) shouldBe data1.summarise(Along(_1))(Counts()).toList.sortBy(_.position)

    data2
      .counts(Over(_1), Default())
      .toList.sortBy(_.position) shouldBe data2.summarise(Over(_1))(Counts()).toList.sortBy(_.position)

    data2
      .counts(Along(_1), Default(12))
      .toList.sortBy(_.position) shouldBe data2.summarise(Along(_1))(Counts()).toList.sortBy(_.position)

    data2
      .counts(Over(_2), Default())
      .toList.sortBy(_.position) shouldBe data2.summarise(Over(_2))(Counts()).toList.sortBy(_.position)

    data2
      .counts(Along(_2), Default(12))
      .toList.sortBy(_.position) shouldBe data2.summarise(Along(_2))(Counts()).toList.sortBy(_.position)

    data3
      .counts(Over(_1), Default())
      .toList.sortBy(_.position) shouldBe data3.summarise(Over(_1))(Counts()).toList.sortBy(_.position)

    data3
      .counts(Along(_1), Default(12))
      .toList.sortBy(_.position) shouldBe data3.summarise(Along(_1))(Counts()).toList.sortBy(_.position)

    data3
      .counts(Over(_2), Default())
      .toList.sortBy(_.position) shouldBe data3.summarise(Over(_2))(Counts()).toList.sortBy(_.position)

    data3
      .counts(Along(_2), Default(12))
      .toList.sortBy(_.position) shouldBe data3.summarise(Along(_2))(Counts()).toList.sortBy(_.position)

    data3
      .counts(Over(_3), Default())
      .toList.sortBy(_.position) shouldBe data3.summarise(Over(_3))(Counts()).toList.sortBy(_.position)

    data3
      .counts(Along(_3), Default(12))
      .toList.sortBy(_.position) shouldBe data3.summarise(Along(_3))(Counts()).toList.sortBy(_.position)
  }

  "A distinctCounts" should "return its correct statistic" in {
    data1
      .distinctCounts(Over(_1), Default())
      .toList.sortBy(_.position) shouldBe data1.summarise(Over(_1))(DistinctCounts()).toList.sortBy(_.position)

    data1
      .distinctCounts(Along(_1), Default(12))
      .toList.sortBy(_.position) shouldBe data1.summarise(Along(_1))(DistinctCounts()).toList.sortBy(_.position)

    data2
      .distinctCounts(Over(_1), Default())
      .toList.sortBy(_.position) shouldBe data2.summarise(Over(_1))(DistinctCounts()).toList.sortBy(_.position)

    data2
      .distinctCounts(Along(_1), Default(12))
      .toList.sortBy(_.position) shouldBe data2.summarise(Along(_1))(DistinctCounts()).toList.sortBy(_.position)

    data2
      .distinctCounts(Over(_2), Default())
      .toList.sortBy(_.position) shouldBe data2.summarise(Over(_2))(DistinctCounts()).toList.sortBy(_.position)

    data2
      .distinctCounts(Along(_2), Default(12))
      .toList.sortBy(_.position) shouldBe data2.summarise(Along(_2))(DistinctCounts()).toList.sortBy(_.position)

    data3
      .distinctCounts(Over(_1), Default())
      .toList.sortBy(_.position) shouldBe data3.summarise(Over(_1))(DistinctCounts()).toList.sortBy(_.position)

    data3
      .distinctCounts(Along(_1), Default(12))
      .toList.sortBy(_.position) shouldBe data3.summarise(Along(_1))(DistinctCounts()).toList.sortBy(_.position)

    data3
      .distinctCounts(Over(_2), Default())
      .toList.sortBy(_.position) shouldBe data3.summarise(Over(_2))(DistinctCounts()).toList.sortBy(_.position)

    data3
      .distinctCounts(Along(_2), Default(12))
      .toList.sortBy(_.position) shouldBe data3.summarise(Along(_2))(DistinctCounts()).toList.sortBy(_.position)

    data3
      .distinctCounts(Over(_3), Default())
      .toList.sortBy(_.position) shouldBe data3.summarise(Over(_3))(DistinctCounts()).toList.sortBy(_.position)

    data3
      .distinctCounts(Along(_3), Default(12))
      .toList.sortBy(_.position) shouldBe data3.summarise(Along(_3))(DistinctCounts()).toList.sortBy(_.position)
  }

  "A predicateCounts" should "return its correct statistic" in {
    data1
      .predicateCounts(Over(_1), Default())(predicate)
      .toList.sortBy(_.position) shouldBe data1
        .summarise(Over(_1))(PredicateCounts(predicate))
        .toList.sortBy(_.position)

    data1
      .predicateCounts(Along(_1), Default(12))(predicate)
      .toList.sortBy(_.position) shouldBe data1
        .summarise(Along(_1))(PredicateCounts(predicate))
        .toList.sortBy(_.position)

    data2
      .predicateCounts(Over(_1), Default())(predicate)
      .toList.sortBy(_.position) shouldBe data2
        .summarise(Over(_1))(PredicateCounts(predicate))
        .toList.sortBy(_.position)

    data2
      .predicateCounts(Along(_1), Default(12))(predicate)
      .toList.sortBy(_.position) shouldBe data2
        .summarise(Along(_1))(PredicateCounts(predicate))
        .toList.sortBy(_.position)

    data2
      .predicateCounts(Over(_2), Default())(predicate)
      .toList.sortBy(_.position) shouldBe data2
        .summarise(Over(_2))(PredicateCounts(predicate))
        .toList.sortBy(_.position)

    data2
      .predicateCounts(Along(_2), Default(12))(predicate)
      .toList.sortBy(_.position) shouldBe data2
        .summarise(Along(_2))(PredicateCounts(predicate))
        .toList.sortBy(_.position)

    data3
      .predicateCounts(Over(_1), Default())(predicate)
      .toList.sortBy(_.position) shouldBe data3
        .summarise(Over(_1))(PredicateCounts(predicate))
        .toList.sortBy(_.position)

    data3
      .predicateCounts(Along(_1), Default(12))(predicate)
      .toList.sortBy(_.position) shouldBe data3
        .summarise(Along(_1))(PredicateCounts(predicate))
        .toList.sortBy(_.position)

    data3
      .predicateCounts(Over(_2), Default())(predicate)
      .toList.sortBy(_.position) shouldBe data3
        .summarise(Over(_2))(PredicateCounts(predicate))
        .toList.sortBy(_.position)

    data3
      .predicateCounts(Along(_2), Default(12))(predicate)
      .toList.sortBy(_.position) shouldBe data3
        .summarise(Along(_2))(PredicateCounts(predicate))
        .toList.sortBy(_.position)

    data3
      .predicateCounts(Over(_3), Default())(predicate)
      .toList.sortBy(_.position) shouldBe data3
        .summarise(Over(_3))(PredicateCounts(predicate))
        .toList.sortBy(_.position)

    data3
      .predicateCounts(Along(_3), Default(12))(predicate)
      .toList.sortBy(_.position) shouldBe data3
        .summarise(Along(_3))(PredicateCounts(predicate))
        .toList.sortBy(_.position)
  }

  "A mean" should "return its correct statistic" in {
    data1
      .mean(Over(_1), Default())
      .toList.sortBy(_.position) shouldBe data1.summarise(Over(_1))(Mean()).toList.sortBy(_.position)

    data1
      .mean(Along(_1), Default(12))
      .toList.sortBy(_.position) shouldBe data1.summarise(Along(_1))(Mean()).toList.sortBy(_.position)

    data2
      .mean(Over(_1), Default())
      .toList.sortBy(_.position) shouldBe data2.summarise(Over(_1))(Mean()).toList.sortBy(_.position)

    data2
      .mean(Along(_1), Default(12))
      .toList.sortBy(_.position) shouldBe data2.summarise(Along(_1))(Mean()).toList.sortBy(_.position)

    data2
      .mean(Over(_2), Default())
      .toList.sortBy(_.position) shouldBe data2.summarise(Over(_2))(Mean()).toList.sortBy(_.position)

    data2
      .mean(Along(_2), Default(12))
      .toList.sortBy(_.position) shouldBe data2.summarise(Along(_2))(Mean()).toList.sortBy(_.position)

    data3
      .mean(Over(_1), Default())
      .toList.sortBy(_.position) shouldBe data3.summarise(Over(_1))(Mean()).toList.sortBy(_.position)

    data3
      .mean(Along(_1), Default(12))
      .toList.sortBy(_.position) shouldBe data3.summarise(Along(_1))(Mean()).toList.sortBy(_.position)

    data3
      .mean(Over(_2), Default())
      .toList.sortBy(_.position) shouldBe data3.summarise(Over(_2))(Mean()).toList.sortBy(_.position)

    data3
      .mean(Along(_2), Default(12))
      .toList.sortBy(_.position) shouldBe data3.summarise(Along(_2))(Mean()).toList.sortBy(_.position)

    data3
      .mean(Over(_3), Default())
      .toList.sortBy(_.position) shouldBe data3.summarise(Over(_3))(Mean()).toList.sortBy(_.position)

    data3
      .mean(Along(_3), Default(12))
      .toList.sortBy(_.position) shouldBe data3.summarise(Along(_3))(Mean()).toList.sortBy(_.position)
  }

  "A standardDeviation" should "return its correct statistic" in {
    data1
      .standardDeviation(Over(_1), Default())(true)
      .mapNaN.toList.sortBy(_.position) shouldBe data1
        .summarise(Over(_1))(StandardDeviation(true))
        .mapNaN.toList.sortBy(_.position)

    data1
      .standardDeviation(Along(_1), Default(12))(false)
      .mapNaN.toList.sortBy(_.position) shouldBe data1
        .summarise(Along(_1))(StandardDeviation())
        .mapNaN.toList.sortBy(_.position)

    data2
      .standardDeviation(Over(_1), Default())(true)
      .mapNaN.toList.sortBy(_.position) shouldBe data2
        .summarise(Over(_1))(StandardDeviation(true))
        .mapNaN.toList.sortBy(_.position)

    data2
      .standardDeviation(Along(_1), Default(12))(false)
      .mapNaN.toList.sortBy(_.position) shouldBe data2
        .summarise(Along(_1))(StandardDeviation())
        .mapNaN.toList.sortBy(_.position)

    data2
      .standardDeviation(Over(_2), Default())(true)
      .mapNaN.toList.sortBy(_.position) shouldBe data2
        .summarise(Over(_2))(StandardDeviation(true))
        .mapNaN.toList.sortBy(_.position)

    data2
      .standardDeviation(Along(_2), Default(12))(false)
      .mapNaN.toList.sortBy(_.position) shouldBe data2
        .summarise(Along(_2))(StandardDeviation())
        .mapNaN.toList.sortBy(_.position)

    data3
      .standardDeviation(Over(_1), Default())(true)
      .mapNaN.toList.sortBy(_.position) shouldBe data3
        .summarise(Over(_1))(StandardDeviation(true))
        .mapNaN.toList.sortBy(_.position)

    data3
      .standardDeviation(Along(_1), Default(12))(false)
      .mapNaN.toList.sortBy(_.position) shouldBe data3
        .summarise(Along(_1))(StandardDeviation())
        .mapNaN.toList.sortBy(_.position)

    data3
      .standardDeviation(Over(_2), Default())(true)
      .mapNaN.toList.sortBy(_.position) shouldBe data3
        .summarise(Over(_2))(StandardDeviation(true))
        .mapNaN.toList.sortBy(_.position)

    data3
      .standardDeviation(Along(_2), Default(12))(false)
      .mapNaN.toList.sortBy(_.position) shouldBe data3
        .summarise(Along(_2))(StandardDeviation())
        .mapNaN.toList.sortBy(_.position)

    data3
      .standardDeviation(Over(_3), Default())(true)
      .mapNaN.toList.sortBy(_.position) shouldBe data3
        .summarise(Over(_3))(StandardDeviation(true))
        .mapNaN.toList.sortBy(_.position)

    data3
      .standardDeviation(Along(_3), Default(12))(false)
      .mapNaN.toList.sortBy(_.position) shouldBe data3
        .summarise(Along(_3))(StandardDeviation())
        .mapNaN.toList.sortBy(_.position)
  }

  "A skewness" should "return its correct statistic" in {
    data1
      .skewness(Over(_1), Default())
      .mapNaN.toList.sortBy(_.position) shouldBe data1.summarise(Over(_1))(Skewness()).mapNaN.toList.sortBy(_.position)

    data1
      .skewness(Along(_1), Default(12))
      .mapNaN.toList.sortBy(_.position) shouldBe data1.summarise(Along(_1))(Skewness()).mapNaN.toList.sortBy(_.position)

    data2
      .skewness(Over(_1), Default())
      .mapNaN.toList.sortBy(_.position) shouldBe data2.summarise(Over(_1))(Skewness()).mapNaN.toList.sortBy(_.position)

    data2
      .skewness(Along(_1), Default(12))
      .mapNaN.toList.sortBy(_.position) shouldBe data2.summarise(Along(_1))(Skewness()).mapNaN.toList.sortBy(_.position)

    data2
      .skewness(Over(_2), Default())
      .mapNaN.toList.sortBy(_.position) shouldBe data2.summarise(Over(_2))(Skewness()).mapNaN.toList.sortBy(_.position)

    data2
      .skewness(Along(_2), Default(12))
      .mapNaN.toList.sortBy(_.position) shouldBe data2.summarise(Along(_2))(Skewness()).mapNaN.toList.sortBy(_.position)

    data3
      .skewness(Over(_1), Default())
      .mapNaN.toList.sortBy(_.position) shouldBe data3.summarise(Over(_1))(Skewness()).mapNaN.toList.sortBy(_.position)

    data3
      .skewness(Along(_1), Default(12))
      .mapNaN.toList.sortBy(_.position) shouldBe data3.summarise(Along(_1))(Skewness()).mapNaN.toList.sortBy(_.position)

    data3
      .skewness(Over(_2), Default())
      .mapNaN.toList.sortBy(_.position) shouldBe data3.summarise(Over(_2))(Skewness()).mapNaN.toList.sortBy(_.position)

    data3
      .skewness(Along(_2), Default(12))
      .mapNaN.toList.sortBy(_.position) shouldBe data3.summarise(Along(_2))(Skewness()).mapNaN.toList.sortBy(_.position)

    data3
      .skewness(Over(_3), Default())
      .mapNaN.toList.sortBy(_.position) shouldBe data3.summarise(Over(_3))(Skewness()).mapNaN.toList.sortBy(_.position)

    data3
      .skewness(Along(_3), Default(12))
      .mapNaN.toList.sortBy(_.position) shouldBe data3.summarise(Along(_3))(Skewness()).mapNaN.toList.sortBy(_.position)
  }

  "A kurtosis" should "return its correct statistic" in {
    data1
      .kurtosis(Over(_1), Default())(true)
      .mapNaN.toList.sortBy(_.position) shouldBe data1
        .summarise(Over(_1))(Kurtosis(true))
        .mapNaN.toList.sortBy(_.position)

    data1
      .kurtosis(Along(_1), Default(12))(false)
      .mapNaN.toList.sortBy(_.position) shouldBe data1
        .summarise(Along(_1))(Kurtosis())
        .mapNaN.toList.sortBy(_.position)

    data2
      .kurtosis(Over(_1), Default())(true)
      .mapNaN.toList.sortBy(_.position) shouldBe data2
        .summarise(Over(_1))(Kurtosis(true))
        .mapNaN.toList.sortBy(_.position)

    data2
      .kurtosis(Along(_1), Default(12))(false)
      .mapNaN.toList.sortBy(_.position) shouldBe data2
        .summarise(Along(_1))(Kurtosis())
        .mapNaN.toList.sortBy(_.position)

    data2
      .kurtosis(Over(_2), Default())(true)
      .mapNaN.toList.sortBy(_.position) shouldBe data2
        .summarise(Over(_2))(Kurtosis(true))
        .mapNaN.toList.sortBy(_.position)

    data2
      .kurtosis(Along(_2), Default(12))(false)
      .mapNaN.toList.sortBy(_.position) shouldBe data2
        .summarise(Along(_2))(Kurtosis())
        .mapNaN.toList.sortBy(_.position)

    data3
      .kurtosis(Over(_1), Default())(true)
      .mapNaN.toList.sortBy(_.position) shouldBe data3
        .summarise(Over(_1))(Kurtosis(true))
        .mapNaN.toList.sortBy(_.position)

    data3
      .kurtosis(Along(_1), Default(12))(false)
      .mapNaN.toList.sortBy(_.position) shouldBe data3
        .summarise(Along(_1))(Kurtosis())
        .mapNaN.toList.sortBy(_.position)

    data3
      .kurtosis(Over(_2), Default())(true)
      .mapNaN.toList.sortBy(_.position) shouldBe data3
        .summarise(Over(_2))(Kurtosis(true))
        .mapNaN.toList.sortBy(_.position)

    data3
      .kurtosis(Along(_2), Default(12))(false)
      .mapNaN.toList.sortBy(_.position) shouldBe data3
        .summarise(Along(_2))(Kurtosis())
        .mapNaN.toList.sortBy(_.position)

    data3
      .kurtosis(Over(_3), Default())(true)
      .mapNaN.toList.sortBy(_.position) shouldBe data3
        .summarise(Over(_3))(Kurtosis(true))
        .mapNaN.toList.sortBy(_.position)

    data3
      .kurtosis(Along(_3), Default(12))(false)
      .mapNaN.toList.sortBy(_.position) shouldBe data3
        .summarise(Along(_3))(Kurtosis())
        .mapNaN.toList.sortBy(_.position)
  }

  "A minimum" should "return its correct statistic" in {
    data1
      .minimum(Over(_1), Default())
      .toList.sortBy(_.position) shouldBe data1.summarise(Over(_1))(Minimum()).toList.sortBy(_.position)

    data1
      .minimum(Along(_1), Default(12))
      .toList.sortBy(_.position) shouldBe data1.summarise(Along(_1))(Minimum()).toList.sortBy(_.position)

    data2
      .minimum(Over(_1), Default())
      .toList.sortBy(_.position) shouldBe data2.summarise(Over(_1))(Minimum()).toList.sortBy(_.position)

    data2
      .minimum(Along(_1), Default(12))
      .toList.sortBy(_.position) shouldBe data2.summarise(Along(_1))(Minimum()).toList.sortBy(_.position)

    data2
      .minimum(Over(_2), Default())
      .toList.sortBy(_.position) shouldBe data2.summarise(Over(_2))(Minimum()).toList.sortBy(_.position)

    data2
      .minimum(Along(_2), Default(12))
      .toList.sortBy(_.position) shouldBe data2.summarise(Along(_2))(Minimum()).toList.sortBy(_.position)

    data3
      .minimum(Over(_1), Default())
      .toList.sortBy(_.position) shouldBe data3.summarise(Over(_1))(Minimum()).toList.sortBy(_.position)

    data3
      .minimum(Along(_1), Default(12))
      .toList.sortBy(_.position) shouldBe data3.summarise(Along(_1))(Minimum()).toList.sortBy(_.position)

    data3
      .minimum(Over(_2), Default())
      .toList.sortBy(_.position) shouldBe data3.summarise(Over(_2))(Minimum()).toList.sortBy(_.position)

    data3
      .minimum(Along(_2), Default(12))
      .toList.sortBy(_.position) shouldBe data3.summarise(Along(_2))(Minimum()).toList.sortBy(_.position)

    data3
      .minimum(Over(_3), Default())
      .toList.sortBy(_.position) shouldBe data3.summarise(Over(_3))(Minimum()).toList.sortBy(_.position)

    data3
      .minimum(Along(_3), Default(12))
      .toList.sortBy(_.position) shouldBe data3.summarise(Along(_3))(Minimum()).toList.sortBy(_.position)
  }

  "A maximum" should "return its correct statistic" in {
    data1
      .maximum(Over(_1), Default())
      .toList.sortBy(_.position) shouldBe data1.summarise(Over(_1))(Maximum()).toList.sortBy(_.position)

    data1
      .maximum(Along(_1), Default(12))
      .toList.sortBy(_.position) shouldBe data1.summarise(Along(_1))(Maximum()).toList.sortBy(_.position)

    data2
      .maximum(Over(_1), Default())
      .toList.sortBy(_.position) shouldBe data2.summarise(Over(_1))(Maximum()).toList.sortBy(_.position)

    data2
      .maximum(Along(_1), Default(12))
      .toList.sortBy(_.position) shouldBe data2.summarise(Along(_1))(Maximum()).toList.sortBy(_.position)

    data2
      .maximum(Over(_2), Default())
      .toList.sortBy(_.position) shouldBe data2.summarise(Over(_2))(Maximum()).toList.sortBy(_.position)

    data2
      .maximum(Along(_2), Default(12))
      .toList.sortBy(_.position) shouldBe data2.summarise(Along(_2))(Maximum()).toList.sortBy(_.position)

    data3
      .maximum(Over(_1), Default())
      .toList.sortBy(_.position) shouldBe data3.summarise(Over(_1))(Maximum()).toList.sortBy(_.position)

    data3
      .maximum(Along(_1), Default(12))
      .toList.sortBy(_.position) shouldBe data3.summarise(Along(_1))(Maximum()).toList.sortBy(_.position)

    data3
      .maximum(Over(_2), Default())
      .toList.sortBy(_.position) shouldBe data3.summarise(Over(_2))(Maximum()).toList.sortBy(_.position)

    data3
      .maximum(Along(_2), Default(12))
      .toList.sortBy(_.position) shouldBe data3.summarise(Along(_2))(Maximum()).toList.sortBy(_.position)

    data3
      .maximum(Over(_3), Default())
      .toList.sortBy(_.position) shouldBe data3.summarise(Over(_3))(Maximum()).toList.sortBy(_.position)

    data3
      .maximum(Along(_3), Default(12))
      .toList.sortBy(_.position) shouldBe data3.summarise(Along(_3))(Maximum()).toList.sortBy(_.position)
  }

  "A maximumAbsolute" should "return its correct statistic" in {
    data1
      .maximumAbsolute(Over(_1), Default())
      .toList.sortBy(_.position) shouldBe data1.summarise(Over(_1))(MaximumAbsolute()).toList.sortBy(_.position)

    data1
      .maximumAbsolute(Along(_1), Default(12))
      .toList.sortBy(_.position) shouldBe data1.summarise(Along(_1))(MaximumAbsolute()).toList.sortBy(_.position)

    data2
      .maximumAbsolute(Over(_1), Default())
      .toList.sortBy(_.position) shouldBe data2.summarise(Over(_1))(MaximumAbsolute()).toList.sortBy(_.position)

    data2
      .maximumAbsolute(Along(_1), Default(12))
      .toList.sortBy(_.position) shouldBe data2.summarise(Along(_1))(MaximumAbsolute()).toList.sortBy(_.position)

    data2
      .maximumAbsolute(Over(_2), Default())
      .toList.sortBy(_.position) shouldBe data2.summarise(Over(_2))(MaximumAbsolute()).toList.sortBy(_.position)

    data2
      .maximumAbsolute(Along(_2), Default(12))
      .toList.sortBy(_.position) shouldBe data2.summarise(Along(_2))(MaximumAbsolute()).toList.sortBy(_.position)

    data3
      .maximumAbsolute(Over(_1), Default())
      .toList.sortBy(_.position) shouldBe data3.summarise(Over(_1))(MaximumAbsolute()).toList.sortBy(_.position)

    data3
      .maximumAbsolute(Along(_1), Default(12))
      .toList.sortBy(_.position) shouldBe data3.summarise(Along(_1))(MaximumAbsolute()).toList.sortBy(_.position)

    data3
      .maximumAbsolute(Over(_2), Default())
      .toList.sortBy(_.position) shouldBe data3.summarise(Over(_2))(MaximumAbsolute()).toList.sortBy(_.position)

    data3
      .maximumAbsolute(Along(_2), Default(12))
      .toList.sortBy(_.position) shouldBe data3.summarise(Along(_2))(MaximumAbsolute()).toList.sortBy(_.position)

    data3
      .maximumAbsolute(Over(_3), Default())
      .toList.sortBy(_.position) shouldBe data3.summarise(Over(_3))(MaximumAbsolute()).toList.sortBy(_.position)

    data3
      .maximumAbsolute(Along(_3), Default(12))
      .toList.sortBy(_.position) shouldBe data3.summarise(Along(_3))(MaximumAbsolute()).toList.sortBy(_.position)
  }

  "A sums" should "return its correct statistic" in {
    data1
      .sums(Over(_1), Default())
      .toList.sortBy(_.position) shouldBe data1.summarise(Over(_1))(Sums()).toList.sortBy(_.position)

    data1
      .sums(Along(_1), Default(12))
      .toList.sortBy(_.position) shouldBe data1.summarise(Along(_1))(Sums()).toList.sortBy(_.position)

    data2
      .sums(Over(_1), Default())
      .toList.sortBy(_.position) shouldBe data2.summarise(Over(_1))(Sums()).toList.sortBy(_.position)

    data2
      .sums(Along(_1), Default(12))
      .toList.sortBy(_.position) shouldBe data2.summarise(Along(_1))(Sums()).toList.sortBy(_.position)

    data2
      .sums(Over(_2), Default())
      .toList.sortBy(_.position) shouldBe data2.summarise(Over(_2))(Sums()).toList.sortBy(_.position)

    data2
      .sums(Along(_2), Default(12))
      .toList.sortBy(_.position) shouldBe data2.summarise(Along(_2))(Sums()).toList.sortBy(_.position)

    data3
      .sums(Over(_1), Default())
      .toList.sortBy(_.position) shouldBe data3.summarise(Over(_1))(Sums()).toList.sortBy(_.position)

    data3
      .sums(Along(_1), Default(12))
      .toList.sortBy(_.position) shouldBe data3.summarise(Along(_1))(Sums()).toList.sortBy(_.position)

    data3
      .sums(Over(_2), Default())
      .toList.sortBy(_.position) shouldBe data3.summarise(Over(_2))(Sums()).toList.sortBy(_.position)

    data3
      .sums(Along(_2), Default(12))
      .toList.sortBy(_.position) shouldBe data3.summarise(Along(_2))(Sums()).toList.sortBy(_.position)

    data3
      .sums(Over(_3), Default())
      .toList.sortBy(_.position) shouldBe data3.summarise(Over(_3))(Sums()).toList.sortBy(_.position)

    data3
      .sums(Along(_3), Default(12))
      .toList.sortBy(_.position) shouldBe data3.summarise(Along(_3))(Sums()).toList.sortBy(_.position)
  }
}

class TestSparkStatistics extends TestStatistics {

  val data1 = toRDD(num1)
  val data2 = toRDD(num2)
  val data3 = toRDD(num3)

  val predicate = (con: Content) => con.value gtr 7.0

  implicit class MapNaN[P <: Nat](rdd: RDD[Cell[P]]) {
    def mapNaN() = rdd
      .map {
        case c if c.content.value.asDouble.map(_.isNaN) == Some(true) =>
          c.mutate(_ => Content(NominalSchema[Boolean](), true))
        case c => c
      }
  }

  "A counts" should "return its correct statistic" in {
    data1
      .counts(Over(_1), Default())
      .toList.sortBy(_.position) shouldBe data1.summarise(Over(_1))(Counts()).toList.sortBy(_.position)

    data1
      .counts(Along(_1), Default(12))
      .toList.sortBy(_.position) shouldBe data1.summarise(Along(_1))(Counts()).toList.sortBy(_.position)

    data2
      .counts(Over(_1), Default())
      .toList.sortBy(_.position) shouldBe data2.summarise(Over(_1))(Counts()).toList.sortBy(_.position)

    data2
      .counts(Along(_1), Default(12))
      .toList.sortBy(_.position) shouldBe data2.summarise(Along(_1))(Counts()).toList.sortBy(_.position)

    data2
      .counts(Over(_2), Default())
      .toList.sortBy(_.position) shouldBe data2.summarise(Over(_2))(Counts()).toList.sortBy(_.position)

    data2
      .counts(Along(_2), Default(12))
      .toList.sortBy(_.position) shouldBe data2.summarise(Along(_2))(Counts()).toList.sortBy(_.position)

    data3
      .counts(Over(_1), Default())
      .toList.sortBy(_.position) shouldBe data3.summarise(Over(_1))(Counts()).toList.sortBy(_.position)

    data3
      .counts(Along(_1), Default(12))
      .toList.sortBy(_.position) shouldBe data3.summarise(Along(_1))(Counts()).toList.sortBy(_.position)

    data3
      .counts(Over(_2), Default())
      .toList.sortBy(_.position) shouldBe data3.summarise(Over(_2))(Counts()).toList.sortBy(_.position)

    data3
      .counts(Along(_2), Default(12))
      .toList.sortBy(_.position) shouldBe data3.summarise(Along(_2))(Counts()).toList.sortBy(_.position)

    data3
      .counts(Over(_3), Default())
      .toList.sortBy(_.position) shouldBe data3.summarise(Over(_3))(Counts()).toList.sortBy(_.position)

    data3
      .counts(Along(_3), Default(12))
      .toList.sortBy(_.position) shouldBe data3.summarise(Along(_3))(Counts()).toList.sortBy(_.position)
  }

  "A distinctCounts" should "return its correct statistic" in {
    data1
      .distinctCounts(Over(_1), Default())
      .toList.sortBy(_.position) shouldBe data1.summarise(Over(_1))(DistinctCounts()).toList.sortBy(_.position)

    data1
      .distinctCounts(Along(_1), Default(12))
      .toList.sortBy(_.position) shouldBe data1.summarise(Along(_1))(DistinctCounts()).toList.sortBy(_.position)

    data2
      .distinctCounts(Over(_1), Default())
      .toList.sortBy(_.position) shouldBe data2.summarise(Over(_1))(DistinctCounts()).toList.sortBy(_.position)

    data2
      .distinctCounts(Along(_1), Default(12))
      .toList.sortBy(_.position) shouldBe data2.summarise(Along(_1))(DistinctCounts()).toList.sortBy(_.position)

    data2
      .distinctCounts(Over(_2), Default())
      .toList.sortBy(_.position) shouldBe data2.summarise(Over(_2))(DistinctCounts()).toList.sortBy(_.position)

    data2
      .distinctCounts(Along(_2), Default(12))
      .toList.sortBy(_.position) shouldBe data2.summarise(Along(_2))(DistinctCounts()).toList.sortBy(_.position)

    data3
      .distinctCounts(Over(_1), Default())
      .toList.sortBy(_.position) shouldBe data3.summarise(Over(_1))(DistinctCounts()).toList.sortBy(_.position)

    data3
      .distinctCounts(Along(_1), Default(12))
      .toList.sortBy(_.position) shouldBe data3.summarise(Along(_1))(DistinctCounts()).toList.sortBy(_.position)

    data3
      .distinctCounts(Over(_2), Default())
      .toList.sortBy(_.position) shouldBe data3.summarise(Over(_2))(DistinctCounts()).toList.sortBy(_.position)

    data3
      .distinctCounts(Along(_2), Default(12))
      .toList.sortBy(_.position) shouldBe data3.summarise(Along(_2))(DistinctCounts()).toList.sortBy(_.position)

    data3
      .distinctCounts(Over(_3), Default())
      .toList.sortBy(_.position) shouldBe data3.summarise(Over(_3))(DistinctCounts()).toList.sortBy(_.position)

    data3
      .distinctCounts(Along(_3), Default(12))
      .toList.sortBy(_.position) shouldBe data3.summarise(Along(_3))(DistinctCounts()).toList.sortBy(_.position)
  }

  "A predicateCounts" should "return its correct statistic" in {
    data1
      .predicateCounts(Over(_1), Default())(predicate)
      .toList.sortBy(_.position) shouldBe data1
        .summarise(Over(_1))(PredicateCounts(predicate))
        .toList.sortBy(_.position)

    data1
      .predicateCounts(Along(_1), Default(12))(predicate)
      .toList.sortBy(_.position) shouldBe data1
        .summarise(Along(_1))(PredicateCounts(predicate))
        .toList.sortBy(_.position)

    data2
      .predicateCounts(Over(_1), Default())(predicate)
      .toList.sortBy(_.position) shouldBe data2
        .summarise(Over(_1))(PredicateCounts(predicate))
        .toList.sortBy(_.position)

    data2
      .predicateCounts(Along(_1), Default(12))(predicate)
      .toList.sortBy(_.position) shouldBe data2
        .summarise(Along(_1))(PredicateCounts(predicate))
        .toList.sortBy(_.position)

    data2
      .predicateCounts(Over(_2), Default())(predicate)
      .toList.sortBy(_.position) shouldBe data2
        .summarise(Over(_2))(PredicateCounts(predicate))
        .toList.sortBy(_.position)

    data2
      .predicateCounts(Along(_2), Default(12))(predicate)
      .toList.sortBy(_.position) shouldBe data2
        .summarise(Along(_2))(PredicateCounts(predicate))
        .toList.sortBy(_.position)

    data3
      .predicateCounts(Over(_1), Default())(predicate)
      .toList.sortBy(_.position) shouldBe data3
        .summarise(Over(_1))(PredicateCounts(predicate))
        .toList.sortBy(_.position)

    data3
      .predicateCounts(Along(_1), Default(12))(predicate)
      .toList.sortBy(_.position) shouldBe data3
        .summarise(Along(_1))(PredicateCounts(predicate))
        .toList.sortBy(_.position)

    data3
      .predicateCounts(Over(_2), Default())(predicate)
      .toList.sortBy(_.position) shouldBe data3
        .summarise(Over(_2))(PredicateCounts(predicate))
        .toList.sortBy(_.position)

    data3
      .predicateCounts(Along(_2), Default(12))(predicate)
      .toList.sortBy(_.position) shouldBe data3
        .summarise(Along(_2))(PredicateCounts(predicate))
        .toList.sortBy(_.position)

    data3
      .predicateCounts(Over(_3), Default())(predicate)
      .toList.sortBy(_.position) shouldBe data3
        .summarise(Over(_3))(PredicateCounts(predicate))
        .toList.sortBy(_.position)

    data3
      .predicateCounts(Along(_3), Default(12))(predicate)
      .toList.sortBy(_.position) shouldBe data3
        .summarise(Along(_3))(PredicateCounts(predicate))
        .toList.sortBy(_.position)
  }

  "A mean" should "return its correct statistic" in {
    data1
      .mean(Over(_1), Default())
      .toList.sortBy(_.position) shouldBe data1.summarise(Over(_1))(Mean()).toList.sortBy(_.position)

    data1
      .mean(Along(_1), Default(12))
      .toList.sortBy(_.position) shouldBe data1.summarise(Along(_1))(Mean()).toList.sortBy(_.position)

    data2
      .mean(Over(_1), Default())
      .toList.sortBy(_.position) shouldBe data2.summarise(Over(_1))(Mean()).toList.sortBy(_.position)

    data2
      .mean(Along(_1), Default(12))
      .toList.sortBy(_.position) shouldBe data2.summarise(Along(_1))(Mean()).toList.sortBy(_.position)

    data2
      .mean(Over(_2), Default())
      .toList.sortBy(_.position) shouldBe data2.summarise(Over(_2))(Mean()).toList.sortBy(_.position)

    data2
      .mean(Along(_2), Default(12))
      .toList.sortBy(_.position) shouldBe data2.summarise(Along(_2))(Mean()).toList.sortBy(_.position)

    data3
      .mean(Over(_1), Default())
      .toList.sortBy(_.position) shouldBe data3.summarise(Over(_1))(Mean()).toList.sortBy(_.position)

    data3
      .mean(Along(_1), Default(12))
      .toList.sortBy(_.position) shouldBe data3.summarise(Along(_1))(Mean()).toList.sortBy(_.position)

    data3
      .mean(Over(_2), Default())
      .toList.sortBy(_.position) shouldBe data3.summarise(Over(_2))(Mean()).toList.sortBy(_.position)

    data3
      .mean(Along(_2), Default(12))
      .toList.sortBy(_.position) shouldBe data3.summarise(Along(_2))(Mean()).toList.sortBy(_.position)

    data3
      .mean(Over(_3), Default())
      .toList.sortBy(_.position) shouldBe data3.summarise(Over(_3))(Mean()).toList.sortBy(_.position)

    data3
      .mean(Along(_3), Default(12))
      .toList.sortBy(_.position) shouldBe data3.summarise(Along(_3))(Mean()).toList.sortBy(_.position)
  }

  "A standardDeviation" should "return its correct statistic" in {
    data1
      .standardDeviation(Over(_1), Default())(true)
      .mapNaN.toList.sortBy(_.position) shouldBe data1
        .summarise(Over(_1))(StandardDeviation(true))
        .mapNaN.toList.sortBy(_.position)

    data1
      .standardDeviation(Along(_1), Default(12))(false)
      .mapNaN.toList.sortBy(_.position) shouldBe data1
        .summarise(Along(_1))(StandardDeviation())
        .mapNaN.toList.sortBy(_.position)

    data2
      .standardDeviation(Over(_1), Default())(true)
      .mapNaN.toList.sortBy(_.position) shouldBe data2
        .summarise(Over(_1))(StandardDeviation(true))
        .mapNaN.toList.sortBy(_.position)

    data2
      .standardDeviation(Along(_1), Default(12))(false)
      .mapNaN.toList.sortBy(_.position) shouldBe data2
        .summarise(Along(_1))(StandardDeviation())
        .mapNaN.toList.sortBy(_.position)

    data2
      .standardDeviation(Over(_2), Default())(true)
      .mapNaN.toList.sortBy(_.position) shouldBe data2
        .summarise(Over(_2))(StandardDeviation(true))
        .mapNaN.toList.sortBy(_.position)

    data2
      .standardDeviation(Along(_2), Default(12))(false)
      .mapNaN.toList.sortBy(_.position) shouldBe data2
        .summarise(Along(_2))(StandardDeviation())
        .mapNaN.toList.sortBy(_.position)

    data3
      .standardDeviation(Over(_1), Default())(true)
      .mapNaN.toList.sortBy(_.position) shouldBe data3
        .summarise(Over(_1))(StandardDeviation(true))
        .mapNaN.toList.sortBy(_.position)

    data3
      .standardDeviation(Along(_1), Default(12))(false)
      .mapNaN.toList.sortBy(_.position) shouldBe data3
        .summarise(Along(_1))(StandardDeviation())
        .mapNaN.toList.sortBy(_.position)

    data3
      .standardDeviation(Over(_2), Default())(true)
      .mapNaN.toList.sortBy(_.position) shouldBe data3
        .summarise(Over(_2))(StandardDeviation(true))
        .mapNaN.toList.sortBy(_.position)

    data3
      .standardDeviation(Along(_2), Default(12))(false)
      .mapNaN.toList.sortBy(_.position) shouldBe data3
        .summarise(Along(_2))(StandardDeviation())
        .mapNaN.toList.sortBy(_.position)

    data3
      .standardDeviation(Over(_3), Default())(true)
      .mapNaN.toList.sortBy(_.position) shouldBe data3
        .summarise(Over(_3))(StandardDeviation(true))
        .mapNaN.toList.sortBy(_.position)

    data3
      .standardDeviation(Along(_3), Default(12))(false)
      .mapNaN.toList.sortBy(_.position) shouldBe data3
        .summarise(Along(_3))(StandardDeviation())
        .mapNaN.toList.sortBy(_.position)
  }

  "A skewness" should "return its correct statistic" in {
    data1
      .skewness(Over(_1), Default())
      .mapNaN.toList.sortBy(_.position) shouldBe data1.summarise(Over(_1))(Skewness()).mapNaN.toList.sortBy(_.position)

    data1
      .skewness(Along(_1), Default(12))
      .mapNaN.toList.sortBy(_.position) shouldBe data1.summarise(Along(_1))(Skewness()).mapNaN.toList.sortBy(_.position)

    data2
      .skewness(Over(_1), Default())
      .mapNaN.toList.sortBy(_.position) shouldBe data2.summarise(Over(_1))(Skewness()).mapNaN.toList.sortBy(_.position)

    data2
      .skewness(Along(_1), Default(12))
      .mapNaN.toList.sortBy(_.position) shouldBe data2.summarise(Along(_1))(Skewness()).mapNaN.toList.sortBy(_.position)

    data2
      .skewness(Over(_2), Default())
      .mapNaN.toList.sortBy(_.position) shouldBe data2.summarise(Over(_2))(Skewness()).mapNaN.toList.sortBy(_.position)

    data2
      .skewness(Along(_2), Default(12))
      .mapNaN.toList.sortBy(_.position) shouldBe data2.summarise(Along(_2))(Skewness()).mapNaN.toList.sortBy(_.position)

    data3
      .skewness(Over(_1), Default())
      .mapNaN.toList.sortBy(_.position) shouldBe data3.summarise(Over(_1))(Skewness()).mapNaN.toList.sortBy(_.position)

    data3
      .skewness(Along(_1), Default(12))
      .mapNaN.toList.sortBy(_.position) shouldBe data3.summarise(Along(_1))(Skewness()).mapNaN.toList.sortBy(_.position)

    data3
      .skewness(Over(_2), Default())
      .mapNaN.toList.sortBy(_.position) shouldBe data3.summarise(Over(_2))(Skewness()).mapNaN.toList.sortBy(_.position)

    data3
      .skewness(Along(_2), Default(12))
      .mapNaN.toList.sortBy(_.position) shouldBe data3.summarise(Along(_2))(Skewness()).mapNaN.toList.sortBy(_.position)

    data3
      .skewness(Over(_3), Default())
      .mapNaN.toList.sortBy(_.position) shouldBe data3.summarise(Over(_3))(Skewness()).mapNaN.toList.sortBy(_.position)

    data3
      .skewness(Along(_3), Default(12))
      .mapNaN.toList.sortBy(_.position) shouldBe data3.summarise(Along(_3))(Skewness()).mapNaN.toList.sortBy(_.position)
  }

  "A kurtosis" should "return its correct statistic" in {
    data1
      .kurtosis(Over(_1), Default())(true)
      .mapNaN.toList.sortBy(_.position) shouldBe data1
        .summarise(Over(_1))(Kurtosis(true))
        .mapNaN.toList.sortBy(_.position)

    data1
      .kurtosis(Along(_1), Default(12))(false)
      .mapNaN.toList.sortBy(_.position) shouldBe data1
        .summarise(Along(_1))(Kurtosis())
        .mapNaN.toList.sortBy(_.position)

    data2
      .kurtosis(Over(_1), Default())(true)
      .mapNaN.toList.sortBy(_.position) shouldBe data2
        .summarise(Over(_1))(Kurtosis(true))
        .mapNaN.toList.sortBy(_.position)

    data2
      .kurtosis(Along(_1), Default(12))(false)
      .mapNaN.toList.sortBy(_.position) shouldBe data2
        .summarise(Along(_1))(Kurtosis())
        .mapNaN.toList.sortBy(_.position)

    data2
      .kurtosis(Over(_2), Default())(true)
      .mapNaN.toList.sortBy(_.position) shouldBe data2
        .summarise(Over(_2))(Kurtosis(true))
        .mapNaN.toList.sortBy(_.position)

    data2
      .kurtosis(Along(_2), Default(12))(false)
      .mapNaN.toList.sortBy(_.position) shouldBe data2
        .summarise(Along(_2))(Kurtosis())
        .mapNaN.toList.sortBy(_.position)

    data3
      .kurtosis(Over(_1), Default())(true)
      .mapNaN.toList.sortBy(_.position) shouldBe data3
        .summarise(Over(_1))(Kurtosis(true))
        .mapNaN.toList.sortBy(_.position)

    data3
      .kurtosis(Along(_1), Default(12))(false)
      .mapNaN.toList.sortBy(_.position) shouldBe data3
        .summarise(Along(_1))(Kurtosis())
        .mapNaN.toList.sortBy(_.position)

    data3
      .kurtosis(Over(_2), Default())(true)
      .mapNaN.toList.sortBy(_.position) shouldBe data3
        .summarise(Over(_2))(Kurtosis(true))
        .mapNaN.toList.sortBy(_.position)

    data3
      .kurtosis(Along(_2), Default(12))(false)
      .mapNaN.toList.sortBy(_.position) shouldBe data3
        .summarise(Along(_2))(Kurtosis())
        .mapNaN.toList.sortBy(_.position)

    data3
      .kurtosis(Over(_3), Default())(true)
      .mapNaN.toList.sortBy(_.position) shouldBe data3
        .summarise(Over(_3))(Kurtosis(true))
        .mapNaN.toList.sortBy(_.position)

    data3
      .kurtosis(Along(_3), Default(12))(false)
      .mapNaN.toList.sortBy(_.position) shouldBe data3
        .summarise(Along(_3))(Kurtosis())
        .mapNaN.toList.sortBy(_.position)
  }

  "A minimum" should "return its correct statistic" in {
    data1
      .minimum(Over(_1), Default())
      .toList.sortBy(_.position) shouldBe data1.summarise(Over(_1))(Minimum()).toList.sortBy(_.position)

    data1
      .minimum(Along(_1), Default(12))
      .toList.sortBy(_.position) shouldBe data1.summarise(Along(_1))(Minimum()).toList.sortBy(_.position)

    data2
      .minimum(Over(_1), Default())
      .toList.sortBy(_.position) shouldBe data2.summarise(Over(_1))(Minimum()).toList.sortBy(_.position)

    data2
      .minimum(Along(_1), Default(12))
      .toList.sortBy(_.position) shouldBe data2.summarise(Along(_1))(Minimum()).toList.sortBy(_.position)

    data2
      .minimum(Over(_2), Default())
      .toList.sortBy(_.position) shouldBe data2.summarise(Over(_2))(Minimum()).toList.sortBy(_.position)

    data2
      .minimum(Along(_2), Default(12))
      .toList.sortBy(_.position) shouldBe data2.summarise(Along(_2))(Minimum()).toList.sortBy(_.position)

    data3
      .minimum(Over(_1), Default())
      .toList.sortBy(_.position) shouldBe data3.summarise(Over(_1))(Minimum()).toList.sortBy(_.position)

    data3
      .minimum(Along(_1), Default(12))
      .toList.sortBy(_.position) shouldBe data3.summarise(Along(_1))(Minimum()).toList.sortBy(_.position)

    data3
      .minimum(Over(_2), Default())
      .toList.sortBy(_.position) shouldBe data3.summarise(Over(_2))(Minimum()).toList.sortBy(_.position)

    data3
      .minimum(Along(_2), Default(12))
      .toList.sortBy(_.position) shouldBe data3.summarise(Along(_2))(Minimum()).toList.sortBy(_.position)

    data3
      .minimum(Over(_3), Default())
      .toList.sortBy(_.position) shouldBe data3.summarise(Over(_3))(Minimum()).toList.sortBy(_.position)

    data3
      .minimum(Along(_3), Default(12))
      .toList.sortBy(_.position) shouldBe data3.summarise(Along(_3))(Minimum()).toList.sortBy(_.position)
  }

  "A maximum" should "return its correct statistic" in {
    data1
      .maximum(Over(_1), Default())
      .toList.sortBy(_.position) shouldBe data1.summarise(Over(_1))(Maximum()).toList.sortBy(_.position)

    data1
      .maximum(Along(_1), Default(12))
      .toList.sortBy(_.position) shouldBe data1.summarise(Along(_1))(Maximum()).toList.sortBy(_.position)

    data2
      .maximum(Over(_1), Default())
      .toList.sortBy(_.position) shouldBe data2.summarise(Over(_1))(Maximum()).toList.sortBy(_.position)

    data2
      .maximum(Along(_1), Default(12))
      .toList.sortBy(_.position) shouldBe data2.summarise(Along(_1))(Maximum()).toList.sortBy(_.position)

    data2
      .maximum(Over(_2), Default())
      .toList.sortBy(_.position) shouldBe data2.summarise(Over(_2))(Maximum()).toList.sortBy(_.position)

    data2
      .maximum(Along(_2), Default(12))
      .toList.sortBy(_.position) shouldBe data2.summarise(Along(_2))(Maximum()).toList.sortBy(_.position)

    data3
      .maximum(Over(_1), Default())
      .toList.sortBy(_.position) shouldBe data3.summarise(Over(_1))(Maximum()).toList.sortBy(_.position)

    data3
      .maximum(Along(_1), Default(12))
      .toList.sortBy(_.position) shouldBe data3.summarise(Along(_1))(Maximum()).toList.sortBy(_.position)

    data3
      .maximum(Over(_2), Default())
      .toList.sortBy(_.position) shouldBe data3.summarise(Over(_2))(Maximum()).toList.sortBy(_.position)

    data3
      .maximum(Along(_2), Default(12))
      .toList.sortBy(_.position) shouldBe data3.summarise(Along(_2))(Maximum()).toList.sortBy(_.position)

    data3
      .maximum(Over(_3), Default())
      .toList.sortBy(_.position) shouldBe data3.summarise(Over(_3))(Maximum()).toList.sortBy(_.position)

    data3
      .maximum(Along(_3), Default(12))
      .toList.sortBy(_.position) shouldBe data3.summarise(Along(_3))(Maximum()).toList.sortBy(_.position)
  }

  "A maximumAbsolute" should "return its correct statistic" in {
    data1
      .maximumAbsolute(Over(_1), Default())
      .toList.sortBy(_.position) shouldBe data1.summarise(Over(_1))(MaximumAbsolute()).toList.sortBy(_.position)

    data1
      .maximumAbsolute(Along(_1), Default(12))
      .toList.sortBy(_.position) shouldBe data1.summarise(Along(_1))(MaximumAbsolute()).toList.sortBy(_.position)

    data2
      .maximumAbsolute(Over(_1), Default())
      .toList.sortBy(_.position) shouldBe data2.summarise(Over(_1))(MaximumAbsolute()).toList.sortBy(_.position)

    data2
      .maximumAbsolute(Along(_1), Default(12))
      .toList.sortBy(_.position) shouldBe data2.summarise(Along(_1))(MaximumAbsolute()).toList.sortBy(_.position)

    data2
      .maximumAbsolute(Over(_2), Default())
      .toList.sortBy(_.position) shouldBe data2.summarise(Over(_2))(MaximumAbsolute()).toList.sortBy(_.position)

    data2
      .maximumAbsolute(Along(_2), Default(12))
      .toList.sortBy(_.position) shouldBe data2.summarise(Along(_2))(MaximumAbsolute()).toList.sortBy(_.position)

    data3
      .maximumAbsolute(Over(_1), Default())
      .toList.sortBy(_.position) shouldBe data3.summarise(Over(_1))(MaximumAbsolute()).toList.sortBy(_.position)

    data3
      .maximumAbsolute(Along(_1), Default(12))
      .toList.sortBy(_.position) shouldBe data3.summarise(Along(_1))(MaximumAbsolute()).toList.sortBy(_.position)

    data3
      .maximumAbsolute(Over(_2), Default())
      .toList.sortBy(_.position) shouldBe data3.summarise(Over(_2))(MaximumAbsolute()).toList.sortBy(_.position)

    data3
      .maximumAbsolute(Along(_2), Default(12))
      .toList.sortBy(_.position) shouldBe data3.summarise(Along(_2))(MaximumAbsolute()).toList.sortBy(_.position)

    data3
      .maximumAbsolute(Over(_3), Default())
      .toList.sortBy(_.position) shouldBe data3.summarise(Over(_3))(MaximumAbsolute()).toList.sortBy(_.position)

    data3
      .maximumAbsolute(Along(_3), Default(12))
      .toList.sortBy(_.position) shouldBe data3.summarise(Along(_3))(MaximumAbsolute()).toList.sortBy(_.position)
  }

  "A sums" should "return its correct statistic" in {
    data1
      .sums(Over(_1), Default())
      .toList.sortBy(_.position) shouldBe data1.summarise(Over(_1))(Sums()).toList.sortBy(_.position)

    data1
      .sums(Along(_1), Default(12))
      .toList.sortBy(_.position) shouldBe data1.summarise(Along(_1))(Sums()).toList.sortBy(_.position)

    data2
      .sums(Over(_1), Default())
      .toList.sortBy(_.position) shouldBe data2.summarise(Over(_1))(Sums()).toList.sortBy(_.position)

    data2
      .sums(Along(_1), Default(12))
      .toList.sortBy(_.position) shouldBe data2.summarise(Along(_1))(Sums()).toList.sortBy(_.position)

    data2
      .sums(Over(_2), Default())
      .toList.sortBy(_.position) shouldBe data2.summarise(Over(_2))(Sums()).toList.sortBy(_.position)

    data2
      .sums(Along(_2), Default(12))
      .toList.sortBy(_.position) shouldBe data2.summarise(Along(_2))(Sums()).toList.sortBy(_.position)

    data3
      .sums(Over(_1), Default())
      .toList.sortBy(_.position) shouldBe data3.summarise(Over(_1))(Sums()).toList.sortBy(_.position)

    data3
      .sums(Along(_1), Default(12))
      .toList.sortBy(_.position) shouldBe data3.summarise(Along(_1))(Sums()).toList.sortBy(_.position)

    data3
      .sums(Over(_2), Default())
      .toList.sortBy(_.position) shouldBe data3.summarise(Over(_2))(Sums()).toList.sortBy(_.position)

    data3
      .sums(Along(_2), Default(12))
      .toList.sortBy(_.position) shouldBe data3.summarise(Along(_2))(Sums()).toList.sortBy(_.position)

    data3
      .sums(Over(_3), Default())
      .toList.sortBy(_.position) shouldBe data3.summarise(Over(_3))(Sums()).toList.sortBy(_.position)

    data3
      .sums(Along(_3), Default(12))
      .toList.sortBy(_.position) shouldBe data3.summarise(Along(_3))(Sums()).toList.sortBy(_.position)
  }
}

