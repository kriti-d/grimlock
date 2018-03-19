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
import commbank.grimlock.framework.environment.implicits._
import commbank.grimlock.framework.environment.tuner._
import commbank.grimlock.framework.metadata._
import commbank.grimlock.framework.position._

import commbank.grimlock.scalding.environment.implicits._

import commbank.grimlock.spark.environment.implicits._

import shapeless.HList
import shapeless.nat.{ _0, _1, _2 }

trait TestCorrelation extends TestGrimlock {

  // see http://www.mathsisfun.com/data/correlation.html for data

  val data1 = List(
    Cell(Position("day1", "temperature"), Content(ContinuousSchema[Double](), 14.2)),
    Cell(Position("day2", "temperature"), Content(ContinuousSchema[Double](), 16.4)),
    Cell(Position("day3", "temperature"), Content(ContinuousSchema[Double](), 11.9)),
    Cell(Position("day4", "temperature"), Content(ContinuousSchema[Double](), 15.2)),
    Cell(Position("day5", "temperature"), Content(ContinuousSchema[Double](), 18.5)),
    Cell(Position("day6", "temperature"), Content(ContinuousSchema[Double](), 22.1)),
    Cell(Position("day7", "temperature"), Content(ContinuousSchema[Double](), 19.4)),
    Cell(Position("day8", "temperature"), Content(ContinuousSchema[Double](), 25.1)),
    Cell(Position("day9", "temperature"), Content(ContinuousSchema[Double](), 23.4)),
    Cell(Position("day10", "temperature"), Content(ContinuousSchema[Double](), 18.1)),
    Cell(Position("day11", "temperature"), Content(ContinuousSchema[Double](), 22.6)),
    Cell(Position("day12", "temperature"), Content(ContinuousSchema[Double](), 17.2)),
    Cell(Position("day1", "sales"), Content(ContinuousSchema[Long](), 215L)),
    Cell(Position("day2", "sales"), Content(ContinuousSchema[Long](), 325L)),
    Cell(Position("day3", "sales"), Content(ContinuousSchema[Long](), 185L)),
    Cell(Position("day4", "sales"), Content(ContinuousSchema[Long](), 332L)),
    Cell(Position("day5", "sales"), Content(ContinuousSchema[Long](), 406L)),
    Cell(Position("day6", "sales"), Content(ContinuousSchema[Long](), 522L)),
    Cell(Position("day7", "sales"), Content(ContinuousSchema[Long](), 412L)),
    Cell(Position("day8", "sales"), Content(ContinuousSchema[Long](), 614L)),
    Cell(Position("day9", "sales"), Content(ContinuousSchema[Long](), 544L)),
    Cell(Position("day10", "sales"), Content(ContinuousSchema[Long](), 421L)),
    Cell(Position("day11", "sales"), Content(ContinuousSchema[Long](), 445L)),
    Cell(Position("day12", "sales"), Content(ContinuousSchema[Long](), 408L))
  )

  val data2 = data1 ++ List(
    Cell(Position("day1", "neg.sales"), Content(ContinuousSchema[Long](), -215L)),
    Cell(Position("day2", "neg.sales"), Content(ContinuousSchema[Long](), -325L)),
    Cell(Position("day3", "neg.sales"), Content(ContinuousSchema[Long](), -185L)),
    Cell(Position("day4", "neg.sales"), Content(ContinuousSchema[Long](), -332L)),
    Cell(Position("day5", "neg.sales"), Content(ContinuousSchema[Long](), -406L)),
    Cell(Position("day6", "neg.sales"), Content(ContinuousSchema[Long](), -522L)),
    Cell(Position("day7", "neg.sales"), Content(ContinuousSchema[Long](), -412L)),
    Cell(Position("day8", "neg.sales"), Content(ContinuousSchema[Long](), -614L)),
    Cell(Position("day9", "neg.sales"), Content(ContinuousSchema[Long](), -544L)),
    Cell(Position("day10", "neg.sales"), Content(ContinuousSchema[Long](), -421L)),
    Cell(Position("day11", "neg.sales"), Content(ContinuousSchema[Long](), -445L)),
    Cell(Position("day12", "neg.sales"), Content(ContinuousSchema[Long](), -408L))
  )

  val data3 = data2
    .map { case c => c.relocate(_ => Position(c.position(_1), c.position(_1), c.position(_0))) }

  val data4 = data2
    .map { case c => c.relocate(_ => c.position.append(c.position(_0).hashCode)) }

  val data5 = data2 :+ Cell(Position("day1", "temperature"), Content(NominalSchema[String](), "foo"))

  val data6 = data2 :+ Cell(Position("day1", "temperature"), Content(NominalSchema[Double](), Double.NaN))

  val result1 = List(
    Cell(Position("(temperature*sales)"), Content(ContinuousSchema[Double](), 0.957506623001595))
  )

  val result2 = List(
    Cell(Position("(sales*neg.sales)"), Content(ContinuousSchema[Double](), -0.9999999999999998)),
    Cell(Position("(temperature*neg.sales)"), Content(ContinuousSchema[Double](), -0.957506623001595)),
    Cell(Position("(temperature*sales)"), Content(ContinuousSchema[Double](), 0.957506623001595))
  )

  val result3 = List(
    Cell(Position("(sales|sales*neg.sales|neg.sales)"), Content(ContinuousSchema[Double](), -0.9999999999999998)),
    Cell(
      Position("(temperature|temperature*neg.sales|neg.sales)"),
      Content(ContinuousSchema[Double](), -0.957506623001595)
    ),
    Cell(Position("(temperature|temperature*sales|sales)"), Content(ContinuousSchema[Double](), 0.957506623001595))
  )

  val result4 = List(
    Cell(Position("(sales*neg.sales)"), Content(ContinuousSchema[Double](), -0.9999999999999997)),
    Cell(Position("(temperature*neg.sales)"), Content(ContinuousSchema[Double](), -0.9575066230015952)),
    Cell(Position("(temperature*sales)"), Content(ContinuousSchema[Double](), 0.9575066230015952))
  )

  val result5 = List(
    Cell(Position("(sales*neg.sales)"), Content(ContinuousSchema[Double](), -0.9999999999999998)),
    Cell(Position("(temperature*neg.sales)"), Content(ContinuousSchema[Double](), Double.NaN)),
    Cell(Position("(temperature*sales)"), Content(ContinuousSchema[Double](), Double.NaN))
  )

  val result6 = List(
    Cell(Position("(sales*neg.sales)"), Content(ContinuousSchema[Double](), -0.9999999999999998)),
    Cell(Position("(temperature*neg.sales)"), Content(ContinuousSchema[Double](), -0.957506623001595)),
    Cell(Position("(temperature*sales)"), Content(ContinuousSchema[Double](), 0.957506623001595))
  )

  val result7 = List(
    Cell(Position("(sales*neg.sales)"), Content(ContinuousSchema[Double](), -0.9999999999999998)),
    Cell(Position("(temperature*neg.sales)"), Content(ContinuousSchema[Double](), Double.NaN)),
    Cell(Position("(temperature*sales)"), Content(ContinuousSchema[Double](), Double.NaN))
  )
}

object TestCorrelation {

  def name[P <: HList] = (l: Position[P], r: Position[P]) =>
    Position(s"(${l.toShortString("|")}*${r.toShortString("|")})").toOption
}

class TestScaldingCorrelation extends TestCorrelation {

  "A correlation" should "return its second over in 2D" in {
    val res = toPipe(data1)
      .correlation(Over(_1), InMemory())(TestCorrelation.name, true)
      .toList.sortBy(_.position)

    res.size shouldBe result1.size
    for (i <- 0 until res.size) {
      res(i).position shouldBe result1(i).position
      res(i).content.value.as[Double].get shouldBe result1(i).content.value.as[Double].get +- 1e-8
    }
  }

  it should "return its multiple second over in 2D" in {
    val res = toPipe(data2)
      .correlation(Over(_1), Default())(TestCorrelation.name, true)
      .toList.sortBy(_.position)

    res.size shouldBe result2.size
    for (i <- 0 until res.size) {
      res(i).position shouldBe result2(i).position
      res(i).content.value.as[Double].get shouldBe result2(i).content.value.as[Double].get +- 1e-8
    }
  }

  it should "return its first along in 3D" in {
    val res = toPipe(data3)
      .correlation(Along(_2), Ternary(Default(12), InMemory(), Default()))(TestCorrelation.name, true)
      .toList.sortBy(_.position)

    res.size shouldBe result3.size
    for (i <- 0 until res.size) {
      res(i).position shouldBe result3(i).position
      res(i).content.value.as[Double].get shouldBe result3(i).content.value.as[Double].get +- 1e-8
    }
  }

  it should "return its second over in 3D" in {
    val res = toPipe(data4)
      .correlation(Over(_1), Ternary(Default(12), InMemory(12), Default(12)))(TestCorrelation.name, true)
      .toList.sortBy(_.position)

    res.size shouldBe result4.size
    for (i <- 0 until res.size) {
      res(i).position shouldBe result4(i).position
      res(i).content.value.as[Double].get shouldBe result4(i).content.value.as[Double].get +- 1e-8
    }
  }

  it should "filter correctly" in {
    val res = toPipe(data5)
      .correlation(Over(_1), Ternary(Default(12), Default(), Unbalanced(12)))(TestCorrelation.name, true, true)
      .toList.sortBy(_.position)

    res.size shouldBe result2.size
    for (i <- 0 until res.size) {
      res(i).position shouldBe result2(i).position
      res(i).content.value.as[Double].get shouldBe result2(i).content.value.as[Double].get +- 1e-8
    }
  }

  it should "!filter,strict correctly" in {
    val res = toPipe(data5)
      .correlation(Over(_1), Ternary(Default(12), Default(12), Default(12)))(TestCorrelation.name, false, true)
      .toList.sortBy(_.position)

    res.size shouldBe result5.size
    for (i <- 0 until res.size) {
      res(i).position shouldBe result5(i).position

      val ref = result5(i).content.value.as[Double].get

      if (ref.isNaN)
        res(i).content.value.as[Double].get.compare(ref) shouldBe 0
      else
        res(i).content.value.as[Double].get - ref shouldBe 0.0 +- 1e-8
    }
  }

  it should "!filter,!strict correctly" in {
    val res = toPipe(data5)
      .correlation(Over(_1), Ternary(Default(12), Unbalanced(12), Default(12)))(TestCorrelation.name, false, false)
      .toList.sortBy(_.position)

    res.size shouldBe result6.size
    for (i <- 0 until res.size) {
      res(i).position shouldBe result6(i).position
      res(i).content.value.as[Double].get shouldBe result6(i).content.value.as[Double].get +- 1e-8
    }
  }

  it should "!filter,strict correctly take 2" in {
    val res = toPipe(data6)
      .correlation(Over(_1), Default())(TestCorrelation.name, false, false)
      .toList.sortBy(_.position)

    res.size shouldBe result7.size
    for (i <- 0 until res.size) {
      res(i).position shouldBe result7(i).position

      val ref = result7(i).content.value.as[Double].get

      if (ref.isNaN)
        res(i).content.value.as[Double].get.compare(ref) shouldBe 0
      else
        res(i).content.value.as[Double].get - ref shouldBe 0.0 +- 1e-8
    }
  }
}

class TestSparkCorrelation extends TestCorrelation {

  "A correlation" should "return its second over in 2D" in {
    val res = toRDD(data1)
      .correlation(Over(_1), InMemory())(TestCorrelation.name, true)
      .toList.sortBy(_.position)

    res.size shouldBe result1.size
    for (i <- 0 until res.size) {
      res(i).position shouldBe result1(i).position
      res(i).content.value.as[Double].get shouldBe result1(i).content.value.as[Double].get +- 1e-8
    }
  }

  it should "return its multiple second over in 2D" in {
    val res = toRDD(data2)
      .correlation(Over(_1), Default())(TestCorrelation.name, true)
      .toList.sortBy(_.position)

    res.size shouldBe result2.size
    for (i <- 0 until res.size) {
      res(i).position shouldBe result2(i).position
      res(i).content.value.as[Double].get shouldBe result2(i).content.value.as[Double].get +- 1e-8
    }
  }

  it should "return its first along in 3D" in {
    val res = toRDD(data3)
      .correlation(Along(_2), Ternary(Default(12), InMemory(), Default()))(TestCorrelation.name, true)
      .toList.sortBy(_.position)

    res.size shouldBe result3.size
    for (i <- 0 until res.size) {
      res(i).position shouldBe result3(i).position
      res(i).content.value.as[Double].get shouldBe result3(i).content.value.as[Double].get +- 1e-8
    }
  }

  it should "return its second over in 3D" in {
    val res = toRDD(data4)
      .correlation(Over(_1), Ternary(Default(12), InMemory(), Default(12)))(TestCorrelation.name, true)
      .toList.sortBy(_.position)

    res.size shouldBe result4.size
    for (i <- 0 until res.size) {
      res(i).position shouldBe result4(i).position
      res(i).content.value.as[Double].get shouldBe result4(i).content.value.as[Double].get +- 1e-8
    }
  }

  it should "filter correctly" in {
    val res = toRDD(data5)
      .correlation(Over(_1), Ternary(Default(12), InMemory(12), Default(12)))(TestCorrelation.name, true, true)
      .toList.sortBy(_.position)

    res.size shouldBe result2.size
    for (i <- 0 until res.size) {
      res(i).position shouldBe result2(i).position
      res(i).content.value.as[Double].get shouldBe result2(i).content.value.as[Double].get +- 1e-8
    }
  }

  it should "!filter,strict correctly" in {
    val res = toRDD(data5)
      .correlation(Over(_1), Ternary(Default(12), Default(), Default()))(TestCorrelation.name, false, true)
      .toList.sortBy(_.position)

    res.size shouldBe result5.size
    for (i <- 0 until res.size) {
      res(i).position shouldBe result5(i).position

      val ref = result5(i).content.value.as[Double].get

      if (ref.isNaN)
        res(i).content.value.as[Double].get.compare(ref) shouldBe 0
      else
        res(i).content.value.as[Double].get - ref shouldBe 0.0 +- 1e-8
    }
  }

  it should "!filter,!strict correctly" in {
    val res = toRDD(data5)
      .correlation(Over(_1), Ternary(Default(12), Default(), Default(12)))(TestCorrelation.name, false, false)
      .toList.sortBy(_.position)

    res.size shouldBe result6.size
    for (i <- 0 until res.size) {
      res(i).position shouldBe result6(i).position
      res(i).content.value.as[Double].get shouldBe result6(i).content.value.as[Double].get +- 1e-8
    }
  }

  it should "!filter,strict correctly take 2" in {
    val res = toRDD(data6)
      .correlation(Over(_1), Ternary(Default(12), Default(12), Default(12)))(TestCorrelation.name, false, false)
      .toList.sortBy(_.position)

    res.size shouldBe result7.size
    for (i <- 0 until res.size) {
      res(i).position shouldBe result7(i).position

      val ref = result7(i).content.value.as[Double].get

      if (ref.isNaN)
        res(i).content.value.as[Double].get.compare(ref) shouldBe 0
      else
        res(i).content.value.as[Double].get - ref shouldBe 0.0 +- 1e-8
    }
  }
}

trait TestMutualInformation extends TestGrimlock {

  // see http://www.eecs.harvard.edu/cs286r/courses/fall10/papers/Chapter2.pdf example 2.2.1 for data

  val data1 = List(
    Cell(Position("iid:0221701", "fid:X"), Content(NominalSchema[String](), "1")),
    Cell(Position("iid:0221702", "fid:X"), Content(NominalSchema[String](), "1")),
    Cell(Position("iid:0221703", "fid:X"), Content(NominalSchema[String](), "1")),
    Cell(Position("iid:0221704", "fid:X"), Content(NominalSchema[String](), "1")),
    Cell(Position("iid:0221705", "fid:X"), Content(NominalSchema[String](), "2")),
    Cell(Position("iid:0221706", "fid:X"), Content(NominalSchema[String](), "2")),
    Cell(Position("iid:0221707", "fid:X"), Content(NominalSchema[String](), "3")),
    Cell(Position("iid:0221708", "fid:X"), Content(NominalSchema[String](), "4")),
    Cell(Position("iid:0221709", "fid:X"), Content(NominalSchema[String](), "1")),
    Cell(Position("iid:0221710", "fid:X"), Content(NominalSchema[String](), "1")),
    Cell(Position("iid:0221711", "fid:X"), Content(NominalSchema[String](), "2")),
    Cell(Position("iid:0221712", "fid:X"), Content(NominalSchema[String](), "2")),
    Cell(Position("iid:0221713", "fid:X"), Content(NominalSchema[String](), "2")),
    Cell(Position("iid:0221714", "fid:X"), Content(NominalSchema[String](), "2")),
    Cell(Position("iid:0221715", "fid:X"), Content(NominalSchema[String](), "3")),
    Cell(Position("iid:0221716", "fid:X"), Content(NominalSchema[String](), "4")),
    Cell(Position("iid:0221717", "fid:X"), Content(NominalSchema[String](), "1")),
    Cell(Position("iid:0221718", "fid:X"), Content(NominalSchema[String](), "1")),
    Cell(Position("iid:0221719", "fid:X"), Content(NominalSchema[String](), "2")),
    Cell(Position("iid:0221720", "fid:X"), Content(NominalSchema[String](), "2")),
    Cell(Position("iid:0221721", "fid:X"), Content(NominalSchema[String](), "3")),
    Cell(Position("iid:0221722", "fid:X"), Content(NominalSchema[String](), "3")),
    Cell(Position("iid:0221723", "fid:X"), Content(NominalSchema[String](), "4")),
    Cell(Position("iid:0221724", "fid:X"), Content(NominalSchema[String](), "4")),
    Cell(Position("iid:0221725", "fid:X"), Content(NominalSchema[String](), "1")),
    Cell(Position("iid:0221726", "fid:X"), Content(NominalSchema[String](), "1")),
    Cell(Position("iid:0221727", "fid:X"), Content(NominalSchema[String](), "1")),
    Cell(Position("iid:0221728", "fid:X"), Content(NominalSchema[String](), "1")),
    Cell(Position("iid:0221729", "fid:X"), Content(NominalSchema[String](), "1")),
    Cell(Position("iid:0221730", "fid:X"), Content(NominalSchema[String](), "1")),
    Cell(Position("iid:0221731", "fid:X"), Content(NominalSchema[String](), "1")),
    Cell(Position("iid:0221732", "fid:X"), Content(NominalSchema[String](), "1")),
    Cell(Position("iid:0221701", "fid:Y"), Content(NominalSchema[String](), "a")),
    Cell(Position("iid:0221702", "fid:Y"), Content(NominalSchema[String](), "a")),
    Cell(Position("iid:0221703", "fid:Y"), Content(NominalSchema[String](), "a")),
    Cell(Position("iid:0221704", "fid:Y"), Content(NominalSchema[String](), "a")),
    Cell(Position("iid:0221705", "fid:Y"), Content(NominalSchema[String](), "a")),
    Cell(Position("iid:0221706", "fid:Y"), Content(NominalSchema[String](), "a")),
    Cell(Position("iid:0221707", "fid:Y"), Content(NominalSchema[String](), "a")),
    Cell(Position("iid:0221708", "fid:Y"), Content(NominalSchema[String](), "a")),
    Cell(Position("iid:0221709", "fid:Y"), Content(NominalSchema[String](), "b")),
    Cell(Position("iid:0221710", "fid:Y"), Content(NominalSchema[String](), "b")),
    Cell(Position("iid:0221711", "fid:Y"), Content(NominalSchema[String](), "b")),
    Cell(Position("iid:0221712", "fid:Y"), Content(NominalSchema[String](), "b")),
    Cell(Position("iid:0221713", "fid:Y"), Content(NominalSchema[String](), "b")),
    Cell(Position("iid:0221714", "fid:Y"), Content(NominalSchema[String](), "b")),
    Cell(Position("iid:0221715", "fid:Y"), Content(NominalSchema[String](), "b")),
    Cell(Position("iid:0221716", "fid:Y"), Content(NominalSchema[String](), "b")),
    Cell(Position("iid:0221717", "fid:Y"), Content(NominalSchema[String](), "c")),
    Cell(Position("iid:0221718", "fid:Y"), Content(NominalSchema[String](), "c")),
    Cell(Position("iid:0221719", "fid:Y"), Content(NominalSchema[String](), "c")),
    Cell(Position("iid:0221720", "fid:Y"), Content(NominalSchema[String](), "c")),
    Cell(Position("iid:0221721", "fid:Y"), Content(NominalSchema[String](), "c")),
    Cell(Position("iid:0221722", "fid:Y"), Content(NominalSchema[String](), "c")),
    Cell(Position("iid:0221723", "fid:Y"), Content(NominalSchema[String](), "c")),
    Cell(Position("iid:0221724", "fid:Y"), Content(NominalSchema[String](), "c")),
    Cell(Position("iid:0221725", "fid:Y"), Content(NominalSchema[String](), "d")),
    Cell(Position("iid:0221726", "fid:Y"), Content(NominalSchema[String](), "d")),
    Cell(Position("iid:0221727", "fid:Y"), Content(NominalSchema[String](), "d")),
    Cell(Position("iid:0221728", "fid:Y"), Content(NominalSchema[String](), "d")),
    Cell(Position("iid:0221729", "fid:Y"), Content(NominalSchema[String](), "d")),
    Cell(Position("iid:0221730", "fid:Y"), Content(NominalSchema[String](), "d")),
    Cell(Position("iid:0221731", "fid:Y"), Content(NominalSchema[String](), "d")),
    Cell(Position("iid:0221732", "fid:Y"), Content(NominalSchema[String](), "d")),
    Cell(Position("iid:0221701", "fid:Z"), Content(NominalSchema[String](), "1")),
    Cell(Position("iid:0221702", "fid:Z"), Content(NominalSchema[String](), "1")),
    Cell(Position("iid:0221703", "fid:Z"), Content(NominalSchema[String](), "1")),
    Cell(Position("iid:0221704", "fid:Z"), Content(NominalSchema[String](), "1")),
    Cell(Position("iid:0221705", "fid:Z"), Content(NominalSchema[String](), "2")),
    Cell(Position("iid:0221706", "fid:Z"), Content(NominalSchema[String](), "2")),
    Cell(Position("iid:0221707", "fid:Z"), Content(NominalSchema[String](), "3")),
    Cell(Position("iid:0221708", "fid:Z"), Content(NominalSchema[String](), "4")),
    Cell(Position("iid:0221709", "fid:Z"), Content(NominalSchema[String](), "1")),
    Cell(Position("iid:0221710", "fid:Z"), Content(NominalSchema[String](), "1")),
    Cell(Position("iid:0221711", "fid:Z"), Content(NominalSchema[String](), "2")),
    Cell(Position("iid:0221712", "fid:Z"), Content(NominalSchema[String](), "2")),
    Cell(Position("iid:0221713", "fid:Z"), Content(NominalSchema[String](), "2")),
    Cell(Position("iid:0221714", "fid:Z"), Content(NominalSchema[String](), "2")),
    Cell(Position("iid:0221715", "fid:Z"), Content(NominalSchema[String](), "3")),
    Cell(Position("iid:0221716", "fid:Z"), Content(NominalSchema[String](), "4")),
    Cell(Position("iid:0221717", "fid:Z"), Content(NominalSchema[String](), "1")),
    Cell(Position("iid:0221718", "fid:Z"), Content(NominalSchema[String](), "1")),
    Cell(Position("iid:0221719", "fid:Z"), Content(NominalSchema[String](), "2")),
    Cell(Position("iid:0221720", "fid:Z"), Content(NominalSchema[String](), "2")),
    Cell(Position("iid:0221721", "fid:Z"), Content(NominalSchema[String](), "3")),
    Cell(Position("iid:0221722", "fid:Z"), Content(NominalSchema[String](), "3")),
    Cell(Position("iid:0221723", "fid:Z"), Content(NominalSchema[String](), "4")),
    Cell(Position("iid:0221724", "fid:Z"), Content(NominalSchema[String](), "4")),
    Cell(Position("iid:0221725", "fid:Z"), Content(NominalSchema[String](), "1")),
    Cell(Position("iid:0221726", "fid:Z"), Content(NominalSchema[String](), "1")),
    Cell(Position("iid:0221727", "fid:Z"), Content(NominalSchema[String](), "1")),
    Cell(Position("iid:0221728", "fid:Z"), Content(NominalSchema[String](), "1")),
    Cell(Position("iid:0221729", "fid:Z"), Content(NominalSchema[String](), "1")),
    Cell(Position("iid:0221730", "fid:Z"), Content(NominalSchema[String](), "1")),
    Cell(Position("iid:0221731", "fid:Z"), Content(NominalSchema[String](), "1")),
    Cell(Position("iid:0221732", "fid:Z"), Content(NominalSchema[String](), "1"))
  )

  val data2 = data1
    .map { case c => c.relocate(_ => Position(c.position(_1), c.position(_1), c.position(_0))) }

  val data3 = data1
    .map { case c => c.relocate(_ => c.position.append(c.position(_0).hashCode)) }

  val data4 = List(
    Cell(Position("iid:0221701", "fid:X"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("iid:0221702", "fid:X"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("iid:0221703", "fid:X"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("iid:0221704", "fid:X"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("iid:0221705", "fid:X"), Content(DiscreteSchema[Long](), 2L)),
    Cell(Position("iid:0221706", "fid:X"), Content(DiscreteSchema[Long](), 2L)),
    Cell(Position("iid:0221707", "fid:X"), Content(DiscreteSchema[Long](), 3L)),
    Cell(Position("iid:0221708", "fid:X"), Content(DiscreteSchema[Long](), 4L)),
    Cell(Position("iid:0221709", "fid:X"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("iid:0221710", "fid:X"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("iid:0221711", "fid:X"), Content(DiscreteSchema[Long](), 2L)),
    Cell(Position("iid:0221712", "fid:X"), Content(DiscreteSchema[Long](), 2L)),
    Cell(Position("iid:0221713", "fid:X"), Content(DiscreteSchema[Long](), 2L)),
    Cell(Position("iid:0221714", "fid:X"), Content(DiscreteSchema[Long](), 2L)),
    Cell(Position("iid:0221715", "fid:X"), Content(DiscreteSchema[Long](), 3L)),
    Cell(Position("iid:0221716", "fid:X"), Content(DiscreteSchema[Long](), 4L)),
    Cell(Position("iid:0221717", "fid:X"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("iid:0221718", "fid:X"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("iid:0221719", "fid:X"), Content(DiscreteSchema[Long](), 2L)),
    Cell(Position("iid:0221720", "fid:X"), Content(DiscreteSchema[Long](), 2L)),
    Cell(Position("iid:0221721", "fid:X"), Content(DiscreteSchema[Long](), 3L)),
    Cell(Position("iid:0221722", "fid:X"), Content(DiscreteSchema[Long](), 3L)),
    Cell(Position("iid:0221723", "fid:X"), Content(DiscreteSchema[Long](), 4L)),
    Cell(Position("iid:0221724", "fid:X"), Content(DiscreteSchema[Long](), 4L)),
    Cell(Position("iid:0221725", "fid:X"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("iid:0221726", "fid:X"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("iid:0221727", "fid:X"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("iid:0221728", "fid:X"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("iid:0221729", "fid:X"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("iid:0221730", "fid:X"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("iid:0221731", "fid:X"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("iid:0221732", "fid:X"), Content(DiscreteSchema[Long](), 1L)),
    Cell(Position("iid:0221701", "fid:Y"), Content(NominalSchema[String](), "a")),
    Cell(Position("iid:0221702", "fid:Y"), Content(NominalSchema[String](), "a")),
    Cell(Position("iid:0221703", "fid:Y"), Content(NominalSchema[String](), "a")),
    Cell(Position("iid:0221704", "fid:Y"), Content(NominalSchema[String](), "a")),
    Cell(Position("iid:0221705", "fid:Y"), Content(NominalSchema[String](), "a")),
    Cell(Position("iid:0221706", "fid:Y"), Content(NominalSchema[String](), "a")),
    Cell(Position("iid:0221707", "fid:Y"), Content(NominalSchema[String](), "a")),
    Cell(Position("iid:0221708", "fid:Y"), Content(NominalSchema[String](), "a")),
    Cell(Position("iid:0221709", "fid:Y"), Content(NominalSchema[String](), "b")),
    Cell(Position("iid:0221710", "fid:Y"), Content(NominalSchema[String](), "b")),
    Cell(Position("iid:0221711", "fid:Y"), Content(NominalSchema[String](), "b")),
    Cell(Position("iid:0221712", "fid:Y"), Content(NominalSchema[String](), "b")),
    Cell(Position("iid:0221713", "fid:Y"), Content(NominalSchema[String](), "b")),
    Cell(Position("iid:0221714", "fid:Y"), Content(NominalSchema[String](), "b")),
    Cell(Position("iid:0221715", "fid:Y"), Content(NominalSchema[String](), "b")),
    Cell(Position("iid:0221716", "fid:Y"), Content(NominalSchema[String](), "b")),
    Cell(Position("iid:0221717", "fid:Y"), Content(NominalSchema[String](), "c")),
    Cell(Position("iid:0221718", "fid:Y"), Content(NominalSchema[String](), "c")),
    Cell(Position("iid:0221719", "fid:Y"), Content(NominalSchema[String](), "c")),
    Cell(Position("iid:0221720", "fid:Y"), Content(NominalSchema[String](), "c")),
    Cell(Position("iid:0221721", "fid:Y"), Content(NominalSchema[String](), "c")),
    Cell(Position("iid:0221722", "fid:Y"), Content(NominalSchema[String](), "c")),
    Cell(Position("iid:0221723", "fid:Y"), Content(NominalSchema[String](), "c")),
    Cell(Position("iid:0221724", "fid:Y"), Content(NominalSchema[String](), "c")),
    Cell(Position("iid:0221725", "fid:Y"), Content(NominalSchema[String](), "d")),
    Cell(Position("iid:0221726", "fid:Y"), Content(NominalSchema[String](), "d")),
    Cell(Position("iid:0221727", "fid:Y"), Content(NominalSchema[String](), "d")),
    Cell(Position("iid:0221728", "fid:Y"), Content(NominalSchema[String](), "d")),
    Cell(Position("iid:0221729", "fid:Y"), Content(NominalSchema[String](), "d")),
    Cell(Position("iid:0221730", "fid:Y"), Content(NominalSchema[String](), "d")),
    Cell(Position("iid:0221731", "fid:Y"), Content(NominalSchema[String](), "d")),
    Cell(Position("iid:0221732", "fid:Y"), Content(NominalSchema[String](), "d")),
    Cell(Position("iid:0221701", "fid:Z"), Content(NominalSchema[String](), "1")),
    Cell(Position("iid:0221702", "fid:Z"), Content(NominalSchema[String](), "1")),
    Cell(Position("iid:0221703", "fid:Z"), Content(NominalSchema[String](), "1")),
    Cell(Position("iid:0221704", "fid:Z"), Content(NominalSchema[String](), "1")),
    Cell(Position("iid:0221705", "fid:Z"), Content(NominalSchema[String](), "2")),
    Cell(Position("iid:0221706", "fid:Z"), Content(NominalSchema[String](), "2")),
    Cell(Position("iid:0221707", "fid:Z"), Content(NominalSchema[String](), "3")),
    Cell(Position("iid:0221708", "fid:Z"), Content(NominalSchema[String](), "4")),
    Cell(Position("iid:0221709", "fid:Z"), Content(NominalSchema[String](), "1")),
    Cell(Position("iid:0221710", "fid:Z"), Content(NominalSchema[String](), "1")),
    Cell(Position("iid:0221711", "fid:Z"), Content(NominalSchema[String](), "2")),
    Cell(Position("iid:0221712", "fid:Z"), Content(NominalSchema[String](), "2")),
    Cell(Position("iid:0221713", "fid:Z"), Content(NominalSchema[String](), "2")),
    Cell(Position("iid:0221714", "fid:Z"), Content(NominalSchema[String](), "2")),
    Cell(Position("iid:0221715", "fid:Z"), Content(NominalSchema[String](), "3")),
    Cell(Position("iid:0221716", "fid:Z"), Content(NominalSchema[String](), "4")),
    Cell(Position("iid:0221717", "fid:Z"), Content(NominalSchema[String](), "1")),
    Cell(Position("iid:0221718", "fid:Z"), Content(NominalSchema[String](), "1")),
    Cell(Position("iid:0221719", "fid:Z"), Content(NominalSchema[String](), "2")),
    Cell(Position("iid:0221720", "fid:Z"), Content(NominalSchema[String](), "2")),
    Cell(Position("iid:0221721", "fid:Z"), Content(NominalSchema[String](), "3")),
    Cell(Position("iid:0221722", "fid:Z"), Content(NominalSchema[String](), "3")),
    Cell(Position("iid:0221723", "fid:Z"), Content(NominalSchema[String](), "4")),
    Cell(Position("iid:0221724", "fid:Z"), Content(NominalSchema[String](), "4")),
    Cell(Position("iid:0221725", "fid:Z"), Content(NominalSchema[String](), "1")),
    Cell(Position("iid:0221726", "fid:Z"), Content(NominalSchema[String](), "1")),
    Cell(Position("iid:0221727", "fid:Z"), Content(NominalSchema[String](), "1")),
    Cell(Position("iid:0221728", "fid:Z"), Content(NominalSchema[String](), "1")),
    Cell(Position("iid:0221729", "fid:Z"), Content(NominalSchema[String](), "1")),
    Cell(Position("iid:0221730", "fid:Z"), Content(NominalSchema[String](), "1")),
    Cell(Position("iid:0221731", "fid:Z"), Content(NominalSchema[String](), "1")),
    Cell(Position("iid:0221732", "fid:Z"), Content(NominalSchema[String](), "1"))
  )

  val result1 = List(
    Cell(Position("fid:X,fid:Y"), Content(ContinuousSchema[Double](), 0.375)),
    Cell(Position("fid:X,fid:Z"), Content(ContinuousSchema[Double](), 1.75)),
    Cell(Position("fid:Y,fid:Z"), Content(ContinuousSchema[Double](), 0.375))
  )

  val result2 = List(
    Cell(Position("fid:X|fid:X,fid:Y|fid:Y"), Content(ContinuousSchema[Double](), 0.375)),
    Cell(Position("fid:X|fid:X,fid:Z|fid:Z"), Content(ContinuousSchema[Double](), 1.75)),
    Cell(Position("fid:Y|fid:Y,fid:Z|fid:Z"), Content(ContinuousSchema[Double](), 0.375))
  )

  val result3 = List(
    Cell(Position("fid:X,fid:Y"), Content(ContinuousSchema[Double](), 0.375)),
    Cell(Position("fid:X,fid:Z"), Content(ContinuousSchema[Double](), 1.75)),
    Cell(Position("fid:Y,fid:Z"), Content(ContinuousSchema[Double](), 0.375))
  )

  val result4 = List(
    Cell(Position("fid:Y,fid:Z"), Content(ContinuousSchema[Double](), 0.375))
  )
}

object TestMutualInformation {

  def name[P <: HList] = (l: Position[P], r: Position[P]) =>
    Position(s"${r.toShortString("|")},${l.toShortString("|")}").toOption
}

class TestScaldingMutualInformation extends TestMutualInformation {

  "A mutualInformation" should "return its second over in 2D" in {
    val res = toPipe(data1)
      .mutualInformation(Over(_1), InMemory())(TestMutualInformation.name, true)
      .toList.sortBy(_.position)

    res.size shouldBe result1.size
    for (i <- 0 until res.size) {
      res(i).position shouldBe result1(i).position
      res(i).content.value.as[Double].get shouldBe result1(i).content.value.as[Double].get +- 1e-8
    }
  }

  it should "return its first along in 2D" in {
    val res = toPipe(data1)
      .mutualInformation(Along(_0), Default())(TestMutualInformation.name, true)
      .toList.sortBy(_.position)

    res.size shouldBe result1.size
    for (i <- 0 until res.size) {
      res(i).position shouldBe result1(i).position
      res(i).content.value.as[Double].get shouldBe result1(i).content.value.as[Double].get +- 1e-8
    }
  }

  it should "return its first along in 3D" in {
    val res = toPipe(data2)
      .mutualInformation(Along(_2), Ternary(Default(12), InMemory(), Default()))(TestMutualInformation.name, true)
      .toList.sortBy(_.position)

    res.size shouldBe result2.size
    for (i <- 0 until res.size) {
      res(i).position shouldBe result2(i).position
      res(i).content.value.as[Double].get shouldBe result2(i).content.value.as[Double].get +- 1e-8
    }
  }

  it should "return its second over in 3D" in {
    val res = toPipe(data3)
      .mutualInformation(Over(_1), Ternary(Default(12), InMemory(12), Default(12)))(TestMutualInformation.name, true)
      .toList.sortBy(_.position)

    res.size shouldBe result3.size
    for (i <- 0 until res.size) {
      res(i).position shouldBe result3(i).position
      res(i).content.value.as[Double].get shouldBe result3(i).content.value.as[Double].get +- 1e-8
    }
  }

  it should "filter correctly" in {
    val res = toPipe(data4)
      .mutualInformation(Over(_1), Ternary(Default(12), Default(), Unbalanced(12)))(TestMutualInformation.name, true)
      .toList.sortBy(_.position)

    res.size shouldBe result4.size
    for (i <- 0 until res.size) {
      res(i).position shouldBe result4(i).position
      res(i).content.value.as[Double].get shouldBe result4(i).content.value.as[Double].get +- 1e-8
    }
  }

  it should "not filter correctly" in {
    val res = toPipe(data4)
      .mutualInformation(
        Over(_1),
        Ternary(Default(12), Unbalanced(12), Unbalanced(12))
      )(
        TestMutualInformation.name,
        false
      )
      .toList.sortBy(_.position)

    res.size shouldBe result1.size
    for (i <- 0 until res.size) {
      res(i).position shouldBe result1(i).position
      res(i).content.value.as[Double].get shouldBe result1(i).content.value.as[Double].get +- 1e-8
    }
  }
}

class TestSparkMutualInformation extends TestMutualInformation {

  "A mutualInformation" should "return its second over in 2D" in {
    val res = toRDD(data1)
      .mutualInformation(Over(_1), InMemory())(TestMutualInformation.name, true)
      .toList.sortBy(_.position)

    res.size shouldBe result1.size
    for (i <- 0 until res.size) {
      res(i).position shouldBe result1(i).position
      res(i).content.value.as[Double].get shouldBe result1(i).content.value.as[Double].get +- 1e-8
    }
  }

  it should "return its first along in 2D" in {
    val res = toRDD(data1)
      .mutualInformation(Along(_0), Default())(TestMutualInformation.name, true)
      .toList.sortBy(_.position)

    res.size shouldBe result1.size
    for (i <- 0 until res.size) {
      res(i).position shouldBe result1(i).position
      res(i).content.value.as[Double].get shouldBe result1(i).content.value.as[Double].get +- 1e-8
    }
  }

  it should "return its first along in 3D" in {
    val res = toRDD(data2)
      .mutualInformation(Along(_2), Ternary(Default(12), InMemory(), Default()))(TestMutualInformation.name, true)
      .toList.sortBy(_.position)

    res.size shouldBe result2.size
    for (i <- 0 until res.size) {
      res(i).position shouldBe result2(i).position
      res(i).content.value.as[Double].get shouldBe result2(i).content.value.as[Double].get +- 1e-8
    }
  }

  it should "return its second over in 3D" in {
    val res = toRDD(data3)
      .mutualInformation(Over(_1), Ternary(Default(12), InMemory(12), Default(12)))(TestMutualInformation.name, true)
      .toList.sortBy(_.position)

    res.size shouldBe result3.size
    for (i <- 0 until res.size) {
      res(i).position shouldBe result3(i).position
      res(i).content.value.as[Double].get shouldBe result3(i).content.value.as[Double].get +- 1e-8
    }
  }

  it should "filter correctly" in {
    val res = toRDD(data4)
      .mutualInformation(Over(_1), Ternary(Default(12), Default(), Default(12)))(TestMutualInformation.name, true)
      .toList.sortBy(_.position)

    res.size shouldBe result4.size
    for (i <- 0 until res.size) {
      res(i).position shouldBe result4(i).position
      res(i).content.value.as[Double].get shouldBe result4(i).content.value.as[Double].get +- 1e-8
    }
  }

  it should "not filter correctly" in {
    val res = toRDD(data4)
      .mutualInformation(Over(_1), Ternary(Default(12), Default(12), Default(12)))(TestMutualInformation.name, false)
      .toList.sortBy(_.position)

    res.size shouldBe result1.size
    for (i <- 0 until res.size) {
      res(i).position shouldBe result1(i).position
      res(i).content.value.as[Double].get shouldBe result1(i).content.value.as[Double].get +- 1e-8
    }
  }
}

