// Copyright 2017,2018,2019,2020 Commonwealth Bank of Australia
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

import shapeless.HList
import shapeless.nat.{ _0, _1 }

// see http://www.mathsisfun.com/data/correlation.html for data
trait TestDistanceCorrelation extends TestGrimlock {
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

object TestDistanceCorrelation {
  def name[P <: HList] = (l: Position[P], r: Position[P]) =>
    Position(s"(${l.toShortString("|")}*${r.toShortString("|")})").toOption
}

// see http://www.eecs.harvard.edu/cs286r/courses/fall10/papers/Chapter2.pdf example 2.2.1 for data
trait TestDistanceMutualInformation extends TestGrimlock {
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

object TestDistanceMutualInformation {
  def name[P <: HList] = (l: Position[P], r: Position[P]) =>
    Position(s"${r.toShortString("|")},${l.toShortString("|")}").toOption
}

