// Copyright 2014,2015,2016,2017,2018,2019 Commonwealth Bank of Australia
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

package commbank.grimlock.spark.examples

import commbank.grimlock.framework._
import commbank.grimlock.framework.content._
import commbank.grimlock.framework.encoding._
import commbank.grimlock.framework.environment.implicits._
import commbank.grimlock.framework.metadata._
import commbank.grimlock.framework.position._
import commbank.grimlock.framework.window._

import commbank.grimlock.spark.environment._
import commbank.grimlock.spark.environment.implicits._

import java.util.Date

import org.apache.spark.sql.SparkSession

import shapeless.{ HList, HNil }
import shapeless.nat.{ _0, _1, _2 }

// Simple gradient feature generator
case class Gradient[
  P <: HList,
  S <: HList,
  R <: HList,
  Q <: HList
](implicit
  ev1: Value.Box[String],
  ev2: Position.IndexConstraints.Aux[P, _2, Value[Date]],
  ev3: Position.IndexConstraints.Aux[R, _0, Value[Date]],
  ev4: Position.AppendConstraints.Aux[S, Value[String], Q]
) extends Window[P, S, R, Q] {
  type I = (Long, Option[Double])
  type T = (Long, Option[Double], Value[Date])
  type O = (Option[Double], Value[Date], Value[Date])

  val DayInMillis = 1000 * 60 * 60 * 24

  // Prepare the sliding window, the state is the time and the value.
  def prepare(cell: Cell[P]): I = (cell.position(_2).value.getTime, cell.content.value.as[Double])

  // Initialise state to the time, value and remainder coordinates.
  def initialise(rem: Position[R], in: I): (T, TraversableOnce[O]) = ((in._1, in._2, rem(_0)), List())

  // For each new cell, output the difference with the previous cell (contained in `t`).
  def update(rem: Position[R], in: I, t: T): (T, TraversableOnce[O]) = {
    // Get current date from `in` and previous date from `t` and compute number of days between the dates.
    val days = (in._1 - t._1) / DayInMillis

    // Get the difference between current and previous values.
    val delta = in._2.flatMap(dc => t._2.map(dt => dc - dt))

    // Generate the gradient (delta / days).
    val grad = delta.map(vd => vd / days)

    // Update state to be current `in` and `rem`, and output the gradient.
    ((in._1, in._2, rem(_0)), List((grad, rem(_0), t._3)))
  }

  // If a gradient is available, output a cell for it.
  def present(pos: Position[S], out: O): TraversableOnce[Cell[Q]] = out._1.map(grad =>
    Cell(pos.append(out._3.toShortString + ".to." + out._2.toShortString), Content(ContinuousSchema[Double](), grad))
  )
}

object DerivedData {
  def main(args: Array[String]) {
    // Define implicit context.
    implicit val ctx = Context(SparkSession.builder().master(args(0)).appName("Grimlock Spark Demo").getOrCreate())

    // Path to data files, output folder
    val path = if (args.length > 1) args(1) else "../../data"
    val output = "spark"

    // Generate gradient features:
    // 1/ Read the data as 3D matrix (instance x feature x date).
    // 2/ Proceed with only the data (ignoring errors).
    // 3/ Compute gradients along the date axis. The result is a 3D matrix (instance x feature x gradient).
    // 4/ Melt third dimension (gradients) into second dimension. The result is a 2D matrix (instance x
    //    feature.from.gradient)
    // 5/ Persist 2D gradient features.
    ctx
      .loadText(
        s"${path}/exampleDerived.txt",
        Cell.shortStringParser(StringCodec :: StringCodec :: DateCodec() :: HNil, "|")
      )
      .data
      .slide(Along(_2))(true, Gradient())
      .contract(_2, _1, Value.concatenate[Value[String], Value[String]](".from."))
      .saveAsText(ctx, s"./demo.${output}/gradient.out", Cell.toShortString(true, "|"))
      .toUnit
  }
}

