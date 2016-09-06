// Copyright 2014,2015,2016 Commonwealth Bank of Australia
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
import commbank.grimlock.framework.content.metadata._
import commbank.grimlock.framework.encoding._
import commbank.grimlock.framework.position._
import commbank.grimlock.framework.window._

import commbank.grimlock.spark.environment._
import commbank.grimlock.spark.environment.Context._
import commbank.grimlock.spark.Matrix._

import org.apache.spark.{ SparkConf, SparkContext }

import shapeless.Nat
import shapeless.nat.{ _1, _2, _3 }
import shapeless.ops.nat.{ LTEq, ToInt }

// Simple gradient feature genertor
case class Gradient[D <: Nat : ToInt](dim: D)(implicit ev: LTEq[D, _3]) extends Window[_3, _2, _1, _3] {
  type I = (Option[Long], Option[Double])
  type T = (Option[Long], Option[Double], Position[_1])
  type O = (Option[Double], Position[_1], Position[_1])

  val DayInMillis = 1000 * 60 * 60 * 24
  val separator = ""

  // Prepare the sliding window, the state is the time and the value.
  def prepare(cell: Cell[_3]): I = (cell.position(dim).asDate.map(_.getTime), cell.content.value.asDouble)

  // Initialise state to the time, value and remainder coordinates.
  def initialise(rem: Position[_1], in: I): (T, TraversableOnce[O]) = ((in._1, in._2, rem), List())

  // For each new cell, output the difference with the previous cell (contained in `t`).
  def update(rem: Position[_1], in: I, t: T): (T, TraversableOnce[O]) = {
    // Get current date from `in` and previous date from `t` and compute number of days between the dates.
    val days = in._1.flatMap(dc => t._1.map(dt => (dc - dt) / DayInMillis))

    // Get the difference between current and previous values.
    val delta = in._2.flatMap(dc => t._2.map(dt => dc - dt))

    // Generate the gradient (delta / days).
    val grad = days.flatMap(td => delta.map(vd => vd / td))

    // Update state to be current `in` and `rem`, and output the gradient.
    ((in._1, in._2, rem), List((grad, rem, t._3)))
  }

  // If a gradient is available, output a cell for it.
  def present(pos: Position[_2], out: O): TraversableOnce[Cell[_3]] = out._1.map(grad =>
    Cell(
      pos.append(out._3.toShortString(separator) + ".to." + out._2.toShortString(separator)),
      Content(ContinuousSchema[Double](), grad)
    )
  )
}

object DerivedData {
  def main(args: Array[String]) {
    // Define implicit context.
    implicit val ctx = Context(new SparkContext(args(0), "Grimlock Spark Demo", new SparkConf()))

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
    loadText(ctx, s"${path}/exampleDerived.txt", Cell.parse3D(third = DateCodec()))
      .data
      .slide(Along(_3))(Gradient(_3))
      .melt(_3, _2, Value.concatenate(".from."))
      .saveAsText(ctx, s"./demo.${output}/gradient.out")
      .toUnit
  }
}

