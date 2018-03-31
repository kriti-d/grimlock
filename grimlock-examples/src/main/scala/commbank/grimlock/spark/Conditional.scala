// Copyright 2014,2015,2016,2017 Commonwealth Bank of Australia
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
import commbank.grimlock.framework.extract._
import commbank.grimlock.framework.position._

import commbank.grimlock.library.aggregate._
import commbank.grimlock.library.squash._
import commbank.grimlock.library.transform._

import commbank.grimlock.spark.environment._
import commbank.grimlock.spark.environment.implicits._

import org.apache.spark.sql.SparkSession

import shapeless.{ HList, HNil }
import shapeless.nat.{ _0, _1 }

object Conditional {
  def main(args: Array[String]) {
    // Define implicit context.
    implicit val ctx = Context(SparkSession.builder().master(args(0)).appName("Grimlock Spark Demo").getOrCreate())

    // Path to data files, output folder
    val path = if (args.length > 1) args(1) else "../../data"
    val output = "spark"

    // Read the data.
    // 1/ Read the data (ignoring errors), this returns a 2D matrix (row x feature).
    val (data, _) = ctx
      .loadText(s"${path}/exampleConditional.txt", Cell.shortStringParser(StringCodec :: StringCodec :: HNil, "|"))

    // Define function that appends the value as a string, or "missing" if no value is available
    def cast[
      P <: HList
    ](implicit
      ev1: Value.Box[String],
      ev2: Position.AppendConstraints[P, Value[String]]
    ) = (cell: Cell[P], value: Option[Value[_]]) => cell
      .position
      .append(value.map(_.toShortString).getOrElse("missing"))
      .toOption

    // Generate 3D matrix (hair color x eye color x gender)
    // 1/ Reshape matrix expanding it with hair color.
    // 2/ Reshape matrix expanding it with eye color.
    // 3/ Reshape matrix expanding it with gender.
    // 4/ Melt the remaining 'value' column of the second dimension into the row key (first) dimension.
    // 5/ Squash the first dimension (row ids + value). As there is only one value for each
    //    hair/eye/gender triplet, any squash function can be used.
    val heg = data
      .expand(_1, "hair", cast)
      .expand(_1, "eye", cast)
      .expand(_1, "gender", cast)
      .contract(_1, _0, Value.concatenate[Value[String], Value[String]]("."))
      .squash(_0, PreservingMaximumPosition())

    // Define an extractor for getting data out of the gender count (gcount) map.
    val extractor = ExtractWithDimension[Coordinates2[String, String], _1, Content]
      .andThenPresent(_.value.as[Double])

    // Get the gender counts. Sum out hair and eye color.
    val gcount = heg
      .summarise(Along(_0))(Sums())
      .summarise(Along(_0))(Sums())
      .gather()

    // Get eye color conditional on gender.
    // 1/ Sum out hair color.
    // 2/ Divide each element by the gender's count to get conditional distribution.
    heg
      .summarise(Along(_0))(Sums())
      .transformWithValue(gcount, Fraction(extractor))
      .saveAsText(ctx, s"./demo.${output}/eye.out", Cell.toShortString(true, "|"))
      .toUnit

    // Get hair color conditional on gender.
    // 1/ Sum out eye color.
    // 2/ Divide each element by the gender's count to get conditional distribution.
    heg
      .summarise(Along(_1))(Sums())
      .transformWithValue(gcount, Fraction(extractor))
      .saveAsText(ctx, s"./demo.${output}/hair.out", Cell.toShortString(true, "|"))
      .toUnit
  }
}

