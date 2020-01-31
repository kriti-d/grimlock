// Copyright 2014,2015,2016,2017,2018,2019,2020 Commonwealth Bank of Australia
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

package commbank.grimlock.scalding.examples

import commbank.grimlock.framework.Cell
import commbank.grimlock.framework.content.Content
import commbank.grimlock.framework.encoding.{ StringCodec, Value }
import commbank.grimlock.framework.environment.implicits._
import commbank.grimlock.framework.extract.ExtractWithDimension
import commbank.grimlock.framework.position.{ Along, Coordinates2, Position }

import commbank.grimlock.library.aggregate.Sums
import commbank.grimlock.library.squash.PreservingMaximumPosition
import commbank.grimlock.library.transform.Fraction

import commbank.grimlock.scalding.Persist
import commbank.grimlock.scalding.environment.Context
import commbank.grimlock.scalding.environment.implicits._

import com.twitter.scalding.{ Args, Job }

import shapeless.{ HList, HNil }
import shapeless.nat.{ _0, _1 }

class Conditional(args: Args) extends Job(args) {
  // Define implicit context.
  implicit val ctx = Context()

  import ctx.encoder

  // Path to data files, output folder
  val path = args.getOrElse("path", "../../data")
  val output = "scalding"

  // Read the data.
  // 1/ Read the data (ignoring errors), this returns a 2D matrix (row x feature).
  val (data, _) = ctx
    .read(
      s"${path}/exampleConditional.txt",
      Persist.textLoader,
      Cell.shortStringParser(StringCodec :: StringCodec :: HNil, "|")
    )

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

