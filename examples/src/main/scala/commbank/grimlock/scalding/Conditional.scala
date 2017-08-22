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

package commbank.grimlock.scalding.examples

import commbank.grimlock.framework._
import commbank.grimlock.framework.content._
import commbank.grimlock.framework.encoding._
import commbank.grimlock.framework.extract._
import commbank.grimlock.framework.position._

import commbank.grimlock.library.aggregate._
import commbank.grimlock.library.squash._
import commbank.grimlock.library.transform._

import commbank.grimlock.scalding.environment._

import com.twitter.scalding.{ Args, Job }

import shapeless.{ Nat, Succ }
import shapeless.nat.{ _1, _2 }

class Conditional(args: Args) extends Job(args) {

  // Define implicit context.
  implicit val ctx = Context()

  // Path to data files, output folder
  val path = args.getOrElse("path", "../../data")
  val output = "scalding"

  // Read the data.
  // 1/ Read the data (ignoring errors), this returns a 2D matrix (row x feature).
  val (data, _) = ctx.loadText(s"${path}/exampleConditional.txt", Cell.parse2D())

  // Define function that appends the value as a string, or "missing" if no value is available
  def cast[P <: Nat](cell: Cell[P], value: Option[Value]): Option[Position[Succ[P]]] = cell
    .position
    .append(value.map(_.toShortString).getOrElse[String]("missing"))
    .toOption

  // Generate 3D matrix (hair color x eye color x gender)
  // 1/ Reshape matrix expanding it with hair color.
  // 2/ Reshape matrix expanding it with eye color.
  // 3/ Reshape matrix expanding it with gender.
  // 4/ Melt the remaining 'value' column of the second dimension into the row key (first) dimension.
  // 5/ Squash the first dimension (row ids + value). As there is only one value for each
  //    hair/eye/gender triplet, any squash function can be used.
  val heg = data
    .reshape(_2, "hair", cast)
    .reshape(_2, "eye", cast)
    .reshape(_2, "gender", cast)
    .melt(_2, _1, Value.concatenate("."))
    .squash(_1, PreservingMaximumPosition())

  // Define an extractor for getting data out of the gender count (gcount) map.
  def extractor = ExtractWithDimension[_2, Content](_2).andThenPresent(_.value.asDouble)

  // Get the gender counts. Sum out hair and eye color.
  val gcount = heg
    .summarise(Along(_1))(Sums())
    .summarise(Along(_1))(Sums())
    .compact()

  // Get eye color conditional on gender.
  // 1/ Sum out hair color.
  // 2/ Divide each element by the gender's count to get conditional distribution.
  heg
    .summarise(Along(_1))(Sums())
    .transformWithValue(gcount, Fraction(extractor))
    .saveAsText(s"./demo.${output}/eye.out")
    .toUnit

  // Get hair color conditional on gender.
  // 1/ Sum out eye color.
  // 2/ Divide each element by the gender's count to get conditional distribution.
  heg
    .summarise(Along(_2))(Sums())
    .transformWithValue(gcount, Fraction(extractor))
    .saveAsText(s"./demo.${output}/hair.out")
    .toUnit
}

