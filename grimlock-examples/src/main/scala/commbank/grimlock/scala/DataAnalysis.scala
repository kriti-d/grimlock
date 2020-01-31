// Copyright 2019,2020 Commonwealth Bank of Australia
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

package commbank.grimlock.scala.examples

import commbank.grimlock.framework.Cell
import commbank.grimlock.framework.encoding.StringCodec
import commbank.grimlock.framework.environment.implicits._
import commbank.grimlock.framework.position.{ Along, Over }

import commbank.grimlock.library.aggregate._

import commbank.grimlock.scala.Persist
import commbank.grimlock.scala.environment.Context
import commbank.grimlock.scala.environment.implicits._

import shapeless.HNil
import shapeless.nat.{ _0, _1 }

object DataAnalysis {
  def main(args: Array[String]) {
    // Define implicit context.
    implicit val ctx = Context()

    import ctx.encoder

    // Path to data files, output folder
    val path = if (args.length > 0) args(0) else "../../data"
    val output = "scala"

    // Read the data (ignoring errors). This returns a 2D matrix (instance x feature).
    val (data, _) = ctx
      .read(
        s"${path}/exampleInput.txt",
        Persist.textLoader,
        Cell.shortStringParser(StringCodec :: StringCodec :: HNil,"|")
      )

    // For the instances:
    //  1/ Compute the number of features for each instance;
    //  2/ Save the counts;
    //  3/ Compute the moments of the counts;
    //  4/ Save the moments.
    data
      .summarise(Over(_0))(Counts())
      .saveAsText(ctx, s"./demo.${output}/feature_count.out", Cell.toShortString(true, "|"))
      .summarise(Along(_0))(
        Moments(
          _.append("mean").toOption,
          _.append("sd").toOption,
          _.append("skewness").toOption,
          _.append("kurtosis").toOption
        )
      )
      .saveAsText(ctx, s"./demo.${output}/feature_density.out", Cell.toShortString(true, "|"))
      .toUnit

    // For the features:
    //  1/ Compute the number of instance that have a value for each features;
    //  2/ Save the counts;
    //  3/ Compute the moments of the counts;
    //  4/ Save the moments.
    data
      .summarise(Over(_1))(Counts())
      .saveAsText(ctx, s"./demo.${output}/instance_count.out", Cell.toShortString(true, "|"))
      .summarise(Along(_0))(
        Moments(
          _.append("mean").toOption,
          _.append("sd").toOption,
          _.append("skewness").toOption,
          _.append("kurtosis").toOption
        )
      )
      .saveAsText(ctx, s"./demo.${output}/instance_density.out", Cell.toShortString(true, "|"))
      .toUnit
  }
}

