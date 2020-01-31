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

import commbank.grimlock.framework.{ Cell, Locate }
import commbank.grimlock.framework.content.Content
import commbank.grimlock.framework.encoding.{ StringCodec, Value }
import commbank.grimlock.framework.environment.implicits._
import commbank.grimlock.framework.extract.{ ExtractWithDimension, ExtractWithDimensionAndKey }
import commbank.grimlock.framework.position.{ Coordinates2, Over }

import commbank.grimlock.library.aggregate.WeightedSums
import commbank.grimlock.library.transform.{ Binarise, Clamp, Indicator, Standardise }

import commbank.grimlock.scala.Persist
import commbank.grimlock.scala.environment.Context
import commbank.grimlock.scala.environment.implicits._

import shapeless.HNil
import shapeless.nat.{ _0, _1 }

object Scoring {
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
        Cell.shortStringParser(StringCodec :: StringCodec :: HNil, "|")
      )

    // Read the statistics (ignoring errors) from the PipelineDataPreparation example.
    val stats = ctx
      .read(
        s"./demo.${output}/stats.out",
        Persist.textLoader,
        Cell.shortStringParser(StringCodec :: StringCodec :: HNil, "|")
      )
      .data
      .gatherByPosition(Over(_0))

    // Read externally learned weights (ignoring errors).
    val weights = ctx
      .read(s"${path}/exampleWeights.txt", Persist.textLoader, Cell.shortStringParser(StringCodec :: HNil, "|"))
      .data
      .gatherByPosition(Over(_0))

    // Define extract object to get data out of statistics map.
    val extractStat = (key: String) =>
      ExtractWithDimensionAndKey[Coordinates2[String, String], _1, String, Content](key)
        .andThenPresent(_.value.as[Double])

    // Define extract object to get data out of weights map.
    val extractWeight = ExtractWithDimension[Coordinates2[String, String], _1, Content]
      .andThenPresent(_.value.as[Double])

    // For the data do:
    //  1/ Create indicators, binarise categorical, and clamp & standardise numerical features;
    //  2/ Compute the scored (as a weighted sum);
    //  3/ Save the results.
    data
      .transformWithValue(
        stats,
        Indicator().andThenRelocate(Locate.RenameDimension(_1, (c: Value[String]) => s"${c.toShortString}.ind")),
        Binarise(Locate.RenameDimensionWithContent(_1, Value.concatenate[Value[String], Value[_]]("="))),
        Clamp(extractStat("min"), extractStat("max"))
          .andThenWithValue(Standardise(extractStat("mean"), extractStat("sd")))
      )
      .summariseWithValue(Over(_0))(weights, WeightedSums(extractWeight))
      .saveAsText(ctx, s"./demo.${output}/scores.out", Cell.toShortString(true, "|"))
      .toUnit
  }
}

