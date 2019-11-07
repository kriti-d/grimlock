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
import commbank.grimlock.framework.extract._
import commbank.grimlock.framework.position._

import commbank.grimlock.library.aggregate._
import commbank.grimlock.library.transform._

import commbank.grimlock.spark.environment._
import commbank.grimlock.spark.environment.implicits._

import org.apache.spark.sql.SparkSession

import shapeless.HNil
import shapeless.nat.{ _0, _1 }

object Scoring {
  def main(args: Array[String]) {
    // Define implicit context.
    implicit val ctx = Context(SparkSession.builder().master(args(0)).appName("Grimlock Spark Demo").getOrCreate())

    // Path to data files, output folder
    val path = if (args.length > 1) args(1) else "../../data"
    val output = "spark"

    // Read the data (ignoring errors). This returns a 2D matrix (instance x feature).
    val (data, _) = ctx
      .loadText(s"${path}/exampleInput.txt", Cell.shortStringParser(StringCodec :: StringCodec :: HNil, "|"))

    // Read the statistics (ignoring errors) from the PipelineDataPreparation example.
    val stats = ctx
      .loadText(s"./demo.${output}/stats.out", Cell.shortStringParser(StringCodec :: StringCodec :: HNil, "|"))
      .data
      .gatherByPosition(Over(_0))

    // Read externally learned weights (ignoring errors).
    val weights = ctx
      .loadText(s"${path}/exampleWeights.txt", Cell.shortStringParser(StringCodec :: HNil, "|"))
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

