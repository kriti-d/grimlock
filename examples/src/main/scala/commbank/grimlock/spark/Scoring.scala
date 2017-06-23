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
import commbank.grimlock.framework.extract._
import commbank.grimlock.framework.position._

import commbank.grimlock.library.aggregate._
import commbank.grimlock.library.transform._

import commbank.grimlock.spark.environment._

import org.apache.spark.{ SparkConf, SparkContext }

import shapeless.nat.{ _1, _2 }

object Scoring {
  def main(args: Array[String]) {
    // Define implicit context.
    implicit val ctx = Context(new SparkContext(args(0), "Grimlock Spark Demo", new SparkConf()))

    // Path to data files, output folder
    val path = if (args.length > 1) args(1) else "../../data"
    val output = "spark"

    // Read the data (ignoring errors). This returns a 2D matrix (instance x feature).
    val (data, _) = ctx.loadText(s"${path}/exampleInput.txt", Cell.parse2D())
    // Read the statistics (ignoring errors) from the PipelineDataPreparation example.
    val stats = ctx.loadText(s"./demo.${output}/stats.out", Cell.parse2D()).data.compact(Over(_1))
    // Read externally learned weights (ignoring errors).
    val weights = ctx.loadText(s"${path}/exampleWeights.txt", Cell.parse1D()).data.compact(Over(_1))

    // Define extract object to get data out of statistics map.
    def extractStat(key: String) = ExtractWithDimensionAndKey[_2, Content](_2, key).andThenPresent(_.value.asDouble)

    // Define extract object to get data out of weights map.
    val extractWeight = ExtractWithDimension[_2, Content](_2).andThenPresent(_.value.asDouble)

    // For the data do:
    //  1/ Create indicators, binarise categorical, and clamp & standardise numerical features;
    //  2/ Compute the scored (as a weighted sum);
    //  3/ Save the results.
    data
      .transformWithValue(
        stats,
        Indicator().andThenRelocate(Locate.RenameDimension(_2, "%1$s.ind")),
        Binarise(Locate.RenameDimensionWithContent(_2)),
        Clamp(extractStat("min"), extractStat("max"))
          .andThenWithValue(Standardise(extractStat("mean"), extractStat("sd")))
      )
      .summariseWithValue(Over(_1))(weights, WeightedSums(extractWeight))
      .saveAsText(s"./demo.${output}/scores.out")
      .toUnit
  }
}

