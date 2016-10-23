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
//import commbank.grimlock.framework.content.metadata._
import commbank.grimlock.framework.partition._
import commbank.grimlock.framework.position._

import commbank.grimlock.library.aggregate._
import commbank.grimlock.library.transform._

import commbank.grimlock.spark.environment._
import commbank.grimlock.spark.environment.Context._
import commbank.grimlock.spark.Matrix._

import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.rdd.RDD

import shapeless.Nat
import shapeless.nat.{ _1, _2 }
import shapeless.ops.nat.{ LTEq, ToInt }

// Define a custom partition. If the instance is 'iid:0364354' or 'iid:0216406' then assign it to the right (test)
// partition. In all other cases assing it to the left (train) partition.
case class CustomPartition[
  D <: Nat : ToInt
](
  dim: D,
  left: String,
  right: String
)(implicit
  ev: LTEq[D, _2]
) extends Partitioner[_2, String] {
  def assign(cell: Cell[_2]): TraversableOnce[String] = {
    val pos = cell.position(dim).toShortString

    if (pos == "iid:0364354" || pos == "iid:0216406") List(right) else List(left)
  }
}

object PipelineDataPreparation {
  def main(args: Array[String]) {
    // Define implicit context.
    implicit val ctx = Context(new SparkContext(args(0), "Grimlock Spark Demo", new SparkConf()))

    // Path to data files, output folder
    val path = if (args.length > 1) args(1) else "../../data"
    val output = "spark"

    // Read the data (ignoring errors). This returns a 2D matrix (instance x feature).
    val (data, _) = loadText(ctx, s"${path}/exampleInput.txt", Cell.parse2D())

    // Perform a split of the data into a training and test set.
    val parts = data
      .split(CustomPartition(_1, "train", "test"))

    // Get the training data
    val train = parts
      .get("train")

    // Compute descriptive statistics on the training data.
    val descriptive = train
      .summarise(Along(_1))(
        Count().andThenRelocate(_.position.append("count").toOption),
        Moments(
          _.append("mean").toOption,
          _.append("sd").toOption,
          _.append("skewness").toOption,
          _.append("kurtosis").toOption
        ),
        Limits(_.append("min").toOption, _.append("max").toOption),
        MaxAbs().andThenRelocate(_.position.append("max.abs").toOption)
      )

    // Compute histogram on the categorical features in the training data.
    val histogram = train
      .histogram(Along(_1))(Locate.AppendDimensionAndContentString(_1))

    // Compute the counts for each categorical features.
    val counts = histogram
      .summarise(Over(_1))(Sum())
      .compact()

    // Define extractor to extract counts from the map.
    val extractCount = ExtractWithDimension[_2, Content](_1).andThenPresent(_.value.asDouble)

    // Compute summary statisics on the histogram.
    val summary = histogram
      .summariseWithValue(Over(_1))(
        counts,
        Count().andThenRelocate(_.position.append("num.cat").toOption),
        Entropy(extractCount).andThenRelocate(_.position.append("entropy").toOption),
        FrequencyRatio().andThenRelocate(_.position.append("freq.ratio").toOption)
      )

    // Combine all statistics and write result to file
    val stats = (descriptive ++ histogram ++ summary)
      .saveAsText(ctx, s"./demo.${output}/stats.out")

    // Determine which features to filter based on statistics. In this case remove all features that occur for 2 or
    // fewer instances. These are removed first to prevent indicator features from being created.
    val rem1 = stats
      .which(cell => (cell.position(_2) equ "count") && (cell.content.value leq 2))
      .names(Over(_1))

    // Also remove constant features (standard deviation is 0, or 1 category). These are removed after indicators have
    // been created.
    val rem2 = stats
      .which(cell =>
        ((cell.position(_2) equ "sd") && (cell.content.value equ 0)) ||
        ((cell.position(_2) equ "num.cat") && (cell.content.value equ 1))
      )
      .names(Over(_1))

    // Finally remove categoricals for which an individual category has only 1 value. These are removed after binarized
    // features have been created.
    val rem3 = stats
      .which(cell => (cell.position(_2) like ".*=.*".r) && (cell.content.value equ 1))
      .names(Over(_2))

    // Define extract object to get data out of statistics map.
    def extractStat(key: String) = ExtractWithDimensionAndKey[_2, Content](_2, key).andThenPresent(_.value.asDouble)

    // For each partition:
    //  1/  Remove sparse features;
    //  2/  Create indicator features;
    //  3a/ Remove constant features;
    //  3b/ Clamp features to min/max value of the training data and standardise, binarise categorical features;
    //  3c/ Remove sparse category features;
    //  4a/ Combine preprocessed data sets;
    //  4b/ Optionally fill the matrix (note: this is expensive);
    //  4c/ Save the result as pipe separated CSV for use in modelling.
    def prepare(key: String, partition: RDD[Cell[_2]]): RDD[Cell[_2]] = {
      val d = partition
        .slice(Over(_2))(rem1, false)

      val ind = d
        .transform(Indicator().andThenRelocate(Locate.RenameDimension(_2, "%1$s.ind")))

      val csb = d
        .slice(Over(_2))(rem2, false)
        .transformWithValue(
          stats.compact(Over(_1)),
          Clamp(extractStat("min"), extractStat("max"))
            .andThenWithValue(Standardise(extractStat("mean"), extractStat("sd"))),
          Binarise(Locate.RenameDimensionWithContent(_2))
        )
        .slice(Over(_2))(rem3, false)

      (ind ++ csb)
        //.fillHomogeneous(Content(ContinuousSchema[Double](), 0.0))
        .saveAsCSV(Over(_1))(ctx, s"./demo.${output}/${key}.csv")
    }

    // Prepare each partition.
    parts
      .forEach(List("train", "test"), prepare)
      .toUnit
  }
}

