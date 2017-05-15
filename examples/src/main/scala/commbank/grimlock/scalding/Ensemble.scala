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
import commbank.grimlock.framework.metadata._
import commbank.grimlock.framework.partition._
import commbank.grimlock.framework.position._

import commbank.grimlock.library.aggregate._

import commbank.grimlock.scalding.environment._

import com.twitter.scalding.{ Args, Job }
import com.twitter.scalding.typed.{ TypedPipe, ValuePipe }

import shapeless.nat.{ _1, _2, _3 }

// Assign cell to 1 or more partitions based on the hash-code in the 3rd dimension.
case class EnsembleSplit(gbm: String, rf: String, lr: String) extends Partitioner[_3, String] {
  // Depending on the hash-code assign the cell to the appropriate partition:
  //   [0, 4) -> gbm
  //   [2, 6) -> rf
  //   [4, 8) -> lr
  //   [8, 9] -> all
  def assign(cell: Cell[_3]): TraversableOnce[String] = cell
    .position(_3)
    .asLong
    .map { case hash =>
      if (hash < 2) List(gbm)
      else if (hash < 4) List(gbm, rf)
      else if (hash < 6) List(rf, lr)
      else if (hash < 8) List(lr)
      else List(gbm, rf, lr)
    }
    .getOrElse(List())
}

// Simple ensemble(-like) model learning
class Ensemble(args: Args) extends Job(args) {

  // Define implicit context.
  implicit val ctx = Context()

  // Path to data files, output folder
  val path = args.getOrElse("path", "../../data")
  val output = "scalding"

  // Define table schema (column name and variable type tuple).
  val schema = List(
    ("entity.id", Content.parser(StringCodec, NominalSchema[String]())),
    ("label", Content.parser(LongCodec, NominalSchema[Long]())),
    ("x", Content.parser(DoubleCodec, ContinuousSchema[Double]())),
    ("y", Content.parser(DoubleCodec, ContinuousSchema[Double]())),
    ("z", Content.parser(DoubleCodec, ContinuousSchema[Double]()))
  )

  // Load the data (generated by ensemble.R); returns a 2D matrix (instance x feature);
  val (data, _) = ctx.loadText(s"${path}/exampleEnsemble.txt", Cell.parseTable(schema, separator = "|"))

  // Ensemble scripts to apply to the data.
  val scripts = List("gbm.R", "lr.R", "rf.R")

  // Weigh each model equally.
  val weights = ValuePipe(Map(
    Position(scripts(0)) -> Content(ContinuousSchema[Double](), 0.33),
    Position(scripts(1)) -> Content(ContinuousSchema[Double](), 0.33),
    Position(scripts(2)) -> Content(ContinuousSchema[Double](), 0.33)
  ))

  // Train and score the data.
  //
  // The partition key is the script to apply to the model. This approach assumes that the scripts split
  // the data into a ~40% train set per script and a shared ~20% test set. The returned scores are for
  // the test set. The resulting scores are expanded with the model name so they can be merged (see below).
  def trainAndScore(key: String, partition: TypedPipe[Cell[_3]]): TypedPipe[Cell[_2]] = partition
    .stream("Rscript " + key, List(key), Cell.toString(false, "|", true), Cell.parse1D("|", StringCodec))
    .data // Keep only the data (ignoring errors).
    .relocate(Locate.AppendValue(key))

  // Define extractor to get weight out of weights map.
  val extractWeight = ExtractWithDimension[_2, Content](_2).andThenPresent(_.value.asDouble)

  // Train and score an ensemble:
  // 1/ Expand with a dimension that holds the hash-code base 10 (for use in partitioning);
  // 2/ Partition the data roughly into 40% for each model, plus shared 20% for scoring;
  // 3/ For each parition, apply the 'train and score' function;
  // 4/ Merge the scores of each model into a single 2D matrix (instance x model);
  // 5/ Apply a weighted sum to the model scores (this can be leared by streaming the scores);
  // 6/ Persist the final scores.
  data
    .relocate(c => c.position.append(math.abs(c.position(_1).hashCode % 10)).toOption)
    .split(EnsembleSplit(scripts(0), scripts(1), scripts(2)))
    .forEach(scripts, trainAndScore)
    .merge(scripts)
    .summariseWithValue(Over(_1))(weights, WeightedSums(extractWeight))
    .saveAsText(s"./demo.${output}/ensemble.scores.out")
    .toUnit
}

