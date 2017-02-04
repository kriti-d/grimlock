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
import commbank.grimlock.framework.content.metadata._
import commbank.grimlock.framework.encoding._
import commbank.grimlock.framework.pairwise._
import commbank.grimlock.framework.position._
import commbank.grimlock.framework.transform._

import commbank.grimlock.library.aggregate._
import commbank.grimlock.library.pairwise._
import commbank.grimlock.library.squash._

import commbank.grimlock.scalding.environment._
import commbank.grimlock.scalding.environment.Context._
import commbank.grimlock.scalding.Matrix._

import com.twitter.scalding.{ Args, Job }

import scala.io.Source

import shapeless.nat.{ _1, _2, _3 }

// Simple bucketing implementation. For numerical values it generates categorical values that are the rounded up
// value. All other values are passed through.
case class CeilingBucketing() extends Transformer[_2, _2] {
  def present(cell: Cell[_2]): TraversableOnce[Cell[_2]] = {
    val con = (cell.content.schema.classification.isOfType(NumericType), cell.content.value.asDouble) match {
      case (true, Some(d)) => Content(NominalSchema[Long](), math.ceil(d).toLong)
      case _ => cell.content
    }

    List(Cell(cell.position, con))
  }
}

class MutualInformation(args: Args) extends Job(args) {

  // Define implicit context.
  implicit val ctx = Context()

  // Path to data files, output folder
  val path = args.getOrElse("path", "../../data")
  val output = "scalding"

  // Read in the dictionary (ignoring errors).
  val (dictionary, _) = Dictionary.load(Source.fromFile(s"${path}/exampleDictionary.txt"))

  // Read the data.
  // 1/ Read the data using the supplied dictionary. This returns a 3D matrix (instance x feature x date).
  // 2/ Proceed with only the data (ignoring errors).
  // 3/ Squash the 3rd dimension, keeping values with minimum (earlier) coordinates. The result is a 2D matrix
  //    (instance x feature).
  // 4/ Bucket all continuous variables by rounding them.
  val data = loadText(
      ctx,
      s"${path}/exampleMutual.txt",
      Cell.parse3DWithDictionary(dictionary, _2, third = DateCodec())
    )
    .data
    .squash(_3, PreservingMinimumPosition())
    .transform(CeilingBucketing())

  // Define extractor for extracting count from histogram count map.
  val extractor = ExtractWithDimension[_2, Content](_1).andThenPresent(_.value.asDouble)

  // Compute histogram on the data.
  val mhist = data
    .histogram(Along(_1))(Locate.AppendContentString(), false)

  // Compute count of histogram elements.
  val mcount = mhist
    .summarise(Over(_1))(Sums())
    .compact()

  // Compute sum of marginal entropy
  // 1/ Compute the marginal entropy over the features (second dimension).
  // 2/ Compute pairwise sum of marginal entropies for all upper triangular values.
  val marginal = mhist
    .summariseWithValue(Over(_1))(mcount, Entropy(extractor).andThenRelocate(_.position.append("marginal").toOption))
    .pairwise(Over(_1))(Upper, Plus(Locate.PrependPairwiseSelectedStringToRemainder(Over[_1, _2](_1), "%s,%s")))

  // Compute histogram on pairwise data.
  // 1/ Generate pairwise values for all upper triangular values.
  // 2/ Compute histogram on pairwise values.
  val jhist = data
    .pairwise(Over(_2))(
      Upper,
      Concatenate(Locate.PrependPairwiseSelectedStringToRemainder(Over[_1, _2](_2), "%s,%s"))
    )
    .histogram(Along(_2))(Locate.AppendContentString(), false)

  // Compute count of histogram elements.
  val jcount = jhist
    .summarise(Over(_1))(Sums())
    .compact()

  // Compute joint entropy
  val joint = jhist
    .summariseWithValue(Over(_1))(
      jcount,
      Entropy(extractor, negate = true).andThenRelocate(_.position.append("joint").toOption)
    )

  // Generate mutual information
  // 1/ Sum marginal and negated joint entropy
  // 2/ Persist mutual information.
  (marginal ++ joint)
    .summarise(Over(_1))(Sums())
    .saveAsText(ctx, s"./demo.${output}/mi.out")
    .toUnit
}

