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

import commbank.grimlock.framework.{ Cell, Locate }
import commbank.grimlock.framework.content.Content
import commbank.grimlock.framework.encoding.{ DateCodec, StringCodec }
import commbank.grimlock.framework.environment.implicits._
import commbank.grimlock.framework.extract.ExtractWithDimension
import commbank.grimlock.framework.metadata.{ Dictionary, NominalSchema, NumericType }
import commbank.grimlock.framework.pairwise.Upper
import commbank.grimlock.framework.position.{ Along, Over, Coordinates2 }
import commbank.grimlock.framework.transform.Transformer

import commbank.grimlock.library.aggregate.{ Entropy, Sums }
import commbank.grimlock.library.pairwise.{ Concatenate, Plus }
import commbank.grimlock.library.squash.PreservingMinimumPosition

import commbank.grimlock.scalding.Persist
import commbank.grimlock.scalding.environment.Context
import commbank.grimlock.scalding.environment.implicits._

import com.twitter.scalding.{ Args, Job }

import scala.io.Source

import shapeless.{ HList, HNil }
import shapeless.nat.{ _0, _1, _2 }

// Simple bucketing implementation. For numerical values it generates categorical values that are the rounded up
// value. All other values are passed through.
case class CeilingBucketing[P <: HList]() extends Transformer[P, P] {
  def present(cell: Cell[P]): TraversableOnce[Cell[P]] = {
    val con = (cell.content.classification.isOfType(NumericType), cell.content.value.as[Double]) match {
      case (true, Some(d)) => Content(NominalSchema[Long](), math.ceil(d).toLong)
      case _ => cell.content
    }

    List(Cell(cell.position, con))
  }
}

class MutualInformation(args: Args) extends Job(args) {
  // Define implicit context.
  implicit val ctx = Context()

  import ctx.encoder

  // Path to data files, output folder
  val path = args.getOrElse("path", "../../data")
  val output = "scalding"

  // Read in the dictionary (ignoring errors).
  val (dictionary, _) = Dictionary.load(Source.fromFile(s"${path}/exampleDictionary.txt"), "|")

  // Read the data.
  // 1/ Read the data using the supplied dictionary. This returns a 3D matrix (instance x feature x date).
  // 2/ Proceed with only the data (ignoring errors).
  // 3/ Squash the 3rd dimension, keeping values with minimum (earlier) coordinates. The result is a 2D matrix
  //    (instance x feature).
  // 4/ Bucket all continuous variables by rounding them.
  val data = ctx
    .read(
      s"${path}/exampleMutual.txt",
      Persist.textLoader,
      Cell.shortStringParser(StringCodec :: StringCodec :: DateCodec() :: HNil, dictionary, _1, "|")
    )
    .data
    .squash(_2, PreservingMinimumPosition())
    .transform(CeilingBucketing())

  // Define extractor for extracting count from histogram count map.
  val extractor = ExtractWithDimension[Coordinates2[String, String], _0, Content]
    .andThenPresent(_.value.as[Double])

  // Compute histogram on the data.
  val mhist = data
    .histogram(Along(_0))(Locate.AppendContentString, false)

  // Compute count of histogram elements.
  val mcount = mhist
    .summarise(Over(_0))(Sums())
    .gather()

  // Compute sum of marginal entropy
  // 1/ Compute the marginal entropy over the features.
  // 2/ Compute pairwise sum of marginal entropies for all upper triangular values.
  val marginal = mhist
    .summariseWithValue(Over(_0))(mcount, Entropy(extractor).andThenRelocate(_.position.append("marginal").toOption))
    .pair(Over(_0))(Upper, Plus(Locate.PrependPairwiseSelectedStringToRemainder(Over(_0), "%s,%s", false, "|")))

  // Compute histogram on pairwise data.
  // 1/ Generate pairwise values for all upper triangular values.
  // 2/ Compute histogram on pairwise values.
  val jhist = data
    .pair(Over(_1))(
      Upper,
      Concatenate(Locate.PrependPairwiseSelectedStringToRemainder(Over(_1), "%s,%s", false, "|"))
    )
    .histogram(Along(_1))(Locate.AppendContentString, false)

  // Compute count of histogram elements.
  val jcount = jhist
    .summarise(Over(_0))(Sums())
    .gather()

  // Compute joint entropy
  val joint = jhist
    .summariseWithValue(Over(_0))(
      jcount,
      Entropy(extractor, negate = true).andThenRelocate(_.position.append("joint").toOption)
    )

  // Generate mutual information
  // 1/ Sum marginal and negated joint entropy
  // 2/ Persist mutual information.
  (marginal ++ joint)
    .summarise(Over(_0))(Sums())
    .saveAsText(ctx, s"./demo.${output}/mi.out", Cell.toShortString(true, "|"))
    .toUnit
}

