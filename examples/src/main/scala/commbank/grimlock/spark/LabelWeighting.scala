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
import commbank.grimlock.framework.content.metadata._
import commbank.grimlock.framework.encoding._
import commbank.grimlock.framework.position._
import commbank.grimlock.framework.transform._

import commbank.grimlock.library.aggregate._
import commbank.grimlock.library.transform._

import commbank.grimlock.spark.environment._
import commbank.grimlock.spark.environment.Context._
import commbank.grimlock.spark.Matrix._

import org.apache.spark.{ SparkConf, SparkContext }

import shapeless.nat.{ _1, _2, _3 }

// Simple transformer that adds weight to a label.
case class AddWeight() extends TransformerWithValue[_2, _3] {
  type V = Map[Position[_1], Content]

  // Adding the weight is a straight forward lookup by the value of the content. Also return this cell
  // (cell.position.append("label"), cell.content) so no additional join is needed with the original label data.
  def presentWithValue(cell: Cell[_2], ext: V): TraversableOnce[Cell[_3]] = List(
    Cell(cell.position.append("label"), cell.content),
    Cell(cell.position.append("weight"), ext(Position(cell.content.value.toShortString)))
  )
}

object LabelWeighting {
  def main(args: Array[String]) {
    // Define implicit context.
    implicit val ctx = Context(new SparkContext(args(0), "Grimlock Spark Demo", new SparkConf()))

    // Path to data files, output folder
    val path = if (args.length > 1) args(1) else "../../data"
    val output = "spark"

    // Read labels and melt the date into the instance id to generate a 1D matrix.
    val labels = loadText(
        ctx,
        s"${path}/exampleLabels.txt",
        Cell.parse2DWithSchema(Content.parser(DoubleCodec, ContinuousSchema[Double]()))
      )
      .data // Keep only the data (ignoring errors).
      .melt(_2, _1, Value.concatenate(":"))

    // Compute histogram over the label values.
    val histogram = labels
      .histogram(Along(_1))(Locate.AppendContentString(), false)

    // Compute the total number of labels and compact result into a Map.
    val sum = labels
      .size(_1)
      .compact(Over(_1))()

    // Define extract object to get data out of sum/min map.
    def extractor(key: String) = ExtractWithKey[_1, Content](key).andThenPresent(_.value.asDouble)

    // Compute the ratio of (total number of labels) / (count for each label).
    val ratio = histogram
      .transformWithValue(Fraction(extractor(Position.indexString[_1]), true), sum)

    // Find the minimum ratio, and compact the result into a Map.
    val min = ratio
      .summarise(Along(_1))(Min().andThenRelocate(_.position.append("min").toOption))
      .compact(Over(_1))()

    // Divide the ratio by the minimum ratio, and compact the result into a Map.
    val weights = ratio
      .transformWithValue(Fraction(extractor("min")), min)
      .compact(Over(_1))()

    // Re-read labels and add the computed weight.
    loadText(
        ctx,
        s"${path}/exampleLabels.txt",
        Cell.parse2DWithSchema(Content.parser(DoubleCodec, ContinuousSchema[Double]()))
      )
      .data // Keep only the data (ignoring errors).
      .transformWithValue(AddWeight(), weights)
      .saveAsText(ctx, s"./demo.${output}/weighted.out")
      .toUnit
  }
}

