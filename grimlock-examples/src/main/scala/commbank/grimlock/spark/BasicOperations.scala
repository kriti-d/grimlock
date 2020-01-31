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

package commbank.grimlock.spark.examples

import commbank.grimlock.framework.Cell
import commbank.grimlock.framework.encoding.StringCodec
import commbank.grimlock.framework.environment.implicits._
import commbank.grimlock.framework.position.{ Over, Position }

import commbank.grimlock.spark.Persist
import commbank.grimlock.spark.environment.Context
import commbank.grimlock.spark.environment.implicits._

import org.apache.spark.sql.SparkSession

import shapeless.{ HList, HNil }
import shapeless.nat.{ _0, _1 }

object BasicOperations {
  def main(args: Array[String]) {
    // Define implicit context.
    implicit val ctx = Context(SparkSession.builder().master(args(0)).appName("Grimlock Spark Demo").getOrCreate())

    import ctx.encoder

    // Path to data files, output folder
    val path = if (args.length > 1) args(1) else "../../data"
    val output = "spark"

    // Read the data (ignoring errors). This returns a 2D matrix (instance x feature).
    val (data, _) = ctx
      .read(
        s"${path}/exampleInput.txt",
        Persist.textLoader,
        Cell.shortStringParser(StringCodec :: StringCodec :: HNil, "|")
      )

    // Get the number of rows.
    data
      .measure(_0)
      .saveAsText(ctx, s"./demo.${output}/row_size.out", Cell.toShortString(true, "|"))
      .toUnit

    // Get all dimensions of the matrix.
    data
      .shape()
      .saveAsText(ctx, s"./demo.${output}/matrix_shape.out", Cell.toShortString(true, "|"))
      .toUnit

    // Get the column names.
    data
      .names(Over(_1))
      .saveAsText(ctx, s"./demo.${output}/column_names.out", Position.toShortString("|"))
      .toUnit

    // Get the type of variables of each column.
    data
      .types(Over(_1))(true)
      .saveAsText(ctx, s"./demo.${output}/column_types.txt", Cell.toShortString(true, "|"))
      .toUnit

    // Transpose the matrix.
    data
      .permute(_1, _0)
      .saveAsText(ctx, s"./demo.${output}/transposed.out", Cell.toShortString(true, "|"))
      .toUnit

    // Construct a simple query
    def simpleQuery[P <: HList](cell: Cell[P]) = (cell.content.value gtr 995) || (cell.content.value equ "F")

    // Find all co-ordinates that match the above simple query.
    val coords = data
      .which(simpleQuery)
      .saveAsText(ctx, s"./demo.${output}/query.txt", Position.toShortString("|"))

    // Get the data for the above coordinates.
    data
      .get(coords)
      .saveAsText(ctx, s"./demo.${output}/values.txt", Cell.toShortString(true, "|"))
      .toUnit

    // Keep columns A and B, and remove row 0221707
    data
      .select(Over(_1))(true, List("fid:A", "fid:B"))
      .select(Over(_0))(false, "iid:0221707")
      .saveAsText(ctx, s"./demo.${output}/sliced.txt", Cell.toShortString(true, "|"))
      .toUnit
  }
}

