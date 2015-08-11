// Copyright 2014,2015 Commonwealth Bank of Australia
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

package au.com.cba.omnia.grimlock.scalding.examples

import au.com.cba.omnia.grimlock.framework._
import au.com.cba.omnia.grimlock.framework.position._

import au.com.cba.omnia.grimlock.scalding.Matrix._
import au.com.cba.omnia.grimlock.scalding.Nameable._
import au.com.cba.omnia.grimlock.scalding.Names._
import au.com.cba.omnia.grimlock.scalding.position.Positions._
import au.com.cba.omnia.grimlock.scalding.position.PositionDistributable._
import au.com.cba.omnia.grimlock.scalding.Types._

import com.twitter.scalding.{ Args, Job }

class BasicOperations(args: Args) extends Job(args) {

  // Path to data files, output folder
  val path = args.getOrElse("path", "../../data")
  val output = "scalding"

  // Read the data. This returns a 2D matrix (instance x feature).
  val data = load2D(s"${path}/exampleInput.txt")

  // Get the number of rows.
  data
    .size(First)
    .save(s"./demo.${output}/row_size.out")

  // Get all dimensions of the matrix.
  data
    .shape()
    .save(s"./demo.${output}/matrix_shape.out")

  // Get the column names.
  data
    .names(Over(Second))
    .save(s"./demo.${output}/column_names.out")

  // Get the type of variables of each column.
  data
    .types(Over(Second), true)
    .save(s"./demo.${output}/column_types.txt")

  // Transpose the matrix.
  data
    .permute(Second, First)
    .save(s"./demo.${output}/transposed.out")

  // Construct a simple query
  def simpleQuery(cell: Cell[Position2D]) = (cell.content.value gtr 995) || (cell.content.value equ "F")

  // Find all co-ordinates that match the above simple query.
  val coords = data
    .which(simpleQuery)
    .save(s"./demo.${output}/query.txt")

  // Get the data for the above coordinates.
  data
    .get(coords)
    .save(s"./demo.${output}/values.txt")

  // Keep columns A and B, and remove row 0221707
  data
    .slice(Over(Second), List("fid:A", "fid:B"), true)
    .slice(Over(First), "iid:0221707", false)
    .save(s"./demo.${output}/sliced.txt")
}

