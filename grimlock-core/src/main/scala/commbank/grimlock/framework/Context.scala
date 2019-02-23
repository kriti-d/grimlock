// Copyright 2016,2017,2018,2019 Commonwealth Bank of Australia
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

package commbank.grimlock.framework.environment

import commbank.grimlock.framework.{ Cell, ParquetConfig, Persist }

import org.apache.hadoop.io.Writable

import scala.reflect.ClassTag

import shapeless.HList

/** Trait for capturing all operating context related state. */
trait Context[C <: Context[C]] {
  /** Type for user defined data. */
  type E[X]

  /** Type for distributed data. */
  type U[X]

  /**
   * Read column oriented, pipe separated matrix text data into a `U[Cell[P]]`.
   *
   * @param file   The text file to read from.
   * @param parser The parser that converts a single line to a cell.
   */
  def loadText[P <: HList](file: String, parser: Persist.TextParser[Cell[P]]): (U[Cell[P]], U[Throwable])

  /**
   * Read binary key-value (sequence) matrix data into a `U[Cell[P]]`.
   *
   * @param file   The text file to read from.
   * @param parser The parser that converts a single key-value to a cell.
   */
  def loadSequence[
    K <: Writable : Manifest,
    V <: Writable : Manifest,
    P <: HList
  ](
    file: String,
    parser: Persist.SequenceParser[K, V, Cell[P]]
  ): (U[Cell[P]], U[Throwable])

  /**
   * Load Parquet data.
   *
   * @param file   File path.
   * @param parser Parser that convers single Parquet structure to cells.
   */
  def loadParquet[
    T,
    P <: HList
  ](
    file: String,
    parser: Persist.ParquetParser[T, Cell[P]]
  )(implicit
    cfg: ParquetConfig[T, C]
  ): (U[Cell[P]], U[Throwable])

  /** All implicits for this context. */
  val implicits: Implicits[C]

  /** All library data/functions for this context. */
  val library: Library[C]

  /** Create empty instance of `U`. */
  def empty[T : ClassTag]: U[T]

  /** Create an instance of `U` from `seq`. */
  def from[T : ClassTag](seq: Seq[T]): U[T]

  /**
   * Create an empty instance of `U`, writes it but discards its output.
   *
   * This is a work around for Scalding when Matrix.materialise is the only output of a job. In that case it is
   * possible for cascading to throw a PlannerException as there is no sink on disk. Calling this function
   * creates an empty `U` and writes it to a null file, causing a flow but discarding the output.
   *
   * See https://github.com/twitter/scalding/wiki/Common-Exceptions-and-possible-reasons
   */
  def nop(): Unit
}

