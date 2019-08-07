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

import commbank.grimlock.framework.{ ParquetConfig, Persist }

import org.apache.hadoop.io.Writable

import scala.reflect.ClassTag

/** Trait for capturing all operating context related state. */
trait Context[C <: Context[C]] {
  /** Type for user defined data. */
  type E[T]

  /** Type for distributed data. */
  type U[T]

  /**
   * Load text data.
   *
   * @param file   The text file to read from.
   * @param parser The parser that converts a single line to a cell.
   *
   * @return A `U[T]` with successfully parsed rows together with a `U[Throwable]` of all parse errors.
   */
  def loadText[T : ClassTag](file: String, parser: Persist.TextParser[T]): (U[T], U[Throwable])

  /**
   * Load sequence (binary key-value) data.
   *
   * @param file   The text file to read from.
   * @param parser The parser that converts a single key-value to a cell.
   *
   * @return A `U[T]` with successfully parsed rows together with a `U[Throwable]` of all parse errors.
   */
  def loadSequence[
    K <: Writable : Manifest,
    V <: Writable : Manifest,
    T : ClassTag
  ](
    file: String,
    parser: Persist.SequenceParser[K, V, T]
  ): (U[T], U[Throwable])

  /**
   * Load Parquet data.
   *
   * @param file   File path.
   * @param parser Parser that convers single Parquet structure to cells.
   *
   * @return A `U[T]` with successfully parsed rows together with a `U[Throwable]` of all parse errors.
   */
  def loadParquet[
    X,
    T : ClassTag
  ](
    file: String,
    parser: Persist.ParquetParser[X, T]
  )(implicit
    cfg: ParquetConfig[X, C]
  ): (U[T], U[Throwable])

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

