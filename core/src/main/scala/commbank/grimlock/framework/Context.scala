// Copyright 2016,2017 Commonwealth Bank of Australia
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

import commbank.grimlock.framework.Cell
import commbank.grimlock.framework.environment.implicits.Implicits

import com.twitter.scrooge.ThriftStruct

import org.apache.hadoop.io.Writable

import scala.reflect.ClassTag

import shapeless.Nat

/** Trait for capturing all operating context related state. */
trait Context[U[_], E[_]] {
  /**
   * Read column oriented, pipe separated matrix text data into a `U[Cell[P]]`.
   *
   * @param file   The text file to read from.
   * @param parser The parser that converts a single line to a cell.
   */
  def loadText[P <: Nat](file: String, parser: Cell.TextParser[P]): (U[Cell[P]], U[String])

  /**
   * Read binary key-value (sequence) matrix data into a `U[Cell[P]]`.
   *
   * @param file   The text file to read from.
   * @param parser The parser that converts a single key-value to a cell.
   */
  def loadSequence[
    K <: Writable : Manifest,
    V <: Writable : Manifest,
    P <: Nat
  ](
    file: String,
    parser: Cell.SequenceParser[K, V, P]
  ): (U[Cell[P]], U[String])

  /**
   * Load Parquet data.
   *
   * @param file   File path.
   * @param parser Parser that convers single Parquet structure to cells.
   */
  def loadParquet[
    T <: ThriftStruct : Manifest,
    P <: Nat
  ](
    file: String,
    parser: Cell.ParquetParser[T, P]
  ): (U[Cell[P]], U[String])

  /** All implicits for this context. */
  val implicits: Implicits[U, E]

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

