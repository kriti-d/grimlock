// Copyright 2016,2017,2018,2019,2020 Commonwealth Bank of Australia
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

import commbank.grimlock.framework.Persist

/** Trait for capturing all operating context related state. */
trait Context[C <: Context[C]] {
  /** Type class needed to encode data as a distributed list. */
  type D[T] <: Encoder[T]

  /** Type for user defined data. */
  type E[T]

  /** Type for distributed data. */
  type U[T]

  /**
   * Read data.
   *
   * @param location The location to read from.
   * @param parser   The parser that converts the type that can be read to the result type.
   *
   * @return A `U[T]` with successfully parsed rows together with a `U[Throwable]` of all parse errors.
   */
  def read[
    X,
    T
  ](
    location: String,
    loader: Persist.Loader[X, C],
    parser: Persist.Parser[X, T]
  )(implicit
    enc: C#D[T]
  ): (U[T], U[Throwable])

  /** Create empty instance of `U`. */
  def empty[T : C#D]: U[T]

  /** Create an instance of `U` from `seq`. */
  def from[T : C#D](seq: Seq[T]): U[T]

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

/** Trait for capturing type class constraints so that a type `T` can be stored in a distributed list. */
trait Encoder[T] extends java.io.Serializable

/** Trait for capturing an operating context which supports Matrix operations. */
trait MatrixContext[C <: MatrixContext[C]] extends Context[C] {
  /** All implicits for this context. */
  val implicits: Implicits[C]

  /** All library data/functions for this context. */
  val library: Library[C]
}

