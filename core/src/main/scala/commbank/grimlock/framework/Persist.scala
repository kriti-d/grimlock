// Copyright 2015,2016,2017 Commonwealth Bank of Australia
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

package commbank.grimlock.framework

import commbank.grimlock.framework.environment.tuner.Tuner

import org.apache.hadoop.io.Writable

/** Trait for persisting data. */
trait Persist[X, U[_]] extends java.io.Serializable {
  /**
   *   Convenience function for suppressing ‘Discarded non-unit value’ compiler warnings.
   *
   *   These occur when the output of a function is not assigned to a variable (for a non-unit return).
   *   This function ensures that such warnings are suppressed, it does not affect the flow or outcome.
   */
  def toUnit(): Unit = ()
}

/** Companion object to `Persist` with various types, implicits, etc. */
object Persist {
  /** Shorthand type for converting a `X` to key value tuple. */
  type SequenceWriter[X, K <: Writable, V <: Writable] = (X) => TraversableOnce[(K, V)]

  /** Shorthand type for converting a `X` to string. */
  type TextWriter[X] = (X) => TraversableOnce[String]

  /**
   * Type for converting pivoted `X`s to string.
   *
   * All data will first be pivoted according to a slice. All `X` belonging to each slice.selected will
   * be grouped into the list.
   */
  type TextWriterByPosition[X] = (List[Option[X]]) => TraversableOnce[String]

  /** Trait for tuners permitted on a call to `saveAsText`. */
  trait SaveAsTextTuner[U[_], T <: Tuner]
}

/** Trait for writing strings as text. */
trait SaveStringsAsText[U[_]] extends Persist[String, U] {
  /**
   * Persist to disk.
   *
   * @param file  Name of the output file.
   * @param tuner The tuner for the job.
   *
   * @return A `U[String]`; that is it returns `data`.
   */
  def saveAsText[T <: Tuner](file: String, tuner: T)(implicit ev: Persist.SaveAsTextTuner[U, T]): U[String]
}

