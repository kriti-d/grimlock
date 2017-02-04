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

import commbank.grimlock.framework.environment._
import commbank.grimlock.framework.utility.UnionTypes.{ In, OneOf }

import org.apache.hadoop.io.Writable

/** Base trait for persisting data. */
trait Persist[X] extends DistributedData with Environment with java.io.Serializable {
  /** The data to persist. */
  val data: U[X]

  /**
   *   Convenience function for suppressing ‘Discarded non-unit value’ compiler warnings.
   *
   *   These occur when the output of a function is not assigned to a variable (for a non-unit return).
   *   This function ensures that such warnings are suppressed, it does not affect the flow or outcome.
   */
  def toUnit(): Unit = ()

  /** Shorthand type for converting a `X` to string. */
  type TextWriter = (X) => TraversableOnce[String]

  /**
   * Type for converting pivoted `X`s to string.
   *
   * All data will first be pivoted according to a slice. All `X` belonging to each slice.selected will
   * be grouped into the list.
   */
  type TextWriterByPosition = (List[Option[X]]) => TraversableOnce[String]

  /** Shorthand type for converting a `X` to key value tuple. */
  type SequenceWriter[K <: Writable, V <: Writable] = (X) => TraversableOnce[(K, V)]

  protected type PersistParition[T] = T In OneOf[Default[NoParameters]]#Or[Redistribute]
}

