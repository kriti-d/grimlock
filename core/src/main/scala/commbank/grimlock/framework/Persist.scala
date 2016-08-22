// Copyright 2015,2016 Commonwealth Bank of Australia
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

import org.apache.hadoop.io.Writable

/** Base trait for persisting data. */
trait Persist[T] extends DistributedData with Environment {
  /** The data to persist. */
  val data: U[T]

  /**
   *   Convenience function for suppressing ‘Discarded non-unit value’ compiler warnings.
   *
   *   These occur when the output of a function is not assigned to a variable (for a non-unit return).
   *   This function ensures that such warnings are suppressed, it does not affect the flow or outcome.
   */
  def toUnit(): Unit = ()

  /** Shorthand type for converting a `T` to string. */
  type TextWriter = (T) => TraversableOnce[String]

  /** Shorthand type for converting a `T` to key value tuple. */
  type SequenceWriter[K <: Writable, V <: Writable] = (T) => TraversableOnce[(K, V)]
}

