// Copyright 2014,2015,2016,2017,2018,2019 Commonwealth Bank of Australia
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

package commbank.grimlock.framework.content2

import commbank.grimlock.framework.encoding2.Value
import commbank.grimlock.framework.metadata.{ ContinuousType, DiscreteType, Type }

/** Contents of a cell in a matrix. */
trait Content {
  /** Type of the value. */
  val classification: Type

  /** The value of the variable. */
  val value: Value[_]

  override def toString: String = "Content(" + classification.toString + "," + value.toString + ")"
}

/** Companion object to `Content` trait. */
object Content {
  /** Type for decoding a string to `Content`. */
  type Decoder = (String) => Option[Content]

  /**
   * Constructor for a `Content`.
   *
   * @param classification The variable type of the value.
   * @param value          The value of the content.
   */
  def apply[
    T <: Type,
    X
  ](
    classification: T,
    value: Value[X]
  )(implicit
    ev: VariableConstraints[T, X]
  ): Content = ContentImpl(classification, value)
}

/** Trait for capturing matching variable `Type`s and storage types. */
trait VariableConstraints[T <: Type, X]

/** Companion object to `VariableConstraints` with type cass instances. */
object VariableConstraints {
  implicit def fractionalIsContinuous[
    T <: ContinuousType.type,
    X
  ](implicit
    ev: Fractional[X]
  ): VariableConstraints[T, X] = new VariableConstraints[T, X] { }

  implicit def integralIsDiscrete[
    T <: DiscreteType.type,
    X
  ](implicit
    ev: Integral[X]
  ): VariableConstraints[T, X] = new VariableConstraints[T, X] { }
}

private case class ContentImpl(classification: Type, value: Value[_]) extends Content

/**
* Schema[Int] => Discrete, Ordinal (?)
* Schema[Double] => Continuous
* Schema[String] => Nominal, Ordinal (?)
*/

