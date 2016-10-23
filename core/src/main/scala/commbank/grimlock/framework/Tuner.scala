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

import commbank.grimlock.framework.utility.UnionTypes._

/** Base trait for tuner parameters. */
trait TunerParameters extends java.io.Serializable { }

/** Indicates that the data should be collated. */
case class Collate() extends TunerParameters { }

/** Indicates that no special operations are to be performed. */
case class NoParameters() extends TunerParameters { }

/**
 * Tune the number of reducers.
 *
 * @param reducers The number of reducers to use.
 */
case class Reducers(reducers: Int) extends TunerParameters { }

/**
 * Redistribute the data across a number of partitions.
 *
 * @param partitions The number of partitions to redistribute among.
 */
case class Redistribute(partitions: Int) extends TunerParameters { }

/**
 * Create a sequence of two tuner parameters
 *
 * @param first  The first parameters of the sequence.
 * @param second The second parameters of the sequence.
 */
case class Sequence[F <: TunerParameters, S <: TunerParameters](first: F, second: S) extends TunerParameters { }

/** Base trait that indicates size/shape of the data for tuning. */
sealed trait Tuner extends java.io.Serializable {
  /** The parameters used for tuning. */
  val parameters: TunerParameters
}

/** Indicates that some of the data can fit in memory (permits map-side only operations). */
case class InMemory[T <: TunerParameters](parameters: T = NoParameters()) extends Tuner { }

/** Indicates that the data is (reasonably) evenly distributed. */
case class Default[T <: TunerParameters](parameters: T = NoParameters()) extends Tuner { }

/** Companion object to `Default`. */
object Default {
  /**
   * Create a tuner with a sequence of two tuner parameters.
   *
   * @param first  The first parameters of the sequence.
   * @param second The second parameters of the sequence.
   */
  def apply[
    F <: TunerParameters,
    S <: TunerParameters
  ](
    first: F,
    second: S
  ): Default[Sequence[F, S]] = Default(Sequence(first, second))
}

/** Indicates that the data is (heavily) skewed. */
case class Unbalanced[T <: TunerParameters](parameters: T) extends Tuner { }

/** Companion object to `Unbalanced`. */
object Unbalanced {
  /**
   * Create an unbalanced tuner with a sequence of two tuner parameters.
   *
   * @param first  The first parameters of the sequence.
   * @param second The second parameters of the sequence.
   */
  def apply[
    F <: TunerParameters,
    S <: TunerParameters
  ](
    first: F,
    second: S
  ): Unbalanced[Sequence[F, S]] = Unbalanced(Sequence(first, second))
}

/** Some common sets of default permitted tuners. */
private[grimlock] object DefaultTuners {

  type TP1[T] = T Is Default[NoParameters]

  type TP2[T] = T In OneOf[Default[NoParameters]]#Or[Default[Reducers]]

  type TP3[T] = T In OneOf[Default[NoParameters]]#
    Or[Default[Reducers]]#
    Or[Default[Sequence[Reducers, Reducers]]]

  type TP4[T] = T In OneOf[InMemory[NoParameters]]#
    Or[Default[NoParameters]]#
    Or[Default[Reducers]]#
    Or[Unbalanced[Reducers]]
}

