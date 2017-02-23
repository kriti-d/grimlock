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

package commbank.grimlock.framework.environment.tuner

import scala.reflect.ClassTag

/** Trait for tuner parameters. */
trait Parameters extends java.io.Serializable { }

/** Indicates that no special operations are to be performed. */
case class NoParameters() extends Parameters { }

/**
 * Tune the number of reducers.
 *
 * @param reducers The number of reducers to use.
 */
case class Reducers(reducers: Int) extends Parameters { }

/**
 * Create a pair of tuner parameters
 *
 * @param first  The first parameter of the pair.
 * @param second The second parameter of the pair.
 */
case class Pair[F <: Parameters, S <: Parameters](first: F, second: S) extends Parameters { }

/** Trait that indicates size/shape of the data for tuning. */
sealed trait Tuner extends java.io.Serializable {
  /** The parameters used for tuning. */
  val parameters: Parameters
}

/** Indicates that some of the data can fit in memory (permits map-side only operations). */
case class InMemory[T <: Parameters](parameters: T = NoParameters()) extends Tuner { }

/** Companion object to `InMemory`. */
object InMemory {
  /**
   * Create an in-memory tuner with a number of reducers.
   *
   * @param reducers The number of reducers.
   */
  def apply(reducers: Int): InMemory[Reducers] = InMemory(Reducers(reducers))
}

/** Indicates that the data is (reasonably) evenly distributed. */
case class Default[T <: Parameters](parameters: T = NoParameters()) extends Tuner { }

/** Companion object to `Default`. */
object Default {
  /**
   * Create a tuner with a pair of tuner parameters.
   *
   * @param first  The first parameter of the pair.
   * @param second The second parameter of the pair.
   */
  def apply[
    F <: Parameters,
    S <: Parameters
  ](
    first: F,
    second: S
  ): Default[Pair[F, S]] = Default(Pair(first, second))

  /**
   * Create a tuner with a number of reducers.
   *
   * @param reducers The number of reducers.
   */
  def apply(reducers: Int): Default[Reducers] = Default(Reducers(reducers))
}

/** Indicates that the data is (heavily) skewed. */
case class Unbalanced[T <: Parameters](parameters: T) extends Tuner { }

/** Companion object to `Unbalanced`. */
object Unbalanced {
  /**
   * Create an unbalanced tuner with a pair of tuner parameters.
   *
   * @param first  The first parameter of the pair.
   * @param second The second parameter of the pair.
   */
  def apply[
    F <: Parameters,
    S <: Parameters
  ](
    first: F,
    second: S
  ): Unbalanced[Pair[F, S]] = Unbalanced(Pair(first, second))

  /**
   * Create an unbalanced tuner with a number of reducers.
   *
   * @param reducers The number of reducers.
   */
  def apply(reducers: Int): Unbalanced[Reducers] = Unbalanced(Reducers(reducers))
}

/**
 * Redistribute the data across a number of partitions.
 *
 * @param partitions The number of partitions to redistribute among.
 */
case class Redistribute(partitions: Int) extends Tuner {
  val parameters = Reducers(partitions)
}

/**
 * A binary tuner.
 *
 * @param first  First tuner.
 * @param second Second tuner.
 */
case class Binary[F <: Tuner, S <: Tuner](first: F, second: S) extends Tuner {
  val parameters = NoParameters()
}

/**
 * A ternary tuner.
 *
 * @param first  First tuner.
 * @param second Second tuner.
 * @param third  Third tuner.
 */
case class Ternary[F <: Tuner, S <: Tuner, T <: Tuner](first: F, second: S, third: T) extends Tuner {
  val parameters = NoParameters()
}

private[grimlock] trait MapSideJoin[K, V, W, U[_], E[_]] extends java.io.Serializable {
  type T

  val empty: T

  def compact(smaller: U[(K, W)])(implicit ev1: ClassTag[K], ev2: ClassTag[W], ev3: Ordering[K]): E[T]
  def join(k: K, v: V, t: T): Option[W]
}

private[grimlock] trait MapMapSideJoin[K, V, W, U[_], E[_]] extends MapSideJoin[K, V, W, U, E] {
  type T = Map[K, W]

  val empty = Map[K, W]()

  def join(k: K, v: V, t: T): Option[W] = t.get(k)
}

private[grimlock] trait SetMapSideJoin[K, V, U[_], E[_]] extends MapSideJoin[K, V, Unit, U, E] {
  type T = Set[K]

  val empty = Set[K]()

  def join(k: K, v: V, t: T): Option[Unit] = t.find { case x => x == k }.map { case x => () }
}

