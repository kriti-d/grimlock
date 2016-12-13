// Copyright 2014,2015,2016 Commonwealth Bank of Australia
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

package commbank.grimlock.framework.aggregate

import commbank.grimlock.framework._
import commbank.grimlock.framework.content._
import commbank.grimlock.framework.position._

import scala.reflect.{ classTag, ClassTag }

import shapeless.Nat
import shapeless.ops.nat.{ GT, GTEq }

/** Base trait that encapsulates the result of an aggregation. */
trait Result[A, T[X] <: Result[X, T]] {
  /** Map over result. */
  def map[B](f: (A) => B): T[B]

  /** FlatMap over result. */
  def flatMap[B](f: (A) => Option[B]): T[B]

  /** Return result as a `TraversableOnce`. */
  def toTraversableOnce: TraversableOnce[A]
}

/** Companion object to `Result`. */
object Result {
  /**
   * Implicit conversion from `Result` to `TraversableOnce`.
   *
   * @param result The `Result` to convert.
   *
   * @return A `TraversableOnce` for this result.
   */
  implicit def resultToTraversableOnce[A, T[X] <: Result[X, T]](result: T[A]): TraversableOnce[A] = result
    .toTraversableOnce
}

/**
 * Aggregation result that consists of at most 1 value.
 *
 * @param result The result of the aggregation.
 */
case class Single[A](result: Option[A]) extends Result[A, Single] {
  def map[B](f: (A) => B): Single[B] = Single(result.map(f(_)))
  def flatMap[B](f: (A) => Option[B]): Single[B] = Single(result.flatMap(f(_)))
  def toTraversableOnce: TraversableOnce[A] = result
}

/** Companion object to `Single`. */
object Single {
  /** Create an empty result. */
  def apply[A](): Single[A] = Single[A](None)

  /**
   * Aggregation result that consists of 1 value.
   *
   * @param result The result of the aggregation.
   */
  def apply[A](result: A): Single[A] = Single[A](Option(result))
}

/**
 * Aggregation result that consists of arbitrary many values.
 *
 * @param result The results of the aggregation.
 */
case class Multiple[A](result: TraversableOnce[A]) extends Result[A, Multiple] {
  def map[B](f: (A) => B): Multiple[B] = Multiple(result.map(f(_)))
  def flatMap[B](f: (A) => Option[B]): Multiple[B] = Multiple(result.flatMap(f(_)))
  def toTraversableOnce: TraversableOnce[A] = result
}

/** Companion object to `Multiple`. */
object Multiple {
  /** Create an empty result. */
  def apply[A](): Multiple[A] = Multiple[A](List())
}

private object Validate {
  def check[A <: AggregatorWithValue[_, _, _]](aggregators: Seq[A]): A =
    // TODO: Is there any way to check this at compile time (and not throw a runtime exception)?
    if (aggregators.size == 1 && aggregators.head.oTag == classTag[Single[_]])
      aggregators.head
    else
      throw new Exception("Only a single aggregator, returning a single value can be used when slice.S =:= Q")
}

/** Base trait for aggregations. */
trait Aggregator[P <: Nat, S <: Nat, Q <: Nat] extends AggregatorWithValue[P, S, Q] { self =>
  type V = Any

  def prepareWithValue(cell: Cell[P], ext: V): Option[T] = prepare(cell)
  def presentWithValue(pos: Position[S], t: T, ext: V): O[Cell[Q]] = present(pos, t)

  /**
   * Prepare for reduction.
   *
   * @param cell Cell which is to be aggregated. Note that its position is prior to `slice.selected` being applied.
   *
   * @return Optional state to reduce (allow for filtering).
   */
  def prepare(cell: Cell[P]): Option[T]

  /**
   * Present the reduced content.
   *
   * @param pos The reduced position. That is, the position returned by `slice.selected`.
   * @param t   The reduced state.
   *
   * @return `Result` cell where the position is derived from `pos` and the content is derived from `t`.
   */
  def present(pos: Position[S], t: T): O[Cell[Q]]

  /**
   * Operator for preparing content prior to aggregating.
   *
   * @param preparer The function to apply prior to aggregation.
   *
   * @return An aggregator that prepares the content and then runs `this`.
   */
  override def withPrepare(preparer: (Cell[P]) => Content) = new Aggregator[P, S, Q] {
    type T = self.T
    type O[A] = self.O[A]

    val tTag = self.tTag
    val oTag = self.oTag

    def prepare(cell: Cell[P]): Option[T] = self.prepare(cell.mutate(preparer))
    def reduce(lt: T, rt: T): T = self.reduce(lt, rt)
    def present(pos: Position[S], t: T): O[Cell[Q]] = self.present(pos, t)
  }

  /**
   * Operator for aggregating and then updating the contents.
   *
   * @param mutator The mutation to apply after the aggregation.
   *
   * @return An aggregator that runs `this` and then updates the resulting contents.
   */
  override def andThenMutate(mutator: (Cell[Q]) => Content) = new Aggregator[P, S, Q] {
    type T = self.T
    type O[A] = self.O[A]

    val tTag = self.tTag
    val oTag = self.oTag

    def prepare(cell: Cell[P]): Option[T] = self.prepare(cell)
    def reduce(lt: T, rt: T): T = self.reduce(lt, rt)
    def present(pos: Position[S], t: T): O[Cell[Q]] = self.present(pos, t).map(_.mutate(mutator))
  }

  /**
   * Operator for aggregating and then relocating the contents.
   *
   * @param relocator The relocation to apply after the aggregation.
   *
   * @return An aggregator that runs `this` and then relocates the resulting contents.
   */
  override def andThenRelocate[
    X <: Nat
  ](
    relocator: Locate.FromCell[Q, X]
  )(implicit
    ev: GTEq[X, Q]
  ) = new Aggregator[P, S, X] {
    type T = self.T
    type O[A] = self.O[A]

    val tTag = self.tTag
    val oTag = self.oTag

    def prepare(cell: Cell[P]): Option[T] = self.prepare(cell)
    def reduce(lt: T, rt: T): T = self.reduce(lt, rt)
    def present(pos: Position[S], t: T): O[Cell[X]] = self
      .present(pos, t)
      .flatMap(c => relocator(c).map(Cell(_, c.content)))
  }
}

/** Companion object to the `Aggregator` trait. */
object Aggregator {
  /** Type to validate an aggregator. */
  type Validate[P <: Nat, S <: Nat, Q <: Nat] = Valid[P, S, Q]

  /** Trait for valid aggregators. */
  trait Valid[P <: Nat, S <: Nat, Q <: Nat] {
    def check(aggregators: Seq[Aggregator[P, S, Q]]): Aggregator[P, S, Q]
  }

  /** Implicit constraint that ensures that aggregators for which Q > S, can return any value. */
  implicit def qGreaterThanS[
    P <: Nat,
    S <: Nat,
    Q <: Nat
  ](implicit
    ev: GT[Q, S]
  ): Valid[P, S, Q] = new Valid[P, S, Q] {
    def check(aggregators: Seq[Aggregator[P, S, Q]]): Aggregator[P, S, Q] = aggregators.toList
  }

  /** Implicit constraint that ensures that aggregators for which Q == S, can return only a Single value. */
  implicit def qEqualSWithSingle[
    P <: Nat,
    S <: Nat,
    Q <: Nat
  ](implicit
    ev: Q =:= S
  ): Valid[P, S, Q] = new Valid[P, S, Q] {
    def check(aggregators: Seq[Aggregator[P, S, Q]]): Aggregator[P, S, Q] = Validate.check(aggregators)
  }

  /** Implicit conversion from `List[Aggregator[P, S, Q]]` to a single `Aggregator[P, S, Q]`. */
  implicit def listToAggregator[
    P <: Nat,
    S <: Nat,
    Q <: Nat
  ](
    aggregators: List[Aggregator[P, S, Q]]
  )(implicit
    ev: GT[Q, S]
  ): Aggregator[P, S, Q] = new Aggregator[P, S, Q] {
    type T = List[(Int, Any)]
    type O[A] = Multiple[A]

    val tTag = classTag[T]
    val oTag = classTag[O[_]]

    def prepare(cell: Cell[P]): Option[T] = {
      val s = aggregators.zipWithIndex.map { case (a, i) => a.prepare(cell).map(t => (i, t)) }.flatten

      if (s.isEmpty) None else Option(s)
    }

    def reduce(lt: T, rt: T): T = (lt ++ rt)
      .groupBy(_._1)
      .map {
        case (i, List((_, t))) => (i, t)
        case (i, List((_, l), (_, r))) => {
          val a = aggregators(i)

          (i, a.reduce(l.asInstanceOf[a.T], r.asInstanceOf[a.T]))
        }
      }
      .toList

    def present(pos: Position[S], t: T): O[Cell[Q]] = Multiple(t
      .flatMap { case (i, s) =>
        val a = aggregators(i)

        a.present(pos, s.asInstanceOf[a.T])
      }
    )
  }
}

/** Base trait for aggregations with a user supplied value. */
trait AggregatorWithValue[P <: Nat, S <: Nat, Q <: Nat] extends java.io.Serializable { self =>
  /** Type of the state being aggregated. */
  type T

  /** Type of the external value. */
  type V

  /** Return type of presented data. */
  type O[A] <: Result[A, O]

  /** ClassTag of type of the state being aggregated. */
  val tTag: ClassTag[T]

  /** ClassTag of type of the return type. */
  val oTag: ClassTag[O[_]]

  /**
   * Prepare for reduction.
   *
   * @param cell Cell which is to be aggregated. Note that its position is prior to `slice.selected` being applied.
   * @param ext  User provided data required for preparation.
   *
   * @return Optional state to reduce (allow for filtering).
   */
  def prepareWithValue(cell: Cell[P], ext: V): Option[T]

  /**
   * Standard reduce method.
   *
   * @param lt Left state to reduce.
   * @param rt Right state to reduce.
   *
   * @return Reduced state
   */
  def reduce(lt: T, rt: T): T

  /**
   * Present the reduced content.
   *
   * @param pos The reduced position. That is, the position returned by `Slice.selected`.
   * @param t   The reduced state.
   * @param ext User provided data required for presentation.
   *
   * @return `Result` cell where the position is derived from `pos` and the content is derived from `t`.
   */
  def presentWithValue(pos: Position[S], t: T, ext: V): O[Cell[Q]]

  /**
   * Operator for preparing content prior to aggregating.
   *
   * @param preparer The function to apply prior to aggregation.
   *
   * @return An aggregator that prepares the content and then runs `this`.
   */
  def withPrepare(preparer: (Cell[P]) => Content) = new AggregatorWithValue[P, S, Q] {
    type T = self.T
    type V = self.V
    type O[A] = self.O[A]

    val tTag = self.tTag
    val oTag = self.oTag

    def prepareWithValue(cell: Cell[P], ext: V): Option[T] = self.prepareWithValue(cell.mutate(preparer), ext)
    def reduce(lt: T, rt: T): T = self.reduce(lt, rt)
    def presentWithValue(pos: Position[S], t: T, ext: V): O[Cell[Q]] = self.presentWithValue(pos, t, ext)
  }

  /**
   * Operator for aggregating and then updating the contents.
   *
   * @param mutator The mutation to apply after the aggregation.
   *
   * @return An aggregator that runs `this` and then updates the resulting contents.
   */
  def andThenMutate(mutator: (Cell[Q]) => Content) = new AggregatorWithValue[P, S, Q] {
    type T = self.T
    type V = self.V
    type O[A] = self.O[A]

    val tTag = self.tTag
    val oTag = self.oTag

    def prepareWithValue(cell: Cell[P], ext: V): Option[T] = self.prepareWithValue(cell, ext)
    def reduce(lt: T, rt: T): T = self.reduce(lt, rt)
    def presentWithValue(pos: Position[S], t: T, ext: V): O[Cell[Q]] = self
      .presentWithValue(pos, t, ext)
      .map(_.mutate(mutator))
  }

  /**
   * Operator for aggregating and then relocating the contents.
   *
   * @param relocator The relocation to apply after the aggregation.
   *
   * @return An aggregator that runs `this` and then relocates the resulting contents.
   */
  def andThenRelocate[
    X <: Nat
  ](
    relocator: Locate.FromCell[Q, X]
  )(implicit
    ev: GTEq[X, Q]
  ) = new AggregatorWithValue[P, S, X] {
    type T = self.T
    type V = self.V
    type O[A] = self.O[A]

    val tTag = self.tTag
    val oTag = self.oTag

    def prepareWithValue(cell: Cell[P], ext: V): Option[T] = self.prepareWithValue(cell, ext)
    def reduce(lt: T, rt: T): T = self.reduce(lt, rt)
    def presentWithValue(pos: Position[S], t: T, ext: V): O[Cell[X]] = self
      .presentWithValue(pos, t, ext)
      .flatMap(c => relocator(c).map(Cell(_, c.content)))
  }

  /**
   * Operator for preparing content prior to aggregating.
   *
   * @param preparer The function to apply prior to aggregation.
   *
   * @return An aggregator that prepares the content and then runs `this`.
   */
  def withPrepareWithValue(preparer: (Cell[P], V) => Content) = new AggregatorWithValue[P, S, Q] {
    type T = self.T
    type V = self.V
    type O[A] = self.O[A]

    val tTag = self.tTag
    val oTag = self.oTag

    def prepareWithValue(cell: Cell[P], ext: V): Option[T] = self
      .prepareWithValue(Cell(cell.position, preparer(cell, ext)), ext)
    def reduce(lt: T, rt: T): T = self.reduce(lt, rt)
    def presentWithValue(pos: Position[S], t: T, ext: V): O[Cell[Q]] = self.presentWithValue(pos, t, ext)
  }

  /**
   * Operator for aggregating and then updating the contents.
   *
   * @param mutator The mutation to apply after the aggregation.
   *
   * @return An aggregator that runs `this` and then updates the resulting contents.
   */
  def andThenMutateWithValue(mutator: (Cell[Q], V) => Content) = new AggregatorWithValue[P, S, Q] {
    type T = self.T
    type V = self.V
    type O[A] = self.O[A]

    val tTag = self.tTag
    val oTag = self.oTag

    def prepareWithValue(cell: Cell[P], ext: V): Option[T] = self.prepareWithValue(cell, ext)
    def reduce(lt: T, rt: T): T = self.reduce(lt, rt)
    def presentWithValue(pos: Position[S], t: T, ext: V): O[Cell[Q]] = self
      .presentWithValue(pos, t, ext)
      .map(c => Cell(c.position, mutator(c, ext)))
  }

  /**
   * Operator for aggregating and then relocating the contents.
   *
   * @param relocator The relocation to apply after the aggregation.
   *
   * @return An aggregator that runs `this` and then relocates the resulting contents.
   */
  def andThenRelocateWithValue[
    X <: Nat
  ](
    relocator: Locate.FromCellWithValue[Q, X, V]
  )(implicit
    ev: GTEq[X, Q]
  ) = new AggregatorWithValue[P, S, X] {
    type T = self.T
    type V = self.V
    type O[A] = self.O[A]

    val tTag = self.tTag
    val oTag = self.oTag

    def prepareWithValue(cell: Cell[P], ext: V): Option[T] = self.prepareWithValue(cell, ext)
    def reduce(lt: T, rt: T): T = self.reduce(lt, rt)
    def presentWithValue(pos: Position[S], t: T, ext: V): O[Cell[X]] = self
      .presentWithValue(pos, t, ext)
      .flatMap(c => relocator(c, ext).map(Cell(_, c.content)))
  }
}

/** Companion object to the `AggregatorWithValue` trait. */
object AggregatorWithValue {
  /** Type to validate an aggregator. */
  type Validate[P <: Nat, S <: Nat, Q <: Nat, W] = Valid[P, S, Q, W]

  /** Trait for valid aggregators. */
  trait Valid[P <: Nat, S <: Nat, Q <: Nat, W] {
    def check(
      aggregators: Seq[AggregatorWithValue[P, S, Q] { type V >: W }]
    ): AggregatorWithValue[P, S, Q] { type V >: W }
  }

  /** Implicit constraint that ensures that aggregators for which Q > S, can return any value. */
  implicit def qGreaterThanSWithValue[
    P <: Nat,
    S <: Nat,
    Q <: Nat,
    W
  ](implicit
    ev: GT[Q, S]
  ): Valid[P, S, Q, W] = new Valid[P, S, Q, W] {
    def check(
      aggregators: Seq[AggregatorWithValue[P, S, Q] { type V >: W }]
    ): AggregatorWithValue[P, S, Q] { type V >: W } = aggregators.toList
  }

  /** Implicit constraint that ensures that aggregators for which Q == S, can return only a Single value. */
  implicit def qEqualSWithSingleWithValue[
    P <: Nat,
    S <: Nat,
    Q <: Nat,
    W
  ](implicit
    ev: Q =:= S
  ): Valid[P, S, Q, W] = new Valid[P, S, Q, W] {
    def check(
      aggregators: Seq[AggregatorWithValue[P, S, Q] { type V >: W }]
    ): AggregatorWithValue[P, S, Q] { type V >: W } = Validate.check(aggregators)
  }

  /**
   * Implicit conversion from `List[AggregatorWithValue[P, S, Q] { type V >: W }]` to a single
   * `AggregatorWithValue[P, S, Q] { type V >: W }`
   */
  implicit def listToAggregatorWithValue[
    P <: Nat,
    S <: Nat,
    Q <: Nat,
    W
  ](
    aggregators: List[AggregatorWithValue[P, S, Q] { type V >: W }]
  )(implicit
    ev: GT[Q, S]
  ): AggregatorWithValue[P, S, Q] { type V >: W } = new AggregatorWithValue[P, S, Q] {
    type T = List[(Int, Any)]
    type V = W
    type O[A] = Multiple[A]

    val tTag = classTag[T]
    val oTag = classTag[O[_]]

    def prepareWithValue(cell: Cell[P], ext: V): Option[T] = {
      val s = aggregators.zipWithIndex.map { case (a, i) => a.prepareWithValue(cell, ext).map(t => (i, t)) }.flatten

      if (s.isEmpty) None else Option(s)
    }

    def reduce(lt: T, rt: T): T = (lt ++ rt)
      .groupBy(_._1)
      .map {
        case (i, List((_, t))) => (i, t)
        case (i, List((_, l), (_, r))) => {
          val a = aggregators(i)

          (i, a.reduce(l.asInstanceOf[a.T], r.asInstanceOf[a.T]))
        }
      }
      .toList

    def presentWithValue(pos: Position[S], t: T, ext: V): O[Cell[Q]] = Multiple(t
      .flatMap { case (i, s) =>
        val a = aggregators(i)

        a.presentWithValue(pos, s.asInstanceOf[a.T], ext)
      }
    )
  }
}

