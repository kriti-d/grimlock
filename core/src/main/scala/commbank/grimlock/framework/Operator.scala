// Copyright 2014,2015,2016,2017 Commonwealth Bank of Australia
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

package commbank.grimlock.framework.pairwise

import commbank.grimlock.framework.{ Cell, Locate }
import commbank.grimlock.framework.content.Content
import commbank.grimlock.framework.position.Position

import shapeless.Nat
import shapeless.ops.nat.GTEq

/** Trait for comparing two positions to determine if pairwise operation is to be applied. */
trait Comparer {
  /**
   * Check, based on left and right positions, if pairwise operation should be computed.
   *
   * @param left  Left position.
   * @param right Right position.
   */
  def keep[P <: Nat](left: Position[P], right: Position[P]): Boolean
}

/** Case object for computing all pairwise combinations. */
case object All extends Comparer {
  def keep[P <: Nat](left: Position[P], right: Position[P]): Boolean = true
}

/** Case object for computing diagonal pairwise combinations (i.e. left == right). */
case object Diagonal extends Comparer {
  def keep[P <: Nat](left: Position[P], right: Position[P]): Boolean = left.compare(right) == 0
}

/** Case object for computing upper triangular pairwise combinations (i.e. right > left). */
case object Upper extends Comparer {
  def keep[P <: Nat](left: Position[P], right: Position[P]): Boolean = right.compare(left) > 0
}

/** Case object for computing upper triangular or diagonal pairwise combinations (i.e. right >= left). */
case object UpperDiagonal extends Comparer {
  def keep[P <: Nat](left: Position[P], right: Position[P]): Boolean = right.compare(left) >= 0
}

/** Case object for computing lower triangular pairwise combinations (i.e. left > right). */
case object Lower extends Comparer {
  def keep[P <: Nat](left: Position[P], right: Position[P]): Boolean = left.compare(right) > 0
}

/** Case object for computing lower triangular or diagonal pairwise combinations (i.e. left >= right). */
case object LowerDiagonal extends Comparer {
  def keep[P <: Nat](left: Position[P], right: Position[P]): Boolean = left.compare(right) >= 0
}

/** Trait for computing pairwise values. */
trait Operator[P <: Nat, Q <: Nat] extends OperatorWithValue[P, Q] { self =>
  type V = Any

  def computeWithValue(left: Cell[P], right: Cell[P], ext: V): TraversableOnce[Cell[Q]] = compute(left, right)

  /**
   * Indicate if the cell is selected as part of the sample.
   *
   * @param left  The left cell to compute with.
   * @param right The right cell to compute with.
   *
   * @note The return value is a `TraversableOnce` to allow, for example, upper or lower triangular matrices to
   *       be returned (this can be done by comparing the selected coordinates)
   */
  def compute(left: Cell[P], right: Cell[P]): TraversableOnce[Cell[Q]]

  /**
   * Operator for preparing content prior to pairwise operations.
   *
   * @param preparer The function to apply prior to pariwise operations.
   *
   * @return An operator that prepares the content and then runs `this`.
   */
  override def withPrepare(preparer: (Cell[P]) => Content) = new Operator[P, Q] {
    def compute(left: Cell[P], right: Cell[P]): TraversableOnce[Cell[Q]] = self
      .compute(left.mutate(preparer), right.mutate(preparer))
  }

  /**
   * Operator for pairwise operations and then updating the contents.
   *
   * @param mutator The mutation to apply after the pairwise operations.
   *
   * @return An operator that runs `this` and then updates the resulting contents.
   */
  override def andThenMutate(mutator: (Cell[Q]) => Content) = new Operator[P, Q] {
    def compute(left: Cell[P], right: Cell[P]): TraversableOnce[Cell[Q]] = self
      .compute(left, right).map(_.mutate(mutator))
  }

  /**
   * Operator for pairwise operations and then relocating the contents.
   *
   * @param locator The relocation to apply after the operation.
   *
   * @return An operator that runs `this` and then relocates the resulting content.
   */
  override def andThenRelocate[X <: Nat](locator: Locate.FromCell[Q, X])(implicit ev: GTEq[X, Q]) = new Operator[P, X] {
    def compute(left: Cell[P], right: Cell[P]): TraversableOnce[Cell[X]] = self
      .compute(left, right)
      .flatMap(c => locator(c).map(Cell(_, c.content)))
  }
}

/** Companion object for the `Operator` trait. */
object Operator {
  /** Converts a `(Cell[P], Cell[P]) => Cell[Q]` to a `Operator[P, Q]`. */
  implicit def funcToOperator[P <: Nat, Q <: Nat](func: (Cell[P], Cell[P]) => Cell[Q]) = new Operator[P, Q] {
    def compute(left: Cell[P], right: Cell[P]): TraversableOnce[Cell[Q]] = List(func(left, right))
  }

  /** Converts a `(Cell[P], Cell[P]) => List[Cell[Q]]` to a `Operator[P, Q]`. */
  implicit def funcListToOperator[P <: Nat, Q <: Nat](func: (Cell[P], Cell[P]) => List[Cell[Q]]) = new Operator[P, Q] {
    def compute(left: Cell[P], right: Cell[P]): TraversableOnce[Cell[Q]] = func(left, right)
  }

  /** Converts a `List[Operator[P, Q]]` to a single `Operator[P, Q]`. */
  implicit def listToOperator[P <: Nat, Q <: Nat](operators: List[Operator[P, Q]]) = new Operator[P, Q] {
    def compute(left: Cell[P], right: Cell[P]): TraversableOnce[Cell[Q]] = operators.flatMap(_.compute(left, right))
  }
}

/** Trait for computing pairwise values with a user provided value. */
trait OperatorWithValue[P <: Nat, Q <: Nat] extends java.io.Serializable { self =>
  /** Type of the external value. */
  type V

  /**
   * Indicate if the cell is selected as part of the sample.
   *
   * @param left  The left cell to compute with.
   * @param right The right cell to compute with.
   * @param ext   The user define the value.
   *
   * @note The return value is a `TraversableOnce` to allow, for example, upper or lower triangular matrices to
   *       be returned (this can be done by comparing the selected coordinates).
   */
  def computeWithValue(left: Cell[P], right: Cell[P], ext: V): TraversableOnce[Cell[Q]]

  /**
   * Operator for preparing content prior to pairwise operations.
   *
   * @param preparer The function to apply prior to pariwise operations.
   *
   * @return An operator that prepares the content and then runs `this`.
   */
  def withPrepare(preparer: (Cell[P]) => Content) = new OperatorWithValue[P, Q] {
    type V = self.V

    def computeWithValue(left: Cell[P], right: Cell[P], ext: V): TraversableOnce[Cell[Q]] = self
      .computeWithValue(left.mutate(preparer), right.mutate(preparer), ext)
  }

  /**
   * Operator for pairwise operations and then updating the contents.
   *
   * @param mutator The mutation to apply after the pairwise operations.
   *
   * @return An operator that runs `this` and then updates the resulting contents.
   */
  def andThenMutate(mutator: (Cell[Q]) => Content) = new OperatorWithValue[P, Q] {
    type V = self.V

    def computeWithValue(left: Cell[P], right: Cell[P], ext: V): TraversableOnce[Cell[Q]] = self
      .computeWithValue(left, right, ext)
      .map(_.mutate(mutator))
  }

  /**
   * Operator for pairwise operations and then relocating the contents.
   *
   * @param locator The relocation to apply after the operation.
   *
   * @return An operator that runs `this` and then relocates the resulting content.
   */
  def andThenRelocate[X <: Nat](locator: Locate.FromCell[Q, X])(implicit ev: GTEq[X, Q]) = new OperatorWithValue[P, X] {
    type V = self.V

    def computeWithValue(left: Cell[P], right: Cell[P], ext: V): TraversableOnce[Cell[X]] = self
      .computeWithValue(left, right, ext)
      .flatMap(c => locator(c).map(Cell(_, c.content)))
  }

  /**
   * Operator for preparing content prior to pairwise operations.
   *
   * @param preparer The function to apply prior to pariwise operations.
   *
   * @return An operator that prepares the content and then runs `this`.
   */
  def withPrepareWithValue(preparer: (Cell[P], V) => Content) = new OperatorWithValue[P, Q] {
    type V = self.V

    def computeWithValue(left: Cell[P], right: Cell[P], ext: V): TraversableOnce[Cell[Q]] = self
      .computeWithValue(Cell(left.position, preparer(left, ext)), Cell(right.position, preparer(right, ext)), ext)
  }

  /**
   * Operator for pairwise operations and then updating the contents.
   *
   * @param mutator The mutation to apply after the pairwise operations.
   *
   * @return An operator that runs `this` and then updates the resulting contents.
   */
  def andThenMutateWithValue(mutator: (Cell[Q], V) => Content) = new OperatorWithValue[P, Q] {
    type V = self.V

    def computeWithValue(left: Cell[P], right: Cell[P], ext: V): TraversableOnce[Cell[Q]] = self
      .computeWithValue(left, right, ext)
      .map(c => Cell(c.position, mutator(c, ext)))
  }

  /**
   * Operator for pairwise operations and then relocating the contents.
   *
   * @param locator The relocation to apply after the operation.
   *
   * @return An operator that runs `this` and then relocates the resulting content.
   */
  def andThenRelocateWithValue[
    X <: Nat
  ](
    locator: Locate.FromCellWithValue[Q, X, V]
  )(implicit
    ev: GTEq[X, Q]
  ) = new OperatorWithValue[P, X] {
    type V = self.V

    def computeWithValue(left: Cell[P], right: Cell[P], ext: V): TraversableOnce[Cell[X]] = self
      .computeWithValue(left, right, ext)
      .flatMap(c => locator(c, ext).map(Cell(_, c.content)))
  }
}

/** Companion object for the `OperatorWithValue` trait. */
object OperatorWithValue {
  /** Converts a `(Cell[P], Cell[P], W) => Cell[Q]` to an `OperatorWithValue[P, Q] { type V >: W }`. */
  implicit def funcToOperatorWithValue[
    P <: Nat,
    Q <: Nat,
    W
  ](
    func: (Cell[P], Cell[P], W) => Cell[Q]
  ) = new OperatorWithValue[P, Q] {
    type V = W

    def computeWithValue(left: Cell[P], right: Cell[P], ext: W): TraversableOnce[Cell[Q]] = List(func(left, right, ext))
  }

  /** Converts a `(Cell[P], Cell[P], W) => List[Cell[Q]]` to an `OperatorWithValue[P, Q] { type V >: W }`. */
  implicit def funcListToOperatorWithValue[
    P <: Nat,
    Q <: Nat,
    W
  ](
    func: (Cell[P], Cell[P], W) => List[Cell[Q]]
  ) = new OperatorWithValue[P, Q] {
    type V = W

    def computeWithValue(left: Cell[P], right: Cell[P], ext: W): TraversableOnce[Cell[Q]] = func(left, right, ext)
  }

  /**
   * Converts a `List[OperatorWithValue[P, Q] { type V >: W }]` to a single `OperatorWithValue[P, Q] { type V >: W }`.
   */
  implicit def listToOperatorWithValue[
    P <: Nat,
    Q <: Nat,
    W
  ](
    operators: List[OperatorWithValue[P, Q] { type V >: W }]
  ) = new OperatorWithValue[P, Q] {
    type V = W

    def computeWithValue(left: Cell[P], right: Cell[P], ext: V): TraversableOnce[Cell[Q]] = operators
      .flatMap(_.computeWithValue(left, right, ext))
  }
}

