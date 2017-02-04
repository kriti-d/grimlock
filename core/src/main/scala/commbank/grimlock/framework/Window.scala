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

package commbank.grimlock.framework.window

import commbank.grimlock.framework._
import commbank.grimlock.framework.content._
import commbank.grimlock.framework.position._

import shapeless.Nat
import shapeless.ops.nat.GTEq

/**
 * Base trait for generating windowed data.
 *
 * Windowed data is derived from two or more values, for example deltas or gradients. To generate this, the process is
 * as follows. First each cell is prepared for windowed operations. This involves return the input data to slide over.
 * Next the input data is grouped according to a `slice`. The data in each group is then sorted by the remaining
 * coordinates. The first cell's data of each group is passed to the initialise method. This allows a window to
 * initialise it's running state. All subsequent cells' data are passed to the update method (together with the running
 * state). The update method can update the running state, and optionally return one or more output values. Finally,
 * the present method returns cells for the output values. Note that the running state can be used to create derived
 * features of different window sizes.
 */
trait Window[P <: Nat, S <: Nat, R <: Nat, Q <: Nat] extends WindowWithValue[P, S, R, Q] { self =>
  type V = Any

  def prepareWithValue(cell: Cell[P], ext: V): I = prepare(cell)
  def presentWithValue(pos: Position[S], out: O, ext: V): TraversableOnce[Cell[Q]] = present(pos, out)

  /**
   * Prepare a sliding window operations.
   *
   * @param cell The cell from which to generate the input data.
   *
   * @return The input data over which the sliding window operates.
   */
  def prepare(cell: Cell[P]): I

  /**
   * Present zero or more cells for the output data.
   *
   * @param pos Selected position from which to generate the derived data.
   * @param out The output data from which to generate the cell.
   *
   * @return Zero or more cells.
   */
  def present(pos: Position[S], out: O): TraversableOnce[Cell[Q]]

  /**
   * Operator for preparing content prior to generating derived data.
   *
   * @param preparer The function to apply prior to generating derived data.
   *
   * @return A windowed function that prepares the content and then runs `this`.
   */
  override def withPrepare(preparer: (Cell[P]) => Content) = new Window[P, S, R, Q] {
    type I = self.I
    type T = self.T
    type O = self.O

    def prepare(cell: Cell[P]): I = self.prepare(cell.mutate(preparer))
    def initialise(rem: Position[R], in: I): (T, TraversableOnce[O]) = self.initialise(rem, in)
    def update(rem: Position[R], in: I, t: T): (T, TraversableOnce[O]) = self.update(rem, in, t)
    def present(pos: Position[S], out: O): TraversableOnce[Cell[Q]] = self.present(pos, out)
  }

  /**
   * Operator for generating derived data and then updating the contents.
   *
   * @param mutator The mutation to apply after generating derived data.
   *
   * @return A windowed function that runs `this` and then updates the resulting contents.
   */
  override def andThenMutate(mutator: (Cell[Q]) => Content) = new Window[P, S, R, Q] {
    type I = self.I
    type T = self.T
    type O = self.O

    def prepare(cell: Cell[P]): I = self.prepare(cell)
    def initialise(rem: Position[R], in: I): (T, TraversableOnce[O]) = self.initialise(rem, in)
    def update(rem: Position[R], in: I, t: T): (T, TraversableOnce[O]) = self.update(rem, in, t)
    def present(pos: Position[S], out: O): TraversableOnce[Cell[Q]] = self.present(pos, out).map(_.mutate(mutator))
  }

  /**
   * Operator for generating derived data and then relocating the contents.
   *
   * @param locator The relocation to apply after generating derived data.
   *
   * @return A windowed function that runs `this` and then relocates the contents.
   */
  override def andThenRelocate[
    X <: Nat
  ](
    locator: Locate.FromCell[Q, X]
  )(implicit
    ev: GTEq[X, Q]
  ) = new Window[P, S, R, X] {
    type I = self.I
    type T = self.T
    type O = self.O

    def prepare(cell: Cell[P]): I = self.prepare(cell)
    def initialise(rem: Position[R], in: I): (T, TraversableOnce[O]) = self.initialise(rem, in)
    def update(rem: Position[R], in: I, t: T): (T, TraversableOnce[O]) = self.update(rem, in, t)
    def present(pos: Position[S], out: O): TraversableOnce[Cell[X]] = self
      .present(pos, out)
      .flatMap(c => locator(c).map(Cell(_, c.content)))
  }
}

/** Companion object for the `Window` trait. */
object Window {
  /** Converts a `List[Window[P, S, R, Q]]` to a single `Window[P, S, R, Q]`. */
  implicit def listToWindow[
    P <: Nat,
    S <: Nat,
    R <: Nat,
    Q <: Nat
  ](
    windows: List[Window[P, S, R, Q]]
  ) = new Window[P, S, R, Q] {
    type I = List[Any]
    type T = List[Any]
    type O = List[TraversableOnce[Any]]

    def prepare(cell: Cell[P]): I = windows.map(_.prepare(cell))

    def initialise(rem: Position[R], in: I): (T, TraversableOnce[O]) = {
      val state = (windows, in)
        .zipped
        .map { case (window, j) => window.initialise(rem, j.asInstanceOf[window.I]) }

      (state.map(_._1), List(state.map(_._2)))
    }

    def update(rem: Position[R], in: I, s: T): (T, TraversableOnce[O]) = {
      val state = (windows, in, s)
        .zipped
        .map { case (window, j, u) => window.update(rem, j.asInstanceOf[window.I], u.asInstanceOf[window.T]) }

      (state.map(_._1), List(state.map(_._2)))
    }

    def present(pos: Position[S], out: O): TraversableOnce[Cell[Q]] = (windows, out)
      .zipped
      .flatMap { case (window, s) => s.flatMap(t => window.present(pos, t.asInstanceOf[window.O])) }
  }
}

/**
 * Base trait for initialising a windowed with a user supplied value.
 *
 * Windowed data is derived from two or more values, for example deltas or gradients. To generate this, the process is
 * as follows. First each cell is prepared for windowed operations. This involves return the input data to slide over.
 * Next the input data is grouped according to a `slice`. The data in each group is then sorted by the remaining
 * coordinates. The first cell's data of each group is passed to the initialise method. This allows a window to
 * initialise it's running state. All subsequent cells' data are passed to the update method (together with the running
 * state). The update method can update the running state, and optionally return one or more output values. Finally,
 * the present method returns cells for the output values. Note that the running state can be used to create derived
 * features of different window sizes.
 */
trait WindowWithValue[P <: Nat, S <: Nat, R <: Nat, Q <: Nat] extends java.io.Serializable { self =>
  /** Type of the external value. */
  type V

  /** Type of the input data. */
  type I

  /** Type of the running state. */
  type T

  /** Type of the output data. */
  type O

  /**
   * Prepare a sliding window operations.
   *
   * @param cell The cell from which to generate the input data.
   * @param ext  User provided data required for preparation.
   *
   * @return The input data over which the sliding window operates.
   */
  def prepareWithValue(cell: Cell[P], ext: V): I

  /**
   * Initialise the running state using the first cell (ordered according to its position).
   *
   * @param rem  The remaining coordinates of the cell.
   * @param in   The input data.
   *
   * @return The running state for this object.
   */
  def initialise(rem: Position[R], in: I): (T, TraversableOnce[O])

  /**
   * Update running state with the state and, optionally, return output data.
   *
   * @param rem  The remaining coordinates of the cell.
   * @param in   The input data.
   * @param t    The running state.
   *
   * @return A tuple consisting of updated running state together with optional output data.
   */
  def update(rem: Position[R], in: I, t: T): (T, TraversableOnce[O])

  /**
   * Present zero or more cells for the output data.
   *
   * @param pos Selected position from which to generate the derived data.
   * @param out The output data from which to generate the cell.
   * @param ext User provided data required for preparation.
   *
   * @return Zero or more cells.
   */
  def presentWithValue(pos: Position[S], out: O, ext: V): TraversableOnce[Cell[Q]]

  /**
   * Operator for preparing content prior to generating derived data.
   *
   * @param preparer The function to apply prior to generating derived data.
   *
   * @return A windowed function that prepares the content and then runs `this`.
   */
  def withPrepare(preparer: (Cell[P]) => Content) = new WindowWithValue[P, S, R, Q] {
    type V = self.V
    type I = self.I
    type T = self.T
    type O = self.O

    def prepareWithValue(cell: Cell[P], ext: V): I = self.prepareWithValue(cell.mutate(preparer), ext)
    def initialise(rem: Position[R], in: I): (T, TraversableOnce[O]) = self.initialise(rem, in)
    def update(rem: Position[R], in: I, t: T): (T, TraversableOnce[O]) = self.update(rem, in, t)
    def presentWithValue(pos: Position[S], out: O, ext: V): TraversableOnce[Cell[Q]] = self
      .presentWithValue(pos, out, ext)
  }

  /**
   * Operator for generating derived data and then updating the contents.
   *
   * @param mutator The mutation to apply after generating derived data.
   *
   * @return A windowed function that runs `this` and then updates the resulting contents.
   */
  def andThenMutate(mutator: (Cell[Q]) => Content) = new WindowWithValue[P, S, R, Q] {
    type V = self.V
    type I = self.I
    type T = self.T
    type O = self.O

    def prepareWithValue(cell: Cell[P], ext: V): I = self.prepareWithValue(cell, ext)
    def initialise(rem: Position[R], in: I): (T, TraversableOnce[O]) = self.initialise(rem, in)
    def update(rem: Position[R], in: I, t: T): (T, TraversableOnce[O]) = self.update(rem, in, t)
    def presentWithValue(pos: Position[S], out: O, ext: V): TraversableOnce[Cell[Q]] = self
      .presentWithValue(pos, out, ext)
      .map(_.mutate(mutator))
  }

  /**
   * Operator for generating derived data and then relocating the contents.
   *
   * @param locator The relocation to apply after generating derived data.
   *
   * @return A windowed function that runs `this` and then relocates the contents.
   */
  def andThenRelocate[
    X <: Nat
  ](
    locator: Locate.FromCell[Q, X]
  )(implicit
    ev: GTEq[X, Q]
  ) = new WindowWithValue[P, S, R, X] {
    type V = self.V
    type I = self.I
    type T = self.T
    type O = self.O

    def prepareWithValue(cell: Cell[P], ext: V): I = self.prepareWithValue(cell, ext)
    def initialise(rem: Position[R], in: I): (T, TraversableOnce[O]) = self.initialise(rem, in)
    def update(rem: Position[R], in: I, t: T): (T, TraversableOnce[O]) = self.update(rem, in, t)
    def presentWithValue(pos: Position[S], out: O, ext: V): TraversableOnce[Cell[X]] = self
      .presentWithValue(pos, out, ext)
      .flatMap(c => locator(c).map(Cell(_, c.content)))
  }

  /**
   * Operator for preparing content prior to generating derived data.
   *
   * @param preparer The function to apply prior to generating derived data.
   *
   * @return A windowed function that prepares the content and then runs `this`.
   */
  def withPrepareWithValue(preparer: (Cell[P], V) => Content) = new WindowWithValue[P, S, R, Q] {
    type V = self.V
    type I = self.I
    type T = self.T
    type O = self.O

    def prepareWithValue(cell: Cell[P], ext: V): I = self
      .prepareWithValue(Cell(cell.position, preparer(cell, ext)), ext)
    def initialise(rem: Position[R], in: I): (T, TraversableOnce[O]) = self.initialise(rem, in)
    def update(rem: Position[R], in: I, t: T): (T, TraversableOnce[O]) = self.update(rem, in, t)
    def presentWithValue(pos: Position[S], out: O, ext: V): TraversableOnce[Cell[Q]] = self
      .presentWithValue(pos, out, ext)
  }

  /**
   * Operator for generating derived data and then updating the contents.
   *
   * @param mutator The mutation to apply after generating derived data.
   *
   * @return A windowed function that runs `this` and then updates the resulting contents.
   */
  def andThenMutateWithValue(mutator: (Cell[Q], V) => Content) = new WindowWithValue[P, S, R, Q] {
    type V = self.V
    type I = self.I
    type T = self.T
    type O = self.O

    def prepareWithValue(cell: Cell[P], ext: V): I = self.prepareWithValue(cell, ext)
    def initialise(rem: Position[R], in: I): (T, TraversableOnce[O]) = self.initialise(rem, in)
    def update(rem: Position[R], in: I, t: T): (T, TraversableOnce[O]) = self.update(rem, in, t)
    def presentWithValue(pos: Position[S], out: O, ext: V): TraversableOnce[Cell[Q]] = self
      .presentWithValue(pos, out, ext)
      .map(c => Cell(c.position, mutator(c, ext)))
  }

  /**
   * Operator for generating derived data and then relocating the contents.
   *
   * @param locator The relocation to apply after generating derived data.
   *
   * @return A windowed function that runs `this` and then relocates the contents.
   */
  def andThenRelocateWithValue[
    X <: Nat
  ](
    locator: Locate.FromCellWithValue[Q, X, V]
  )(implicit
    ev: GTEq[X, Q]
  ) = new WindowWithValue[P, S, R, X] {
    type V = self.V
    type I = self.I
    type T = self.T
    type O = self.O

    def prepareWithValue(cell: Cell[P], ext: V): I = self.prepareWithValue(cell, ext)
    def initialise(rem: Position[R], in: I): (T, TraversableOnce[O]) = self.initialise(rem, in)
    def update(rem: Position[R], in: I, t: T): (T, TraversableOnce[O]) = self.update(rem, in, t)
    def presentWithValue(pos: Position[S], out: O, ext: V): TraversableOnce[Cell[X]] = self
      .presentWithValue(pos, out, ext)
      .flatMap(c => locator(c, ext).map(Cell(_, c.content)))
  }
}

/** Companion object for the `WindowWithValue` trait. */
object WindowWithValue {
  /**
   * Converts a `List[WindowWithValue[S, R, Q] { type V >: W }]` to a single
   * `WindowWithValue[S, R, Q] { type V >: W }`.
   */
  implicit def listToWindowWithValue[
    P <: Nat,
    S <: Nat,
    R <: Nat,
    Q <: Nat,
    W
  ](
    t: List[WindowWithValue[P, S, R, Q] { type V >: W }]
  ) = new WindowWithValue[P, S, R, Q] {
    type V = W
    type I = List[Any]
    type T = List[Any]
    type O = List[TraversableOnce[Any]]

    def prepareWithValue(cell: Cell[P], ext: V): I = t.map(_.prepareWithValue(cell, ext))

    def initialise(rem: Position[R], in: I): (T, TraversableOnce[O]) = {
      val state = (t, in)
        .zipped
        .map { case (window, j) => window.initialise(rem, j.asInstanceOf[window.I]) }

      (state.map(_._1), List(state.map(_._2)))
    }

    def update(rem: Position[R], in: I, s: T): (T, TraversableOnce[O]) = {
      val state = (t, in, s)
        .zipped
        .map { case (window, j, u) => window.update(rem, j.asInstanceOf[window.I], u.asInstanceOf[window.T]) }

      (state.map(_._1), List(state.map(_._2)))
    }

    def presentWithValue(pos: Position[S], out: O, ext: V): TraversableOnce[Cell[Q]] = (t, out)
      .zipped
      .flatMap { case (window, s) => s.flatMap(t => window.presentWithValue(pos, t.asInstanceOf[window.O], ext)) }
  }
}

