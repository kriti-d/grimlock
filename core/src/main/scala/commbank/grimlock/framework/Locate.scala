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

import commbank.grimlock.framework.content.Content
import commbank.grimlock.framework.encoding.Value
import commbank.grimlock.framework.position.{ Position, Slice }

import shapeless.{ Nat, Succ }
import shapeless.ops.nat.{ LTEq, ToInt }

object Locate {
  /** Extract position. */
  type FromPosition[P <: Nat, Q <: Nat] = (Position[P]) => Option[Position[Q]]

  /** Extract position for left and right positions. */
  type FromPairwisePositions[P <: Nat, Q <: Nat] = (Position[P], Position[P]) => Option[Position[Q]]

  /** Extract position from cell. */
  type FromCell[P <: Nat, Q <: Nat] = (Cell[P]) => Option[Position[Q]]

  /** Extract position from cell with user provided value. */
  type FromCellWithValue[P <: Nat, Q <: Nat, V] = (Cell[P], V) => Option[Position[Q]]

  /** Extract position from cell and an optional value. */
  type FromCellAndOptionalValue[P <: Nat, Q <: Nat] = (Cell[P], Option[Value]) => Option[Position[Q]]

  /** Extract position for left and right cells. */
  type FromPairwiseCells[P <: Nat, Q <: Nat] = (Cell[P], Cell[P]) => Option[Position[Q]]

  /** Extract position for the selected cell and its remainder. */
  type FromSelectedAndRemainder[S <: Nat, R <: Nat, Q <: Nat] = (Position[S], Position[R]) => Option[Position[Q]]

  /** Extract position for the selected cell and its current and prior remainder. */
  type FromSelectedAndPairwiseRemainder[
    S <: Nat,
    R <: Nat,
    Q <: Nat
  ] = (Position[S], Position[R], Position[R]) => Option[Position[Q]]

  /** Extract position from selected position and a value. */
  type FromSelectedAndOutput[S <: Nat, T, Q <: Nat] = (Position[S], T) => Option[Position[Q]]

  /** Extract position from selected position and a content. */
  type FromSelectedAndContent[S <: Nat, Q <: Nat] = (Position[S], Content) => Option[Position[Q]]

  /**
   * Rename a dimension.
   *
   * @param dim  The dimension to rename.
   * @param name The rename pattern. Use `%1$``s` for the coordinate.
   */
  def RenameDimension[
    P <: Nat,
    D <: Nat : ToInt
  ](
    dim: D,
    name: String
  )(implicit
    ev: LTEq[D, P]
  ): FromCell[P, P] = (cell: Cell[P]) => cell
    .position
    .update(dim, name.format(cell.position(dim).toShortString))
    .toOption

  /**
   * Rename coordinate according a name pattern.
   *
   * @param dim  Dimension for which to update coordinate name.
   * @param name Pattern for the new name of the coordinate at `dim`. Use `%[12]$``s` for the string
   *             representations of the coordinate, and the content.
   */
  def RenameDimensionWithContent[
    P <: Nat,
    D <: Nat : ToInt
  ](
    dim: D,
    name: String = "%1$s=%2$s"
  )(implicit
    ev: LTEq[D, P]
  ): FromCell[P, P] = (cell: Cell[P]) => cell
    .position
    .update(dim, name.format(cell.position(dim).toShortString, cell.content.value.toShortString))
    .toOption

  /**
   * Append a coordinate to the position.
   *
   * @param name The coordinate to append to the outcome cell.
   */
  def AppendValue[P <: Nat](name: Value): FromCell[P, Succ[P]] = (cell: Cell[P]) => cell.position.append(name).toOption

  /**
   * Extract position use a name pattern.
   *
   * @param slice     Encapsulates the dimension(s) from which to construct the name.
   * @param pattern   Name pattern of the new coordinate. Use `%[12]$``s` for the string representations of the
   *                  left and right selected positions respectively.
   * @param all       Indicates if all positions should be returned (true), or only if left and right remainder
   *                  are equal.
   * @param separator Separator to use when converting left and right positions to string.
   *
   * @note If a position is returned then it's always right cell's remainder with an additional coordinate prepended.
   */
  def PrependPairwiseSelectedStringToRemainder[
    P <: Nat
  ](
    slice: Slice[P],
    pattern: String,
    all: Boolean = false,
    separator: String = "|"
  ): FromPairwiseCells[P, Succ[slice.R]] = (left: Cell[P], right: Cell[P]) => {
    val reml = slice.remainder(left.position)
    val remr = slice.remainder(right.position)

    if (all || reml == remr)
      reml
        .prepend(
          pattern.format(
            slice.selected(left.position).toShortString(separator),
            slice.selected(right.position).toShortString(separator)
          )
        )
        .toOption
    else
      None
  }

  /**
   * Append with a dimension from the remainder.
   *
   * @param dim The dimension (out of `rem`) to append to the cell's position.
   */
  def AppendRemainderDimension[
    S <: Nat,
    R <: Nat,
    D <: Nat : ToInt
  ](
    dim: D
  )(implicit
    ev: LTEq[D, R]
  ): FromSelectedAndRemainder[S, R, Succ[S]] = (sel: Position[S], rem: Position[R]) => sel.append(rem(dim)).toOption

  /**
   * Extract position using string of `rem`.
   *
   * @param separator The separator to use for the appended coordinate.
   */
  def AppendRemainderString[
    S <: Nat,
    R <: Nat
  ](
    separator: String = "|"
  ): FromSelectedAndRemainder[S, R, Succ[S]] = (sel: Position[S], rem: Position[R]) => sel
    .append(rem.toShortString(separator))
    .toOption

  /**
   * Extract position using string of current and previous `rem`.
   *
   * @param pattern   Name pattern of the new coordinate. Use `%[12]$``s` for the string representations of the
   *                  previous and current remainder positions respectively.
   * @param separator The separator to use for the appended coordinate.
   */
  def AppendPairwiseString[
    S <: Nat,
    R <: Nat
  ](
    pattern: String,
    separator: String = "|"
  ): FromSelectedAndPairwiseRemainder[S, R, Succ[S]] = (sel: Position[S], curr: Position[R], prev: Position[R]) => sel
    .append(pattern.format(prev.toShortString(separator), curr.toShortString(separator)))
    .toOption

  /**
   * Expand position by appending a string coordinate from double.
   *
   * @param name The name pattern. Use `%1$``s` for the output pattern.
   */
  def AppendDoubleString[
    S <: Nat
  ](
    name: String = "%1$f%%"
  ): FromSelectedAndOutput[S, Double, Succ[S]] = (sel: Position[S], value: Double) => sel
    .append(name.format(value))
    .toOption

  /** Append the content string to the position. */
  def AppendContentString[S <: Nat](): FromSelectedAndContent[S, Succ[S]] = (sel: Position[S], con: Content) => sel
    .append(con.value.toShortString)
    .toOption

  /**
   * Append a coordinate according a name pattern.
   *
   * @param dim  Dimension for which to update coordinate name.
   * @param name Pattern for the new name of the coordinate at `dim`. Use `%[12]$``s` for the string
   *             representations of the coordinate, and the content.
   */
  def AppendDimensionAndContentString[
    S <: Nat,
    D <: Nat : ToInt
  ](
    dim: D,
    name: String = "%1$s=%2$s"
  )(implicit
    ev: LTEq[D, S]
  ): FromSelectedAndContent[S, Succ[S]] = (sel: Position[S], con: Content) => sel
    .append(name.format(sel(dim).toShortString, con.value.toShortString))
    .toOption
}

