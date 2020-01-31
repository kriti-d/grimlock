// Copyright 2015,2016,2017,2018,2019,2020 Commonwealth Bank of Australia
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

import shapeless.{ HList, Nat }

object Locate {
  /** Extract position from cell. */
  type FromCell[P <: HList, Q <: HList] = (Cell[P]) => Option[Position[Q]]

  /** Extract position from cell with user provided value. */
  type FromCellWithValue[P <: HList, T, Q <: HList] = (Cell[P], T) => Option[Position[Q]]

  /** Extract position for left and right cells. */
  type FromPairwiseCells[P <: HList, Q <: HList] = (Cell[P], Cell[P]) => Option[Position[Q]]

  /** Extract position. */
  type FromPosition[P <: HList, Q <: HList] = (Position[P]) => Option[Position[Q]]

  /** Extract position from selected position and a value. */
  type FromPositionWithValue[P <: HList, T, Q <: HList] = (Position[P], T) => Option[Position[Q]]

  /** Extract position for left and right positions. */
  type FromPairwisePositions[P <: HList, Q <: HList] = (Position[P], Position[P]) => Option[Position[Q]]

  /** Extract position for the selected cell and its remainder. */
  type FromSelectedAndRemainder[S <: HList, R <: HList, Q <: HList] = (Position[S], Position[R]) => Option[Position[Q]]

  /** Extract position for the selected cell and its current and prior remainder. */
  type FromSelectedAndPairwiseRemainder[
    S <: HList,
    R <: HList,
    Q <: HList
  ] = (Position[S], Position[R], Position[R]) => Option[Position[Q]]

  /** Append the content string to the position. */
  def AppendContentString[
    P <: HList
  ](implicit
    ev1: Value.Box[String],
    ev2: Position.AppendConstraints[P, Value[String]]
  ): FromPositionWithValue[P, Content, ev2.Q] = (pos: Position[P], con: Content) => pos
    .append(con.value.toShortString)
    .toOption

  /**
   * Append a coordinate using the content value and a selected coordinate.
   *
   * @param dim    Dimension for which to get the coordinate.
   * @param create Function that creates the new coordinate to append.
   */
  def AppendDimensionWithContent[
    P <: HList,
    D <: Nat,
    V <: Value[_],
    T <% Value[T]
  ](
    dim: D,
    create: (V, Value[_]) => T
  )(implicit
    ev1: Position.IndexConstraints.Aux[P, D, V],
    ev2: Position.AppendConstraints[P, Value[T]]
  ): FromPositionWithValue[P, Content, ev2.Q] = (pos: Position[P], con: Content) => pos
    .append(create(pos(dim), con.value))
    .toOption

  /**
   * Append to the selected position, a coordinate constructed from the pairwise remainder strings.
   *
   * @param pattern   Name pattern of the new coordinate. Use `%[12]$``s` for the string representations of the
   *                  previous and current remainder positions respectively.
   * @param separator The separator used to create a string from the remainder position.
   */
  def AppendPairwiseRemainderString[
    S <: HList,
    R <: HList
  ](
    pattern: String,
    separator: String
  )(implicit
    ev1: Value.Box[String],
    ev2: Position.AppendConstraints[S, Value[String]]
  ): FromSelectedAndPairwiseRemainder[S, R, ev2.Q] = (sel: Position[S], curr: Position[R], prev: Position[R]) => sel
    .append(pattern.format(prev.toShortString(separator), curr.toShortString(separator)))
    .toOption

  /**
   * Append a coordinate to the selected position using the coordinate at dimension from the remainder.
   *
   * @param dim The dimension of the remainder's coordinate to append to the selected position.
   */
  def AppendRemainderDimension[
    S <: HList,
    R <: HList,
    D <: Nat,
    V <: Value[_]
  ](
    dim: D
  )(implicit
    ev1: Position.IndexConstraints.Aux[R, D, V],
    ev2: Position.AppendConstraints[S, V]
  ): FromSelectedAndRemainder[S, R, ev2.Q] = (sel: Position[S], rem: Position[R]) => sel.append(rem(dim)).toOption

  /**
   * Append the remainder position (as a string) to the selected position.
   *
   * @param separator The separator used to create a string from the remainder position.
   */
  def AppendRemainderString[
    S <: HList,
    R <: HList
  ](
    separator: String
  )(implicit
    ev1: Value.Box[String],
    ev2: Position.AppendConstraints[S, Value[String]]
  ): FromSelectedAndRemainder[S, R, ev2.Q] = (sel: Position[S], rem: Position[R]) => sel
    .append(rem.toShortString(separator))
    .toOption

  /**
   * Append a coordinate to the position.
   *
   * @param name The coordinate to append to the position.
   */
  def AppendValue[
    P <: HList,
    T <% Value[T]
  ](
    name: T
  )(implicit
    ev: Position.AppendConstraints[P, Value[T]]
  ): FromCell[P, ev.Q] = (cell: Cell[P]) => cell.position.append(name).toOption

  /**
   * Prepend the remainder position with a coordinate constructed from the pairwise selected strings.
   *
   * @param slice     Encapsulates the dimension(s) from which to construct the new position.
   * @param pattern   Name pattern of the new coordinate. Use `%[12]$``s` for the string representations of the
   *                  left and right selected positions respectively.
   * @param all       Indicates if all positions should be returned (true), or only if left and right remainder
   *                  are equal.
   * @param separator Separator to use when converting left and right positions to string.
   *
   * @note If a position is returned then it's always right cell's remainder with an additional coordinate prepended.
   */
  def PrependPairwiseSelectedStringToRemainder[
    P <: HList,
    S <: HList,
    R <: HList
  ](
    slice: Slice[P, S, R],
    pattern: String,
    all: Boolean,
    separator: String
  )(implicit
    ev1: Value.Box[String],
    ev2: Position.PrependConstraints[R, Value[String]]
  ): FromPairwiseCells[P, ev2.Q] = (left: Cell[P], right: Cell[P]) => {
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
   * Rename a dimension using it's coordinate.
   *
   * @param dim    Dimension for which to update the coordinate.
   * @param update The function that returns the updated coordinate.
   */
  def RenameDimension[
    P <: HList,
    D <: Nat,
    V <: Value[_],
    T <% Value[T]
  ](
    dim: D,
    update: (V) => T
  )(implicit
    ev1: Position.IndexConstraints.Aux[P, D, V],
    ev2: Position.UpdateConstraints[P, D, Value[T]]
  ): FromCell[P, ev2.Q] = (cell: Cell[P]) => cell
    .position
    .update(dim, update(cell.position(dim)))
    .toOption

  /**
   * Rename a dimension using it's coordinate and content value.
   *
   * @param dim    Dimension for which to update the coordinate.
   * @param update The function that returns the updated coordinate.
   */
  def RenameDimensionWithContent[
    P <: HList,
    D <: Nat,
    V <: Value[_],
    T <% Value[T]
  ](
    dim: D,
    update: (V, Value[_]) => T
  )(implicit
    ev1: Position.IndexConstraints.Aux[P, D, V],
    ev2: Position.UpdateConstraints[P, D, Value[T]]
  ): FromCell[P, ev2.Q] = (cell: Cell[P]) => cell
    .position
    .update(dim, update(cell.position(dim), cell.content.value))
    .toOption
}

