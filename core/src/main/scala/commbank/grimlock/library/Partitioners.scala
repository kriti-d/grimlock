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

package commbank.grimlock.library.partition

import commbank.grimlock.framework.Cell
import commbank.grimlock.framework.encoding.{ Codec, Value }
import commbank.grimlock.framework.partition.Partitioner
import commbank.grimlock.framework.position.Position

import java.util.Date

import shapeless.{ HList, Nat }

/**
 * Binary partition based on the hash code of a coordinate.
 *
 * @param dim   The dimension to partition on.
 * @param ratio The binary split ratio (relative to `base`).
 * @param left  The identifier for the left partition.
 * @param right The identifier for the right partition.
 * @param base  The base for the ratio.
 *
 * @note The hash code modulo `base` is used for comparison with the ratio. While the position is assigned to the left
 *       partition if it is less or equal to the `ratio` value.
 */
case class BinaryHashSplit[
  P <: HList,
  D <: Nat,
  I
](
  dim: D,
  ratio: Int,
  left: I,
  right: I,
  base: Int = 100
)(implicit
  ev: Position.IndexConstraints[P, D]
) extends Partitioner[P, I] {
  def assign(cell: Cell[P]): TraversableOnce[I] = List(
    if (math.abs(cell.position(dim).hashCode % base) <= ratio) left else right
  )
}

/**
 * Ternary partition based on the hash code of a coordinate.
 *
 * @param dim    The dimension to partition on.
 * @param lower  The lower ternary split ratio (relative to `base`).
 * @param upper  The upper ternary split ratio (relative to `base`).
 * @param left   The identifier for the left partition.
 * @param middle The identifier for the middle partition.
 * @param right  The identifier for the right partition.
 * @param base   The base for the ratio.
 *
 * @note The hash code modulo `base` is used for comparison with lower/upper. While the position is assigned to the
 *       partition `left` if it is less or equal to `lower`, `middle` if it is less of equal to `upper` or else to
 *       `right`.
 */
case class TernaryHashSplit[
  P <: HList,
  D <: Nat,
  I
](
  dim: D,
  lower: Int,
  upper: Int,
  left: I,
  middle: I,
  right: I,
  base: Int = 100
)(implicit
  ev: Position.IndexConstraints[P, D]
) extends Partitioner[P, I] {
  def assign(cell: Cell[P]): TraversableOnce[I] = {
    val hash = math.abs(cell.position(dim).hashCode % base)

    List(if (hash <= lower) left else if (hash <= upper) middle else right)
  }
}

/**
 * Partition based on the hash code of a coordinate.
 *
 * @param dim    The dimension to partition on.
 * @param ranges A `Map` holding the partitions and hash code ranges (relative to `base`) for each partition.
 * @param base   The base for hash code.
 *
 * @note The hash code modulo `base` is used for comparison with the range. While a position falls in a range if it is
 *       (strictly) greater than the lower value (first value in tuple) and less or equal to the upper value (second
 *       value in tuple).
 */
case class HashSplit[
  P <: HList,
  D <: Nat,
  I
](
  dim: D,
  ranges: Map[I, (Int, Int)],
  base: Int = 100
)(implicit
  ev: Position.IndexConstraints[P, D]
) extends Partitioner[P, I] {
  def assign(cell: Cell[P]): TraversableOnce[I] = {
    val hash = math.abs(cell.position(dim).hashCode % base)

    ranges.collect { case (k, (l, u)) if (hash > l && hash <= u) => k }
  }
}

/**
 * Binary partition based on the date of a coordinate.
 *
 * @param dim   The dimension to partition on.
 * @param date  The date around which to split.
 * @param left  The identifier for the left partition.
 * @param right The identifier for the right partition.
 * @param codec The date codec used for comparison.
 *
 * @note The position is assigned to the `left` partition if it is less or equal to the `date` value, to `right`
 *       otherwise.
 */
case class BinaryDateSplit[
  P <: HList,
  D <: Nat,
  I
](
  dim: D,
  date: Date,
  left: I,
  right: I,
  codec: Codec[Date]
)(implicit
  ev: Position.IndexConstraints.Aux[P, D, Value[Date]]
) extends Partitioner[P, I] {
  def assign(cell: Cell[P]): TraversableOnce[I] = List(
    if (codec.compare(cell.position(dim).value, date) <= 0) left else right
  )
}

/**
 * Ternary partition based on the date of a coordinate.
 *
 * @param dim    The dimension to partition on.
 * @param lower  The lower date around which to split.
 * @param upper  The upper date around which to split.
 * @param left   The identifier for the left partition.
 * @param middle The identifier for the middle partition.
 * @param right  The identifier for the right partition.
 * @param codec  The date codec used for comparison.
 *
 * @note The position is assigned to the partition `left` if it is less or equal to `lower`, `middle` if it is less or
 *       equal to `upper` or else to `right`.
 */
case class TernaryDateSplit[
  P <: HList,
  D <: Nat,
  I
](
  dim: D,
  lower: Date,
  upper: Date,
  left: I,
  middle: I,
  right: I,
  codec: Codec[Date]
)(implicit
  ev: Position.IndexConstraints.Aux[P, D, Value[Date]]
) extends Partitioner[P, I] {
  def assign(cell: Cell[P]): TraversableOnce[I] = {
    val date = cell.position(dim).value

    List(if (codec.compare(date, lower) <= 0) left else if (codec.compare(date, upper) <= 0) middle else right)
  }
}

/**
 * Partition based on the date of a coordinate.
 *
 * @param dim    The dimension to partition on.
 * @param ranges A `Map` holding the partitions and date ranges for each partition.
 * @param codec  The date codec used for comparison.
 *
 * @note A position falls in a range if it is (strictly) greater than the lower value (first value in tuple) and less
 *       or equal to the upper value (second value in tuple).
 */
case class DateSplit[
  P <: HList,
  D <: Nat,
  I
](
  dim: D,
  ranges: Map[I, (Date, Date)],
  codec: Codec[Date]
)(implicit
  ev: Position.IndexConstraints.Aux[P, D, Value[Date]]
) extends Partitioner[P, I] {
  def assign(cell: Cell[P]): TraversableOnce[I] = ranges
    .flatMap { case (k, (lower, upper)) =>
      val date = cell.position(dim).value

      if (codec.compare(date, lower) > 0 && codec.compare(date, upper) <= 0) Option(k) else None
    }
}

