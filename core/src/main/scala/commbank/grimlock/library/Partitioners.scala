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

package commbank.grimlock.library.partition

import commbank.grimlock.framework.Cell
import commbank.grimlock.framework.encoding.{ DateCodec, DateValue }
import commbank.grimlock.framework.partition.Partitioner

import java.util.Date

import shapeless.Nat
import shapeless.ops.nat.{ LTEq, ToInt }

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
  D <: Nat : ToInt,
  P <: Nat,
  I
](
  dim: D,
  ratio: Int,
  left: I,
  right: I,
  base: Int = 100
)(implicit
  ev: LTEq[D, P]
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
  D <: Nat : ToInt,
  P <: Nat,
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
  ev: LTEq[D, P]
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
  D <: Nat : ToInt,
  P <: Nat,
  I
](
  dim: D,
  ranges: Map[I, (Int, Int)],
  base: Int = 100
)(implicit
  ev: LTEq[D, P]
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
  D <: Nat : ToInt,
  P <: Nat,
  I
](
  dim: D,
  date: Date,
  left: I,
  right: I,
  codec: DateCodec
)(implicit
  ev: LTEq[D, P]
) extends Partitioner[P, I] {
  def assign(cell: Cell[P]): TraversableOnce[I] = codec
    .compare(cell.position(dim), DateValue(date, codec))
    .map(cmp => if (cmp <= 0) left else right)
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
  D <: Nat : ToInt,
  P <: Nat,
  I
](
  dim: D,
  lower: Date,
  upper: Date,
  left: I,
  middle: I,
  right: I,
  codec: DateCodec
)(implicit
  ev: LTEq[D, P]
) extends Partitioner[P, I] {
  def assign(cell: Cell[P]): TraversableOnce[I] =
    (codec.compare(cell.position(dim), DateValue(lower, codec)),
     codec.compare(cell.position(dim), DateValue(upper, codec))) match {
      case (Some(l), Some(u)) => List(if (l <= 0) left else if (u <= 0) middle else right)
      case _ => List()
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
  D <: Nat : ToInt,
  P <: Nat,
  I
](
  dim: D,
  ranges: Map[I, (Date, Date)],
  codec: DateCodec
)(implicit
  ev: LTEq[D, P]
) extends Partitioner[P, I] {
  def assign(cell: Cell[P]): TraversableOnce[I] = ranges
    .flatMap { case (k, (lower, upper)) =>
      (codec.compare(cell.position(dim), DateValue(lower, codec)),
       codec.compare(cell.position(dim), DateValue(upper, codec))) match {
        case (Some(l), Some(u)) if (l > 0 && u <= 0) => Option(k)
        case _ => None
      }
    }
}

