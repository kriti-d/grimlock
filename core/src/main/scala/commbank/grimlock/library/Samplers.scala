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

package commbank.grimlock.library.sample

import commbank.grimlock.framework.Cell
import commbank.grimlock.framework.extract.Extract
import commbank.grimlock.framework.position.Position
import commbank.grimlock.framework.sample.{ Sampler, SamplerWithValue }

import scala.util.Random

import shapeless.{ HList, Nat }

/**
 * Randomly sample to a ratio.
 *
 * @param ratio The sampling ratio.
 * @param rnd   The random number generator.
 *
 * @note This randomly samples ignoring the position.
 */
case class RandomSample[P <: HList](ratio: Double, rnd: Random = new Random()) extends Sampler[P] {
  def select(cell: Cell[P]): Boolean = rnd.nextDouble() < ratio
}

/**
 * Sample based on the hash code of a dimension.
 *
 * @param dim   The dimension to sample from.
 * @param ratio The sample ratio (relative to `base`).
 * @param base  The base of the sampling ratio.
 */
case class HashSample[
  P <: HList,
  D <: Nat
](
  dim: D,
  ratio: Int,
  base: Int
)(implicit
  ev: Position.IndexConstraints[P, D]
) extends Sampler[P] {
  def select(cell: Cell[P]): Boolean = math.abs(cell.position(dim).hashCode % base) < ratio
}

/**
 * Sample to a defined size based on the hash code of a dimension.
 *
 * @param dim   The dimension to sample from.
 * @param count Object that will extract, for `cell`, its corresponding number of values.
 * @param size  The size to sample to.
 */
case class HashSampleToSize[
  P <: HList,
  D <: Nat,
  W
](
  dim: D,
  count: Extract[P, W, Double],
  size: Long
)(implicit
  ev: Position.IndexConstraints[P, D]
) extends SamplerWithValue[P] {
  type V = W

  def selectWithValue(cell: Cell[P], ext: V): Boolean = count
    .extract(cell, ext)
    .map(cnt => math.abs(cell.position(dim).hashCode % cnt) < size)
    .getOrElse(false)
}

