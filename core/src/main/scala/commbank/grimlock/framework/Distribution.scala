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

package commbank.grimlock.framework.distribution

import commbank.grimlock.framework._
import commbank.grimlock.framework.content._
import commbank.grimlock.framework.content.metadata._
import commbank.grimlock.framework.position._
import commbank.grimlock.framework.utility._

import com.tdunning.math.stats.AVLTreeDigest

import scala.collection.immutable.SortedMap
import scala.math.BigDecimal
import scala.reflect.ClassTag

import shapeless.Nat
import shapeless.nat.{ _0, _1 }
import shapeless.ops.nat.{ Diff, GT }

/** Trait for computing approximate distributions from a matrix. */
trait ApproximateDistribution[L <: Nat, P <: Nat] { self: Matrix[L, P] =>
  /** Specifies tuners permitted on a call to `histogram`. */
  type HistogramTuners[_]

  /**
   * Compute histogram.
   *
   * @param slice     Encapsulates the dimension(s) to compute histogram on.
   * @param position  Function for extracting the position of the histogram.
   * @param filter    Indicator if numerical values shoud be filtered or not.
   * @param tuner     The tuner for the job.
   *
   * @return A `U[Cell[Q]]` with the histogram.
   *
   * @note The histogram is computed on the positions returned by `position`.
   */
  def histogram[
    Q <: Nat,
    T <: Tuner : HistogramTuners
  ](
    slice: Slice[L, P]
  )(
    position: Locate.FromSelectedAndContent[slice.S, Q],
    filter: Boolean = true,
    tuner: T
  )(implicit
    ev1: ClassTag[Position[Q]],
    ev2: GT[Q, slice.S],
    ev3: Diff.Aux[P, _1, L]
  ): U[Cell[Q]]

  /** Specifies tuners permitted on a call to `quantile`. */
  type QuantileTuners[_]

  /**
   * Compute sample quantiles.
   *
   * @param slice     Encapsulates the dimension(s) to compute quantiles on.
   * @param probs     List of probabilities; values must lie in (0, 1).
   * @param quantiser Function that determines the quantile indices into the order statistics.
   * @param name      Function for extracting the position of the quantile.
   * @param filter    Indicator if categorical values should be filtered or not.
   * @param nan       Indicator if NaN quantiles should be output or not.
   * @param tuner     The tuner for the job.
   *
   * @return A `U[Cell[Q]]` with the quantiles.
   *
   * @note Non numeric values result in `NaN` quantiles, while missing counts result in no quantiles.
   */
  def quantile[
    Q <: Nat,
    T <: Tuner : QuantileTuners
  ](
    slice: Slice[L, P]
  )(
    probs: List[Double],
    quantiser: Quantile.Quantiser,
    name: Locate.FromSelectedAndOutput[slice.S, Double, Q],
    filter: Boolean = true,
    nan: Boolean = false,
    tuner: T
  )(implicit
    ev1: slice.R =:!= _0,
    ev2: ClassTag[Position[slice.S]],
    ev3: GT[Q, slice.S],
    ev4: Diff.Aux[P, _1, L]
  ): U[Cell[Q]]

  /** Specifies tuners permitted on a call to `countMapQuantiles`. */
  type CountMapQuantilesTuners[_]

  /**
   * Compute quantiles using a count Map.
   *
   * @param slice     Encapsulates the dimension(s) to compute quantiles on.
   * @param probs     List of probabilities; values must lie in (0, 1).
   * @param quantiser Function that determines the quantile indices into the order statistics.
   * @param name      Function for extracting the position of the quantile.
   * @param filter    Indicator if categorical values should be filtered or not.
   * @param nan       Indicator if NaN quantiles should be output or not.
   * @param tuner     The tuner for the job.
   *
   * @return A `U[Cell[Q]]` with the quantiles.
   *
   * @note Only use this if all distinct values and their counts fit in memory.
   */
  def countMapQuantiles[
    Q <: Nat,
    T <: Tuner : CountMapQuantilesTuners
  ](
    slice: Slice[L, P]
  )(
    probs: List[Double],
    quantiser: Quantile.Quantiser,
    name: Locate.FromSelectedAndOutput[slice.S, Double, Q],
    filter: Boolean = true,
    nan: Boolean = false,
    tuner: T
  )(implicit
    ev1: slice.R =:!= _0,
    ev2: ClassTag[Position[slice.S]],
    ev3: GT[Q, slice.S],
    ev4: Diff.Aux[P, _1, L]
  ): U[Cell[Q]]

  /** Specifies tuners permitted on a call to `tDigestQuantiles`. */
  type TDigestQuantilesTuners[_]

  /**
   * Compute approximate quantiles using a t-digest.
   *
   * @param slice       Encapsulates the dimension(s) to compute quantiles on.
   * @param probs       The quantile probabilities to compute.
   * @param compression The t-digest compression parameter.
   * @param name        Names each quantile output.
   * @param filter      Indicator if categorical values should be filtered or not.
   * @param nan         Indicator if NaN quantiles should be output or not.
   * @param tuner       The tuner for the job.
   *
   * @return A `U[Cell[Q]]` with the approximate quantiles.
   *
   * @see https://github.com/tdunning/t-digest
   */
  def tDigestQuantiles[
    Q <: Nat,
    T <: Tuner : TDigestQuantilesTuners
  ](
    slice: Slice[L, P]
  )(
    probs: List[Double],
    compression: Double,
    name: Locate.FromSelectedAndOutput[slice.S, Double, Q],
    filter: Boolean = true,
    nan: Boolean = false,
    tuner: T
  )(implicit
    ev1: slice.R =:!= _0,
    ev2: ClassTag[Position[slice.S]],
    ev3: GT[Q, slice.S],
    ev4: Diff.Aux[P, _1, L]
  ): U[Cell[Q]]

  /** Specifies tuners permitted on a call to `uniformQuantiles`. */
  type UniformQuantilesTuners[_]

  /**
   * Compute `count` uniformly spaced approximate quantiles using an online streaming parallel histogram.
   *
   * @param slice       Encapsulates the dimension(s) to compute quantiles on.
   * @param count       The number of quantiles to compute.
   * @param name        Names each quantile output.
   * @param filter      Indicator if categorical values should be filtered or not.
   * @param nan         Not used.
   * @param tuner       The tuner for the job.
   *
   * @return A `U[Cell[Q]]` with the approximate quantiles.
   *
   * @see http://www.jmlr.org/papers/volume11/ben-haim10a/ben-haim10a.pdf
   */
  def uniformQuantiles[
    Q <: Nat,
    T <: Tuner : UniformQuantilesTuners
  ](
    slice: Slice[L, P]
  )(
    count: Long,
    name: Locate.FromSelectedAndOutput[slice.S, Double, Q],
    filter: Boolean = true,
    nan: Boolean = false,
    tuner: T
  )(implicit
    ev1: slice.R =:!= _0,
    ev2: ClassTag[Position[slice.S]],
    ev3: GT[Q, slice.S],
    ev4: Diff.Aux[P, _1, L]
  ): U[Cell[Q]]
}

/** Contains implementations for the quantisers as per R's quantile function. */
object Quantile {
  /** Type of quantiser function. */
  type Quantiser = (Double, Long) => (Long, Double)

  /**
   * Compute quantile indices and gamma coefficient according to R's Type 1 rule; inverse of empirical
   * distribution function.
   *
   * @see https://stat.ethz.ch/R-manual/R-devel/library/stats/html/quantile.html
   */
  val Type1: Quantiser = (p: Double, n: Long) => {
    val (j, g) = TypeX(p, n, 0)

    (j, if (math.abs(g) < 1e-6) 0 else 1)
  }

  /**
   * Compute quantile indices and gamma coefficient according to R's Type 2 rule; similar to type 1 but
   * with averaging at discontinuities.
   *
   * @see https://stat.ethz.ch/R-manual/R-devel/library/stats/html/quantile.html
   */
  val Type2: Quantiser = (p: Double, n: Long) => {
    val (j, g) = TypeX(p, n, 0)

    (j, if (math.abs(g) < 1e-6) 0.5 else 1)
  }

  /**
   * Compute quantile indices and gamma coefficient according to R's Type 3 rule; nearest even order statistic.
   *
   * @see https://stat.ethz.ch/R-manual/R-devel/library/stats/html/quantile.html
   */
  val Type3: Quantiser = (p: Double, n: Long) => {
    val (j, g) = TypeX(p, n, -0.5)

    (j, if (math.abs(g) < 1e-6 && j % 2 == 0) 0 else 1)
  }

  /**
   * Compute quantile indices and gamma coefficient according to R's Type 4 rule; linear interpolation of
   * the empirical cdf.
   *
   * @see https://stat.ethz.ch/R-manual/R-devel/library/stats/html/quantile.html
   */
  val Type4: Quantiser = (p: Double, n: Long) => TypeX(p, n, 0)

  /**
   * Compute quantile indices and gamma coefficient according to R's Type 5 rule; a piecewise linear function
   * where the knots are the values midway through the steps of the empirical cdf.
   *
   * @see https://stat.ethz.ch/R-manual/R-devel/library/stats/html/quantile.html
   */
  val Type5: Quantiser = (p: Double, n: Long) => TypeX(p, n, 0.5)

  /**
   * Compute quantile indices and gamma coefficient according to R's Type 6 rule; p[k] = E[F(x[k])].
   *
   * @see https://stat.ethz.ch/R-manual/R-devel/library/stats/html/quantile.html
   */
  val Type6: Quantiser = (p: Double, n: Long) => TypeX(p, n, p)

  /**
   * Compute quantile indices and gamma coefficient according to R's Type 7 rule; p[k] = mode[F(x[k])].
   *
   * @see https://stat.ethz.ch/R-manual/R-devel/library/stats/html/quantile.html
   */
  val Type7: Quantiser = (p: Double, n: Long) => TypeX(p, n, 1 - p)

  /**
   * Compute quantile indices and gamma coefficient according to R's Type 8 rule; p[k] =~ median[F(x[k])]. The
   * resulting quantile estimates are approximately median-unbiased regardless of the distribution of x.
   *
   * @see https://stat.ethz.ch/R-manual/R-devel/library/stats/html/quantile.html
   */
  val Type8: Quantiser = (p: Double, n: Long) => TypeX(p, n, (p + 1) / 3)

  /**
   * Compute quantile indices and gamma coefficient according to R's Type 9 rule; quantile estimates are
   * approximately unbiased for the expected order statistics if x is normally distributed.
   *
   * @see https://stat.ethz.ch/R-manual/R-devel/library/stats/html/quantile.html
   */
  val Type9: Quantiser = (p: Double, n: Long) => TypeX(p, n, (p / 4) + (3.0 / 8))

  private val TypeX = (p: Double, n: Long, m: Double) => {
    val npm = n * p + m
    val j = math.floor(npm).toLong

    (j, npm - j)
  }
}

private[grimlock] case class QuantileImpl[
  P <: Nat,
  S <: Nat,
  Q <: Nat
](
  probs: List[Double],
  quantiser: Quantile.Quantiser,
  position: Locate.FromSelectedAndOutput[S, Double, Q],
  nan: Boolean
) {
  type C = (Long, List[(Long, Double, Double)])
  type T = (Double, Long, Long)
  type O = (Double, Double)

  def prepare(cell: Cell[P]): Double = cell.content.value.asDouble.getOrElse(Double.NaN)

  def initialise(curr: Double, count: Long): (T, C, List[O]) = {
    val index = 0
    val bounds = QuantileImpl.boundaries(probs, quantiser, count)
    val target = QuantileImpl.next(bounds, index)

    val first =
      if (count == 0)
        bounds.map { case (_, _, p) => (p, Double.NaN) }
      else if (count == 1)
        bounds.map { case (_, _, p) => (p, curr) }
      else
        bounds.collect { case (j, _, p) if (j == index && count != 0) => (p, curr) }

    ((curr, index, target), (count, bounds), first)
  }

  def update(curr: Double, t: T, state: C): (T, List[O]) = {
    val index = t._2 + 1
    val prev = t._1
    val target = t._3
    val count = state._1
    val bounds = state._2

    val last = if (index == (count - 1)) bounds.collect { case (j, _, p) if (j == count) => (p, curr) } else List()

    if (index == target)
      ((curr, index, QuantileImpl.next(bounds, index)),
        last ++ bounds.collect { case (j, g, p) if (j == index) => (p, (1 - g) * prev + g * curr) })
    else
      ((curr, index, target), last)
  }

  def present(pos: Position[S], out: O): Option[Cell[Q]] =
    if (!out._2.isNaN || nan)
      position(pos, out._1).map(Cell(_, Content(ContinuousSchema[Double](), QuantileImpl.round(out._2))))
    else
      None
}

private[grimlock] object QuantileImpl {
  /**
   * Get the quantile boundary indices.
   *
   * @param probs     The quantile probabilities.
   * @param quantiser The quantiser to use.
   * @param count     The number of data points.
   *
   * @return List of `(index, gamma, probability)` tuples for each probability in `probs`.
   */
  def boundaries(
    probs: List[Double],
    quantiser: Quantile.Quantiser,
    count: Long
  ): List[(Long, Double, Double)] = probs
    .sorted
    .map(p => (quantiser(p, count), p))
    .map { case ((j, g), p) => (j, g, p) }

  /**
   * Get the next boundary index give the current index.
   *
   * @param boundaries The list of boundaries.
   * @param index      The current index.
   *
   * @return The next index.
   */
  def next(boundaries: List[(Long, Double, Double)], index: Long): Long = boundaries
    .collectFirst { case (j, _, _) if j > index => j }
    .getOrElse(0)

  /**
   * Round to fixed decimal places.
   *
   * @param value  The value to round.
   * @param places The rounding precision.
   *
   * @return The rounded value.
   */
  def round(value: Double, places: Int = 6): Double =
    if (value.isNaN) value else BigDecimal(value).setScale(places, BigDecimal.RoundingMode.HALF_UP).toDouble
}

/** Defines convenience functions for dealing with count maps. */
private[grimlock] object CountMap {
  /** Type of the underlying data. */
  type T = Map[Double, Long]

  /**
   * Create a count map from a double.
   *
   * @param d The double value to create a count map from.
   *
   * @return A count map.
   */
  def from(d: Double): T = Map(d -> 1L)

  /**
   * Reduce two map counts.
   *
   * @param l The left map count.
   * @param r The right map count.
   *
   * @return A reduced map count.
   */
  def reduce(l: T, r: T): T = {
    val (big, small) = if (l.size > r.size) (l, r) else (r, l)

    big ++ small.map { case (k, v) => k -> big.get(k).map(_ + v).getOrElse(v) }
  }

  /**
   * Return a list of cells from a map count.
   *
   * @param t     A map count object.
   * @param probs The requested quantile probabilities.
   * @param pos   The base position.
   * @param name  Names each quantile output (appends to `pos`).
   * @param nan   Indicator if NaN quantiles should be output or not.
   *
   * @return A list of cells, one for each requested quantile probability.
   */
  def toCells[
    S <: Nat,
    Q <: Nat
  ](
    t: T,
    probs: List[Double],
    pos: Position[S],
    quantiser: Quantile.Quantiser,
    name: Locate.FromSelectedAndOutput[S, Double, Q],
    nan: Boolean
  )(implicit
    ev: GT[Q, S]
  ): List[Cell[Q]] = {
    val (values, counts) = t.toList.sorted.unzip
    val cumsum = counts.scan(0L)(_ + _).tail

    QuantileImpl.boundaries(probs, quantiser, cumsum.last).flatMap(toCell(pos, _, values, cumsum, name, nan))
  }

  private def toCell[
    S <: Nat,
    Q <: Nat
  ](
    pos: Position[S],
    boundary: (Long, Double, Double),
    values: List[Double],
    cumsum: List[Long],
    name: Locate.FromSelectedAndOutput[S, Double, Q],
    nan: Boolean
  ): Option[Cell[Q]] = {
    val (j, g, q) = boundary

    val value =
      if (j == 0)
        values(0)
      else if (j == cumsum.last)
        values.last
      else {
        val curr = values(cumsum.indexWhere(_ > j))
        val prev = values(cumsum.indexWhere(_ > (j - 1)))

        (1 - g) * prev + g * curr
      }

    if (value.isNaN && !nan)
      None
    else
      name(pos, q).map(p => Cell(p, Content(ContinuousSchema[Double](), QuantileImpl.round(value))))
  }
}

/**
 * Defines convenience functions for dealing with TDigest objects.
 *
 * @see https://github.com/tdunning/t-digest
 */
private[grimlock] object TDigest {
  /** Type of the underlying data. */
  type T = AVLTreeDigest

  /**
   * Create a t-digest from a double.
   *
   * @param d           The double value to create t-digest from.
   * @param compression The t-digest compression parameter.
   *
   * @return A `Some(TDigest)` if `d` is not NaN, `None` otherwise.
   */
  def from(d: Double, compression: Double): Option[T] =
    if (d.isNaN)
      None
    else {
      val t = new AVLTreeDigest(compression)

      t.add(d)

      Option(t)
    }

  /**
   * Reduce two t-digests.
   *
   * @param l The left t-digest.
   * @param r The right t-digest.
   *
   * @return A reduced digest.
   */
  def reduce(l: T, r: T): T = {
    l.add(r)

    l
  }

  /**
   * Return a list of cells from a t-digest.
   *
   * @param t     A t-digest object.
   * @param probs The requested quantile probabilities.
   * @param pos   The base position.
   * @param name  Names each quantile output (appends to `pos`).
   * @param nan   Indicator if NaN quantiles should be output or not.
   *
   * @return A list of cells, one for each requested quantile probability.
   */
  def toCells[
    S <: Nat,
    Q <: Nat
  ](
    t: T,
    probs: List[Double],
    pos: Position[S],
    name: Locate.FromSelectedAndOutput[S, Double, Q],
    nan: Boolean
  )(implicit
    ev: GT[Q, S]
  ): List[Cell[Q]] = for {
    q <- probs
    p <- name(pos, q)

    result = t.quantile(q)

    if (!result.isNaN || nan)
  } yield Cell(p, Content(ContinuousSchema[Double](), result))
}

/**
 * Implements a streaming histogram.
 *
 * @param maxBins The maximum number of bins of the histogram.
 * @param pairs   The histogram bin pairs (centroid, count).
 *
 * @see http://www.jmlr.org/papers/volume11/ben-haim10a/ben-haim10a.pdf
 */
private[grimlock] case class StreamingHistogram(maxBins: Long, pairs: SortedMap[Double, Long] = SortedMap()) {
  /**
   * Implement the update procedure; Algorithm 1 from the above paper.
   *
   * @param p The point to add.
   *
   * @return An updated histogram.
   */
  def update(p: Double): StreamingHistogram = {
    val updated = if (pairs.contains(p)) pairs.updated(p, pairs(p) + 1) else compact(pairs + (p -> 1))

    StreamingHistogram(maxBins, updated)
  }

  /**
   * Implement the merge procedure; Algorithm 2 from the above paper.
   *
   * @param that The histogram to merge with.
   *
   * @return A merged histogram.
   */
  def merge(that: StreamingHistogram): StreamingHistogram = {
    val (big, small) = if (pairs.size > that.pairs.size) (pairs, that.pairs) else (that.pairs, pairs)

    StreamingHistogram(maxBins, compact(big ++ small.map { case (k, v) => k -> big.get(k).map(_ + v).getOrElse(v) }))
  }

  /**
   * Implement the sum procedure; Algorithm 3 from the above paper.
   *
   * @param b The point for which to return the estimated number of points in the interval [-Inf, b].
   *
   * @return The estimated number of points in the interval [-Inf, b].
   */
  def sum(b: Double): Long = {
    val p = pairs.keys.toList
    val i = p.lastIndexWhere(_ <= b)

    if (i == p.size - 1)
      pairs.values.sum
    else {
      val pi = p(i)
      val pj = p(i + 1)
      val mi = pairs(pi)
      val mj = pairs(pj)
      val mb = mi + ((mj - mi) / (pj - pi)) * (b - pi)
      val s = ((mi + mb) / 2.0) * ((b - pi) / (pj - pi))

      math.round(s + (mi / 2.0) + pairs.values.slice(0, i).sum)
    }
  }

  /**
   * Implement the uniform procedure; Algorithm 4 from the above paper.
   *
   * @param b The number of bins to return.
   *
   * @return A list of boundaries such that between any 2 consecutive boundaries there approximately the same
   *         number of points.
   */
  def uniform(b: Long): List[Double] = {
    val p = pairs.keys.toList
    val m = pairs.values.toList

    val sums = p.map(sum)
    val summ = m.sum.toDouble / b

    (1L to b - 1)
      .toList
      .map { case j =>
        val s = j * summ
        val i = sums.lastIndexWhere(_ < s)
        val d = s - sums(i)
        val pi = p(i)
        val pj = p(i + 1)
        val mi = m(i)
        val mj = m(i + 1)
        val a = mj - mi
        val b = 2 * mi
        val c = -2 * d
        val z = (-b + math.sqrt(b * b - 4 * a * c)) / (2 * a)

        pi + (pj - pi) * z
      }
  }

  private def compact(h: SortedMap[Double, Long]): SortedMap[Double, Long] =
    if (h.size <= maxBins)
      h
    else {
      val q = h.keys.toList
      val i = q
        .zip(q.tail)
        .map { case (i, j) => j - i }
        .zipWithIndex
        .sortBy(_._1)
        .head
        ._2
      val qi = q(i)
      val qj = q(i + 1)
      val ki = h(qi)
      val kj = h(qj)
      val merged = ((qi * ki + qj * kj) / (ki + kj)) -> (ki + kj)

      compact(h - qi - qj + merged)
    }
}

/**
 * Defines convenience functions for dealing with streaming histograms.
 *
 * @see http://www.jmlr.org/papers/volume11/ben-haim10a/ben-haim10a.pdf
 */
private[grimlock] object StreamingHistogram {
  /** Type of the underlying data. */
  type T = StreamingHistogram

  /**
   * Create a streaming histogram from a double.
   *
   * @param d       The double value to create streaming histogram from.
   * @param maxBins The maximum number of bins of the histogram.
   *
   * @return A `StreamingHistogram`.
   */
  def from(d: Double, maxBins: Long): T = StreamingHistogram(maxBins).update(d)

  /**
   * Reduce two streaming histograms.
   *
   * @param l The left streaming histogram.
   * @param r The right streaming histogram.
   *
   * @return A reduced streaming histogram.
   */
  def reduce(l: T, r: T): T = l.merge(r)

  /**
   * Return a list of cells from a streaming histogram.
   *
   * @param t     A StreamingHistogram.
   * @param bins  The number of quantiles to return.
   * @param pos   The base position.
   * @param name  Names each quantile output (appends to `pos`).
   * @param nan   Indicator if NaN quantiles should be output or not.
   *
   * @return A list of cells, of `bins` uniformly spaced quantiles.
   */
  def toCells[
    S <: Nat,
    Q <: Nat
  ](
    t: T,
    bins: Long,
    pos: Position[S],
    name: Locate.FromSelectedAndOutput[S, Double, Q],
    nan: Boolean
  )(implicit
    ev: GT[Q, S]
  ): List[Cell[Q]] = t
    .uniform(bins)
    .zipWithIndex
    .flatMap { case (u, i) =>
      if (u.isNaN && !nan)
        None
      else
        name(pos, (i + 1) / bins.toDouble).map(p => Cell(p, Content(ContinuousSchema[Double], u)))
    }
}

