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

package commbank.grimlock.spark

import au.com.cba.omnia.ebenezer.scrooge.ScroogeReadSupport

import commbank.grimlock.framework.{
  Cell,
  Consume,
  Compactable,
  Default,
  ExtractWithDimension,
  ExtractWithKey,
  Locate,
  Matrix => FwMatrix,
  Matrix1D => FwMatrix1D,
  Matrix2D => FwMatrix2D,
  Matrix3D => FwMatrix3D,
  Matrix4D => FwMatrix4D,
  Matrix5D => FwMatrix5D,
  Matrix6D => FwMatrix6D,
  Matrix7D => FwMatrix7D,
  Matrix8D => FwMatrix8D,
  Matrix9D => FwMatrix9D,
  NoParameters,
  Redistribute,
  ReducibleMatrix => FwReducibleMatrix,
  Reducers,
  ReshapeableMatrix => FwReshapeableMatrix,
  Sequence,
  Stream,
  Tuner,
  TunerParameters,
  Type
}
import commbank.grimlock.framework.aggregate.{ Aggregator, AggregatorWithValue }
import commbank.grimlock.framework.content.Content
import commbank.grimlock.framework.content.metadata.{ DiscreteSchema, NominalSchema }
import commbank.grimlock.framework.DefaultTuners.{ TP1, TP2, TP3 }
import commbank.grimlock.framework.encoding.Value
import commbank.grimlock.framework.pairwise.{ Comparer, Diagonal, Lower, Operator, OperatorWithValue, Upper }
import commbank.grimlock.framework.partition.{ Partitioner, PartitionerWithValue }
import commbank.grimlock.framework.position.{ Along, Over, Position, Slice }
import commbank.grimlock.framework.position.Position.listSetAdditiveCollection
import commbank.grimlock.framework.sample.{ Sampler, SamplerWithValue }
import commbank.grimlock.framework.squash.{ Squasher, SquasherWithValue }
import commbank.grimlock.framework.transform.{ Transformer, TransformerWithValue }
import commbank.grimlock.framework.utility.{ =:!=, Distinct, Escape }
import commbank.grimlock.framework.utility.UnionTypes.{ In, Is, OneOf }
import commbank.grimlock.framework.window.{ Window, WindowWithValue }

import commbank.grimlock.spark.distribution.ApproximateDistribution
import commbank.grimlock.spark.environment.{ DistributedData, Environment, UserData }
import commbank.grimlock.spark.SparkImplicits._

import com.twitter.scrooge.ThriftStruct

import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.rdd.RDD

import parquet.hadoop.ParquetInputFormat

import scala.collection.immutable.HashSet
import scala.collection.immutable.ListSet
import scala.reflect.{ classTag, ClassTag }

import shapeless.{ Nat, Succ, Witness }
import shapeless.nat.{ _0, _1, _2, _3, _4, _5, _6, _7, _8, _9 }
import shapeless.ops.nat.{ Diff, LTEq, GT, GTEq, ToInt }
import shapeless.syntax.sized._

private[spark] object SparkImplicits {
  implicit class RDDTuner[T](rdd: RDD[T]) {
    def tunedDistinct(parameters: TunerParameters)(implicit ev: Ordering[T]): RDD[T] = parameters match {
      case Reducers(reducers) => rdd.distinct(reducers)(ev)
      case _ => rdd.distinct()
    }

    def redistribute(parameters: TunerParameters): RDD[T] = parameters match {
      case Redistribute(reducers) => rdd.repartition(reducers)
      case _ => rdd
    }
  }

  implicit class PairRDDTuner[K, V](rdd: RDD[(K, V)])(implicit kt: ClassTag[K], vt: ClassTag[V]) {
    def tunedJoin[W](parameters: TunerParameters, other: RDD[(K, W)]): RDD[(K, (V, W))] = parameters match {
      case Reducers(reducers) => rdd.join(other, reducers)
      case _ => rdd.join(other)
    }

    def tunedLeftJoin[W](parameters: TunerParameters, other: RDD[(K, W)]): RDD[(K, (V, Option[W]))] = parameters match {
      case Reducers(reducers) => rdd.leftOuterJoin(other, reducers)
      case _ => rdd.leftOuterJoin(other)
    }

    def tunedOuterJoin[
      W
    ](
      parameters: TunerParameters,
      other: RDD[(K, W)]
    ): RDD[(K, (Option[V], Option[W]))] = parameters match {
      case Reducers(reducers) => rdd.fullOuterJoin(other, reducers)
      case _ => rdd.fullOuterJoin(other)
    }

    def tunedReduce(parameters: TunerParameters, reduction: (V, V) => V): RDD[(K, V)] = parameters match {
      case Reducers(reducers) => rdd.reduceByKey(reduction, reducers)
      case _ => rdd.reduceByKey(reduction)
    }

    def tunedGroupByKey(parameters: TunerParameters): RDD[(K, Iterable[V])] = parameters match {
      case Reducers(reducers) => rdd.groupByKey(reducers)
      case _ => rdd.groupByKey
    }
  }
}

/** Base trait for matrix operations using a `RDD[Cell[P]]`. */
trait Matrix[L <: Nat, P <: Nat] extends FwMatrix[L, P] with Persist[Cell[P]] with UserData {
  protected implicit def positionOrdering[N <: Nat] = Position.ordering[N]()

  type ChangeTuners[T] = TP2[T]
  def change[
    T <: Tuner : ChangeTuners
  ](
    slice: Slice[L, P],
    tuner: T = Default()
  )(
    positions: U[Position[slice.S]],
    schema: Content.Parser,
    writer: TextWriter
  )(implicit
    ev1: ClassTag[Position[slice.S]],
    ev2: Diff.Aux[P, _1, L]
  ): (U[Cell[P]], U[String]) = {
    val result = data
      .keyBy(c => slice.selected(c.position))
      .tunedLeftJoin(tuner.parameters, positions.keyBy(p => p))
      .flatMap { case (_, (c, po)) =>
        po match {
          case Some(_) => schema(c.content.value.toShortString)
            .map(con => List(Right(Cell(c.position, con))))
            .getOrElse(writer(c).map(Left(_)))
          case None => List(Right(c))
        }
      }

    (result.collect { case Right(cell) => cell }, result.collect { case Left(error) => error })
  }

  type CompactTuners[T] = TP2[T]
  def compact()(implicit ev: ClassTag[Position[P]]): E[Map[Position[P], Content]] = data
    .map(c => (c.position, c.content))
    .collectAsMap
    .toMap

  def compact[
    T <: Tuner : CompactTuners
  ](
    slice: Slice[L, P],
    tuner: T = Default()
  )(implicit
    ev1: slice.S =:!= _0,
    ev2: ClassTag[Position[slice.S]],
    ev3: Compactable[L, P],
    ev4: Diff.Aux[P, _1, L]
  ): E[Map[Position[slice.S], ev3.C[slice.R]]] = data
    .map(c => (slice.selected(c.position), ev3.toMap(slice)(c)))
    .tunedReduce(tuner.parameters, (l, r) => ev3.combineMaps(slice)(l, r))
    .values
    .reduce { case (lm, rm) => lm ++ rm }

  type DomainTuners[T] = TP1[T]

  type FillHeterogeneousTuners[T] = TP3[T]
  def fillHeterogeneous[
    T <: Tuner : FillHeterogeneousTuners
  ](
    slice: Slice[L, P],
    tuner: T = Default()
  )(
    values: U[Cell[slice.S]]
  )(implicit
    ev1: ClassTag[Position[P]],
    ev2: ClassTag[Position[slice.S]],
    ev3: Diff.Aux[P, _1, L]
  ): U[Cell[P]] = {
    val (p1, p2) = tuner.parameters match {
      case Sequence(f, s) => (f, s)
      case p => (NoParameters(), p)
    }

    domain(Default())
      .keyBy(p => slice.selected(p))
      .tunedJoin(p1, values.keyBy(c => c.position))
      .map { case (_, (p, c)) => (p, Cell(p, c.content)) }
      .tunedLeftJoin(p2, data.keyBy(c => c.position))
      .map { case (_, (c, co)) => co.getOrElse(c) }
  }

  type FillHomogeneousTuners[T] = TP2[T]
  def fillHomogeneous[
    T <: Tuner : FillHomogeneousTuners
  ](
    value: Content,
    tuner: T = Default()
  )(implicit
    ev1: ClassTag[Position[P]]
  ): U[Cell[P]] = domain(Default())
    .keyBy(p => p)
    .tunedLeftJoin(tuner.parameters, data.keyBy(c => c.position))
    .map { case (p, (_, co)) => co.getOrElse(Cell(p, value)) }

  type GetTuners[T] = TP2[T]
  def get[
    T <: Tuner : GetTuners
  ](
    positions: U[Position[P]],
    tuner: T = Default()
  )(implicit
    ev: ClassTag[Position[P]]
  ): U[Cell[P]] = data
    .keyBy(c => c.position)
    .tunedJoin(tuner.parameters, positions.keyBy(p => p))
    .map { case (_, (c, _)) => c }

  type JoinTuners[T] = TP3[T]
  def join[
    T <: Tuner : JoinTuners
  ](
    slice: Slice[L, P],
    tuner: T = Default()
  )(
    that: U[Cell[P]]
  )(implicit
    ev1: P =:!= _1,
    ev2: ClassTag[Position[slice.S]],
    ev3: Diff.Aux[P, _1, L]
  ): U[Cell[P]] = {
    val (p1, p2) = tuner.parameters match {
      case Sequence(f, s) => (f, s)
      case p => (NoParameters(), p)
    }

    val keep = data
      .map(c => slice.selected(c.position))
      .tunedDistinct(tuner.parameters)
      .map(p => (p, ()))
      .tunedJoin(p1, that
        .map(c => slice.selected(c.position))
        .tunedDistinct(tuner.parameters)
        .map(p => (p, ())))

    (data ++ that)
      .keyBy(c => slice.selected(c.position))
      .tunedJoin(p2, keep)
      .map { case (_, (c, _)) => c }
  }

  type MaterialiseTuners[T] = TP1[T]
  def materialise[T <: Tuner : MaterialiseTuners](tuner: T = Default()): List[Cell[P]] = data.collect.toList

  type NamesTuners[T] = TP2[T]
  def names[
    T <: Tuner : NamesTuners
  ](
    slice: Slice[L, P],
    tuner: T = Default()
  )(implicit
    ev1: slice.S =:!= _0,
    ev2: ClassTag[Position[slice.S]],
    ev3: Diff.Aux[P, _1, L]
  ): U[Position[slice.S]] = data
    .map(c => slice.selected(c.position))
    .tunedDistinct(tuner.parameters)

  type PairwiseTuners[T] = T In OneOf[Default[NoParameters]]#
    Or[Default[Reducers]]#
    Or[Default[Sequence[Reducers, Reducers]]]#
    Or[Default[Sequence[Reducers, Sequence[Reducers, Reducers]]]]#
    Or[Default[Sequence[Sequence[Reducers, Reducers], Reducers]]]#
    Or[Default[Sequence[Sequence[Reducers, Reducers], Sequence[Reducers, Reducers]]]]
  def pairwise[
    Q <: Nat,
    T <: Tuner : PairwiseTuners
  ](
    slice: Slice[L, P],
    tuner: T = Default()
  )(
    comparer: Comparer,
    operators: Operator[P, Q]*
  )(implicit
    ev1: slice.S =:!= _0,
    ev2: GT[Q, slice.R],
    ev3: ClassTag[Position[slice.S]],
    ev4: ClassTag[Position[slice.R]],
    ev5: Diff.Aux[P, _1, L]
  ): U[Cell[Q]] = {
    val operator: Operator[P, Q] = if (operators.size == 1) operators.head else operators.toList

    pairwiseTuples(slice, comparer, data, data, tuner).flatMap { case (lc, rc) => operator.compute(lc, rc) }
  }

  def pairwiseWithValue[
    Q <: Nat,
    W,
    T <: Tuner : PairwiseTuners
  ](
    slice: Slice[L, P],
    tuner: T = Default()
  )(
    comparer: Comparer,
    value: E[W],
    operators: OperatorWithValue[P, Q] { type V >: W }*
  )(implicit
    ev1: slice.S =:!= _0,
    ev2: GT[Q, slice.R],
    ev3: ClassTag[Position[slice.S]],
    ev4: ClassTag[Position[slice.R]],
    ev5: Diff.Aux[P, _1, L]
  ): U[Cell[Q]] = {
    val operator: OperatorWithValue[P, Q] { type V >: W } =
      if (operators.size == 1) operators.head else operators.toList

    pairwiseTuples(slice, comparer, data, data, tuner)
      .flatMap { case (lc, rc) => operator.computeWithValue(lc, rc, value) }
  }

  def pairwiseBetween[
    Q <: Nat,
    T <: Tuner : PairwiseTuners
  ](
    slice: Slice[L, P],
    tuner: T = Default()
  )(
    comparer: Comparer,
    that: U[Cell[P]],
    operators: Operator[P, Q]*
  )(implicit
    ev1: slice.S =:!= _0,
    ev2: GT[Q, slice.R],
    ev3: ClassTag[Position[slice.S]],
    ev4: ClassTag[Position[slice.R]],
    ev5: Diff.Aux[P, _1, L]
  ): U[Cell[Q]] = {
    val operator: Operator[P, Q] = if (operators.size == 1) operators.head else operators.toList

    pairwiseTuples(slice, comparer, data, that, tuner).flatMap { case (lc, rc) => operator.compute(lc, rc) }
  }

  def pairwiseBetweenWithValue[
    Q <: Nat,
    W,
    T <: Tuner : PairwiseTuners
  ](
    slice: Slice[L, P],
    tuner: T = Default()
  )(
    comparer: Comparer,
    that: U[Cell[P]],
    value: E[W],
    operators: OperatorWithValue[P, Q] { type V >: W }*
  )(implicit
    ev1: slice.S =:!= _0,
    ev2: GT[Q, slice.R],
    ev3: ClassTag[Position[slice.S]],
    ev4: ClassTag[Position[slice.R]],
    ev5: Diff.Aux[P, _1, L]
  ): U[Cell[Q]] = {
    val operator: OperatorWithValue[P, Q] { type V >: W } =
      if (operators.size == 1) operators.head else operators.toList

    pairwiseTuples(slice, comparer, data, that, tuner)
      .flatMap { case (lc, rc) => operator.computeWithValue(lc, rc, value) }
  }

  def relocate[Q <: Nat](locate: Locate.FromCell[P, Q])(implicit ev: GTEq[Q, P]): U[Cell[Q]] = data
    .flatMap(c => locate(c).map(Cell(_, c.content)))

  def relocateWithValue[
    Q <: Nat,
    W
  ](
    value: E[W],
    locate: Locate.FromCellWithValue[P, Q, W]
  )(implicit
    ev: GTEq[Q, P]
  ): U[Cell[Q]] = data.flatMap(c => locate(c, value).map(Cell(_, c.content)))

  type SaveAsIVTuners[T] = PersistReduceAndParition[T]

  type SaveAsTextTuners[T] = PersistParition[T]
  def saveAsText[
    T <: Tuner : SaveAsTextTuners
  ](
    ctx: C,
    file: String,
    writer: TextWriter,
    tuner: T = Default()
  ): U[Cell[P]] = saveText(ctx, file, writer, tuner)

  type SetTuners[T] = TP2[T]
  def set[
    T <: Tuner : SetTuners
  ](
    values: U[Cell[P]],
    tuner: T = Default()
  )(implicit
    ev: ClassTag[Position[P]]
  ): U[Cell[P]] = data
    .keyBy(c => c.position)
    .tunedOuterJoin(tuner.parameters, values.keyBy(c => c.position))
    .map { case (_, (co, cn)) => cn.getOrElse(co.get) }

  type ShapeTuners[T] = TP2[T]
  def shape[T <: Tuner : ShapeTuners](tuner: T = Default()): U[Cell[_1]] = data
    .flatMap(c => c.position.coordinates.map(_.toString).zipWithIndex)
    .tunedDistinct(tuner.parameters)
    .groupBy { case (s, i) => i }
    .map { case (i, s) => Cell(Position(i + 1), Content(DiscreteSchema[Long](), s.size)) }

  type SizeTuners[T] = TP2[T]
  def size[
    T <: Tuner : SizeTuners
  ](
    dim: Nat,
    distinct: Boolean,
    tuner: T = Default()
  )(implicit
    ev1: LTEq[dim.N, P],
    ev2: ToInt[dim.N],
    ev3: Witness.Aux[dim.N]
  ): U[Cell[_1]] = {
    val coords = data.map(c => c.position(ev3.value))
    val dist = if (distinct) coords else coords.tunedDistinct(tuner.parameters)(Value.ordering)

    dist
      .context
      .parallelize(List(Cell(Position(Nat.toInt[dim.N]), Content(DiscreteSchema[Long](), dist.count))))
  }

  type SliceTuners[T] = TP2[T]
  def slice[
    T <: Tuner : SliceTuners
  ](
    slice: Slice[L, P],
    tuner: T = Default()
  )(
    keep: Boolean,
    positions: U[Position[slice.S]]
  )(implicit
    ev1: ClassTag[Position[slice.S]],
    ev2: Diff.Aux[P, _1, L]
  ): U[Cell[P]] = data
    .keyBy(c => slice.selected(c.position))
    .tunedLeftJoin(tuner.parameters, positions.map(p => (p, ())))
    .collect { case (_, (c, o)) if (o.isEmpty != keep) => c }

  type SlideTuners[T] = TP1[T]
  def slide[
    Q <: Nat,
    T <: Tuner : SlideTuners
  ](
    slice: Slice[L, P],
    tuner: T = Default()
  )(
    ascending: Boolean,
    windows: Window[P, slice.S, slice.R, Q]*
  )(implicit
    ev1: slice.R =:!= _0,
    ev2: GT[Q, slice.S],
    ev3: ClassTag[Position[slice.S]],
    ev4: ClassTag[Position[slice.R]],
    ev5: Diff.Aux[P, _1, L]
  ): U[Cell[Q]] = {
    val window: Window[P, slice.S, slice.R, Q] = if (windows.size == 1) windows.head else windows.toList

    data
      .map(c => (slice.selected(c.position), (slice.remainder(c.position), window.prepare(c))))
      .groupByKey
      .flatMap { case (p, itr) =>
        itr
          .toList
          .sortBy { case (r, _) => r }(Position.ordering(ascending))
          .scanLeft(Option.empty[(window.T, TraversableOnce[window.O])]) {
            case (None, (r, i)) => Option(window.initialise(r, i))
            case (Some((t, _)), (r, i)) => Option(window.update(r, i, t))
          }
          .flatMap {
            case Some((_, o)) => o.flatMap(window.present(p, _))
            case _ => List()
          }
      }
  }

  def slideWithValue[
    Q <: Nat,
    W,
    T <: Tuner : SlideTuners
  ](
    slice: Slice[L, P],
    tuner: T = Default()
  )(
    ascending: Boolean,
    value: E[W],
    windows: WindowWithValue[P, slice.S, slice.R, Q] { type V >: W }*
  )(implicit
    ev1: slice.R =:!= _0,
    ev2: GT[Q, slice.S],
    ev3: ClassTag[Position[slice.S]],
    ev4: ClassTag[Position[slice.R]],
    ev5: Diff.Aux[P, _1, L]
  ): U[Cell[Q]] = {
    val window: WindowWithValue[P, slice.S, slice.R, Q] { type V >: W } =
      if (windows.size == 1) windows.head else windows.toList

    data
      .map(c => (slice.selected(c.position), (slice.remainder(c.position), window.prepareWithValue(c, value))))
      .groupByKey
      .flatMap { case (p, itr) =>
        itr
          .toList
          .sortBy { case (r, _) => r }(Position.ordering(ascending))
          .scanLeft(Option.empty[(window.T, TraversableOnce[window.O])]) {
            case (None, (r, i)) => Option(window.initialise(r, i))
            case (Some((t, _)), (r, i)) => Option(window.update(r, i, t))
          }
          .flatMap {
            case Some((_, o)) => o.flatMap(window.presentWithValue(p, _, value))
            case _ => List()
          }
      }
  }

  def split[I](partitioners: Partitioner[P, I]*): U[(I, Cell[P])] = {
    val partitioner: Partitioner[P, I] = if (partitioners.size == 1) partitioners.head else partitioners.toList

    data.flatMap(c => partitioner.assign(c).map(q => (q, c)))
  }

  def splitWithValue[
    I,
    W
  ](
    value: E[W],
    partitioners: PartitionerWithValue[P, I] { type V >: W }*
  ): U[(I, Cell[P])] = {
    val partitioner: PartitionerWithValue[P, I] { type V >: W } =
      if (partitioners.size == 1) partitioners.head else partitioners.toList

    data.flatMap(c => partitioner.assignWithValue(c, value).map(q => (q, c)))
  }

  type StreamTuners[T] = T Is Default[Reducers]
  def stream[
    Q <: Nat,
    T <: Tuner : StreamTuners
  ](
    command: String,
    files: List[String],
    writer: TextWriter,
    parser: Cell.TextParser[Q],
    hash: (Position[P]) => Int,
    tuner: T = Default(Reducers(1))
  ): (U[Cell[Q]], U[String]) = {
    val reducers = tuner.parameters match { case Reducers(r) => r }

    val result = data
      .flatMap(c => writer(c).map(s => (hash(c.position) % reducers, s)))
      .tunedGroupByKey(tuner.parameters)
      .flatMap { case (key, itr) => Stream.delegate(command, files)(key, itr.toIterator) }
      .flatMap(parser)

    (result.collect { case Right(c) => c }, result.collect { case Left(e) => e })
  }

  def streamByPosition[Q <: Nat, T <: Tuner : StreamTuners](
    slice: Slice[L, P],
    tuner: T
  )(
    command: String,
    files: List[String],
    writer: TextWriterByPosition,
    parser: Cell.TextParser[Q]
  )(implicit
    ev1: GTEq[Q, slice.S],
    ev2: ClassTag[slice.S],
    ev3: Diff.Aux[P, _1, L]
  ): (U[Cell[Q]], U[String]) = {
    val reducers = tuner.parameters match { case Reducers(r) => r }
    val murmur = new scala.util.hashing.MurmurHash3.ArrayHashing[Value]()
    val (rows, _) = pivot(slice, tuner.parameters)

    val result = rows
      .flatMap { case (key, list) =>
        writer(list.map(_._2)).map(s => (murmur.hash(key.coordinates.toArray) % reducers, s))
      }
      .tunedGroupByKey(tuner.parameters)
      .flatMap { case (key, itr) => Stream.delegate(command, files)(key, itr.toIterator) }
      .flatMap(parser)

    (result.collect { case Right(c) => c }, result.collect { case Left(e) => e })
  }

  def subset(samplers: Sampler[P]*): U[Cell[P]] = {
    val sampler: Sampler[P] = if (samplers.size == 1) samplers.head else samplers.toList

    data.filter { case c => sampler.select(c) }
  }

  def subsetWithValue[W](value: E[W], samplers: SamplerWithValue[P] { type V >: W }*): U[Cell[P]] = {
    val sampler: SamplerWithValue[P] { type V >: W } = if (samplers.size == 1) samplers.head else samplers.toList

    data.filter { case c => sampler.selectWithValue(c, value) }
  }

  type SummariseTuners[T] = TP2[T]
  def summarise[
    Q <: Nat,
    T <: Tuner : SummariseTuners
  ](
    slice: Slice[L, P],
    tuner: T = Default()
  )(
    aggregators: Aggregator[P, slice.S, Q]*
  )(implicit
    ev1: GTEq[Q, slice.S],
    ev2: ClassTag[Position[slice.S]],
    ev3: Aggregator.Validate[P, slice.S, Q],
    ev4: Diff.Aux[P, _1, L]
  ): U[Cell[Q]] = {
    val aggregator = ev3.check(aggregators)

    implicit val tag = aggregator.tTag

    data
      .flatMap(c => aggregator.prepare(c).map(t => (slice.selected(c.position), t)))
      .tunedReduce(tuner.parameters, (lt, rt) => aggregator.reduce(lt, rt))
      .flatMap { case (p, t) => aggregator.present(p, t) }
  }

  def summariseWithValue[
    Q <: Nat,
    W,
    T <: Tuner : SummariseTuners
  ](
    slice: Slice[L, P],
    tuner: T = Default()
  )(
    value: E[W],
    aggregators: AggregatorWithValue[P, slice.S, Q] { type V >: W }*
  )(implicit
    ev1: GTEq[Q, slice.S],
    ev2: ClassTag[Position[slice.S]],
    ev3: AggregatorWithValue.Validate[P, slice.S, Q, W],
    ev4: Diff.Aux[P, _1, L]
  ): U[Cell[Q]] = {
    val aggregator = ev3.check(aggregators)

    implicit val tag = aggregator.tTag

    data
      .flatMap(c => aggregator.prepareWithValue(c, value).map(t => (slice.selected(c.position), t)))
      .tunedReduce(tuner.parameters, (lt, rt) => aggregator.reduce(lt, rt))
      .flatMap { case (p, t) => aggregator.presentWithValue(p, t, value) }
  }

  def toSequence[K <: Writable, V <: Writable](writer: SequenceWriter[K, V]): U[(K, V)] = data.flatMap(writer(_))

  def toText(writer: TextWriter): U[String] = data.flatMap(writer(_))

  def toVector(melt: (List[Value]) => Value): U[Cell[_1]] = data
    .map { case Cell(p, c) => Cell(Position(melt(p.coordinates)), c) }

  def transform[Q <: Nat](transformers: Transformer[P, Q]*)(implicit ev: GTEq[Q, P]): U[Cell[Q]] = {
    val transformer: Transformer[P, Q] = if (transformers.size == 1) transformers.head else transformers.toList

    data.flatMap(c => transformer.present(c))
  }

  def transformWithValue[
    Q <: Nat,
    W
  ](
    value: E[W],
    transformers: TransformerWithValue[P, Q] { type V >: W }*
  )(implicit
    ev: GTEq[Q, P]
  ): U[Cell[Q]] = {
    val transformer: TransformerWithValue[P, Q] { type V >: W } =
      if (transformers.size == 1) transformers.head else transformers.toList

    data.flatMap(c => transformer.presentWithValue(c, value))
  }

  type TypesTuners[T] = TP2[T]
  def types[
    T <: Tuner : TypesTuners
  ](
    slice: Slice[L, P],
    tuner: T = Default()
  )(
    specific: Boolean
  )(implicit
    ev1: slice.S =:!= _0,
    ev2: ClassTag[Position[slice.S]],
    ev3: Diff.Aux[P, _1, L]
  ): U[Cell[slice.S]] = data
    .map { case Cell(p, c) => (slice.selected(p), c.schema.classification) }
    .tunedReduce(tuner.parameters, (lt, rt) => lt.getCommonType(rt))
    .map { case (p, t) => Cell(p, Content(NominalSchema[Type](), if (specific) t else t.getRootType)) }

  type UniqueTuners[T] = TP2[T]
  def unique[T <: Tuner : UniqueTuners](tuner: T = Default()): U[Content] = {
    val ordering = new Ordering[Content] { def compare(l: Content, r: Content) = l.toString.compare(r.toString) }

    data
      .map(_.content)
      .tunedDistinct(tuner.parameters)(ordering)
  }

  def uniqueByPosition[
    T <: Tuner : UniqueTuners
  ](
    slice: Slice[L, P],
    tuner: T = Default()
  )(implicit
    ev1: slice.S =:!= _0,
    ev2: Diff.Aux[P, _1, L]
  ): U[(Position[slice.S], Content)] = {
    val ordering = new Ordering[Cell[slice.S]] {
      def compare(l: Cell[slice.S], r: Cell[slice.S]) = l.toString().compare(r.toString)
    }

    data
      .map { case Cell(p, c) => Cell(slice.selected(p), c) }
      .tunedDistinct(tuner.parameters)(ordering)
      .map { case Cell(p, c) => (p, c) }
  }

  type WhichTuners[T] = TP2[T]
  def which(predicate: Cell.Predicate[P])(implicit ev: ClassTag[Position[P]]): U[Position[P]] = data
    .collect { case c if predicate(c) => c.position }

  def whichByPosition[
    T <: Tuner : WhichTuners
  ](
    slice: Slice[L, P],
    tuner: T = Default()
  )(
    predicates: List[(U[Position[slice.S]], Cell.Predicate[P])]
  )(implicit
    ev1: ClassTag[Position[slice.S]],
    ev2: ClassTag[Position[P]],
    ev3: Diff.Aux[P, _1, L]
  ): U[Position[P]] = {
    val pp = predicates
      .map { case (pos, pred) => pos.map(p => (p, pred)) }
      .reduce((l, r) => l ++ r)

    data
      .keyBy(c => slice.selected(c.position))
      .tunedJoin(tuner.parameters, pp.keyBy { case (p, pred) => p })
      .collect { case (_, (c, (_, predicate))) if predicate(c) => c.position }
  }

  protected def saveDictionary[
    D <: Nat : ToInt
  ](
    ctx: C,
    dim: D,
    file: String,
    dictionary: String,
    separator: String
  )(implicit
    ev1: LTEq[dim.N, P],
    ev2: Witness.Aux[dim.N],
    ev3: ToInt[dim.N],
    ev4: Diff.Aux[P, L, _1],
    ev5: Diff.Aux[P, _1, L]
  ): U[(Position[_1], Long)] = {
    val numbered = names(Over(dim))
      .zipWithIndex

    numbered
      .map { case (Position(c), i) => c.toShortString + separator + i }
      .redistribute(Redistribute(1))
      .saveAsTextFile(dictionary.format(file, Nat.toInt[D]))

    numbered
  }

  private def pairwiseTuples[
    T <: Tuner : PairwiseTuners
  ](
    slice: Slice[L, P],
    comparer: Comparer,
    ldata: U[Cell[P]],
    rdata: U[Cell[P]],
    tuner: T
  )(implicit
    ev1: ClassTag[Position[slice.S]],
    ev2: Diff.Aux[P, _1, L]
  ): U[(Cell[P], Cell[P])] = {
    val (rr, rj, lr, lj) = tuner.parameters match {
      case Sequence(Sequence(rr, rj), Sequence(lr, lj)) => (rr, rj, lr, lj)
      case Sequence(rj @ Reducers(_), Sequence(lr, lj)) => (NoParameters(), rj, lr, lj)
      case Sequence(Sequence(rr, rj), lj @ Reducers(_)) => (rr, rj, NoParameters(), lj)
      case Sequence(rj @ Reducers(_), lj @ Reducers(_)) => (NoParameters(), rj, NoParameters(), lj)
      case lj @ Reducers(_) => (NoParameters(), NoParameters(), NoParameters(), lj)
      case _ => (NoParameters(), NoParameters(), NoParameters(), NoParameters())
    }

    ldata
      .map(c => slice.selected(c.position))
      .tunedDistinct(lr)
      .cartesian(rdata.map(c => slice.selected(c.position)).tunedDistinct(rr))
      .collect { case (l, r) if comparer.keep(l, r) => (l, r) }
      .keyBy { case (l, _) => l }
      .tunedJoin(lj, ldata.keyBy { case Cell(p, _) => slice.selected(p) })
      .keyBy { case (_, ((_, r),  _)) => r }
      .tunedJoin(rj, rdata.keyBy { case Cell(p, _) => slice.selected(p) })
      .map { case (_, ((_, (_, lc)), rc)) => (lc, rc) }
  }

  protected def pivot(
    slice: Slice[L, P],
    parameters: TunerParameters
  )(implicit
    ev: Diff.Aux[P, _1, L]
  ): (U[(Position[slice.S], List[(Position[slice.R], Option[Cell[P]])])], U[List[Position[slice.R]]]) = {
    val columns = data
      .map(c => ((), HashSet(slice.remainder(c.position))))
      .reduceByKey(_ ++ _)
      .map { case (_, set) => set.toList.sorted }

    val dict = columns.first

    val pivoted = data
      .map(c => (slice.selected(c.position), Map(slice.remainder(c.position) -> c)))
      .tunedReduce(parameters, _ ++ _)
      .map { case (key, map) => (key, dict.map(c => (c, map.get(c)))) }

    (pivoted, columns)
  }
}

/** Base trait for methods that reduce the number of dimensions or that can be filled using a `RDD[Cell[P]]`. */
trait ReducibleMatrix[L <: Nat, P <: Nat] extends FwReducibleMatrix[L, P] { self: Matrix[L, P] =>
  def melt[
    D <: Nat : ToInt,
    I <: Nat : ToInt
  ](
    dim: D,
    into: I,
    merge: (Value, Value) => Value
  )(implicit
    ev1: LTEq[D, P],
    ev2: LTEq[I, P],
    ev3: D =:!= I,
    ev4: Diff.Aux[P, _1, L]
  ): U[Cell[L]] = data.map { case Cell(p, c) => Cell(p.melt(dim, into, merge), c) }

  type SquashTuners[T] = TP2[T]
  def squash[
    D <: Nat : ToInt,
    T <: Tuner : SquashTuners
  ](
    dim: D,
    squasher: Squasher[P],
    tuner: T = Default()
  )(implicit
    ev1: LTEq[D, P],
    ev2: ClassTag[Position[L]],
    ev3: Diff.Aux[P, _1, L]
  ): U[Cell[L]] = {
    implicit val tag: ClassTag[squasher.T] = squasher.tTag

    data
      .flatMap(c => squasher.prepare(c, dim).map(t => (c.position.remove(dim), t)))
      .tunedReduce(tuner.parameters, (lt, rt) => squasher.reduce(lt, rt))
      .flatMap { case (p, t) => squasher.present(t).map(c => Cell(p, c)) }
  }

  def squashWithValue[
    D <: Nat : ToInt,
    W,
    T <: Tuner : SquashTuners
  ](
    dim: D,
    value: E[W],
    squasher: SquasherWithValue[P] { type V >: W },
    tuner: T = Default()
  )(implicit
    ev1: LTEq[D, P],
    ev2: ClassTag[Position[L]],
    ev3: Diff.Aux[P, _1, L]
  ): U[Cell[L]] = {
    implicit val tag: ClassTag[squasher.T] = squasher.tTag

    data
      .flatMap(c => squasher.prepareWithValue(c, dim, value).map(t => (c.position.remove(dim), t)))
      .tunedReduce(tuner.parameters, (lt, rt) => squasher.reduce(lt, rt))
      .flatMap { case (p, t) => squasher.presentWithValue(t, value).map(c => Cell(p, c)) }
  }
}

/** Base trait for methods that reshapes the number of dimension of a matrix using a `RDD[Cell[P]]`. */
trait ReshapeableMatrix[L <: Nat, P <: Nat] extends FwReshapeableMatrix[L, P] { self: Matrix[L, P] =>
  type ReshapeTuners[T] = TP2[T]
  def reshape[
    D <: Nat : ToInt,
    Q <: Nat,
    T <: Tuner : ReshapeTuners
  ](
    dim: D,
    coordinate: Value,
    locate: Locate.FromCellAndOptionalValue[P, Q],
    tuner: T = Default()
  )(implicit
    ev1: LTEq[D, P],
    ev2: GT[Q, P],
    ev3: ClassTag[Position[L]],
    ev4: Diff.Aux[P, _1, L]
  ): U[Cell[Q]] = {
    val keys = data
      .collect[(Position[L], Value)] { case c if (c.position(dim) equ coordinate) =>
        (c.position.remove(dim), c.content.value)
      }

    data
      .collect[(Position[L], Cell[P])] { case c if (c.position(dim) neq coordinate) =>
        (c.position.remove(dim), c)
      }
      .tunedLeftJoin(tuner.parameters, keys)
      .flatMap { case (_, (c, v)) => locate(c, v).map(Cell(_, c.content)) }
  }
}

// TODO: Make this work on more than 2D matrices and share with Scalding
trait MatrixDistance { self: Matrix[_1, _2] with ReducibleMatrix[_1, _2] =>

  import commbank.grimlock.library.aggregate._
  import commbank.grimlock.library.pairwise._
  import commbank.grimlock.library.transform._
  import commbank.grimlock.library.window._

  import commbank.grimlock.spark.environment.Context._

  /**
   * Compute correlations.
   *
   * @param slice  Encapsulates the dimension for which to compute correlations.
   * @param stuner The summarise tuner for the job.
   * @param ptuner The pairwise tuner for the job.
   *
   * @return A `U[Cell[_1]]` with all pairwise correlations.
   */
  def correlation[
    ST <: Tuner : SummariseTuners,
    PT <: Tuner : PairwiseTuners
  ](
    slice: Slice[_1, _2],
    stuner: ST = Default(),
    ptuner: PT = Default()
  )(implicit
    ev1: ClassTag[Position[slice.S]],
    ev2: ClassTag[Position[slice.R]],
    ev3: ToInt[slice.dimension.N],
    ev: Witness.Aux[slice.dimension.N]
  ): U[Cell[_1]] = {
    implicit def x(data: U[Cell[slice.S]]): Matrix1D = Matrix1D(data.asInstanceOf[U[Cell[_1]]])
    implicit def y(data: U[Cell[Succ[slice.R]]]): Matrix2D = Matrix2D(data.asInstanceOf[U[Cell[_2]]])
    implicit val z = new LTEq[slice.dimension.N, _2] { }

    val mean = data
      .summarise(slice, stuner)(Mean())
      .compact(Over(_1))

    val centered = data
      .transformWithValue(
        mean,
        Subtract(ExtractWithDimension[_2, Content](slice.dimension).andThenPresent(_.value.asDouble))
      )

    val denom = centered
      .transform(Power(2))
      .summarise(slice, stuner)(Sum())
      .pairwise(Over(_1), ptuner)(
        Lower,
        Times(Locate.PrependPairwiseSelectedStringToRemainder(Over(_1), "(%1$s*%2$s)"))
      )
      .transform(SquareRoot())
      .compact(Over(_1))

    centered
      .pairwise(slice, ptuner)(
        Lower,
        Times(Locate.PrependPairwiseSelectedStringToRemainder(slice, "(%1$s*%2$s)"))
      )
      .summarise(Over(_1), stuner)(Sum())
      .transformWithValue(denom, Fraction(ExtractWithDimension[_1, Content](_1).andThenPresent(_.value.asDouble)))
  }

  /**
   * Compute mutual information.
   *
   * @param slice  Encapsulates the dimension for which to compute mutual information.
   * @param stuner The summarise tuner for the job.
   * @param ptuner The pairwise tuner for the job.
   *
   * @return A `U[Cell[_1]]` with all pairwise mutual information.
   */
  def mutualInformation[
    ST <: Tuner : SummariseTuners,
    PT <: Tuner : PairwiseTuners
  ](
    slice: Slice[_1, _2],
    stuner: ST = Default(),
    ptuner: PT = Default()
  )(implicit
    ev1: ClassTag[Position[slice.S]],
    ev2: ClassTag[Position[slice.R]],
    ev3: ToInt[slice.dimension.N]
  ): U[Cell[_1]] = {
    implicit def x(data: U[Cell[Succ[slice.R]]]): Matrix2D = Matrix2D(data.asInstanceOf[U[Cell[_2]]])

    val first = _1
    val second = _2

    val dim = Nat.toInt(slice.dimension)

    val s = slice match {
      case Over(_) if (dim == 1) => Along[_2, _3](second)
      case Over(_) => Along[_2, _3](first)
      case Along(_) if (dim == 1) => Along[_2, _3](first)
      case Along(_) => Along[_2, _3](second)
    }

    val extractor = ExtractWithDimension[_2, Content](_1).andThenPresent(_.value.asDouble)

    val mhist = data
      .relocate(c => c.position.append(c.content.value.toShortString).toOption)
      .summarise(s, stuner)(Count())

    val mcount = mhist
      .summarise(Over(_1), stuner)(Sum())
      .compact()

    val marginal = mhist
      .summariseWithValue(Over(_1), stuner)(
        mcount,
        Entropy(extractor).andThenRelocate(_.position.append("marginal").toOption)
      )
      .pairwise(Over(_1), ptuner)(
        Upper,
        Plus(Locate.PrependPairwiseSelectedStringToRemainder[_1, _2](Over(_1), "%s,%s"))
      )

    val jhist = data
      .pairwise(slice, ptuner)(
        Upper,
        Concatenate(Locate.PrependPairwiseSelectedStringToRemainder(slice, "%s,%s"))
      )
      .relocate(c => c.position.append(c.content.value.toShortString).toOption)
      .summarise(Along(_2), stuner)(Count())

    val jcount = jhist
      .summarise(Over(_1), stuner)(Sum())
      .compact()

    val joint = jhist
      .summariseWithValue(Over(_1), stuner)(
        jcount,
        Entropy(extractor, negate = true).andThenRelocate(_.position.append("joint").toOption)
      )

    (marginal ++ joint)
      .summarise(Over(_1), stuner)(Sum())
  }

  /**
   * Compute Gini index.
   *
   * @param slice  Encapsulates the dimension for which to compute the Gini index.
   * @param stuner The summarise tuner for the job.
   * @param wtuner The window tuner for the job.
   * @param ptuner The pairwise tuner for the job.
   *
   * @return A `U[Cell[_1]]` with all pairwise Gini indices.
   */
  def gini[
    ST <: Tuner : SummariseTuners,
    WT <: Tuner : SlideTuners,
    PT <: Tuner : PairwiseTuners
  ](
    slice: Slice[_1, _2],
    stuner: ST = Default(),
    wtuner: WT = Default(),
    ptuner: PT = Default()
  )(implicit
    ev1: ClassTag[Position[slice.S]],
    ev2: ClassTag[Position[slice.R]]
  ): U[Cell[_1]] = {
    implicit def x(data: U[Cell[slice.S]]): Matrix1D = Matrix1D(data.asInstanceOf[U[Cell[_1]]])
    implicit def y(data: U[Cell[Succ[slice.S]]]): Matrix2D = Matrix2D(data.asInstanceOf[U[Cell[_2]]])

    def isPositive = (cell: Cell[_2]) => cell.content.value.asDouble.map(_ > 0).getOrElse(false)
    def isNegative = (cell: Cell[_2]) => cell.content.value.asDouble.map(_ <= 0).getOrElse(false)

    val extractor = ExtractWithDimension[_2, Content](_1).andThenPresent(_.value.asDouble)

    val pos = data
      .transform(Compare(isPositive))
      .summarise(slice, stuner)(Sum(false))
      .compact(Over(_1))

    val neg = data
      .transform(Compare(isNegative))
      .summarise(slice, stuner)(Sum(false))
      .compact(Over(_1))

    val tpr = data
      .transform(Compare(isPositive))
      .slide(slice, wtuner)(true, CumulativeSum(Locate.AppendRemainderString()))
      .transformWithValue(pos, Fraction(extractor))
      .slide(Over(_1), wtuner)(true, BinOp((l, r) => r + l, Locate.AppendPairwiseString("%2$s.%1$s")))

    val fpr = data
      .transform(Compare(isNegative))
      .slide(slice, wtuner)(true, CumulativeSum(Locate.AppendRemainderString()))
      .transformWithValue(neg, Fraction(extractor))
      .slide(Over(_1), wtuner)(true, BinOp((l, r) => r - l, Locate.AppendPairwiseString("%2$s.%1$s")))

    tpr
      .pairwiseBetween(Along(_1), ptuner)(
        Diagonal,
        fpr,
        Times(Locate.PrependPairwiseSelectedStringToRemainder[_1, _2](Along(_1), "(%1$s*%2$s)"))
      )
      .summarise(Along(_1), stuner)(Sum())
      .transformWithValue(Map(Position("one") -> 1.0), Subtract(ExtractWithKey[_1, Double]("one"), true))
  }
}

object Matrix extends Consume with DistributedData with Environment {
  def loadText[P <: Nat](ctx: C, file: String, parser: Cell.TextParser[P]): (U[Cell[P]], U[String]) = {
    val rdd = ctx.context.textFile(file).flatMap(parser(_))

    (rdd.collect { case Right(c) => c }, rdd.collect { case Left(e) => e })
  }

  def loadSequence[
    K <: Writable : Manifest,
    V <: Writable : Manifest,
    P <: Nat
  ](
    ctx: C,
    file: String,
    parser: Cell.SequenceParser[K, V, P]
  ): (U[Cell[P]], U[String]) = {
    val pipe = ctx.context.sequenceFile[K, V](file).flatMap { case (k, v) => parser(k, v) }

    (pipe.collect { case Right(c) => c }, pipe.collect { case Left(e) => e })
  }

  def loadParquet[
    T <: ThriftStruct : Manifest,
    P <: Nat
  ](
    ctx: C,
    file: String,
    parser: Cell.ParquetParser[T, P]
  ): (U[Cell[P]], U[String]) = {
    val job = new Job()

    ParquetInputFormat.setReadSupportClass(job, classOf[ScroogeReadSupport[T]])

    val pipe = ctx.context.newAPIHadoopFile(
      file,
      classOf[ParquetInputFormat[T]],
      classOf[Void],
      classTag[T].runtimeClass.asInstanceOf[Class[T]],
      job.getConfiguration
    ).flatMap { case (_, v) => parser(v) }

    (pipe.collect { case Right(c) => c }, pipe.collect { case Left(e) => e })
  }
}

/**
 * Rich wrapper around a `RDD[Cell[_1]]`.
 *
 * @param data `RDD[Cell[_1]]`.
 */
case class Matrix1D(
  data: RDD[Cell[_1]]
) extends FwMatrix1D
  with Matrix[_0, _1]
  with ApproximateDistribution[_0, _1] {
  def domain[T <: Tuner : DomainTuners](tuner: T = Default()): U[Position[_1]] = names(Over(_1), tuner)

  def saveAsIV[
    T <: Tuner : SaveAsIVTuners
  ](
    ctx: C,
    file: String,
    dictionary: String,
    separator: String,
    tuner: T = Default()
  ): U[Cell[_1]] = {
    val (rc, rd) = tuner.parameters match {
      case Sequence(c, d) => (c, d)
      case c @ Reducers(_) => (c, NoParameters())
      case d @ Redistribute(_) => (NoParameters(), d)
      case _ => (NoParameters(), NoParameters())
    }

    data
      .keyBy(c => c.position)
      .tunedJoin(rc, saveDictionary(ctx, _1, file, dictionary, separator))
      .map { case (_, (c, i)) => i + separator + c.content.value.toShortString }
      .redistribute(rd)
      .saveAsTextFile(file)

    data
  }
}

/**
 * Rich wrapper around a `RDD[Cell[_2]]`.
 *
 * @param data `RDD[Cell[_2]]`.
 */
case class Matrix2D(
  data: RDD[Cell[_2]]
) extends FwMatrix2D
  with Matrix[_1, _2]
  with ReducibleMatrix[_1, _2]
  with ReshapeableMatrix[_1, _2]
  with MatrixDistance
  with ApproximateDistribution[_1, _2] {
  def domain[T <: Tuner : DomainTuners](tuner: T = Default()): U[Position[_2]] = names(Over(_1), tuner)
    .map { case Position(c) => c }
    .cartesian(names(Over(_2), tuner).map { case Position(c) => c })
    .map { case (c1, c2) => Position(c1, c2) }

  def permute[
    D1 <: Nat : ToInt,
    D2 <: Nat : ToInt
  ](
    dim1: D1,
    dim2: D2
  )(implicit
    ev1: LTEq[D1, _2],
    ev2: LTEq[D2, _2],
    ev3: D1 =:!= D2
  ): U[Cell[_2]] = {
    val l = List(Nat.toInt[D1], Nat.toInt[D2]).zipWithIndex.sortBy(_._1).map(_._2)

    data.map { case Cell(p, c) => Cell(p.permute(ListSet(l:_*).sized(2).get), c) }
  }

  type SaveAsCSVTuners[T] = PersistReduceAndParition[T]
  def saveAsCSV[
    T <: Tuner : SaveAsCSVTuners
  ](
    slice: Slice[_1, _2],
    tuner: T = Default()
  )(
    ctx: C,
    file: String,
    separator: String,
    escapee: Escape,
    writeHeader: Boolean,
    header: String,
    writeRowId: Boolean,
    rowId: String
  )(implicit
    ev: ClassTag[Position[slice.S]]
  ): U[Cell[_2]] = {
    val (rc, rd) = tuner.parameters match {
      case Sequence(c, d) => (c, d)
      case c @ Reducers(_) => (c, NoParameters())
      case d @ Redistribute(_) => (NoParameters(), d)
      case _ => (NoParameters(), NoParameters())
    }

    val (pivoted, columns) = pivot(slice, rc)

    if (writeHeader)
      columns
        .map { case lst =>
          (if (writeRowId) escapee.escape(rowId) + separator else "") + lst
            .map(p => escapee.escape(p.coordinates.head.toShortString))
            .mkString(separator)
        }
        .redistribute(Redistribute(1))
        .saveAsTextFile(header.format(file))

    pivoted
      .map { case (p, lst) =>
        (if (writeRowId) escapee.escape(p.coordinates.head.toShortString) + separator else "") + lst
          .map(_._2.map(c => escapee.escape(c.content.value.toShortString)).getOrElse(""))
          .mkString(separator)
      }
      .redistribute(rd)
      .saveAsTextFile(file)

    data
  }

  def saveAsIV[
    T <: Tuner : SaveAsIVTuners
  ](
    ctx: C,
    file: String,
    dictionary: String,
    separator: String,
    tuner: T = Default()
  ): U[Cell[_2]] = {
    val (rc, rd) = tuner.parameters match {
      case Sequence(c, d) => (c, d)
      case c @ Reducers(_) => (c, NoParameters())
      case d @ Redistribute(_) => (NoParameters(), d)
      case _ => (NoParameters(), NoParameters())
    }

    data
      .keyBy { case c => Position(c.position(_1)) }
      .tunedJoin(rc, saveDictionary(ctx, _1, file, dictionary, separator))
      .values
      .keyBy { case (c, i) => Position(c.position(_2)) }
      .tunedJoin(rc, saveDictionary(ctx, _2, file, dictionary, separator))
      .map { case (_, ((c, i), j)) => i + separator + j + separator + c.content.value.toShortString }
      .redistribute(rd)
      .saveAsTextFile(file)

    data
  }

  type SaveAsVWTuners[T] = PersistReduceAndParition[T]
  def saveAsVW[
    T <: Tuner : SaveAsVWTuners
  ](
    slice: Slice[_1, _2],
    tuner: T = Default()
  )(
    ctx: C,
    file: String,
    dictionary: String,
    tag: Boolean,
    separator: String
  )(implicit
    ev: ClassTag[Position[slice.S]]
  ): U[Cell[_2]] = saveVW(slice, tuner)(ctx, file, None, None, tag, dictionary, separator)

  def saveAsVWWithLabels[
    T <: Tuner : SaveAsVWTuners
  ](
    slice: Slice[_1, _2],
    tuner: T = Default()
  )(
    ctx: C,
    file: String,
    labels: U[Cell[slice.S]],
    dictionary: String,
    tag: Boolean,
    separator: String
  )(implicit
    ev: ClassTag[Position[slice.S]]
  ): U[Cell[_2]] = saveVW(slice, tuner)(ctx, file, Some(labels), None, tag, dictionary, separator)

  def saveAsVWWithImportance[
    T <: Tuner : SaveAsVWTuners
  ](
    slice: Slice[_1, _2],
    tuner: T = Default()
  )(
    ctx: C,
    file: String,
    importance: U[Cell[slice.S]],
    dictionary: String,
    tag: Boolean,
    separator: String
  )(implicit
    ev: ClassTag[Position[slice.S]]
  ): U[Cell[_2]] = saveVW(slice, tuner)(ctx, file, None, Some(importance), tag, dictionary, separator)

  def saveAsVWWithLabelsAndImportance[
    T <: Tuner : SaveAsVWTuners
  ](
    slice: Slice[_1, _2],
    tuner: T = Default()
  )(
    ctx: C,
    file: String,
    labels: U[Cell[slice.S]],
    importance: U[Cell[slice.S]],
    dictionary: String,
    tag: Boolean,
    separator: String
  )(implicit
    ev: ClassTag[Position[slice.S]]
  ): U[Cell[_2]] = saveVW(slice, tuner)(ctx, file, Some(labels), Some(importance), tag, dictionary, separator)

  private def saveVW[
    T <: Tuner : SaveAsVWTuners
  ](
    slice: Slice[_1, _2],
    tuner: T
  )(
    ctx: C,
    file: String,
    labels: Option[U[Cell[slice.S]]],
    importance: Option[U[Cell[slice.S]]],
    tag: Boolean,
    dictionary: String,
    separator: String
  )(implicit
    ev: ClassTag[Position[slice.S]]
  ): U[Cell[_2]] = {
    val (rc, rd) = tuner.parameters match {
      case Sequence(c, d) => (c, d)
      case c @ Reducers(_) => (c, NoParameters())
      case d @ Redistribute(_) => (NoParameters(), d)
      case _ => (NoParameters(), NoParameters())
    }

    val (pivoted, columns) = pivot(slice, rc)
    val dict = columns.map(_.zipWithIndex.toMap)

    dict
      .flatMap(_.map { case (p, i) => p.coordinates.head.toShortString + separator + i })
      .redistribute(Redistribute(1))
      .saveAsTextFile(dictionary.format(file))

    val d = dict.first

    val features = pivoted
      .map { case (key, lst) =>
        (
          key,
          lst
            .flatMap { case (p, c) => c.flatMap(_.content.value.asDouble.map(v => d(p) + ":" + v)) }
            .mkString((if (tag) key.coordinates.head.toShortString else "") + "| ", " ", "")
        )
      }

    val weighted = importance match {
      case Some(imp) => features
        .join(imp.keyBy { case c => c.position })
        .flatMap { case (p, (s, c)) => c.content.value.asDouble.map { case i => (p, i + " " + s) } }
      case None => features
    }

    val examples = labels match {
      case Some(lab) => weighted
        .join(lab.keyBy { case c => c.position })
        .flatMap { case (p, (s, c)) => c.content.value.asDouble.map { case l => (p, l + " " + s) } }
      case None => weighted
    }

    examples
      .map { case (p, s) => s }
      .redistribute(rd)
      .saveAsTextFile(file)

    data
  }
}

/**
 * Rich wrapper around a `RDD[Cell[_3]]`.
 *
 * @param data `RDD[Cell[_3]]`.
 */
case class Matrix3D(
  data: RDD[Cell[_3]]
) extends FwMatrix3D
  with Matrix[_2, _3]
  with ReducibleMatrix[_2, _3]
  with ReshapeableMatrix[_2, _3]
  with ApproximateDistribution[_2, _3] {
  def domain[T <: Tuner : DomainTuners](tuner: T = Default()): U[Position[_3]] = names(Over(_1), tuner)
    .map { case Position(c) => c }
    .cartesian(names(Over(_2), tuner).map { case Position(c) => c })
    .cartesian(names(Over(_3), tuner).map { case Position(c) => c })
    .map { case ((c1, c2), c3) => Position(c1, c2, c3) }

  def permute[
    D1 <: Nat : ToInt,
    D2 <: Nat : ToInt,
    D3 <: Nat : ToInt
  ](
    dim1: D1,
    dim2: D2,
    dim3: D3
  )(implicit
    ev1: LTEq[D1, _3],
    ev2: LTEq[D2, _3],
    ev3: LTEq[D3, _3],
    ev4: Distinct[(D1, D2, D3)]
  ): U[Cell[_3]] = {
    val l = List(Nat.toInt[D1], Nat.toInt[D2], Nat.toInt[D3]).zipWithIndex.sortBy(_._1).map(_._2)

    data.map { case Cell(p, c) => Cell(p.permute(ListSet(l:_*).sized(3).get), c) }
  }

  def saveAsIV[
    T <: Tuner : SaveAsIVTuners
  ](
    ctx: C,
    file: String,
    dictionary: String,
    separator: String,
    tuner: T = Default()
  ): U[Cell[_3]] = {
    val (rc, rd) = tuner.parameters match {
      case Sequence(c, d) => (c, d)
      case c @ Reducers(_) => (c, NoParameters())
      case d @ Redistribute(_) => (NoParameters(), d)
      case _ => (NoParameters(), NoParameters())
    }

    data
      .keyBy { case c => Position(c.position(_1)) }
      .tunedJoin(rc, saveDictionary(ctx, _1, file, dictionary, separator))
      .values
      .keyBy { case (c, i) => Position(c.position(_2)) }
      .tunedJoin(rc, saveDictionary(ctx, _2, file, dictionary, separator))
      .map { case (_, ((c, i), j)) => (c, i, j) }
      .keyBy { case (c, i, j) => Position(c.position(_3)) }
      .tunedJoin(rc, saveDictionary(ctx, _3, file, dictionary, separator))
      .map { case (_, ((c, i, j), k)) => i + separator + j + separator + k + separator + c.content.value.toShortString }
      .redistribute(rd)
      .saveAsTextFile(file)

    data
  }
}

/**
 * Rich wrapper around a `RDD[Cell[_4]]`.
 *
 * @param data `RDD[Cell[_4]]`.
 */
case class Matrix4D(
  data: RDD[Cell[_4]]
) extends FwMatrix4D
  with Matrix[_3, _4]
  with ReducibleMatrix[_3, _4]
  with ReshapeableMatrix[_3, _4]
  with ApproximateDistribution[_3, _4] {
  def domain[T <: Tuner : DomainTuners](tuner: T = Default()): U[Position[_4]] = names(Over(_1), tuner)
    .map { case Position(c) => c }
    .cartesian(names(Over(_2), tuner).map { case Position(c) => c })
    .cartesian(names(Over(_3), tuner).map { case Position(c) => c })
    .cartesian(names(Over(_4), tuner).map { case Position(c) => c })
    .map { case (((c1, c2), c3), c4) => Position(c1, c2, c3, c4) }

  def permute[
    D1 <: Nat : ToInt,
    D2 <: Nat : ToInt,
    D3 <: Nat : ToInt,
    D4 <: Nat : ToInt
  ](
    dim1: D1,
    dim2: D2,
    dim3: D3,
    dim4: D4
  )(implicit
    ev1: LTEq[D1, _4],
    ev2: LTEq[D2, _4],
    ev3: LTEq[D3, _4],
    ev4: LTEq[D4, _4],
    ev5: Distinct[(D1, D2, D3, D4)]
  ): U[Cell[_4]] = {
    val l = List(Nat.toInt[D1], Nat.toInt[D2], Nat.toInt[D3], Nat.toInt[D4]).zipWithIndex.sortBy(_._1).map(_._2)

    data.map { case Cell(p, c) => Cell(p.permute(ListSet(l:_*).sized(4).get), c) }
  }

  def saveAsIV[
    T <: Tuner : SaveAsIVTuners
  ](
    ctx: C,
    file: String,
    dictionary: String,
    separator: String,
    tuner: T = Default()
  ): U[Cell[_4]] = {
    val (rc, rd) = tuner.parameters match {
      case Sequence(c, d) => (c, d)
      case c @ Reducers(_) => (c, NoParameters())
      case d @ Redistribute(_) => (NoParameters(), d)
      case _ => (NoParameters(), NoParameters())
    }

    data
      .keyBy { case c => Position(c.position(_1)) }
      .tunedJoin(rc, saveDictionary(ctx, _1, file, dictionary, separator))
      .values
      .keyBy { case (c, i) => Position(c.position(_2)) }
      .tunedJoin(rc, saveDictionary(ctx, _2, file, dictionary, separator))
      .map { case (_, ((c, i), j)) => (c, i, j) }
      .keyBy { case (c, i, j) => Position(c.position(_3)) }
      .tunedJoin(rc, saveDictionary(ctx, _3, file, dictionary, separator))
      .map { case (_, ((c, i, j), k)) => (c, i, j, k) }
      .keyBy { case (c, i, j, p) => Position(c.position(_4)) }
      .tunedJoin(rc, saveDictionary(ctx, _4, file, dictionary, separator))
      .map { case (_, ((c, i, j, k), l)) =>
        i + separator + j + separator + k + separator + l + separator + c.content.value.toShortString
      }
      .redistribute(rd)
      .saveAsTextFile(file)

    data
  }
}

/**
 * Rich wrapper around a `RDD[Cell[_5]]`.
 *
 * @param data `RDD[Cell[_5]]`.
 */
case class Matrix5D(
  data: RDD[Cell[_5]]
) extends FwMatrix5D
  with Matrix[_4, _5]
  with ReducibleMatrix[_4, _5]
  with ReshapeableMatrix[_4, _5]
  with ApproximateDistribution[_4, _5] {
  def domain[T <: Tuner : DomainTuners](tuner: T = Default()): U[Position[_5]] = names(Over(_1), tuner)
    .map { case Position(c) => c }
    .cartesian(names(Over(_2), tuner).map { case Position(c) => c })
    .cartesian(names(Over(_3), tuner).map { case Position(c) => c })
    .cartesian(names(Over(_4), tuner).map { case Position(c) => c })
    .cartesian(names(Over(_5), tuner).map { case Position(c) => c })
    .map { case ((((c1, c2), c3), c4), c5) => Position(c1, c2, c3, c4, c5) }

  def permute[
    D1 <: Nat : ToInt,
    D2 <: Nat : ToInt,
    D3 <: Nat : ToInt,
    D4 <: Nat : ToInt,
    D5 <: Nat : ToInt
  ](
    dim1: D1,
    dim2: D2,
    dim3: D3,
    dim4: D4,
    dim5: D5
  )(implicit
    ev1: LTEq[D1, _5],
    ev2: LTEq[D2, _5],
    ev3: LTEq[D3, _5],
    ev4: LTEq[D4, _5],
    ev5: LTEq[D5, _5],
    ev6: Distinct[(D1, D2, D3, D4, D5)]
  ): U[Cell[_5]] = {
    val l = List(Nat.toInt[D1], Nat.toInt[D2], Nat.toInt[D3], Nat.toInt[D4], Nat.toInt[D5])
      .zipWithIndex
      .sortBy(_._1).map(_._2)

    data.map { case Cell(p, c) => Cell(p.permute(ListSet(l:_*).sized(5).get), c) }
  }

  def saveAsIV[
    T <: Tuner : SaveAsIVTuners
  ](
    ctx: C,
    file: String,
    dictionary: String,
    separator: String,
    tuner: T = Default()
  ): U[Cell[_5]] = {
    val (rc, rd) = tuner.parameters match {
      case Sequence(c, d) => (c, d)
      case c @ Reducers(_) => (c, NoParameters())
      case d @ Redistribute(_) => (NoParameters(), d)
      case _ => (NoParameters(), NoParameters())
    }

    data
      .keyBy { case c => Position(c.position(_1)) }
      .tunedJoin(rc, saveDictionary(ctx, _1, file, dictionary, separator))
      .values
      .keyBy { case (c, i) => Position(c.position(_2)) }
      .tunedJoin(rc, saveDictionary(ctx, _2, file, dictionary, separator))
      .map { case (_, ((c, i), j)) => (c, i, j) }
      .keyBy { case (c, pi, pj) => Position(c.position(_3)) }
      .tunedJoin(rc, saveDictionary(ctx, _3, file, dictionary, separator))
      .map { case (_, ((c, i, j), k)) => (c, i, j, k) }
      .keyBy { case (c, i, j, k) => Position(c.position(_4)) }
      .tunedJoin(rc, saveDictionary(ctx, _4, file, dictionary, separator))
      .map { case (_, ((c, i, j, k), l)) => (c, i, j, k, l) }
      .keyBy { case (c, i, j, k, l) => Position(c.position(_5)) }
      .tunedJoin(rc, saveDictionary(ctx, _5, file, dictionary, separator))
      .map { case (_, ((c, i, j, k, l), m)) =>
        i + separator + j + separator + k + separator + l + separator + m + separator + c.content.value.toShortString
      }
      .redistribute(rd)
      .saveAsTextFile(file)

    data
  }
}

/**
 * Rich wrapper around a `RDD[Cell[_6]]`.
 *
 * @param data `RDD[Cell[_6]]`.
 */
case class Matrix6D(
  data: RDD[Cell[_6]]
) extends FwMatrix6D
  with Matrix[_5, _6]
  with ReducibleMatrix[_5, _6]
  with ReshapeableMatrix[_5, _6]
  with ApproximateDistribution[_5, _6] {
  def domain[T <: Tuner : DomainTuners](tuner: T = Default()): U[Position[_6]] = names(Over(_1), tuner)
    .map { case Position(c) => c }
    .cartesian(names(Over(_2), tuner).map { case Position(c) => c })
    .cartesian(names(Over(_3), tuner).map { case Position(c) => c })
    .cartesian(names(Over(_4), tuner).map { case Position(c) => c })
    .cartesian(names(Over(_5), tuner).map { case Position(c) => c })
    .cartesian(names(Over(_6), tuner).map { case Position(c) => c })
    .map { case (((((c1, c2), c3), c4), c5), c6) => Position(c1, c2, c3, c4, c5, c6) }

  def permute[
    D1 <: Nat : ToInt,
    D2 <: Nat : ToInt,
    D3 <: Nat : ToInt,
    D4 <: Nat : ToInt,
    D5 <: Nat : ToInt,
    D6 <: Nat : ToInt
  ](
    dim1: D1,
    dim2: D2,
    dim3: D3,
    dim4: D4,
    dim5: D5,
    dim6: D6
  )(implicit
    ev1: LTEq[D1, _6],
    ev2: LTEq[D2, _6],
    ev3: LTEq[D3, _6],
    ev4: LTEq[D4, _6],
    ev5: LTEq[D5, _6],
    ev6: LTEq[D6, _6],
    ev7: Distinct[(D1, D2, D3, D4, D5, D6)]
  ): U[Cell[_6]] = {
    val l = List(Nat.toInt[D1], Nat.toInt[D2], Nat.toInt[D3], Nat.toInt[D4], Nat.toInt[D5], Nat.toInt[D6])
      .zipWithIndex
      .sortBy(_._1).map(_._2)

    data.map { case Cell(p, c) => Cell(p.permute(ListSet(l:_*).sized(6).get), c) }
  }

  def saveAsIV[
    T <: Tuner : SaveAsIVTuners
  ](
    ctx: C,
    file: String,
    dictionary: String,
    separator: String,
    tuner: T = Default()
  ): U[Cell[_6]] = {
    val (rc, rd) = tuner.parameters match {
      case Sequence(c, d) => (c, d)
      case c @ Reducers(_) => (c, NoParameters())
      case d @ Redistribute(_) => (NoParameters(), d)
      case _ => (NoParameters(), NoParameters())
    }

    data
      .keyBy { case c => Position(c.position(_1)) }
      .tunedJoin(rc, saveDictionary(ctx, _1, file, dictionary, separator))
      .values
      .keyBy { case (c, i) => Position(c.position(_2)) }
      .tunedJoin(rc, saveDictionary(ctx, _2, file, dictionary, separator))
      .map { case (_, ((c, i), j)) => (c, i, j) }
      .keyBy { case (c, pi, pj) => Position(c.position(_3)) }
      .tunedJoin(rc, saveDictionary(ctx, _3, file, dictionary, separator))
      .map { case (_, ((c, i, j), k)) => (c, i, j, k) }
      .keyBy { case (c, i, j, k) => Position(c.position(_4)) }
      .tunedJoin(rc, saveDictionary(ctx, _4, file, dictionary, separator))
      .map { case (_, ((c, i, j, k), l)) => (c, i, j, k, l) }
      .keyBy { case (c, i, j, k, l) => Position(c.position(_5)) }
      .tunedJoin(rc, saveDictionary(ctx, _5, file, dictionary, separator))
      .map { case (_, ((c, i, j, k, l), m)) => (c, i, j, k, l, m) }
      .keyBy { case (c, i, j, k, l, m) => Position(c.position(_6)) }
      .tunedJoin(rc, saveDictionary(ctx, _6, file, dictionary, separator))
      .map { case (_, ((c, i, j, k, l, m), n)) =>
        i + separator +
        j + separator +
        k + separator +
        l + separator +
        m + separator +
        n + separator +
        c.content.value.toShortString
      }
      .redistribute(rd)
      .saveAsTextFile(file)

    data
  }
}

/**
 * Rich wrapper around a `RDD[Cell[_7]]`.
 *
 * @param data `RDD[Cell[_7]]`.
 */
case class Matrix7D(
  data: RDD[Cell[_7]]
) extends FwMatrix7D
  with Matrix[_6, _7]
  with ReducibleMatrix[_6, _7]
  with ReshapeableMatrix[_6, _7]
  with ApproximateDistribution[_6, _7] {
  def domain[T <: Tuner : DomainTuners](tuner: T = Default()): U[Position[_7]] = names(Over(_1), tuner)
    .map { case Position(c) => c }
    .cartesian(names(Over(_2), tuner).map { case Position(c) => c })
    .cartesian(names(Over(_3), tuner).map { case Position(c) => c })
    .cartesian(names(Over(_4), tuner).map { case Position(c) => c })
    .cartesian(names(Over(_5), tuner).map { case Position(c) => c })
    .cartesian(names(Over(_6), tuner).map { case Position(c) => c })
    .cartesian(names(Over(_7), tuner).map { case Position(c) => c })
    .map { case ((((((c1, c2), c3), c4), c5), c6), c7) => Position(c1, c2, c3, c4, c5, c6, c7) }

  def permute[
    D1 <: Nat : ToInt,
    D2 <: Nat : ToInt,
    D3 <: Nat : ToInt,
    D4 <: Nat : ToInt,
    D5 <: Nat : ToInt,
    D6 <: Nat : ToInt,
    D7 <: Nat : ToInt
  ](
    dim1: D1,
    dim2: D2,
    dim3: D3,
    dim4: D4,
    dim5: D5,
    dim6: D6,
    dim7: D7
  )(implicit
    ev1: LTEq[D1, _7],
    ev2: LTEq[D2, _7],
    ev3: LTEq[D3, _7],
    ev4: LTEq[D4, _7],
    ev5: LTEq[D5, _7],
    ev6: LTEq[D6, _7],
    ev7: LTEq[D7, _7],
    ev8: Distinct[(D1, D2, D3, D4, D5, D6, D7)]
  ): U[Cell[_7]] = {
    val l = List(
        Nat.toInt[D1],
        Nat.toInt[D2],
        Nat.toInt[D3],
        Nat.toInt[D4],
        Nat.toInt[D5],
        Nat.toInt[D6],
        Nat.toInt[D7]
      )
      .zipWithIndex
      .sortBy(_._1).map(_._2)

    data.map { case Cell(p, c) => Cell(p.permute(ListSet(l:_*).sized(7).get), c) }
  }

  def saveAsIV[
    T <: Tuner : SaveAsIVTuners
  ](
    ctx: C,
    file: String,
    dictionary: String,
    separator: String,
    tuner: T = Default()
  ): U[Cell[_7]] = {
    val (rc, rd) = tuner.parameters match {
      case Sequence(c, d) => (c, d)
      case c @ Reducers(_) => (c, NoParameters())
      case d @ Redistribute(_) => (NoParameters(), d)
      case _ => (NoParameters(), NoParameters())
    }

    data
      .keyBy { case c => Position(c.position(_1)) }
      .tunedJoin(rc, saveDictionary(ctx, _1, file, dictionary, separator))
      .values
      .keyBy { case (c, i) => Position(c.position(_2)) }
      .tunedJoin(rc, saveDictionary(ctx, _2, file, dictionary, separator))
      .map { case (_, ((c, i), j)) => (c, i, j) }
      .keyBy { case (c, pi, pj) => Position(c.position(_3)) }
      .tunedJoin(rc, saveDictionary(ctx, _3, file, dictionary, separator))
      .map { case (_, ((c, i, j), k)) => (c, i, j, k) }
      .keyBy { case (c, i, j, k) => Position(c.position(_4)) }
      .tunedJoin(rc, saveDictionary(ctx, _4, file, dictionary, separator))
      .map { case (_, ((c, i, j, k), l)) => (c, i, j, k, l) }
      .keyBy { case (c, i, j, k, l) => Position(c.position(_5)) }
      .tunedJoin(rc, saveDictionary(ctx, _5, file, dictionary, separator))
      .map { case (_, ((c, i, j, k, l), m)) => (c, i, j, k, l, m) }
      .keyBy { case (c, i, j, k, l, m) => Position(c.position(_6)) }
      .tunedJoin(rc, saveDictionary(ctx, _6, file, dictionary, separator))
      .map { case (_, ((c, i, j, k, l, m), n)) => (c, i, j, k, l, m, n) }
      .keyBy { case (c, i, j, k, l, m, n) => Position(c.position(_7)) }
      .tunedJoin(rc, saveDictionary(ctx, _7, file, dictionary, separator))
      .map { case (_, ((c, i, j, k, l, m, n), o)) =>
        i + separator +
        j + separator +
        k + separator +
        l + separator +
        m + separator +
        n + separator +
        o + separator +
        c.content.value.toShortString
      }
      .redistribute(rd)
      .saveAsTextFile(file)

    data
  }
}

/**
 * Rich wrapper around a `RDD[Cell[_8]]`.
 *
 * @param data `RDD[Cell[_8]]`.
 */
case class Matrix8D(
  data: RDD[Cell[_8]]
) extends FwMatrix8D
  with Matrix[_7, _8]
  with ReducibleMatrix[_7, _8]
  with ReshapeableMatrix[_7, _8]
  with ApproximateDistribution[_7, _8] {
  def domain[T <: Tuner : DomainTuners](tuner: T = Default()): U[Position[_8]] = names(Over(_1), tuner)
    .map { case Position(c) => c }
    .cartesian(names(Over(_2), tuner).map { case Position(c) => c })
    .cartesian(names(Over(_3), tuner).map { case Position(c) => c })
    .cartesian(names(Over(_4), tuner).map { case Position(c) => c })
    .cartesian(names(Over(_5), tuner).map { case Position(c) => c })
    .cartesian(names(Over(_6), tuner).map { case Position(c) => c })
    .cartesian(names(Over(_7), tuner).map { case Position(c) => c })
    .cartesian(names(Over(_8), tuner).map { case Position(c) => c })
    .map { case (((((((c1, c2), c3), c4), c5), c6), c7), c8) => Position(c1, c2, c3, c4, c5, c6, c7, c8) }

  def permute[
    D1 <: Nat : ToInt,
    D2 <: Nat : ToInt,
    D3 <: Nat : ToInt,
    D4 <: Nat : ToInt,
    D5 <: Nat : ToInt,
    D6 <: Nat : ToInt,
    D7 <: Nat : ToInt,
    D8 <: Nat : ToInt
  ](
    dim1: D1,
    dim2: D2,
    dim3: D3,
    dim4: D4,
    dim5: D5,
    dim6: D6,
    dim7: D7,
    dim8: D8
  )(implicit
    ev1: LTEq[D1, _8],
    ev2: LTEq[D2, _8],
    ev3: LTEq[D3, _8],
    ev4: LTEq[D4, _8],
    ev5: LTEq[D5, _8],
    ev6: LTEq[D6, _8],
    ev7: LTEq[D7, _8],
    ev8: LTEq[D8, _8],
    ev9: Distinct[(D1, D2, D3, D4, D5, D6, D7, D8)]
  ): U[Cell[_8]] = {
    val l = List(
        Nat.toInt[D1],
        Nat.toInt[D2],
        Nat.toInt[D3],
        Nat.toInt[D4],
        Nat.toInt[D5],
        Nat.toInt[D6],
        Nat.toInt[D7],
        Nat.toInt[D8]
      )
      .zipWithIndex
      .sortBy(_._1).map(_._2)

    data.map { case Cell(p, c) => Cell(p.permute(ListSet(l:_*).sized(8).get), c) }
  }

  def saveAsIV[
    T <: Tuner : SaveAsIVTuners
  ](
    ctx: C,
    file: String,
    dictionary: String,
    separator: String,
    tuner: T = Default()
  ): U[Cell[_8]] = {
    val (rc, rd) = tuner.parameters match {
      case Sequence(c, d) => (c, d)
      case c @ Reducers(_) => (c, NoParameters())
      case d @ Redistribute(_) => (NoParameters(), d)
      case _ => (NoParameters(), NoParameters())
    }

    data
      .keyBy { case c => Position(c.position(_1)) }
      .tunedJoin(rc, saveDictionary(ctx, _1, file, dictionary, separator))
      .values
      .keyBy { case (c, i) => Position(c.position(_2)) }
      .tunedJoin(rc, saveDictionary(ctx, _2, file, dictionary, separator))
      .map { case (_, ((c, i), j)) => (c, i, j) }
      .keyBy { case (c, pi, pj) => Position(c.position(_3)) }
      .tunedJoin(rc, saveDictionary(ctx, _3, file, dictionary, separator))
      .map { case (_, ((c, i, j), k)) => (c, i, j, k) }
      .keyBy { case (c, i, j, k) => Position(c.position(_4)) }
      .tunedJoin(rc, saveDictionary(ctx, _4, file, dictionary, separator))
      .map { case (_, ((c, i, j, k), l)) => (c, i, j, k, l) }
      .keyBy { case (c, i, j, k, l) => Position(c.position(_5)) }
      .tunedJoin(rc, saveDictionary(ctx, _5, file, dictionary, separator))
      .map { case (_, ((c, i, j, k, l), m)) => (c, i, j, k, l, m) }
      .keyBy { case (c, i, j, k, l, m) => Position(c.position(_6)) }
      .tunedJoin(rc, saveDictionary(ctx, _6, file, dictionary, separator))
      .map { case (_, ((c, i, j, k, l, m), n)) => (c, i, j, k, l, m, n) }
      .keyBy { case (c, i, j, k, l, m, n) => Position(c.position(_7)) }
      .tunedJoin(rc, saveDictionary(ctx, _7, file, dictionary, separator))
      .map { case (_, ((c, i, j, k, l, m, n), o)) => (c, i, j, k, l, m, n, o) }
      .keyBy { case (c, i, j, k, l, m, n, o) => Position(c.position(_8)) }
      .tunedJoin(rc, saveDictionary(ctx, _8, file, dictionary, separator))
      .map { case (_, ((c, i, j, k, l, m, n, o), p)) =>
        i + separator +
        j + separator +
        k + separator +
        l + separator +
        m + separator +
        n + separator +
        o + separator +
        p + separator +
        c.content.value.toShortString
      }
      .redistribute(rd)
      .saveAsTextFile(file)

    data
  }
}

/**
 * Rich wrapper around a `RDD[Cell[_9]]`.
 *
 * @param data `RDD[Cell[_9]]`.
 */
case class Matrix9D(
  data: RDD[Cell[_9]]
) extends FwMatrix9D
  with Matrix[_8, _9]
  with ReducibleMatrix[_8, _9]
  with ApproximateDistribution[_8, _9] {
  def domain[T <: Tuner : DomainTuners](tuner: T = Default()): U[Position[_9]] = names(Over(_1), tuner)
    .map { case Position(c) => c }
    .cartesian(names(Over(_2), tuner).map { case Position(c) => c })
    .cartesian(names(Over(_3), tuner).map { case Position(c) => c })
    .cartesian(names(Over(_4), tuner).map { case Position(c) => c })
    .cartesian(names(Over(_5), tuner).map { case Position(c) => c })
    .cartesian(names(Over(_6), tuner).map { case Position(c) => c })
    .cartesian(names(Over(_7), tuner).map { case Position(c) => c })
    .cartesian(names(Over(_8), tuner).map { case Position(c) => c })
    .cartesian(names(Over(_9), tuner).map { case Position(c) => c })
    .map { case ((((((((c1, c2), c3), c4), c5), c6), c7), c8), c9) => Position(c1, c2, c3, c4, c5, c6, c7, c8, c9) }

  def permute[
    D1 <: Nat : ToInt,
    D2 <: Nat : ToInt,
    D3 <: Nat : ToInt,
    D4 <: Nat : ToInt,
    D5 <: Nat : ToInt,
    D6 <: Nat : ToInt,
    D7 <: Nat : ToInt,
    D8 <: Nat : ToInt,
    D9 <: Nat : ToInt
  ](
    dim1: D1,
    dim2: D2,
    dim3: D3,
    dim4: D4,
    dim5: D5,
    dim6: D6,
    dim7: D7,
    dim8: D8,
    dim9: D9
  )(implicit
    ev1: LTEq[D1, _9],
    ev2: LTEq[D2, _9],
    ev3: LTEq[D3, _9],
    ev4: LTEq[D4, _9],
    ev5: LTEq[D5, _9],
    ev6: LTEq[D6, _9],
    ev7: LTEq[D7, _9],
    ev8: LTEq[D8, _9],
    ev9: LTEq[D9, _9],
    ev10: Distinct[(D1, D2, D3, D4, D5, D6, D7, D8, D9)]
  ): U[Cell[_9]] = {
    val l = List(
        Nat.toInt[D1],
        Nat.toInt[D2],
        Nat.toInt[D3],
        Nat.toInt[D4],
        Nat.toInt[D5],
        Nat.toInt[D6],
        Nat.toInt[D7],
        Nat.toInt[D8],
        Nat.toInt[D9]
      )
      .zipWithIndex
      .sortBy(_._1).map(_._2)

    data.map { case Cell(p, c) => Cell(p.permute(ListSet(l:_*).sized(9).get), c) }
  }

  def saveAsIV[
    T <: Tuner : SaveAsIVTuners
  ](
    ctx: C,
    file: String,
    dictionary: String,
    separator: String,
    tuner: T = Default()
  ): U[Cell[_9]] = {
    val (rc, rd) = tuner.parameters match {
      case Sequence(c, d) => (c, d)
      case c @ Reducers(_) => (c, NoParameters())
      case d @ Redistribute(_) => (NoParameters(), d)
      case _ => (NoParameters(), NoParameters())
    }

    data
      .keyBy { case c => Position(c.position(_1)) }
      .tunedJoin(rc, saveDictionary(ctx, _1, file, dictionary, separator))
      .values
      .keyBy { case (c, i) => Position(c.position(_2)) }
      .tunedJoin(rc, saveDictionary(ctx, _2, file, dictionary, separator))
      .map { case (_, ((c, i), j)) => (c, i, j) }
      .keyBy { case (c, pi, pj) => Position(c.position(_3)) }
      .tunedJoin(rc, saveDictionary(ctx, _3, file, dictionary, separator))
      .map { case (_, ((c, i, j), k)) => (c, i, j, k) }
      .keyBy { case (c, i, j, k) => Position(c.position(_4)) }
      .tunedJoin(rc, saveDictionary(ctx, _4, file, dictionary, separator))
      .map { case (_, ((c, i, j, k), l)) => (c, i, j, k, l) }
      .keyBy { case (c, i, j, k, l) => Position(c.position(_5)) }
      .tunedJoin(rc, saveDictionary(ctx, _5, file, dictionary, separator))
      .map { case (_, ((c, i, j, k, l), m)) => (c, i, j, k, l, m) }
      .keyBy { case (c, i, j, k, l, m) => Position(c.position(_6)) }
      .tunedJoin(rc, saveDictionary(ctx, _6, file, dictionary, separator))
      .map { case (_, ((c, i, j, k, l, m), n)) => (c, i, j, k, l, m, n) }
      .keyBy { case (c, i, j, k, l, m, n) => Position(c.position(_7)) }
      .tunedJoin(rc, saveDictionary(ctx, _7, file, dictionary, separator))
      .map { case (_, ((c, i, j, k, l, m, n), o)) => (c, i, j, k, l, m, n, o) }
      .keyBy { case (c, i, j, k, l, m, n, o) => Position(c.position(_8)) }
      .tunedJoin(rc, saveDictionary(ctx, _8, file, dictionary, separator))
      .map { case (_, ((c, i, j, k, l, m, n, o), p)) => (c, i, j, k, l, m, n, o, p) }
      .keyBy { case (c, i, j, k, l, m, n, o, p) => Position(c.position(_9)) }
      .tunedJoin(rc, saveDictionary(ctx, _9, file, dictionary, separator))
      .map { case (_, ((c, i, j, k, l, m, n, o, p), q)) =>
        i + separator +
        j + separator +
        k + separator +
        l + separator +
        m + separator +
        n + separator +
        o + separator +
        p + separator +
        q + separator +
        c.content.value.toShortString
      }
      .redistribute(rd)
      .saveAsTextFile(file)

    data
  }
}

