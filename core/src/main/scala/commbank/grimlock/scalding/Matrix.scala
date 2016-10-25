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

package commbank.grimlock.scalding

import au.com.cba.omnia.ebenezer.scrooge.ParquetScroogeSource

import commbank.grimlock.framework.{
  Cell,
  Collate,
  Consume,
  Compactable,
  Default,
  ExtractWithDimension,
  ExtractWithKey,
  InMemory,
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
  Tuner,
  TunerParameters,
  Type,
  Unbalanced
}
import commbank.grimlock.framework.aggregate.{ Aggregator, AggregatorWithValue }
import commbank.grimlock.framework.content.Content
import commbank.grimlock.framework.content.metadata.{ DiscreteSchema, NominalSchema }
import commbank.grimlock.framework.DefaultTuners.{ TP1, TP2, TP4 }
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

import commbank.grimlock.scalding.distribution.ApproximateDistribution
import commbank.grimlock.scalding.environment.{ DistributedData, Environment, UserData }
import commbank.grimlock.scalding.ScaldingImplicits._

import com.twitter.scalding.{ TextLine, WritableSequenceFile }
import com.twitter.scalding.typed.{ Grouped, TypedPipe, TypedSink, ValuePipe }
import com.twitter.scrooge.ThriftStruct

import java.io.{ File, OutputStreamWriter, PrintWriter }
import java.lang.{ ProcessBuilder, Thread }
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.attribute.PosixFilePermissions
import java.nio.file.{ Files, Paths }

import org.apache.hadoop.io.Writable

import scala.collection.immutable.HashSet
import scala.collection.immutable.ListSet
import scala.collection.JavaConverters._
import scala.io.Source
import scala.reflect.ClassTag

import shapeless.{ Nat, Succ, Witness }
import shapeless.nat.{ _0, _1, _2, _3, _4, _5, _6, _7, _8, _9 }
import shapeless.ops.nat.{ Diff, LTEq, GT, GTEq, ToInt }
import shapeless.syntax.sized._

private[scalding] object ScaldingImplicits {
  implicit class GroupedTuner[K <: Position[_], V](grouped: Grouped[K, V]) {
    private implicit def serialisePosition[T <: Position[_]](key: T): Array[Byte] = key
      .toShortString("|")
      .toCharArray
      .map(_.toByte)

    def tunedJoin[W](
      tuner: Tuner,
      parameters: TunerParameters,
      smaller: TypedPipe[(K, W)]
    )(implicit
      ev: Ordering[K]
    ): TypedPipe[(K, (V, W))] = (tuner, parameters) match {
      case (Default(_), Reducers(reducers)) => grouped.withReducers(reducers).join(Grouped(smaller))
      case (Unbalanced(_), Reducers(reducers)) => grouped.sketch(reducers).join(smaller)
      case _ => grouped.join(Grouped(smaller))
    }

    def tunedLeftJoin[W](
      tuner: Tuner,
      parameters: TunerParameters,
      smaller: TypedPipe[(K, W)]
    )(implicit
      ev: Ordering[K]
    ): TypedPipe[(K, (V, Option[W]))] = (tuner, parameters) match {
      case (Default(_), Reducers(reducers)) => grouped.withReducers(reducers).leftJoin(Grouped(smaller))
      case (Unbalanced(_), Reducers(reducers)) => grouped.sketch(reducers).leftJoin(smaller)
      case _ => grouped.leftJoin(Grouped(smaller))
    }

    def tuneReducers(parameters: TunerParameters): Grouped[K, V] = parameters match {
      case Reducers(reducers) => grouped.withReducers(reducers)
      case _ => grouped
    }
  }

  implicit class PipeTuner[P](pipe: TypedPipe[P]) {
    def collate(parameters: TunerParameters): TypedPipe[P] = parameters match {
      case Collate() => pipe.forceToDisk
      case _ => pipe
    }

    def mapSideJoin[V, Q](value: ValuePipe[V], f: (P, V) => TraversableOnce[Q], empty: V): TypedPipe[Q] = pipe
      .flatMapWithValue(value) {
        case (c, Some(v)) => f(c, v)
        case (c, None) => f(c, empty)
      }

    def redistribute(parameters: TunerParameters): TypedPipe[P] = parameters match {
      case Redistribute(reducers) => pipe.shard(reducers)
      case _ => pipe
    }

    def toHashSetValue[Q](f: (P) => Q): ValuePipe[HashSet[Q]] = {
      val semigroup = new com.twitter.algebird.Semigroup[HashSet[Q]] {
        def plus(l: HashSet[Q], r: HashSet[Q]): HashSet[Q] = l ++ r
      }

      pipe.map(p => HashSet(f(p))).sum(semigroup)
    }

    def toListValue[Q](f: (P) => Q): ValuePipe[List[Q]] = pipe.map(p => List(f(p))).sum

    def tunedDistinct(parameters: TunerParameters)(implicit ev: Ordering[P]): TypedPipe[P] = parameters match {
      case Reducers(reducers) => pipe.asKeys.withReducers(reducers).sum.keys
      case _ => pipe.asKeys.sum.keys
    }
  }
}

/** Base trait for matrix operations using a `TypedPipe[Cell[P]]`. */
trait Matrix[L <: Nat, P <: Nat] extends FwMatrix[L, P] with Persist[Cell[P]] with UserData {
  protected implicit def positionOrdering[N <: Nat] = Position.ordering[N]()

  type ChangeTuners[T] = TP4[T]
  def change[
    T <: Tuner : ChangeTuners
  ](
    slice: Slice[L, P],
    tuner: T = InMemory()
  )(
    positions: U[Position[slice.S]],
    schema: Content.Parser,
    writer: TextWriter
  )(implicit
    ev1: ClassTag[Position[slice.S]],
    ev2: Diff.Aux[P, _1, L]
  ): (U[Cell[P]], U[String]) = {
    val update = (change: Boolean, cell: Cell[P]) => change match {
      case true => schema(cell.content.value.toShortString)
        .map(con => List(Right(Cell(cell.position, con))))
        .getOrElse(writer(cell).map(Left(_)))
      case false => List(Right(cell))
    }

    val result = tuner match {
      case InMemory(_) =>
        data
          .mapSideJoin(
            positions.toHashSetValue((p: Position[slice.S]) => p),
            (c: Cell[P], v: HashSet[Position[slice.S]]) => update(v.contains(slice.selected(c.position)), c),
            HashSet.empty[Position[slice.S]]
          )
      case _ =>
        data
          .groupBy { case c => slice.selected(c.position) }
          .tunedLeftJoin(tuner, tuner.parameters, positions.map { case p => (p, ()) })
          .flatMap { case (_, (c, o)) => update(!o.isEmpty, c) }
    }

    (result.collect { case Right(cell) => cell }, result.collect { case Left(error) => error })
  }

  type CompactTuners[T] = TP2[T]
  def compact()(implicit ev: ClassTag[Position[P]]): E[Map[Position[P], Content]] = {
    val semigroup = new com.twitter.algebird.Semigroup[Map[Position[P], Content]] {
      def plus(l: Map[Position[P], Content], r: Map[Position[P], Content]) = l ++ r
    }

    data
      .map(c => Map(c.position -> c.content))
      .sum(semigroup)
  }

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
  ): E[Map[Position[slice.S], ev3.C[slice.R]]] = {
    val semigroup = new com.twitter.algebird.Semigroup[Map[Position[slice.S], ev3.C[slice.R]]] {
      def plus(l: Map[Position[slice.S], ev3.C[slice.R]], r: Map[Position[slice.S], ev3.C[slice.R]]) = l ++ r
    }

    data
      .map(c => (slice.selected(c.position), ev3.toMap(slice)(c)))
      .group
      .tuneReducers(tuner.parameters)
      .reduce[Map[Position[slice.S], ev3.C[slice.R]]] { case (lm, rm) => ev3.combineMaps(slice)(lm, rm) }
      .values
      .sum(semigroup)
  }

  type DomainTuners[T] = TP1[T]

  type FillHeterogeneousTuners[T] = T In OneOf[InMemory[NoParameters]]#
    Or[InMemory[Reducers]]#
    Or[Default[NoParameters]]#
    Or[Default[Reducers]]
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
    type X = Map[Position[slice.S], Content]

    val vals = values.groupBy { case c => c.position }
    val dense = tuner match {
      case InMemory(_) =>
        val semigroup = new com.twitter.algebird.Semigroup[X] { def plus(l: X, r: X) = l ++ r }

        domain(Default())
          .mapSideJoin(
            vals.map { case (p, c) => Map(p -> c.content) }.sum(semigroup),
            (p: Position[P], v: X) => v.get(slice.selected(p)).map { case c => Cell(p, c) },
            Map.empty[Position[slice.S], Content]
          )
      case _ =>
        domain(Default())
          .groupBy { case p => slice.selected(p) }
          .tunedJoin(tuner, tuner.parameters, vals)
          .map { case (_, (p, c)) => Cell(p, c.content) }
    }

    dense
      .groupBy { case c => c.position }
      .tuneReducers(tuner.parameters)
      .leftJoin(data.groupBy { case c => c.position })
      .map { case (p, (fc, co)) => co.getOrElse(fc) }
  }

  type FillHomogeneousTuners[T] = TP2[T]
  def fillHomogeneous[
    T <: Tuner : FillHomogeneousTuners
  ](
    value: Content,
    tuner: T = Default()
  )(implicit
    ev: ClassTag[Position[P]]
  ): U[Cell[P]] = domain(Default())
    .asKeys
    .tuneReducers(tuner.parameters)
    .leftJoin(data.groupBy { case c => c.position })
    .map { case (p, (_, co)) => co.getOrElse(Cell(p, value)) }

  type GetTuners[T] = TP4[T]
  def get[
    T <: Tuner : GetTuners
  ](
    positions: U[Position[P]],
    tuner: T = InMemory()
  )(implicit
    ev: ClassTag[Position[P]]
  ): U[Cell[P]] = tuner match {
    case InMemory(_) =>
      data
        .mapSideJoin(
          positions.toHashSetValue((p: Position[P]) => p),
          (c: Cell[P], v: HashSet[Position[P]]) => if (v.contains(c.position)) Option(c) else None,
          HashSet.empty[Position[P]]
        )
    case _ =>
      data
        .groupBy { case c => c.position }
        .tunedJoin(tuner, tuner.parameters, positions.map { case p => (p, ()) })
        .map { case (_, (c, _)) => c }
  }

  type JoinTuners[T] = T In OneOf[InMemory[NoParameters]]#
    Or[InMemory[Reducers]]#
    Or[Default[NoParameters]]#
    Or[Default[Reducers]]#
    Or[Default[Sequence[Reducers, Reducers]]]#
    Or[Unbalanced[Reducers]]#
    Or[Unbalanced[Sequence[Reducers, Reducers]]]
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
    val (p1, p2) = (tuner, tuner.parameters) match {
      case (_, Sequence(f, s)) => (f, s)
      case (InMemory(_), p) => (p, NoParameters())
      case (_, p) => (NoParameters(), p)
    }
    val keep = data
      .map(c => slice.selected(c.position))
      .tunedDistinct(p1)
      .asKeys
      .tuneReducers(p1)
      .join(that.map(c => slice.selected(c.position)).tunedDistinct(p1).asKeys)
      .map { case (p, _) => (p, ()) } // TODO: Does this need a forceToDisk?

    tuner match {
      case InMemory(_) =>
        (data ++ that)
          .mapSideJoin(
            keep.toHashSetValue[Position[slice.S]](_._1),
            (c: Cell[P], v: HashSet[Position[slice.S]]) =>
              if (v.contains(slice.selected(c.position))) Option(c) else None,
            HashSet.empty[Position[slice.S]]
          )
      case _ =>
        (data ++ that)
          .groupBy { case c => slice.selected(c.position) }
          .tunedJoin(tuner, p2, keep)
          .map { case (_, (c, _)) => c }
    }
  }

  type MaterialiseTuners[T] = T Is Default[Execution]
  def materialise[T <: Tuner : MaterialiseTuners](tuner: T): List[Cell[P]] = {
    val context = tuner.parameters match {
      case Execution(ctx) => ctx
    }

    data
      .toIterableExecution
      .waitFor(context.config, context.mode) match {
        case scala.util.Success(itr) => itr.toList
        case _ => List.empty
      }
  }

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

  type PairwiseTuners[T] = T In OneOf[InMemory[NoParameters]]#
    Or[Default[NoParameters]]#
    Or[Default[Reducers]]#
    Or[Default[Sequence[Redistribute, Redistribute]]]#
    Or[Default[Sequence[Redistribute, Reducers]]]#
    Or[Default[Sequence[Redistribute, Sequence[Redistribute, Reducers]]]]#
    Or[Default[Sequence[Reducers, Redistribute]]]#
    Or[Default[Sequence[Reducers, Reducers]]]#
    Or[Default[Sequence[Reducers, Sequence[Redistribute, Reducers]]]]#
    Or[Default[Sequence[Sequence[Redistribute, Reducers], Redistribute]]]#
    Or[Default[Sequence[Sequence[Redistribute, Reducers], Reducers]]]#
    Or[Default[Sequence[Sequence[Redistribute, Reducers], Sequence[Redistribute, Reducers]]]]#
    Or[Unbalanced[Sequence[Reducers, Reducers]]]#
    Or[Unbalanced[Sequence[Sequence[Redistribute, Reducers], Sequence[Redistribute, Reducers]]]]
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
    operators: OperatorWithValue[P, Q] { type V >: W}*
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
      .flatMapWithValue(value) { case ((lc, rc), vo) => operator.computeWithValue(lc, rc, vo.get) }
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
      .flatMapWithValue(value) { case ((lc, rc), vo) => operator.computeWithValue(lc, rc, vo.get) }
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
  ): U[Cell[Q]] = data.flatMapWithValue(value) { case (c, vo) => locate(c, vo.get).map(Cell(_, c.content)) }

  def saveAsText(ctx: C, file: String, writer: TextWriter): U[Cell[P]] = saveText(ctx, file, writer)

  type SetTuners[T] = TP2[T]
  def set[
    T <: Tuner : SetTuners
  ](
    values: U[Cell[P]],
    tuner: T = Default()
  )(implicit
    ev: ClassTag[Position[P]]
  ): U[Cell[P]] = data
    .groupBy { case c => c.position }
    .tuneReducers(tuner.parameters)
    .outerJoin(values.groupBy { case c => c.position })
    .map { case (_, (co, cn)) => cn.getOrElse(co.get) }

  type ShapeTuners[T] = TP2[T]
  def shape[T <: Tuner : ShapeTuners](tuner: T = Default()): U[Cell[_1]] = data
    .flatMap(c => c.position.coordinates.map(_.toString).zipWithIndex.map(_.swap))
    .tunedDistinct(tuner.parameters)
    .group
    .size
    .map { case (i, s) => Cell(Position(Position.indexString(i + 1)), Content(DiscreteSchema[Long](), s)) }

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
      .map(_ => 1L)
      .sum
      .map(sum => Cell(Position(Position.indexString[dim.N]), Content(DiscreteSchema[Long](), sum)))
  }

  type SliceTuners[T] = TP4[T]
  def slice[
    T <: Tuner : SliceTuners
  ](
    slice: Slice[L, P],
    tuner: T = InMemory()
  )(
    positions: U[Position[slice.S]],
    keep: Boolean
  )(implicit
    ev1: ClassTag[Position[slice.S]],
    ev2: Diff.Aux[P, _1, L]
  ): U[Cell[P]] = tuner match {
    case InMemory(_) =>
      data
        .mapSideJoin(
          positions.toHashSetValue((p: Position[slice.S]) => p),
          (c: Cell[P], v: HashSet[Position[slice.S]]) =>
            if (v.contains(slice.selected(c.position)) == keep) Option(c) else None,
          HashSet.empty[Position[slice.S]]
        )
    case _ =>
      data
        .groupBy { case c => slice.selected(c.position) }
        .tunedLeftJoin(tuner, tuner.parameters, positions.map { case p => (p, ()) })
        .collect { case (_, (c, o)) if (o.isEmpty != keep) => c }
  }

  type SlideTuners[T] = T In OneOf[Default[NoParameters]]#
    Or[Default[Redistribute]]#
    Or[Default[Reducers]]#
    Or[Default[Sequence[Redistribute, Reducers]]]
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

    val (partitions, reducers) = tuner.parameters match {
      case Sequence(rp @ Redistribute(_), rr @ Reducers(_)) => (rp, rr)
      case rp @ Redistribute(_) => (rp, NoParameters())
      case rr @ Reducers(_) => (NoParameters(), rr)
      case _ => (NoParameters(), NoParameters())
    }

    data
      .map { case c => (slice.selected(c.position), (slice.remainder(c.position), window.prepare(c))) }
      .redistribute(partitions)
      .group
      .tuneReducers(reducers)
      .sortBy { case (r, _) => r }(Position.ordering(ascending))
      .scanLeft(Option.empty[(window.T, TraversableOnce[window.O])]) {
        case (None, (r, i)) => Option(window.initialise(r, i))
        case (Some((t, _)), (r, i)) => Option(window.update(r, i, t))
      }
      .flatMap {
        case (p, Some((_, o))) => o.flatMap(window.present(p, _))
        case _ => None
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

    val (partitions, reducers) = tuner.parameters match {
      case Sequence(rp @ Redistribute(_), rr @ Reducers(_)) => (rp, rr)
      case rp @ Redistribute(_) => (rp, NoParameters())
      case rr @ Reducers(_) => (NoParameters(), rr)
      case _ => (NoParameters(), NoParameters())
    }

    data
      .mapWithValue(value) {
        case (c, vo) => (slice.selected(c.position), (slice.remainder(c.position), window.prepareWithValue(c, vo.get)))
      }
      .redistribute(partitions)
      .group
      .tuneReducers(reducers)
      .sortBy { case (r, _) => r }(Position.ordering(ascending))
      .scanLeft(Option.empty[(window.T, TraversableOnce[window.O])]) {
        case (None, (r, i)) => Option(window.initialise(r, i))
        case (Some((t, _)), (r, i)) => Option(window.update(r, i, t))
      }
      .flatMapWithValue(value) {
        case ((p, Some((_, o))), vo) => o.flatMap(window.presentWithValue(p, _, vo.get))
        case _ => None
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

    data.flatMapWithValue(value) { case (c, vo) => partitioner.assignWithValue(c, vo.get).map(q => (q, c)) }
  }

  def stream[
    Q <: Nat
  ](
    command: String,
    files: List[String],
    writer: TextWriter,
    parser: Cell.TextParser[Q]
  ): (U[Cell[Q]], U[String]) = {
    val contents = files.map(f => (new File(f).getName, Files.readAllBytes(Paths.get(f))))
    val smfn = (k: Unit, itr: Iterator[String]) => {
      val tmp = Files.createTempDirectory("grimlock-")
      tmp.toFile.deleteOnExit

      contents.foreach {
        case (file, content) =>
          val path = Paths.get(tmp.toString, file)

          Files.write(path, content)
          Files.setPosixFilePermissions(path, PosixFilePermissions.fromString("rwxr-x---"))
      }

      val process = new ProcessBuilder(command.split(' ').toList.asJava).directory(tmp.toFile).start

      new Thread() {
        override def run() {
          Source.fromInputStream(process.getErrorStream, "ISO-8859-1").getLines.foreach(System.err.println)
        }
      }.start

      new Thread() {
        override def run() {
          val out = new PrintWriter(new OutputStreamWriter(process.getOutputStream, UTF_8))

          itr.foreach(out.println)
          out.close
        }
      }.start

      val result = Source.fromInputStream(process.getInputStream, "UTF-8").getLines

      new Iterator[String] {
        def next(): String = result.next

        def hasNext: Boolean = {
          if (result.hasNext) {
            true
          } else {
            val status = process.waitFor

            if (status != 0) {
              throw new Exception(s"Subprocess '${command}' exited with status ${status}")
            }

            false
          }
        }
      }
    }

    val result = data
      .flatMap(writer(_))
      .groupAll
      .mapGroup(smfn)
      .values
      .flatMap(parser(_))

    (result.collect { case Right(c) => c }, result.collect { case Left(e) => e })
  }

  def subset(samplers: Sampler[P]*): U[Cell[P]] = {
    val sampler: Sampler[P] = if (samplers.size == 1) samplers.head else samplers.toList

    data.filter { case c => sampler.select(c) }
  }

  def subsetWithValue[W](value: E[W], samplers: SamplerWithValue[P] { type V >: W }*): U[Cell[P]] = {
    val sampler: SamplerWithValue[P] { type V >: W } = if (samplers.size == 1) samplers.head else samplers.toList

    data.filterWithValue(value) { case (c, vo) => sampler.selectWithValue(c, vo.get) }
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

    data
      .flatMap { case c => aggregator.prepare(c).map(t => (slice.selected(c.position), t)) }
      .group
      .tuneReducers(tuner.parameters)
      .reduce[aggregator.T] { case (lt, rt) => aggregator.reduce(lt, rt) }
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

    data
      .flatMapWithValue(value) {
        case (c, vo) => aggregator.prepareWithValue(c, vo.get).map(t => (slice.selected(c.position), t))
      }
      .group
      .tuneReducers(tuner.parameters)
      .reduce[aggregator.T] { case (lt, rt) => aggregator.reduce(lt, rt) }
      .flatMapWithValue(value) { case ((p, t), vo) => aggregator.presentWithValue(p, t, vo.get) }
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

    data.flatMapWithValue(value) { case (c, vo) => transformer.presentWithValue(c, vo.get) }
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
    .map { case Cell(p, c) => (slice.selected(p), c.schema.kind) }
    .group
    .tuneReducers(tuner.parameters)
    .reduce[Type] { case (lt, rt) => lt.getCommonType(rt) }
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

  type WhichTuners[T] = TP4[T]
  def which(predicate: Cell.Predicate[P])(implicit ev: ClassTag[Position[P]]): U[Position[P]] = data
    .collect { case c if predicate(c) => c.position }

  def whichByPosition[
    T <: Tuner : WhichTuners
  ](
    slice: Slice[L, P],
    tuner: T = InMemory()
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

    tuner match {
      case InMemory(_) =>
        type X = Map[Position[slice.S], List[Cell.Predicate[P]]]
        val semigroup = new com.twitter.algebird.Semigroup[X] {
          def plus(l: X, r: X) = (l.toSeq ++ r.toSeq).groupBy(_._1).mapValues(_.map(_._2).toList.flatten)
        }

        data
          .mapSideJoin(
            pp.map { case (pos, pred) => Map(pos -> List(pred)) }.sum(semigroup),
            (c: Cell[P], v: X) => v.get(slice.selected(c.position)).flatMap {
              case lst => if (lst.exists((pred) => pred(c))) Option(c.position) else None
            },
            Map.empty[Position[slice.S], List[Cell.Predicate[P]]]
          )
      case _ =>
        data
          .groupBy { case c => slice.selected(c.position) }
          .tunedJoin(tuner, tuner.parameters, pp)
          .collect { case (_, (c, predicate)) if predicate(c) => c.position }
    }
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
  ): Grouped[Position[_1], Int] = {
    import ctx._

    val slice = Over[L, P](dim)

    val numbered = names(slice)
      .groupAll
      .mapGroup { case (_, itr) => itr.zipWithIndex }
      .map { case (_, (p, i)) => (p, i) }

    numbered
      .map { case (Position(c), i) => c.toShortString + separator + i }
      .write(TypedSink(TextLine(dictionary.format(file, Nat.toInt[D]))))

    Grouped(numbered)
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
    tuner match {
      case InMemory(_) =>
        ldata
          .mapSideJoin(
            rdata.toListValue((c: Cell[P]) => c),
            (lc: Cell[P], v: List[Cell[P]]) => v.collect {
              case rc if comparer.keep(slice.selected(lc.position), slice.selected(rc.position)) => (lc, rc)
            },
            List.empty[Cell[P]]
          )
      case _ =>
        val (rr, rj, lr, lj) = tuner.parameters match {
          case Sequence(Sequence(rr, rj), Sequence(lr, lj)) => (rr, rj, lr, lj)
          case Sequence(rj @ Reducers(_), Sequence(lr, lj)) => (NoParameters(), rj, lr, lj)
          case Sequence(rr @ Redistribute(_), Sequence(lr, lj)) => (rr, NoParameters(), lr, lj)
          case Sequence(Sequence(rr, rj), lj @ Reducers(_)) => (rr, rj, NoParameters(), lj)
          case Sequence(Sequence(rr, rj), lr @ Redistribute(_)) => (rr, rj, lr, NoParameters())
          case Sequence(rj @ Reducers(_), lj @ Reducers(_)) => (NoParameters(), rj, NoParameters(), lj)
          case Sequence(rr @ Redistribute(_), lj @ Reducers(_)) => (rr, NoParameters(), NoParameters(), lj)
          case Sequence(rj @ Reducers(_), lr @ Redistribute(_)) => (NoParameters(), rj, lr, NoParameters())
          case Sequence(rr @ Redistribute(_), lr @ Redistribute(_)) => (rr, NoParameters(), lr, NoParameters())
          case lj @ Reducers(_) => (NoParameters(), NoParameters(), NoParameters(), lj)
          case _ => (NoParameters(), NoParameters(), NoParameters(), NoParameters())
        }
        val ordering = Position.ordering[slice.S]()
        val right = rdata
          .map { case Cell(p, _) => slice.selected(p) }
          .redistribute(rr)
          .distinct(ordering)
          .map { case p => List(p) }
          .sum
        val keys = ldata
          .map { case Cell(p, _) => slice.selected(p) }
          .redistribute(lr)
          .distinct(ordering)
          .flatMapWithValue(right) {
            case (l, Some(v)) => v.collect { case r if comparer.keep(l, r) => (r, l) }
            case _ => None
          }
          .forceToDisk // TODO: Should this be configurable?

        ldata
          .groupBy { case Cell(p, _) => slice.selected(p) }
          .tunedJoin(tuner, lj, rdata
            .groupBy { case Cell(p, _) => slice.selected(p) }
            .tunedJoin(tuner, rj, keys)
            .map { case (r, (c, l)) => (l, c) })
          .values
    }
  }
}

/** Base trait for methods that reduce the number of dimensions or that can be filled using a `TypedPipe[Cell[P]]`. */
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
  ): U[Cell[L]] = data
    .map { case c => (c.position.remove(dim), squasher.prepare(c, dim)) }
    .group
    .tuneReducers(tuner.parameters)
    .reduce[squasher.T] { case (lt, rt) => squasher.reduce(lt, rt) }
    .flatMap { case (p, t) => squasher.present(t).map(c => Cell(p, c)) }

  def squashWithValue[
    D <: Nat : ToInt,
    W,
    T <: Tuner : SquashTuners
  ](
    dim: D,
    value: E[W],
    squasher: SquasherWithValue[P] { type V >: W},
    tuner: T = Default()
  )(implicit
    ev1: LTEq[D, P],
    ev2: ClassTag[Position[L]],
    ev3: Diff.Aux[P, _1, L]
  ): U[Cell[L]] = data
    .mapWithValue(value) { case (c, vo) => (c.position.remove(dim), squasher.prepareWithValue(c, dim, vo.get)) }
    .group
    .tuneReducers(tuner.parameters)
    .reduce[squasher.T] { case (lt, rt) => squasher.reduce(lt, rt) }
    .flatMapWithValue(value) { case ((p, t), vo) => squasher.presentWithValue(t, vo.get).map(c => Cell(p, c)) }
}

/** Base trait for methods that reshapes the number of dimension of a matrix using a `TypedPipe[Cell[P]]`. */
trait ReshapeableMatrix[L <: Nat, P <: Nat] extends FwReshapeableMatrix[L, P] { self: Matrix[L, P] =>
  type ReshapeTuners[T] = TP4[T]
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
      .collect[(Position[L], Value)] {
        case c if (c.position(dim) equ coordinate) => (c.position.remove(dim), c.content.value)
      }
    val values = data
      .collect { case c if (c.position(dim) neq coordinate) => (c.position.remove(dim), c) }
    val append = (c: Cell[P], v: Option[Value]) => locate(c, v).map(Cell(_, c.content))

    tuner match {
      case InMemory(_) =>
        values
          .mapSideJoin(
            keys.toListValue((t: (Position[L], Value)) => t),
            (t: (Position[L], Cell[P]), v: List[(Position[L], Value)]) => append(t._2, v.find(_._1 == t._1).map(_._2)),
            List.empty[(Position[L], Value)]
          )
      case _ =>
        values
          .group
          .tunedLeftJoin(tuner, tuner.parameters, keys)
          .flatMap { case (_, (c, v)) => append(c, v) }
    }
  }
}

// TODO: Make this work on more than 2D matrices and share with Spark
trait MatrixDistance { self: Matrix[_1, _2] with ReducibleMatrix[_1, _2] =>

  import commbank.grimlock.library.aggregate._
  import commbank.grimlock.library.pairwise._
  import commbank.grimlock.library.transform._
  import commbank.grimlock.library.window._

  import commbank.grimlock.scalding.environment.Context._

  /**
   * Compute correlations.
   *
   * @param slice  Encapsulates the dimension for which to compute correlations.
   * @param stuner The sumamrise tuner for the job.
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
    ev4: Witness.Aux[slice.dimension.N]
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
      .transformWithValue(ValuePipe(Map(Position("one") -> 1.0)), Subtract(ExtractWithKey[_1, Double]("one"), true))
  }
}

object Matrix extends Consume with DistributedData with Environment {
  def loadText[P <: Nat](ctx: C, file: String, parser: Cell.TextParser[P]): (U[Cell[P]], U[String]) = {
    val pipe = TypedPipe.from(TextLine(file)).flatMap { parser(_) }

    (pipe.collect { case Right(c) => c }, pipe.collect { case Left(e) => e })
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
    val pipe = TypedPipe.from(WritableSequenceFile[K, V](file)).flatMap { case (k, v) => parser(k, v) }

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
    val pipe = TypedPipe.from(ParquetScroogeSource[T](file)).flatMap { parser(_) }

    (pipe.collect { case Right(c) => c }, pipe.collect { case Left(e) => e })
  }
}

/**
 * Rich wrapper around a `TypedPipe[Cell[_1]]`.
 *
 * @param data `TypedPipe[Cell[_1]]`.
 */
case class Matrix1D(
  data: TypedPipe[Cell[_1]]
) extends FwMatrix1D
  with Matrix[_0, _1]
  with ApproximateDistribution[_0, _1] {
  def domain[T <: Tuner : DomainTuners](tuner: T = Default()): U[Position[_1]] = names(Over(_1), tuner)

  def saveAsIV(ctx: C, file: String, dictionary: String, separator: String): U[Cell[_1]] = {
    import ctx._

    data
      .groupBy { case c => c.position }
      .join(saveDictionary(ctx, _1, file, dictionary, separator))
      .map { case (_, (c, i)) => i + separator + c.content.value.toShortString }
      .write(TypedSink(TextLine(file)))

    data
  }
}

/**
 * Rich wrapper around a `TypedPipe[Cell[_2]]`.
 *
 * @param data `TypedPipe[Cell[_2]]`.
 */
case class Matrix2D(
  data: TypedPipe[Cell[_2]]
) extends FwMatrix2D
  with Matrix[_1, _2]
  with ReducibleMatrix[_1, _2]
  with ReshapeableMatrix[_1, _2]
  with MatrixDistance
  with ApproximateDistribution[_1, _2] {
  def domain[T <: Tuner : DomainTuners](tuner: T = Default()): U[Position[_2]] = names(Over(_1), tuner)
    .map { case Position(c) => c }
    .cross(names(Over(_2), tuner).map { case Position(c) => c })
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

  def saveAsCSV(
    slice: Slice[_1, _2]
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
    import ctx._

    implicit val x = new LTEq[_1, slice.S] {}
    implicit val y = new LTEq[_1, slice.R] {}

    val escape = (str: String) => escapee.escape(str)
    val semigroup = new com.twitter.algebird.Semigroup[HashSet[String]] {
      def plus(l: HashSet[String], r: HashSet[String]): HashSet[String] = l ++ r
    }
    val columns = data
      .map { case c => HashSet(escape(slice.remainder(c.position)(_1).toShortString)) }
      .sum(semigroup)
      .map { _.toList.sorted }

    if (writeHeader) {
      columns
        .map { case lst => (if (writeRowId) escape(rowId) + separator else "") + lst.mkString(separator) }
        .write(TypedSink(TextLine(header.format(file))))
    }

    data
      .groupBy { case c => slice.selected(c.position)(_1).toShortString }
      .mapValues { case Cell(p, c) =>
        Map(escape(slice.remainder(p)(_1).toShortString) -> escape(c.value.toShortString))
      }
      .sum
      .flatMapWithValue(columns) { case ((key, values), optCols) =>
        optCols.map { case cols => (key, cols.map { case c => values.getOrElse(c, "") }) }
      }
      .map { case (i, lst) => (if (writeRowId) escape(i) + separator else "") + lst.mkString(separator) }
      .write(TypedSink(TextLine(file)))

    data
  }

  def saveAsIV(ctx: C, file: String, dictionary: String, separator: String): U[Cell[_2]] = {
    import ctx._

    data
      .groupBy { case c => Position(c.position(_1)) }
      .join(saveDictionary(ctx, _1, file, dictionary, separator))
      .values
      .groupBy { case (c, i) => Position(c.position(_2)) }
      .join(saveDictionary(ctx, _2, file, dictionary, separator))
      .map { case (_, ((c, i), j)) => i + separator + j + separator + c.content.value.toShortString }
      .write(TypedSink(TextLine(file)))

    data
  }

  def saveAsVW(
    slice: Slice[_1, _2]
  )(
    ctx: C,
    file: String,
    dictionary: String,
    tag: Boolean,
    separator: String
  )(implicit
    ev: ClassTag[Position[slice.S]]
  ): U[Cell[_2]] = saveVW(slice)(ctx, file, None, None, tag, dictionary, separator)

  def saveAsVWWithLabels(
    slice: Slice[_1, _2]
  )(
    ctx: C,
    file: String,
    labels: U[Cell[slice.S]],
    dictionary: String,
    tag: Boolean,
    separator: String
  )(implicit
    ev: ClassTag[Position[slice.S]]
  ): U[Cell[_2]] = saveVW(slice)(ctx, file, Option(labels), None, tag, dictionary, separator)

  def saveAsVWWithImportance(
    slice: Slice[_1, _2]
  )(
    ctx: C,
    file: String,
    importance: U[Cell[slice.S]],
    dictionary: String,
    tag: Boolean,
    separator: String
  )(implicit
    ev: ClassTag[Position[slice.S]]
  ): U[Cell[_2]] = saveVW(slice)(ctx, file, None, Option(importance), tag, dictionary, separator)

  def saveAsVWWithLabelsAndImportance(
    slice: Slice[_1, _2]
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
  ): U[Cell[_2]] = saveVW(slice)(ctx, file, Option(labels), Option(importance), tag, dictionary, separator)

  private def saveVW(
    slice: Slice[_1, _2]
  )(
    ctx: C,
    file: String,
    labels: Option[U[Cell[slice.S]]],
    importance: Option[U[Cell[slice.S]]],
    tag: Boolean,
    dictionary: String,
    separator: String
  ): U[Cell[_2]] = {
    import ctx._

    implicit val x = new LTEq[_1, slice.S] {}
    implicit val y = new LTEq[_1, slice.R] {}

    val dict = data
      .map { c => (slice.remainder(c.position)(_1).toShortString, ()) }
      .group
      .sum
      .map(_.swap)
      .group
      .mapGroup { case (_, itr) => itr.zipWithIndex }
      .map { case (_, si) => si }

    dict
      .map { case (s, i) => s + separator + i }
      .write(TypedSink(TextLine(dictionary.format(file))))

    val features = data
      .groupBy { c => slice.remainder(c.position)(_1).toShortString }
      .join(dict)
      .flatMap { case (_, (c, i)) => c.content.value.asDouble.map { case v => (slice.selected(c.position), (i, v)) } }
      .group
      .sortBy { case (i, _) => i }
      .foldLeft("|") { case (b, (i, v)) => b + " " + i + ":" + v }

    val tagged = if (tag) features.map { case (p, s) => (p, p(_1).toShortString + s) } else features.toTypedPipe

    val weighted = importance match {
      case Some(imp) => tagged
        .join(imp.groupBy { case c => c.position })
        .flatMap { case (p, (s, c)) => c.content.value.asDouble.map { case i => (p, i + " " + s) } }
      case None => tagged
    }

    val examples = labels match {
      case Some(lab) => weighted
        .join(lab.groupBy { case c => c.position })
        .flatMap { case (p, (s, c)) => c.content.value.asDouble.map { case l => (p, l + " " + s) } }
      case None => weighted
    }

    examples
      .map { case (p, s) => s }
      .write(TypedSink(TextLine(file)))

    data
  }
}

/**
 * Rich wrapper around a `TypedPipe[Cell[_3]]`.
 *
 * @param data `TypedPipe[Cell[_3]]`.
 */
case class Matrix3D(
  data: TypedPipe[Cell[_3]]
) extends FwMatrix3D
  with Matrix[_2, _3]
  with ReducibleMatrix[_2, _3]
  with ReshapeableMatrix[_2, _3]
  with ApproximateDistribution[_2, _3] {
  def domain[T <: Tuner : DomainTuners](tuner: T = Default()): U[Position[_3]] = names(Over(_1), tuner)
    .map { case Position(c) => c }
    .cross(names(Over(_2), tuner).map { case Position(c) => c })
    .cross(names(Over(_3), tuner).map { case Position(c) => c })
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

  def saveAsIV(ctx: C, file: String, dictionary: String, separator: String): U[Cell[_3]] = {
    import ctx._

    data
      .groupBy { case c => Position(c.position(_1)) }
      .join(saveDictionary(ctx, _1, file, dictionary, separator))
      .values
      .groupBy { case (c, i) => Position(c.position(_2)) }
      .join(saveDictionary(ctx, _2, file, dictionary, separator))
      .map { case (_, ((c, i), j)) => (c, i, j) }
      .groupBy { case (c, i, j) => Position(c.position(_3)) }
      .join(saveDictionary(ctx, _3, file, dictionary, separator))
      .map { case (_, ((c, i, j), k)) => i + separator + j + separator + k + separator + c.content.value.toShortString }
      .write(TypedSink(TextLine(file)))

    data
  }
}

/**
 * Rich wrapper around a `TypedPipe[Cell[_4]]`.
 *
 * @param data `TypedPipe[Cell[_4]]`.
 */
case class Matrix4D(
  data: TypedPipe[Cell[_4]]
) extends FwMatrix4D
  with Matrix[_3, _4]
  with ReducibleMatrix[_3, _4]
  with ReshapeableMatrix[_3, _4]
  with ApproximateDistribution[_3, _4] {
  def domain[T <: Tuner : DomainTuners](tuner: T = Default()): U[Position[_4]] = names(Over(_1), tuner)
    .map { case Position(c) => c }
    .cross(names(Over(_2), tuner).map { case Position(c) => c })
    .cross(names(Over(_3), tuner).map { case Position(c) => c })
    .cross(names(Over(_4), tuner).map { case Position(c) => c })
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

  def saveAsIV(ctx: C, file: String, dictionary: String, separator: String): U[Cell[_4]] = {
    import ctx._

    data
      .groupBy { case c => Position(c.position(_1)) }
      .join(saveDictionary(ctx, _1, file, dictionary, separator))
      .values
      .groupBy { case (c, i) => Position(c.position(_2)) }
      .join(saveDictionary(ctx, _2, file, dictionary, separator))
      .map { case (_, ((c, i), j)) => (c, i, j) }
      .groupBy { case (c, i, j) => Position(c.position(_3)) }
      .join(saveDictionary(ctx, _3, file, dictionary, separator))
      .map { case (_, ((c, i, j), k)) => (c, i, j, k) }
      .groupBy { case (c, i, j, k) => Position(c.position(_4)) }
      .join(saveDictionary(ctx, _4, file, dictionary, separator))
      .map { case (_, ((c, i, j, k), l)) =>
        i + separator + j + separator + k + separator + l + separator + c.content.value.toShortString
      }
      .write(TypedSink(TextLine(file)))

    data
  }
}

/**
 * Rich wrapper around a `TypedPipe[Cell[_5]]`.
 *
 * @param data `TypedPipe[Cell[_5]]`.
 */
case class Matrix5D(
  data: TypedPipe[Cell[_5]]
) extends FwMatrix5D
  with Matrix[_4, _5]
  with ReducibleMatrix[_4, _5]
  with ReshapeableMatrix[_4, _5]
  with ApproximateDistribution[_4, _5] {
  def domain[T <: Tuner : DomainTuners](tuner: T = Default()): U[Position[_5]] = names(Over(_1), tuner)
    .map { case Position(c) => c }
    .cross(names(Over(_2), tuner).map { case Position(c) => c })
    .cross(names(Over(_3), tuner).map { case Position(c) => c })
    .cross(names(Over(_4), tuner).map { case Position(c) => c })
    .cross(names(Over(_5), tuner).map { case Position(c) => c })
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

  def saveAsIV(ctx: C, file: String, dictionary: String, separator: String): U[Cell[_5]] = {
    import ctx._

    data
      .groupBy { case c => Position(c.position(_1)) }
      .join(saveDictionary(ctx, _1, file, dictionary, separator))
      .values
      .groupBy { case (c, i) => Position(c.position(_2)) }
      .join(saveDictionary(ctx, _2, file, dictionary, separator))
      .map { case (_, ((c, i), j)) => (c, i, j) }
      .groupBy { case (c, i, j) => Position(c.position(_3)) }
      .join(saveDictionary(ctx, _3, file, dictionary, separator))
      .map { case (_, ((c, i, j), k)) => (c, i, j, k) }
      .groupBy { case (c, i, j, k) => Position(c.position(_4)) }
      .join(saveDictionary(ctx, _4, file, dictionary, separator))
      .map { case (_, ((c, i, j, k), l)) => (c, i, j, k, l) }
      .groupBy { case (c, i, j, k, l) => Position(c.position(_5)) }
      .join(saveDictionary(ctx, _5, file, dictionary, separator))
      .map { case (_, ((c, i, j, k, l), m)) =>
        i + separator + j + separator + k + separator + l + separator + m + separator + c.content.value.toShortString
      }
      .write(TypedSink(TextLine(file)))

    data
  }
}

/**
 * Rich wrapper around a `TypedPipe[Cell[_6]]`.
 *
 * @param data `TypedPipe[Cell[_6]]`.
 */
case class Matrix6D(
  data: TypedPipe[Cell[_6]]
) extends FwMatrix6D
  with Matrix[_5, _6]
  with ReducibleMatrix[_5, _6]
  with ReshapeableMatrix[_5, _6]
  with ApproximateDistribution[_5, _6] {
  def domain[T <: Tuner : DomainTuners](tuner: T = Default()): U[Position[_6]] = names(Over(_1), tuner)
    .map { case Position(c) => c }
    .cross(names(Over(_2), tuner).map { case Position(c) => c })
    .cross(names(Over(_3), tuner).map { case Position(c) => c })
    .cross(names(Over(_4), tuner).map { case Position(c) => c })
    .cross(names(Over(_5), tuner).map { case Position(c) => c })
    .cross(names(Over(_6), tuner).map { case Position(c) => c })
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

  def saveAsIV(ctx: C, file: String, dictionary: String, separator: String): U[Cell[_6]] = {
    import ctx._

    data
      .groupBy { case c => Position(c.position(_1)) }
      .join(saveDictionary(ctx, _1, file, dictionary, separator))
      .values
      .groupBy { case (c, i) => Position(c.position(_2)) }
      .join(saveDictionary(ctx, _2, file, dictionary, separator))
      .map { case (_, ((c, i), j)) => (c, i, j) }
      .groupBy { case (c, i, j) => Position(c.position(_3)) }
      .join(saveDictionary(ctx, _3, file, dictionary, separator))
      .map { case (_, ((c, i, j), k)) => (c, i, j, k) }
      .groupBy { case (c, i, j, k) => Position(c.position(_4)) }
      .join(saveDictionary(ctx, _4, file, dictionary, separator))
      .map { case (_, ((c, i, j, k), l)) => (c, i, j, k, l) }
      .groupBy { case (c, i, j, k, l) => Position(c.position(_5)) }
      .join(saveDictionary(ctx, _5, file, dictionary, separator))
      .map { case (_, ((c, i, j, k, l), m)) => (c, i, j, k, l, m) }
      .groupBy { case (c, i, j, k, l, m) => Position(c.position(_6)) }
      .join(saveDictionary(ctx, _6, file, dictionary, separator))
      .map { case (_, ((c, i, j, k, l, m), n)) =>
        i + separator +
        j + separator +
        k + separator +
        l + separator +
        m + separator +
        n + separator +
        c.content.value.toShortString
      }
      .write(TypedSink(TextLine(file)))

    data
  }
}

/**
 * Rich wrapper around a `TypedPipe[Cell[_7]]`.
 *
 * @param data `TypedPipe[Cell[_7]]`.
 */
case class Matrix7D(
  data: TypedPipe[Cell[_7]]
) extends FwMatrix7D
  with Matrix[_6, _7]
  with ReducibleMatrix[_6, _7]
  with ReshapeableMatrix[_6, _7]
  with ApproximateDistribution[_6, _7] {
  def domain[T <: Tuner : DomainTuners](tuner: T = Default()): U[Position[_7]] = names(Over(_1), tuner)
    .map { case Position(c) => c }
    .cross(names(Over(_2), tuner).map { case Position(c) => c })
    .cross(names(Over(_3), tuner).map { case Position(c) => c })
    .cross(names(Over(_4), tuner).map { case Position(c) => c })
    .cross(names(Over(_5), tuner).map { case Position(c) => c })
    .cross(names(Over(_6), tuner).map { case Position(c) => c })
    .cross(names(Over(_7), tuner).map { case Position(c) => c })
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

  def saveAsIV(ctx: C, file: String, dictionary: String, separator: String): U[Cell[_7]] = {
    import ctx._

    data
      .groupBy { case c => Position(c.position(_1)) }
      .join(saveDictionary(ctx, _1, file, dictionary, separator))
      .values
      .groupBy { case (c, i) => Position(c.position(_2)) }
      .join(saveDictionary(ctx, _2, file, dictionary, separator))
      .map { case (_, ((c, i), j)) => (c, i, j) }
      .groupBy { case (c, i, j) => Position(c.position(_3)) }
      .join(saveDictionary(ctx, _3, file, dictionary, separator))
      .map { case (_, ((c, i, j), k)) => (c, i, j, k) }
      .groupBy { case (c, i, j, k) => Position(c.position(_4)) }
      .join(saveDictionary(ctx, _4, file, dictionary, separator))
      .map { case (_, ((c, i, j, k), l)) => (c, i, j, k, l) }
      .groupBy { case (c, i, j, k, l) => Position(c.position(_5)) }
      .join(saveDictionary(ctx, _5, file, dictionary, separator))
      .map { case (_, ((c, i, j, k, l), m)) => (c, i, j, k, l, m) }
      .groupBy { case (c, i, j, k, l, m) => Position(c.position(_6)) }
      .join(saveDictionary(ctx, _6, file, dictionary, separator))
      .map { case (_, ((c, i, j, k, l, m), n)) => (c, i, j, k, l, m, n) }
      .groupBy { case (c, i, j, k, l, m, n) => Position(c.position(_7)) }
      .join(saveDictionary(ctx, _7, file, dictionary, separator))
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
      .write(TypedSink(TextLine(file)))

    data
  }
}

/**
 * Rich wrapper around a `TypedPipe[Cell[_8]]`.
 *
 * @param data `TypedPipe[Cell[_8]]`.
 */
case class Matrix8D(
  data: TypedPipe[Cell[_8]]
) extends FwMatrix8D
  with Matrix[_7, _8]
  with ReducibleMatrix[_7, _8]
  with ReshapeableMatrix[_7, _8]
  with ApproximateDistribution[_7, _8] {
  def domain[T <: Tuner : DomainTuners](tuner: T = Default()): U[Position[_8]] = names(Over(_1), tuner)
    .map { case Position(c) => c }
    .cross(names(Over(_2), tuner).map { case Position(c) => c })
    .cross(names(Over(_3), tuner).map { case Position(c) => c })
    .cross(names(Over(_4), tuner).map { case Position(c) => c })
    .cross(names(Over(_5), tuner).map { case Position(c) => c })
    .cross(names(Over(_6), tuner).map { case Position(c) => c })
    .cross(names(Over(_7), tuner).map { case Position(c) => c })
    .cross(names(Over(_8), tuner).map { case Position(c) => c })
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

  def saveAsIV(ctx: C, file: String, dictionary: String, separator: String): U[Cell[_8]] = {
    import ctx._

    data
      .groupBy { case c => Position(c.position(_1)) }
      .join(saveDictionary(ctx, _1, file, dictionary, separator))
      .values
      .groupBy { case (c, i) => Position(c.position(_2)) }
      .join(saveDictionary(ctx, _2, file, dictionary, separator))
      .map { case (_, ((c, i), j)) => (c, i, j) }
      .groupBy { case (c, i, j) => Position(c.position(_3)) }
      .join(saveDictionary(ctx, _3, file, dictionary, separator))
      .map { case (_, ((c, i, j), k)) => (c, i, j, k) }
      .groupBy { case (c, i, j, k) => Position(c.position(_4)) }
      .join(saveDictionary(ctx, _4, file, dictionary, separator))
      .map { case (_, ((c, i, j, k), l)) => (c, i, j, k, l) }
      .groupBy { case (c, i, j, k, l) => Position(c.position(_5)) }
      .join(saveDictionary(ctx, _5, file, dictionary, separator))
      .map { case (_, ((c, i, j, k, l), m)) => (c, i, j, k, l, m) }
      .groupBy { case (c, i, j, k, l, m) => Position(c.position(_6)) }
      .join(saveDictionary(ctx, _6, file, dictionary, separator))
      .map { case (_, ((c, i, j, k, l, m), n)) => (c, i, j, k, l, m, n) }
      .groupBy { case (c, i, j, k, l, m, n) => Position(c.position(_7)) }
      .join(saveDictionary(ctx, _7, file, dictionary, separator))
      .map { case (_, ((c, i, j, k, l, m, n), o)) => (c, i, j, k, l, m, n, o) }
      .groupBy { case (c, i, j, k, l, m, n, o) => Position(c.position(_8)) }
      .join(saveDictionary(ctx, _8, file, dictionary, separator))
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
      .write(TypedSink(TextLine(file)))

    data
  }
}

/**
 * Rich wrapper around a `TypedPipe[Cell[_9]]`.
 *
 * @param data `TypedPipe[Cell[_9]]`.
 */
case class Matrix9D(
  data: TypedPipe[Cell[_9]]
) extends FwMatrix9D
  with Matrix[_8, _9]
  with ReducibleMatrix[_8, _9]
  with ApproximateDistribution[_8, _9] {
  def domain[T <: Tuner : DomainTuners](tuner: T = Default()): U[Position[_9]] = names(Over(_1), tuner)
    .map { case Position(c) => c }
    .cross(names(Over(_2), tuner).map { case Position(c) => c })
    .cross(names(Over(_3), tuner).map { case Position(c) => c })
    .cross(names(Over(_4), tuner).map { case Position(c) => c })
    .cross(names(Over(_5), tuner).map { case Position(c) => c })
    .cross(names(Over(_6), tuner).map { case Position(c) => c })
    .cross(names(Over(_7), tuner).map { case Position(c) => c })
    .cross(names(Over(_8), tuner).map { case Position(c) => c })
    .cross(names(Over(_9), tuner).map { case Position(c) => c })
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

  def saveAsIV(ctx: C, file: String, dictionary: String, separator: String): U[Cell[_9]] = {
    import ctx._

    data
      .groupBy { case c => Position(c.position(_1)) }
      .join(saveDictionary(ctx, _1, file, dictionary, separator))
      .values
      .groupBy { case (c, i) => Position(c.position(_2)) }
      .join(saveDictionary(ctx, _2, file, dictionary, separator))
      .map { case (_, ((c, i), j)) => (c, i, j) }
      .groupBy { case (c, i, j) => Position(c.position(_3)) }
      .join(saveDictionary(ctx, _3, file, dictionary, separator))
      .map { case (_, ((c, i, j), k)) => (c, i, j, k) }
      .groupBy { case (c, i, j, k) => Position(c.position(_4)) }
      .join(saveDictionary(ctx, _4, file, dictionary, separator))
      .map { case (_, ((c, i, j, k), l)) => (c, i, j, k, l) }
      .groupBy { case (c, i, j, k, l) => Position(c.position(_5)) }
      .join(saveDictionary(ctx, _5, file, dictionary, separator))
      .map { case (_, ((c, i, j, k, l), m)) => (c, i, j, k, l, m) }
      .groupBy { case (c, i, j, k, l, m) => Position(c.position(_6)) }
      .join(saveDictionary(ctx, _6, file, dictionary, separator))
      .map { case (_, ((c, i, j, k, l, m), n)) => (c, i, j, k, l, m, n) }
      .groupBy { case (c, i, j, k, l, m, n) => Position(c.position(_7)) }
      .join(saveDictionary(ctx, _7, file, dictionary, separator))
      .map { case (_, ((c, i, j, k, l, m, n), o)) => (c, i, j, k, l, m, n, o) }
      .groupBy { case (c, i, j, k, l, m, n, o) => Position(c.position(_8)) }
      .join(saveDictionary(ctx, _8, file, dictionary, separator))
      .map { case (_, ((c, i, j, k, l, m, n, o), p)) => (c, i, j, k, l, m, n, o, p) }
      .groupBy { case (c, i, j, k, l, m, n, o, p) => Position(c.position(_9)) }
      .join(saveDictionary(ctx, _9, file, dictionary, separator))
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
      .write(TypedSink(TextLine(file)))

    data
  }
}

