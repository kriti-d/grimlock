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

package commbank.grimlock.scalding

import au.com.cba.omnia.ebenezer.scrooge.ParquetScroogeSource

import commbank.grimlock.framework.{
  Binary,
  Cell,
  Consume,
  Compactable,
  Default,
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
  Stream,
  Ternary,
  Tuner,
  Type,
  Unbalanced
}
import commbank.grimlock.framework.aggregate.{ Aggregator, AggregatorWithValue }
import commbank.grimlock.framework.content.Content
import commbank.grimlock.framework.content.metadata.{ DiscreteSchema, NominalSchema }
import commbank.grimlock.framework.DefaultTuners.{ TP1, TP3, TP4 }
import commbank.grimlock.framework.encoding.Value
import commbank.grimlock.framework.pairwise.{ Comparer, Operator, OperatorWithValue }
import commbank.grimlock.framework.partition.{ Partitioner, PartitionerWithValue }
import commbank.grimlock.framework.position.{ Position, Slice }
import commbank.grimlock.framework.position.Position.listSetAdditiveCollection
import commbank.grimlock.framework.sample.{ Sampler, SamplerWithValue }
import commbank.grimlock.framework.squash.{ Squasher, SquasherWithValue }
import commbank.grimlock.framework.transform.{ Transformer, TransformerWithValue }
import commbank.grimlock.framework.utility.{ =:!=, Distinct, Escape }
import commbank.grimlock.framework.utility.UnionTypes.{ In, Is, OneOf }
import commbank.grimlock.framework.window.{ Window, WindowWithValue }

import commbank.grimlock.scalding.distance.PairwiseDistance
import commbank.grimlock.scalding.distribution.ApproximateDistribution
import commbank.grimlock.scalding.environment.{ DistributedData, Environment, UserData }
import commbank.grimlock.scalding.statistics.Statistics
import commbank.grimlock.scalding.ScaldingImplicits._

import com.twitter.algebird.Semigroup
import com.twitter.scalding.{ TextLine, WritableSequenceFile }
import com.twitter.scalding.typed.TypedPipe
import com.twitter.scrooge.ThriftStruct

import org.apache.hadoop.io.Writable

import scala.collection.immutable.ListSet
import scala.reflect.ClassTag

import shapeless.{ Nat, Witness }
import shapeless.nat.{ _0, _1, _2, _3, _4, _5, _6, _7, _8, _9 }
import shapeless.ops.nat.{ Diff, LTEq, GT, GTEq, ToInt }
import shapeless.syntax.sized._

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
    val msj = Option(SetMapSideJoin[Position[slice.S], Cell[P]]())

    val result = data
      .map { case c => (slice.selected(c.position), c) }
      .tunedLeftJoin(tuner, positions.map { case p => (p, ()) }, msj)
      .flatMap {
        case (_, (c, Some(_))) => schema(c.content.value.toShortString)
          .map { case con => List(Right(Cell(c.position, con))) }
          .getOrElse(writer(c).map { case e => Left(e) })
        case (_, (c, None)) => List(Right(c))
      }

    (result.collect { case Right(cell) => cell }, result.collect { case Left(error) => error })
  }

  type CompactTuners[T] = TP1[T]
  def compact()(implicit ev: ClassTag[Position[P]]): E[Map[Position[P], Content]] = {
    val semigroup = new Semigroup[Map[Position[P], Content]] {
      def plus(l: Map[Position[P], Content], r: Map[Position[P], Content]) = l ++ r
    }

    data
      .map { case c => Map(c.position -> c.content) }
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
    val semigroup = new Semigroup[Map[Position[slice.S], ev3.C[slice.R]]] {
      def plus(l: Map[Position[slice.S], ev3.C[slice.R]], r: Map[Position[slice.S], ev3.C[slice.R]]) = l ++ r
    }

    data
      .map { case c => (slice.selected(c.position), ev3.toMap(slice)(c)) }
      .tunedReduce(tuner, (l, r) => ev3.combineMaps(slice)(l, r))
      .values
      .sum(semigroup)
  }

  type DomainTuners[T] = TP3[T]
  def domain[T <: Tuner : DomainTuners](tuner: T = Default()): U[Position[P]] = naturalDomain(tuner)

  type FillHeterogeneousTuners[T] = T In OneOf[Default[NoParameters]]#
    Or[Ternary[InMemory[NoParameters], InMemory[NoParameters], Default[NoParameters]]]#
    Or[Ternary[InMemory[NoParameters], Default[NoParameters], Default[NoParameters]]]#
    Or[Ternary[InMemory[NoParameters], InMemory[NoParameters], Default[Reducers]]]#
    Or[Ternary[InMemory[NoParameters], Default[NoParameters], Default[Reducers]]]#
    Or[Ternary[InMemory[NoParameters], Default[Reducers], Default[Reducers]]]#
    Or[Ternary[Default[NoParameters], Default[NoParameters], Default[Reducers]]]#
    Or[Ternary[Default[NoParameters], Default[Reducers], Default[Reducers]]]#
    Or[Ternary[Default[Reducers], Default[Reducers], Default[Reducers]]]
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
    val msj = Option(MapMapSideJoin[Position[slice.S], Position[P], Content]())

    val (dt, vt, jt) = tuner match {
      case Ternary(f, s, t) => (f, s, t)
      case _ => (Default(), Default(), Default())
    }

    naturalDomain(dt)
      .map { case p => (slice.selected(p), p) }
      .tunedJoin(vt, values.map { case c => (c.position, c.content) }, msj)
      .map { case (_, (p, c)) => (p, Cell(p, c)) }
      .tunedLeftJoin(jt, data.map { case c => (c.position, c) })
      .map { case (_, (c, co)) => co.getOrElse(c) }
  }

  type FillHomogeneousTuners[T] = T In OneOf[Default[NoParameters]]# // Default[NoParameters], Default[NoParameters]
    Or[Binary[InMemory[NoParameters], Default[NoParameters]]]#
    Or[Binary[InMemory[NoParameters], Default[Reducers]]]#
    Or[Binary[InMemory[Reducers], Default[NoParameters]]]#
    Or[Binary[InMemory[Reducers], Default[Reducers]]]#
    Or[Binary[Default[NoParameters], Default[Reducers]]]#
    Or[Binary[Default[Reducers], Default[NoParameters]]]#
    Or[Binary[Default[Reducers], Default[Reducers]]]
  def fillHomogeneous[
    T <: Tuner : FillHomogeneousTuners
  ](
    value: Content,
    tuner: T = Default()
  )(implicit
    ev: ClassTag[Position[P]]
  ): U[Cell[P]] = {
    val (dt, jt) = tuner match {
      case Binary(f, s) => (f, s)
      case _ => (Default(), Default())
    }

    naturalDomain(dt)
      .map { case p => (p, ()) }
      .tunedLeftJoin(jt, data.map { case c => (c.position, c) })
      .map { case (p, (_, co)) => co.getOrElse(Cell(p, value)) }
  }

  type GetTuners[T] = TP4[T]
  def get[
    T <: Tuner : GetTuners
  ](
    positions: U[Position[P]],
    tuner: T = InMemory()
  )(implicit
    ev: ClassTag[Position[P]]
  ): U[Cell[P]] = {
    val msj = Option(SetMapSideJoin[Position[P], Cell[P]]())

    data
      .map { case c => (c.position, c) }
      .tunedJoin(tuner, positions.map { case p => (p, ()) }, msj)
      .map { case (_, (c, _)) => c }
  }

  type JoinTuners[T] = T In OneOf[InMemory[NoParameters]]#
    Or[Default[NoParameters]]#
    Or[Binary[InMemory[NoParameters], Default[NoParameters]]]#
    Or[Binary[InMemory[NoParameters], Default[Reducers]]]#
    Or[Binary[InMemory[NoParameters], Unbalanced[Reducers]]]#
    Or[Binary[InMemory[Reducers], Default[Reducers]]]#
    Or[Binary[InMemory[Reducers], Unbalanced[Reducers]]]#
    Or[Binary[Default[NoParameters], Default[Reducers]]]#
    Or[Binary[Default[NoParameters], Unbalanced[Reducers]]]#
    Or[Binary[Default[Reducers], Default[Reducers]]]#
    Or[Binary[Default[Reducers], Unbalanced[Reducers]]]
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
    def msj[V] = Option(SetMapSideJoin[Position[slice.S], V]())

    val (t1, t2) = tuner match {
      case Binary(f, s) => (f, s)
      case t => (t, t)
    }

    val keep = data
      .map { case c => slice.selected(c.position) }
      .tunedDistinct(t1)
      .map { case p => (p, ()) }
      .tunedJoin(
        t1,
        that
          .map { case c => slice.selected(c.position) }
          .tunedDistinct(t1)
          .map { case p => (p, ()) },
        msj
      )
      .map { case (p, _) => (p, ()) }
      .forceToDisk // TODO: Should this be configurable?

    (data ++ that)
      .map { case c => (slice.selected(c.position), c) }
      .tunedJoin(t2, keep, msj)
      .map { case (_, (c, _)) => c }
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

  type NamesTuners[T] = TP1[T]
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
    .map { case c => slice.selected(c.position) }
    .tunedDistinct(tuner)

  type PairwiseTuners[T] = T In OneOf[InMemory[NoParameters]]#
    Or[Default[NoParameters]]#
    Or[Ternary[InMemory[NoParameters], Default[Reducers], Default[Reducers]]]#
    Or[Ternary[InMemory[NoParameters], Default[Reducers], Unbalanced[Reducers]]]#
    Or[Ternary[InMemory[NoParameters], Unbalanced[Reducers], Default[Reducers]]]#
    Or[Ternary[InMemory[NoParameters], Unbalanced[Reducers], Unbalanced[Reducers]]]#
    Or[Ternary[Default[Reducers], Default[Reducers], Default[Reducers]]]#
    Or[Ternary[Default[Reducers], Default[Reducers], Unbalanced[Reducers]]]#
    Or[Ternary[Default[Reducers], Unbalanced[Reducers], Default[Reducers]]]#
    Or[Ternary[Default[Reducers], Unbalanced[Reducers], Unbalanced[Reducers]]]
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
      .flatMapWithValue(value) { case ((lc, rc), vo) =>
        vo.toList.flatMap { case v => operator.computeWithValue(lc, rc, v) }
      }
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
      .flatMapWithValue(value) { case ((lc, rc), vo) =>
        vo.toList.flatMap { case v => operator.computeWithValue(lc, rc, v) }
      }
  }

  def relocate[Q <: Nat](locate: Locate.FromCell[P, Q])(implicit ev: GTEq[Q, P]): U[Cell[Q]] = data
    .flatMap { case c => locate(c).map { case p => Cell(p, c.content) } }

  def relocateWithValue[
    Q <: Nat,
    W
  ](
    value: E[W],
    locate: Locate.FromCellWithValue[P, Q, W]
  )(implicit
    ev: GTEq[Q, P]
  ): U[Cell[Q]] = data.flatMapWithValue(value) { case (c, vo) =>
    vo.flatMap { case v => locate(c, v).map { case p => Cell(p, c.content) } }
  }

  type SaveAsIVTuners[T] = T In OneOf[Default[NoParameters]]#  // Default[NoParameters], Default[NoParameters]
    Or[Binary[InMemory[NoParameters], Default[NoParameters]]]#
    Or[Binary[InMemory[NoParameters], Redistribute]]#
    Or[Binary[Default[NoParameters], Redistribute]]#
    Or[Binary[Default[Reducers], Default[NoParameters]]]#
    Or[Binary[Default[Reducers], Redistribute]]#
    Or[Binary[Unbalanced[Reducers], Default[NoParameters]]]#
    Or[Binary[Unbalanced[Reducers], Redistribute]]

  type SaveAsTextTuners[T] = PersistParition[T]
  def saveAsText[
    T <: Tuner : SaveAsTextTuners
  ](
    ctx: C,
    file: String,
    writer: TextWriter,
    tuner: T = Default()
  ): U[Cell[P]] = saveText(ctx, file, writer, tuner)

  type SetTuners[T] = TP1[T]
  def set[
    T <: Tuner : SetTuners
  ](
    values: U[Cell[P]],
    tuner: T = Default()
  )(implicit
    ev: ClassTag[Position[P]]
  ): U[Cell[P]] = data
    .map { case c => (c.position, c) }
    .tunedOuterJoin(tuner, values.map { case c => (c.position, c) })
    .flatMap { case (_, (co, cn)) => cn.orElse(co) }

  type ShapeTuners[T] = TP1[T]
  def shape[T <: Tuner : ShapeTuners](tuner: T = Default()): U[Cell[_1]] = data
    .flatMap { case c => c.position.coordinates.map { case c => c.toString }.zipWithIndex }
    .tunedDistinct(tuner)
    .map { case (s, i) => i }
    .tunedSize(tuner)
    .map { case (i, c) => Cell(Position(i + 1), Content(DiscreteSchema[Long](), c)) }

  type SizeTuners[T] = TP1[T]
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
    val coords = data.map { case c => c.position(ev3.value) }
    val dist = if (distinct) coords else coords.tunedDistinct(tuner)(Value.ordering)

    dist
      .map { case c => 1L }
      .sum
      .map { case sum => Cell(Position(Nat.toInt[dim.N]), Content(DiscreteSchema[Long](), sum)) }
  }

  type SliceTuners[T] = TP4[T]
  def slice[
    T <: Tuner : SliceTuners
  ](
    slice: Slice[L, P],
    tuner: T = InMemory()
  )(
    keep: Boolean,
    positions: U[Position[slice.S]]
  )(implicit
    ev1: ClassTag[Position[slice.S]],
    ev2: Diff.Aux[P, _1, L]
  ): U[Cell[P]] = {
    val msj = Option(SetMapSideJoin[Position[slice.S], Cell[P]]())

    data
      .map { case c => (slice.selected(c.position), c) }
      .tunedLeftJoin(tuner, positions.map { case p => (p, ()) }, msj)
      .collect { case (_, (c, o)) if (o.isEmpty != keep) => c }
  }

  type SlideTuners[T] = T In OneOf[Default[NoParameters]]#
    Or[Default[Reducers]]#
    Or[Redistribute]
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
      .map { case c => (slice.selected(c.position), (slice.remainder(c.position), window.prepare(c))) }
      .tunedRedistribute(tuner) // TODO: Is this needed?
      .tuneReducers(tuner)
      .sortBy { case (r, _) => r }(Position.ordering(ascending))
      .scanLeft(Option.empty[(window.T, TraversableOnce[window.O])]) {
        case (None, (r, i)) => Option(window.initialise(r, i))
        case (Some((t, _)), (r, i)) => Option(window.update(r, i, t))
      }
      .flatMap {
        case (p, Some((_, to))) => to.flatMap { case o => window.present(p, o) }
        case _ => List()
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
      .flatMapWithValue(value) { case (c, vo) =>
        vo.map { case v => (slice.selected(c.position), (slice.remainder(c.position), window.prepareWithValue(c, v))) }
      }
      .tunedRedistribute(tuner) // TODO: Is this needed?
      .tuneReducers(tuner)
      .sortBy { case (r, _) => r }(Position.ordering(ascending))
      .scanLeft(Option.empty[(window.T, TraversableOnce[window.O])]) {
        case (None, (r, i)) => Option(window.initialise(r, i))
        case (Some((t, _)), (r, i)) => Option(window.update(r, i, t))
      }
      .flatMapWithValue(value) {
        case ((p, Some((_, to))), Some(v)) => to.flatMap { case o => window.presentWithValue(p, o, v) }
        case _ => List()
      }
  }

  def split[I](partitioners: Partitioner[P, I]*): U[(I, Cell[P])] = {
    val partitioner: Partitioner[P, I] = if (partitioners.size == 1) partitioners.head else partitioners.toList

    data.flatMap { case c => partitioner.assign(c).map { case q => (q, c) } }
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

    data.flatMapWithValue(value) { case (c, vo) =>
      vo.toList.flatMap { case v => partitioner.assignWithValue(c, v).map { case q => (q, c) } }
    }
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
      .flatMap { case c => writer(c).map { case s => (hash(c.position) % reducers, s) } }
      .tuneReducers(tuner)
      .tunedStream(tuner, (key, itr) => Stream.delegate(command, files)(key, itr).flatMap { case s => parser(s) })

    (result.collect { case (_, Right(c)) => c }, result.collect { case (_, Left(e)) => e })
  }

  def streamByPosition[
    Q <: Nat,
    T <: Tuner : StreamTuners
  ](
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
    val (rows, _) = pivot(slice, tuner)

    val result = rows
      .flatMap { case (key, list) =>
        writer(list.map { case (_, v) => v }).map { case s => (murmur.hash(key.coordinates.toArray) % reducers, s) }
      }
      .tuneReducers(tuner)
      .tunedStream(tuner, (key, itr) => Stream.delegate(command, files)(key, itr).flatMap { case s => parser(s) })

    (result.collect { case (_, Right(c)) => c }, result.collect { case (_, Left(e)) => e })
  }

  def subset(samplers: Sampler[P]*): U[Cell[P]] = {
    val sampler: Sampler[P] = if (samplers.size == 1) samplers.head else samplers.toList

    data.filter { case c => sampler.select(c) }
  }

  def subsetWithValue[W](value: E[W], samplers: SamplerWithValue[P] { type V >: W }*): U[Cell[P]] = {
    val sampler: SamplerWithValue[P] { type V >: W } = if (samplers.size == 1) samplers.head else samplers.toList

    data.filterWithValue(value) { case (c, vo) => vo.map { case v => sampler.selectWithValue(c, v) }.getOrElse(false) }
  }

  type SummariseTuners[T] = TP1[T]
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
      .flatMap { case c => aggregator.prepare(c).map { case t => (slice.selected(c.position), t) } }
      .tunedReduce(tuner, (lt, rt) => aggregator.reduce(lt, rt))
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
      .flatMapWithValue(value) { case (c, vo) =>
        vo.flatMap { case v => aggregator.prepareWithValue(c, v).map { case t => (slice.selected(c.position), t) } }
      }
      .tunedReduce(tuner, (lt, rt) => aggregator.reduce(lt, rt))
      .flatMapWithValue(value) { case ((p, t), vo) =>
        vo.toList.flatMap { case v => aggregator.presentWithValue(p, t, v) }
      }
  }

  def toSequence[K <: Writable, V <: Writable](writer: SequenceWriter[K, V]): U[(K, V)] = data
    .flatMap { case c => writer(c) }

  def toText(writer: TextWriter): U[String] = data.flatMap { case c => writer(c) }

  def toVector(melt: (List[Value]) => Value): U[Cell[_1]] = data
    .map { case Cell(p, c) => Cell(Position(melt(p.coordinates)), c) }

  def transform[Q <: Nat](transformers: Transformer[P, Q]*)(implicit ev: GTEq[Q, P]): U[Cell[Q]] = {
    val transformer: Transformer[P, Q] = if (transformers.size == 1) transformers.head else transformers.toList

    data.flatMap { case c => transformer.present(c) }
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

    data.flatMapWithValue(value) { case (c, vo) => vo.toList.flatMap { case v => transformer.presentWithValue(c, v) } }
  }

  type TypesTuners[T] = TP1[T]
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
    .tunedReduce(tuner, (lt, rt) => lt.getCommonType(rt))
    .map { case (p, t) => Cell(p, Content(NominalSchema[Type](), if (specific) t else t.getRootType)) }

  type UniqueTuners[T] = TP1[T]
  def unique[T <: Tuner : UniqueTuners](tuner: T = Default()): U[Content] = {
    val ordering = new Ordering[Content] { def compare(l: Content, r: Content) = l.toString.compare(r.toString) }

    data
      .map { case c => c.content }
      .tunedDistinct(tuner)(ordering)
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
      .tunedDistinct(tuner)(ordering)
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
    val msj = Option(MapMapSideJoin[Position[slice.S], Cell[P], List[Cell.Predicate[P]]]())

    val pp = predicates
      .map { case (pos, pred) => pos.map { case p => (p, pred) } }
      .reduce((l, r) => l ++ r)
      .map { case (pos, pred) => (pos, List(pred)) }
      .tunedReduce(tuner, _ ++ _)

    data
      .map { case c => (slice.selected(c.position), c) }
      .tunedJoin(tuner, pp, msj)
      .collect { case (_, (c, lst)) if (lst.exists(pred => pred(c))) => c.position }
  }

  protected def coordinates[
    D <: Nat : ToInt
  ](
    dim: D,
    tuner: Tuner
  )(implicit
    ev: LTEq[D, P]
  ): U[Value] = data.map { case c => c.position(dim) }.tunedDistinct(tuner)(Value.ordering)

  protected def getSaveAsIVTuners(tuner: Tuner): (Tuner, Tuner) = tuner match {
    case Binary(j, r) => (j, r)
    case _ => (Default(), Default())
  }

  protected def naturalDomain(tuner: Tuner): U[Position[P]]

  protected def saveDictionary[
    D <: Nat : ToInt
  ](
    ctx: C,
    dim: D,
    file: String,
    dictionary: String,
    separator: String,
    tuner: Tuner
  )(implicit
    ev1: LTEq[D, P],
    ev2: Witness.Aux[dim.N],
    ev3: ToInt[dim.N],
    ev4: Diff.Aux[P, L, _1],
    ev5: Diff.Aux[P, _1, L]
  ): U[(Position[_1], Int)] = {
    val numbered = coordinates(dim, tuner)
      .groupAll
      .mapGroup { case (_, itr) => itr.zipWithIndex }
      .map { case (_, (c, i)) => (Position(c), i) }

    numbered
      .map { case (Position(c), i) => c.toShortString + separator + i }
      .tunedSaveAsText(ctx, Redistribute(1), dictionary.format(file, Nat.toInt[D]))

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
    tuner match {
      case InMemory(_) =>
        ldata
          .tunedCross[Cell[P]](
            tuner,
            (lc, rc) => comparer.keep(slice.selected(lc.position), slice.selected(rc.position)),
            rdata
          )
      case _ =>
        def msj[V] = Option(MapMapSideJoin[Position[slice.S], Cell[P], V]())

        val (ct, lt, rt) = tuner match {
          case Ternary(a, s, r) => (a, s, r)
          case _ => (Default(), Default(), Default())
        }

        ldata
          .map { case c => (slice.selected(c.position), c) }
          .tunedJoin(
            lt,
            ldata
              .map { case Cell(p, _) => slice.selected(p) }
              .tunedDistinct(lt)
              .tunedCross[Position[slice.S]](
                ct,
                (lp, rp) => comparer.keep(lp, rp),
                rdata.map { case Cell(p, _) => slice.selected(p) }.tunedDistinct(rt)
              )
              .forceToDisk, // TODO: Should this be configurable?
            msj
          )
          .map { case (_, (lc, rp)) => (rp, lc) }
          .tunedJoin(rt, rdata.map { case c => (slice.selected(c.position), c) }, msj)
          .map { case (_, (lc, rc)) => (lc, rc) }
    }
  }

  protected def pivot(
    slice: Slice[L, P],
    tuner: Tuner
  )(implicit
    ev: Diff.Aux[P, _1, L]
  ): (U[(Position[slice.S], List[(Position[slice.R], Option[Cell[P]])])], E[List[Position[slice.R]]]) = {
    def setSemigroup = new Semigroup[Set[Position[slice.R]]] {
      def plus(l: Set[Position[slice.R]], r: Set[Position[slice.R]]) = l ++ r
    }
    def mapSemigroup = new Semigroup[Map[Position[slice.R], Cell[P]]] {
      def plus(l: Map[Position[slice.R], Cell[P]], r: Map[Position[slice.R], Cell[P]]) = l ++ r
    }

    val columns = data
      .map { case c => Set(slice.remainder(c.position)) }
      .sum(setSemigroup)
      .map { case s => s.toList.sorted }

    val pivoted = data
      .map { case c => (slice.selected(c.position), Map(slice.remainder(c.position) -> c)) }
      .tuneReducers(tuner)
      .sum(mapSemigroup)
      .flatMapWithValue(columns) { case ((key, map), opt) =>
        opt.map { case cols => (key, cols.map { case c => (c, map.get(c)) }) }
      }

    (pivoted, columns)
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

  type SquashTuners[T] = TP1[T]
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
    data
      .flatMap { case c => squasher.prepare(c, dim).map { case t => (c.position.remove(dim), t) } }
      .tunedReduce(tuner, (lt, rt) => squasher.reduce(lt, rt))
      .flatMap { case (p, t) => squasher.present(t).map { case c => Cell(p, c) } }
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
    data
      .flatMapWithValue(value) { case (c, vo) =>
        vo.flatMap { case v => squasher.prepareWithValue(c, dim, v).map { case t => (c.position.remove(dim), t) } }
      }
      .tunedReduce(tuner, (lt, rt) => squasher.reduce(lt, rt))
      .flatMapWithValue(value) { case ((p, t), vo) =>
        vo.flatMap { case v => squasher.presentWithValue(t, v).map { case c => Cell(p, c) } }
      }
  }
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
    val msj = Option(MapMapSideJoin[Position[L], Cell[P], Value]())

    val keys = data
      .collect[(Position[L], Value)] { case c if (c.position(dim) equ coordinate) =>
        (c.position.remove(dim), c.content.value)
      }

    data
      .collect { case c if (c.position(dim) neq coordinate) => (c.position.remove(dim), c) }
      .tunedLeftJoin(tuner, keys, msj)
      .flatMap { case (_, (c, v)) => locate(c, v).map { case p => Cell(p, c.content) } }
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
    val pipe = TypedPipe.from(ParquetScroogeSource[T](file)).flatMap { case s => parser(s) }

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
  with ApproximateDistribution[_0, _1]
  with Statistics[_0, _1] {
  def saveAsIV[
    T <: Tuner : SaveAsIVTuners
  ](
    ctx: C,
    file: String,
    dictionary: String,
    separator: String,
    tuner: T = Default()
  ): U[Cell[_1]] = {
    def msj[V] = Option(MapMapSideJoin[Position[_1], V, Int]())

    val (jt, rt) = getSaveAsIVTuners(tuner)

    data
      .map { case c => (c.position, c) }
      .tunedJoin(jt, saveDictionary(ctx, _1, file, dictionary, separator, jt), msj)
      .map { case (_, (c, i)) => i + separator + c.content.value.toShortString }
      .tunedSaveAsText(ctx, rt, file)

    data
  }

  protected def naturalDomain(tuner: Tuner): U[Position[_1]] = coordinates(_1, tuner)
    .map { case c1 => Position(c1) }
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
  with ApproximateDistribution[_1, _2]
  with PairwiseDistance[_1, _2]
  with Statistics[_1, _2] {
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
    val l = List(Nat.toInt[D1], Nat.toInt[D2]).zipWithIndex.sortBy { case (d, _) => d }.map { case (_, i) => i }

    data.map { case Cell(p, c) => Cell(p.permute(ListSet(l:_*).sized(2).get), c) }
  }

  type SaveAsCSVTuners[T] = T In OneOf[Default[NoParameters]]# // Default[NoParameters], Default[NoParameters]
    Or[Default[Reducers]]#                                     // Default[Reducers],     Default[NoParameters]
    Or[Redistribute]#                                          // Default[NoParameters], Redistribute
    Or[Binary[Default[Reducers], Redistribute]]
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
    val (pt, rt) = tuner match {
      case Binary(p, r) => (p, r)
      case r @ Redistribute(_) => (Default(), r)
      case p @ Default(_) => (p, Default())
      case _ => (Default(), Default())
    }

    val (pivoted, columns) = pivot(slice, pt)

    if (writeHeader)
      columns
        .map { case lst =>
          (if (writeRowId) escapee.escape(rowId) + separator else "") + lst
            .map { case p => escapee.escape(p.coordinates.head.toShortString) }
            .mkString(separator)
        }
        .toTypedPipe
        .tunedSaveAsText(ctx, Redistribute(1), header.format(file))

    pivoted
      .map { case (p, lst) =>
        (if (writeRowId) escapee.escape(p.coordinates.head.toShortString) + separator else "") + lst
          .map { case (_, v) => v.map { case c => escapee.escape(c.content.value.toShortString) }.getOrElse("") }
          .mkString(separator)
      }
      .tunedSaveAsText(ctx, rt, file)

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
    def msj[V] = Option(MapMapSideJoin[Position[_1], V, Int]())

    val (jt, rt) = getSaveAsIVTuners(tuner)

    data
      .map { case c => (Position(c.position(_1)), c) }
      .tunedJoin(jt, saveDictionary(ctx, _1, file, dictionary, separator, jt), msj)
      .map { case (_, (c, i)) => (Position(c.position(_2)), (c, i)) }
      .tunedJoin(jt, saveDictionary(ctx, _2, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i), j)) => i + separator + j + separator + c.content.value.toShortString }
      .tunedSaveAsText(ctx, rt, file)

    data
  }

  type SaveAsVWTuners[T] = T In OneOf[Default[NoParameters]]#      // Def[NoParam], Def[NoParam], Def[NoParam]
    Or[Default[Reducers]]#                                         // Def[Red], Def[Red], Def[NoParam]
    Or[Binary[Default[Reducers], Redistribute]]#                   // Def[Red], Def[Red], Redis
    Or[Binary[Default[NoParameters], Redistribute]]#               // Def[NoParam], Def[NoParam], Redis
    Or[Binary[Default[NoParameters], InMemory[NoParameters]]]#     // Def[NoParam], InMem[NoParam], Def[NoParam]
    Or[Binary[Default[NoParameters], Default[Reducers]]]#          // Def[NoParam], Def[Red], Def[NoParam]
    Or[Ternary[Default[NoParameters], InMemory[NoParameters], Redistribute]]#
    Or[Ternary[Default[NoParameters], Default[Reducers], Redistribute]]#
    Or[Ternary[Default[NoParameters], Unbalanced[Reducers], Default[NoParameters]]]#
    Or[Ternary[Default[NoParameters], Unbalanced[Reducers], Redistribute]]#
    Or[Ternary[Default[Reducers], InMemory[NoParameters], Default[NoParameters]]]#
    Or[Ternary[Default[Reducers], InMemory[NoParameters], Redistribute]]#
    Or[Ternary[Default[Reducers], Unbalanced[Reducers], Default[NoParameters]]]#
    Or[Ternary[Default[Reducers], Unbalanced[Reducers], Redistribute]]
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
  ): U[Cell[_2]] = saveVW(slice, tuner)(ctx, file, Option(labels), None, tag, dictionary, separator)

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
  ): U[Cell[_2]] = saveVW(slice, tuner)(ctx, file, None, Option(importance), tag, dictionary, separator)

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
  ): U[Cell[_2]] = saveVW(slice, tuner)(ctx, file, Option(labels), Option(importance), tag, dictionary, separator)

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
    val msj = Option(MapMapSideJoin[Position[slice.S], String, Cell[slice.S]]())

    val (pt, jt, rt) = tuner match {
      case Ternary(f, s, t) => (f, s, t)
      case Binary(t @ Default(_), r @ Redistribute(_)) => (t, t, r)
      case Binary(p, j) => (p, j, Default())
      case t @ Default(Reducers(_)) => (t, t, Default())
      case _ => (Default(), Default(), Default())
    }

    val (pivoted, columns) = pivot(slice, pt)
    val dict = columns.map { case l => l.zipWithIndex.toMap }

    dict
      .flatMap { case m => m.map { case (p, i) => p.coordinates.head.toShortString + separator + i } }
      .tunedSaveAsText(ctx, Redistribute(1), dictionary.format(file))

    val features = pivoted
      .flatMapWithValue(dict) { case ((key, lst), dct) =>
        dct.map { case d =>
          (
            key,
            lst
              .flatMap { case (p, v) =>
                v.flatMap { case c => c.content.value.asDouble.map { case w => d(p) + ":" + w } }
              }
              .mkString((if (tag) key.coordinates.head.toShortString else "") + "| ", " ", "")
          )
        }
      }

    val weighted = importance match {
      case Some(imp) => features
        .tunedJoin(jt, imp.map { case c => (c.position, c) }, msj)
        .flatMap { case (p, (s, c)) => c.content.value.asDouble.map { case i => (p, i + " " + s) } }
      case None => features
    }

    val examples = labels match {
      case Some(lab) => weighted
        .tunedJoin(jt, lab.map { case c => (c.position, c) }, msj)
        .flatMap { case (p, (s, c)) => c.content.value.asDouble.map { case l => (p, l + " " + s) } }
      case None => weighted
    }

    examples
      .map { case (p, s) => s }
      .tunedSaveAsText(ctx, rt, file)

    data
  }

  protected def naturalDomain(tuner: Tuner): U[Position[_2]] = coordinates(_1, tuner)
    .tunedCross[Value](tuner, (_, _) => true, coordinates(_2, tuner))
    .map { case (c1, c2) => Position(c1, c2) }
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
  with ApproximateDistribution[_2, _3]
  with PairwiseDistance[_2, _3]
  with Statistics[_2, _3] {
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
    val l = List(Nat.toInt[D1], Nat.toInt[D2], Nat.toInt[D3])
      .zipWithIndex
      .sortBy { case (d, _) => d }
      .map { case (_, i) => i }

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
    def msj[V] = Option(MapMapSideJoin[Position[_1], V, Int]())

    val (jt, rt) = getSaveAsIVTuners(tuner)

    data
      .map { case c => (Position(c.position(_1)), c) }
      .tunedJoin(jt, saveDictionary(ctx, _1, file, dictionary, separator, jt), msj)
      .map { case (_, (c, i)) => (Position(c.position(_2)), (c, i)) }
      .tunedJoin(jt, saveDictionary(ctx, _2, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i), j)) => (Position(c.position(_3)), (c, i, j)) }
      .tunedJoin(jt, saveDictionary(ctx, _3, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i, j), k)) => i + separator + j + separator + k + separator + c.content.value.toShortString }
      .tunedSaveAsText(ctx, rt, file)

    data
  }

  protected def naturalDomain(tuner: Tuner): U[Position[_3]] = coordinates(_1, tuner)
    .tunedCross[Value](tuner, (_, _) => true, coordinates(_2, tuner))
    .tunedCross[Value](tuner, (_, _) => true, coordinates(_3, tuner))
    .map { case ((c1, c2), c3) => Position(c1, c2, c3) }
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
  with ApproximateDistribution[_3, _4]
  with PairwiseDistance[_3, _4]
  with Statistics[_3, _4] {
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
    val l = List(Nat.toInt[D1], Nat.toInt[D2], Nat.toInt[D3], Nat.toInt[D4])
      .zipWithIndex
      .sortBy { case (d, _) => d }
      .map { case (_, i) => i }

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
    def msj[V] = Option(MapMapSideJoin[Position[_1], V, Int]())

    val (jt, rt) = getSaveAsIVTuners(tuner)

    data
      .map { case c => (Position(c.position(_1)), c) }
      .tunedJoin(jt, saveDictionary(ctx, _1, file, dictionary, separator, jt), msj)
      .map { case (_, (c, i)) => (Position(c.position(_2)), (c, i)) }
      .tunedJoin(jt, saveDictionary(ctx, _2, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i), j)) => (Position(c.position(_3)), (c, i, j)) }
      .tunedJoin(jt, saveDictionary(ctx, _3, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i, j), k)) => (Position(c.position(_4)), (c, i, j, k)) }
      .tunedJoin(jt, saveDictionary(ctx, _4, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i, j, k), l)) =>
        i + separator + j + separator + k + separator + l + separator + c.content.value.toShortString
      }
      .tunedSaveAsText(ctx, rt, file)

    data
  }

  protected def naturalDomain(tuner: Tuner): U[Position[_4]] = coordinates(_1, tuner)
    .tunedCross[Value](tuner, (_, _) => true, coordinates(_2, tuner))
    .tunedCross[Value](tuner, (_, _) => true, coordinates(_3, tuner))
    .tunedCross[Value](tuner, (_, _) => true, coordinates(_4, tuner))
    .map { case (((c1, c2), c3), c4) => Position(c1, c2, c3, c4) }
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
  with ApproximateDistribution[_4, _5]
  with PairwiseDistance[_4, _5]
  with Statistics[_4, _5] {
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
      .sortBy { case (d, _) => d }
      .map { case (_, i) => i }

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
    def msj[V] = Option(MapMapSideJoin[Position[_1], V, Int]())

    val (jt, rt) = getSaveAsIVTuners(tuner)

    data
      .map { case c => (Position(c.position(_1)), c) }
      .tunedJoin(jt, saveDictionary(ctx, _1, file, dictionary, separator, jt), msj)
      .map { case (_, (c, i)) => (Position(c.position(_2)), (c, i)) }
      .tunedJoin(jt, saveDictionary(ctx, _2, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i), j)) => (Position(c.position(_3)), (c, i, j)) }
      .tunedJoin(jt, saveDictionary(ctx, _3, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i, j), k)) => (Position(c.position(_4)), (c, i, j, k)) }
      .tunedJoin(jt, saveDictionary(ctx, _4, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i, j, k), l)) => (Position(c.position(_5)), (c, i, j, k, l)) }
      .tunedJoin(jt, saveDictionary(ctx, _5, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i, j, k, l), m)) =>
        i + separator + j + separator + k + separator + l + separator + m + separator + c.content.value.toShortString
      }
      .tunedSaveAsText(ctx, rt, file)

    data
  }

  protected def naturalDomain(tuner: Tuner): U[Position[_5]] = coordinates(_1, tuner)
    .tunedCross[Value](tuner, (_, _) => true, coordinates(_2, tuner))
    .tunedCross[Value](tuner, (_, _) => true, coordinates(_3, tuner))
    .tunedCross[Value](tuner, (_, _) => true, coordinates(_4, tuner))
    .tunedCross[Value](tuner, (_, _) => true, coordinates(_5, tuner))
    .map { case ((((c1, c2), c3), c4), c5) => Position(c1, c2, c3, c4, c5) }
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
  with ApproximateDistribution[_5, _6]
  with PairwiseDistance[_5, _6]
  with Statistics[_5, _6] {
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
      .sortBy { case (d, _) => d }
      .map { case (_, i) => i }

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
    def msj[V] = Option(MapMapSideJoin[Position[_1], V, Int]())

    val (jt, rt) = getSaveAsIVTuners(tuner)

    data
      .map { case c => (Position(c.position(_1)), c) }
      .tunedJoin(jt, saveDictionary(ctx, _1, file, dictionary, separator, jt), msj)
      .map { case (_, (c, i)) => (Position(c.position(_2)), (c, i)) }
      .tunedJoin(jt, saveDictionary(ctx, _2, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i), j)) => (Position(c.position(_3)), (c, i, j)) }
      .tunedJoin(jt, saveDictionary(ctx, _3, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i, j), k)) => (Position(c.position(_4)), (c, i, j, k)) }
      .tunedJoin(jt, saveDictionary(ctx, _4, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i, j, k), l)) => (Position(c.position(_5)), (c, i, j, k, l)) }
      .tunedJoin(jt, saveDictionary(ctx, _5, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i, j, k, l), m)) => (Position(c.position(_6)), (c, i, j, k, l, m)) }
      .tunedJoin(jt, saveDictionary(ctx, _6, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i, j, k, l, m), n)) =>
        i + separator +
        j + separator +
        k + separator +
        l + separator +
        m + separator +
        n + separator +
        c.content.value.toShortString
      }
      .tunedSaveAsText(ctx, rt, file)

    data
  }

  protected def naturalDomain(tuner: Tuner): U[Position[_6]] = coordinates(_1, tuner)
    .tunedCross[Value](tuner, (_, _) => true, coordinates(_2, tuner))
    .tunedCross[Value](tuner, (_, _) => true, coordinates(_3, tuner))
    .tunedCross[Value](tuner, (_, _) => true, coordinates(_4, tuner))
    .tunedCross[Value](tuner, (_, _) => true, coordinates(_5, tuner))
    .tunedCross[Value](tuner, (_, _) => true, coordinates(_6, tuner))
    .map { case (((((c1, c2), c3), c4), c5), c6) => Position(c1, c2, c3, c4, c5, c6) }
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
  with ApproximateDistribution[_6, _7]
  with PairwiseDistance[_6, _7]
  with Statistics[_6, _7] {
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
      .sortBy { case (d, _) => d }
      .map { case (_, i) => i }

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
    def msj[V] = Option(MapMapSideJoin[Position[_1], V, Int]())

    val (jt, rt) = getSaveAsIVTuners(tuner)

    data
      .map { case c => (Position(c.position(_1)), c) }
      .tunedJoin(jt, saveDictionary(ctx, _1, file, dictionary, separator, jt), msj)
      .map { case (_, (c, i)) => (Position(c.position(_2)), (c, i)) }
      .tunedJoin(jt, saveDictionary(ctx, _2, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i), j)) => (Position(c.position(_3)), (c, i, j)) }
      .tunedJoin(jt, saveDictionary(ctx, _3, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i, j), k)) => (Position(c.position(_4)), (c, i, j, k)) }
      .tunedJoin(jt, saveDictionary(ctx, _4, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i, j, k), l)) => (Position(c.position(_5)), (c, i, j, k, l)) }
      .tunedJoin(jt, saveDictionary(ctx, _5, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i, j, k, l), m)) => (Position(c.position(_6)), (c, i, j, k, l, m)) }
      .tunedJoin(jt, saveDictionary(ctx, _6, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i, j, k, l, m), n)) => (Position(c.position(_7)), (c, i, j, k, l, m, n)) }
      .tunedJoin(jt, saveDictionary(ctx, _7, file, dictionary, separator, jt), msj)
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
      .tunedSaveAsText(ctx, rt, file)

    data
  }

  protected def naturalDomain(tuner: Tuner): U[Position[_7]] = coordinates(_1, tuner)
    .tunedCross[Value](tuner, (_, _) => true, coordinates(_2, tuner))
    .tunedCross[Value](tuner, (_, _) => true, coordinates(_3, tuner))
    .tunedCross[Value](tuner, (_, _) => true, coordinates(_4, tuner))
    .tunedCross[Value](tuner, (_, _) => true, coordinates(_5, tuner))
    .tunedCross[Value](tuner, (_, _) => true, coordinates(_6, tuner))
    .tunedCross[Value](tuner, (_, _) => true, coordinates(_7, tuner))
    .map { case ((((((c1, c2), c3), c4), c5), c6), c7) => Position(c1, c2, c3, c4, c5, c6, c7) }
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
  with ApproximateDistribution[_7, _8]
  with PairwiseDistance[_7, _8]
  with Statistics[_7, _8] {
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
      .sortBy { case (d, _) => d }
      .map { case (_, i) => i }

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
    def msj[V] = Option(MapMapSideJoin[Position[_1], V, Int]())

    val (jt, rt) = getSaveAsIVTuners(tuner)

    data
      .map { case c => (Position(c.position(_1)), c) }
      .tunedJoin(jt, saveDictionary(ctx, _1, file, dictionary, separator, jt), msj)
      .map { case (_, (c, i)) => (Position(c.position(_2)), (c, i)) }
      .tunedJoin(jt, saveDictionary(ctx, _2, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i), j)) => (Position(c.position(_3)), (c, i, j)) }
      .tunedJoin(jt, saveDictionary(ctx, _3, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i, j), k)) => (Position(c.position(_4)), (c, i, j, k)) }
      .tunedJoin(jt, saveDictionary(ctx, _4, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i, j, k), l)) => (Position(c.position(_5)), (c, i, j, k, l)) }
      .tunedJoin(jt, saveDictionary(ctx, _5, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i, j, k, l), m)) => (Position(c.position(_6)), (c, i, j, k, l, m)) }
      .tunedJoin(jt, saveDictionary(ctx, _6, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i, j, k, l, m), n)) => (Position(c.position(_7)), (c, i, j, k, l, m, n)) }
      .tunedJoin(jt, saveDictionary(ctx, _7, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i, j, k, l, m, n), o)) => (Position(c.position(_8)), (c, i, j, k, l, m, n, o)) }
      .tunedJoin(jt, saveDictionary(ctx, _8, file, dictionary, separator, jt), msj)
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
      .tunedSaveAsText(ctx, rt, file)

    data
  }

  protected def naturalDomain(tuner: Tuner): U[Position[_8]] = coordinates(_1, tuner)
    .tunedCross[Value](tuner, (_, _) => true, coordinates(_2, tuner))
    .tunedCross[Value](tuner, (_, _) => true, coordinates(_3, tuner))
    .tunedCross[Value](tuner, (_, _) => true, coordinates(_4, tuner))
    .tunedCross[Value](tuner, (_, _) => true, coordinates(_5, tuner))
    .tunedCross[Value](tuner, (_, _) => true, coordinates(_6, tuner))
    .tunedCross[Value](tuner, (_, _) => true, coordinates(_7, tuner))
    .tunedCross[Value](tuner, (_, _) => true, coordinates(_8, tuner))
    .map { case (((((((c1, c2), c3), c4), c5), c6), c7), c8) => Position(c1, c2, c3, c4, c5, c6, c7, c8) }
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
  with ApproximateDistribution[_8, _9]
  with PairwiseDistance[_8, _9]
  with Statistics[_8, _9] {
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
      .sortBy { case (d, _) => d }
      .map { case (_, i) => i }

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
    def msj[V] = Option(MapMapSideJoin[Position[_1], V, Int]())

    val (jt, rt) = getSaveAsIVTuners(tuner)

    data
      .map { case c => (Position(c.position(_1)), c) }
      .tunedJoin(jt, saveDictionary(ctx, _1, file, dictionary, separator, jt), msj)
      .map { case (_, (c, i)) => (Position(c.position(_2)), (c, i)) }
      .tunedJoin(jt, saveDictionary(ctx, _2, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i), j)) => (Position(c.position(_3)), (c, i, j)) }
      .tunedJoin(jt, saveDictionary(ctx, _3, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i, j), k)) => (Position(c.position(_4)), (c, i, j, k)) }
      .tunedJoin(jt, saveDictionary(ctx, _4, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i, j, k), l)) => (Position(c.position(_5)), (c, i, j, k, l)) }
      .tunedJoin(jt, saveDictionary(ctx, _5, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i, j, k, l), m)) => (Position(c.position(_6)), (c, i, j, k, l, m)) }
      .tunedJoin(jt, saveDictionary(ctx, _6, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i, j, k, l, m), n)) => (Position(c.position(_7)), (c, i, j, k, l, m, n)) }
      .tunedJoin(jt, saveDictionary(ctx, _7, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i, j, k, l, m, n), o)) => (Position(c.position(_8)), (c, i, j, k, l, m, n, o)) }
      .tunedJoin(jt, saveDictionary(ctx, _8, file, dictionary, separator, jt), msj)
      .map { case (_, ((c, i, j, k, l, m, n, o), p)) => (Position(c.position(_9)), (c, i, j, k, l, m, n, o, p)) }
      .tunedJoin(jt, saveDictionary(ctx, _9, file, dictionary, separator, jt), msj)
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
      .tunedSaveAsText(ctx, rt, file)

    data
  }

  protected def naturalDomain(tuner: Tuner): U[Position[_9]] = coordinates(_1, tuner)
    .tunedCross[Value](tuner, (_, _) => true, coordinates(_2, tuner))
    .tunedCross[Value](tuner, (_, _) => true, coordinates(_3, tuner))
    .tunedCross[Value](tuner, (_, _) => true, coordinates(_4, tuner))
    .tunedCross[Value](tuner, (_, _) => true, coordinates(_5, tuner))
    .tunedCross[Value](tuner, (_, _) => true, coordinates(_6, tuner))
    .tunedCross[Value](tuner, (_, _) => true, coordinates(_7, tuner))
    .tunedCross[Value](tuner, (_, _) => true, coordinates(_8, tuner))
    .tunedCross[Value](tuner, (_, _) => true, coordinates(_9, tuner))
    .map { case ((((((((c1, c2), c3), c4), c5), c6), c7), c8), c9) => Position(c1, c2, c3, c4, c5, c6, c7, c8, c9) }
}

