// Copyright 2019 Commonwealth Bank of Australia
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

package commbank.grimlock.scala.environment

import commbank.grimlock.framework.{ Cell, MatrixWithParseErrors }
import commbank.grimlock.framework.content.Content
import commbank.grimlock.framework.distance.PairwiseDistance.{ CorrelationTuner, MutualInformationTuner }
import commbank.grimlock.framework.distribution.ApproximateDistribution.{
  CountMapQuantilesTuner,
  HistogramTuner,
  QuantilesTuner,
  TDigestQuantilesTuner,
  UniformQuantilesTuner
}
import commbank.grimlock.framework.encoding.Value
import commbank.grimlock.framework.environment.tuner.{ Default, NoParameters }
import commbank.grimlock.framework.Matrix.{
  DomainTuner,
  ExpandTuner,
  FillHeterogeneousTuner,
  FillHomogeneousTuner,
  GatherTuner,
  GetTuner,
  JoinTuner,
  MeasureTuner,
  MutateTuner,
  PairTuner,
  SaveAsCSVTuner,
  SaveAsIVTuner,
  SaveAsVWTuner,
  ShapeTuner,
  SelectTuner,
  SetTuner,
  SlideTuner,
  SquashTuner,
  SummariseTuner,
  TypesTuner,
  UniqueTuner,
  WhichTuner
}
import commbank.grimlock.framework.partition.Partitions.{ ForAllTuner, IdsTuner }
import commbank.grimlock.framework.Persist.SaveAsTextTuner
import commbank.grimlock.framework.position.{
  Coordinates1,
  Coordinates2,
  Coordinates3,
  Coordinates4,
  Coordinates5,
  Coordinates6,
  Coordinates7,
  Coordinates8,
  Coordinates9,
  Position
}
import commbank.grimlock.framework.position.Positions.NamesTuner
import commbank.grimlock.framework.statistics.Statistics.{
  CountsTuner,
  DistinctCountsTuner,
  KurtosisTuner,
  MaximumAbsoluteTuner,
  MaximumTuner,
  MeanTuner,
  MinimumTuner,
  PredicateCountsTuner,
  StandardDeviationTuner,
  SkewnessTuner,
  SumsTuner
}

import commbank.grimlock.scala.{
  Matrix,
  Matrix1D,
  Matrix2D,
  Matrix3D,
  Matrix4D,
  Matrix5D,
  Matrix6D,
  Matrix7D,
  Matrix8D,
  Matrix9D,
  MultiDimensionMatrix,
  SaveStringsAsText
}
import commbank.grimlock.scala.content.{ Contents, IndexedContents }
import commbank.grimlock.scala.partition.Partitions
import commbank.grimlock.scala.position.Positions

import shapeless.{ ::, HList, HNil }
import shapeless.nat.{ _0, _1, _2, _3, _4, _5, _6, _7, _8 }

package object implicits {
  // *** Matrix/Position shared tuners

  /** Implicits for checking tuners on a call to `names`. */
  implicit def slistNamesTunerDn = new NamesTuner[Context.U, Default[NoParameters]] { }

  //  *** Persist tuners

  /** Implicits for checking tuners on a call to `saveAstext`. */
  implicit def slistSaveAsTextTunerDn = new SaveAsTextTuner[Context.U, Default[NoParameters]] { }

  // *** Pairwise distance tuners

  /** Implicits for checking tuners on a call to `corrrelation`. */
  implicit def slistCorrelationTunerDnDnDn = new CorrelationTuner[Context.U, Default[NoParameters]] { }

  /** Implicits for checking tuners on a call to `mutualInformation`. */
  implicit def slistMutualInformationTunerDnDnDn = new MutualInformationTuner[Context.U, Default[NoParameters]] { }

  // *** Distribution tuners

  /** Implicits for checking tuners on a call to `histogram`. */
  implicit def slistHistogramTunerDn = new HistogramTuner[Context.U, Default[NoParameters]] { }

  /** Implicits for checking tuners on a call to `quantiles`. */
  implicit def slistQuantilesTunerDn = new QuantilesTuner[Context.U, Default[NoParameters]] { }

  /** Implicits for checking tuners on a call to `countMapQuantiles`. */
  implicit def slistCountMapQuantilesTunerDn = new CountMapQuantilesTuner[Context.U, Default[NoParameters]] { }

  /** Implicits for checking tuners on a call to `tDigestQuantiles`. */
  implicit def slistTDigestQuantilesTunerDn = new TDigestQuantilesTuner[Context.U, Default[NoParameters]] { }

  /** Implicits for checking tuners on a call to `uniformQuantiles`. */
  implicit def slistUniformQuantilesTunerDn = new UniformQuantilesTuner[Context.U, Default[NoParameters]] { }

  // *** Partition tuners

  /** Implicits for checking tuners on a call to `forAll`. */
  implicit def slistForAllTunerDn = new ForAllTuner[Context.U, Default[NoParameters]] { }

  /** Implicits for checking tuners on a call to `ids`. */
  implicit def slistIdsTunerDn = new IdsTuner[Context.U, Default[NoParameters]] { }

  // *** Statistics tuners

  /** Implicits for checking tuners on a call to `counts`. */
  implicit def slistCountsTunerDn = new CountsTuner[Context.U, Default[NoParameters]] { }

  /** Implicits for checking tuners on a call to `distinctCounts`. */
  implicit def slistDistinctCountsTunerDn = new DistinctCountsTuner[Context.U, Default[NoParameters]] { }

  /** Implicits for checking tuners on a call to `predicateCounts`. */
  implicit def slistPredicateCountsTunerDn = new PredicateCountsTuner[Context.U, Default[NoParameters]] { }

  /** Implicits for checking tuners on a call to `mean`. */
  implicit def slistMeanTunerDn = new MeanTuner[Context.U, Default[NoParameters]] { }

  /** Implicits for checking tuners on a call to `standardDeviation`. */
  implicit def slistStandardDeviationTunerDn = new StandardDeviationTuner[Context.U, Default[NoParameters]] { }

  /** Implicits for checking tuners on a call to `skewness`. */
  implicit def slistSkewnessTunerDn = new SkewnessTuner[Context.U, Default[NoParameters]] { }

  /** Implicits for checking tuners on a call to `kurtosis`. */
  implicit def slistKurtosisTunerDn = new KurtosisTuner[Context.U, Default[NoParameters]] { }

  /** Implicits for checking tuners on a call to `minimum`. */
  implicit def slistMinimumTunerDn = new MinimumTuner[Context.U, Default[NoParameters]] { }

  /** Implicits for checking tuners on a call to `maximum`. */
  implicit def slistMaximumTunerDn = new MaximumTuner[Context.U, Default[NoParameters]] { }

  /** Implicits for checking tuners on a call to `maximumAbsolute`. */
  implicit def slistMaximumAbsoluteTunerDn = new MaximumAbsoluteTuner[Context.U, Default[NoParameters]] { }

  /** Implicits for checking tuners on a call to `sums`. */
  implicit def slistSumsTunerDn = new SumsTuner[Context.U, Default[NoParameters]] { }

  // *** Matrix tuners

  /** Implicits for checking tuners on a call to `domain`. */
  implicit def slistDomainTunerDn = new DomainTuner[Context.U, Default[NoParameters]] { }

  /** Implicits for checking tuners on a call to `expand`. */
  implicit def slistExpandTunerDn = new ExpandTuner[Context.U, Default[NoParameters]] { }

  /** Implicits for checking tuners on a call to `fillHeterogeneous`. */
  implicit def slistFillHeterogeneousTunerDnDnDn = new FillHeterogeneousTuner[Context.U, Default[NoParameters]] { }

  /** Implicits for checking tuners on a call to `fillHomogeneous`. */
  implicit def slistFillHomogeneousTunerDnDn = new FillHomogeneousTuner[Context.U, Default[NoParameters]] { }

  /** Implicits for checking tuners on a call to `gather`. */
  implicit def slistGatherTunerDn = new GatherTuner[Context.U, Default[NoParameters]] { }

  /** Implicits for checking tuners on a call to `get`. */
  implicit def slistGetTunerDn = new GetTuner[Context.U, Default[NoParameters]] { }

  /** Implicits for checking tuners on a call to `join`. */
  implicit def slistJoinTunerDnDn = new JoinTuner[Context.U, Default[NoParameters]] { }

  /** Implicits for checking tuners on a call to `measure`. */
  implicit def slistMeasureTunerDn = new MeasureTuner[Context.U, Default[NoParameters]] { }

  /** Implicits for checking tuners on a call to `mutate`. */
  implicit def slistMutateTunerDn = new MutateTuner[Context.U, Default[NoParameters]] { }

  /** Implicits for checking tuners on a call to `pair*`. */
  implicit def slistPairTunerDnDnDn = new PairTuner[Context.U, Default[NoParameters]] { }

  /** Implicits for checking tuners on a call to `saveAsIV`. */
  implicit def slistSaveAsIVTunerDnDn = new SaveAsIVTuner[Context.U, Default[NoParameters]] { }

  /** Implicits for checking tuners on a call to `select`. */
  implicit def slistSelectTunerDn = new SelectTuner[Context.U, Default[NoParameters]] { }

  /** Implicits for checking tuners on a call to `set`. */
  implicit def slistSetTunerDn = new SetTuner[Context.U, Default[NoParameters]] { }

  /** Implicits for checking tuners on a call to `shape`. */
  implicit def slistShapeTunerDn = new ShapeTuner[Context.U, Default[NoParameters]] { }

  /** Implicits for checking tuners on a call to `slide`. */
  implicit def slistSlideTunerDn = new SlideTuner[Context.U, Default[NoParameters]] { }

  /** Implicits for checking tuners on a call to `summmarise`. */
  implicit def slistSummariseTunerDn = new SummariseTuner[Context.U, Default[NoParameters]] { }

  /** Implicits for checking tuners on a call to `types`. */
  implicit def slistTypesTunerDn = new TypesTuner[Context.U, Default[NoParameters]] { }

  /** Implicits for checking tuners on a call to `unique`. */
  implicit def slistUniqueTunerDn = new UniqueTuner[Context.U, Default[NoParameters]] { }

  /** Implicits for checking tuners on a call to `which`. */
  implicit def slistWhichTunerDn = new WhichTuner[Context.U, Default[NoParameters]] { }

  /** Implicits for checking tuners on a call to `squash`. */
  implicit def slistSquashTunerDn = new SquashTuner[Context.U, Default[NoParameters]] { }

  /** Implicits for checking tuners on a call to `saveAsCSV`. */
  implicit def slistSaveAsCSVDnDnTuner = new SaveAsCSVTuner[Context.U, Default[NoParameters]] { }

  /** Implicits for checking tuners on a call to `saveAsVW`. */
  implicit def slistSaveAsVWTunerDnDnDn = new SaveAsVWTuner[Context.U, Default[NoParameters]] { }

  /** Converts a `Cell[P]` into a `List[Cell[P]]`. */
  implicit def cellToSList[
    P <: HList
  ](
    c: Cell[P]
  )(implicit
    ctx: Context
  ): Context.U[Cell[P]] = ctx.implicits.cell.cellToU(c)

  /** Converts a `List[Content]` to a `Contents`. */
  implicit def slistToContents(
    data: Context.U[Content]
  )(implicit
    ctx: Context
  ): Contents = ctx.implicits.content.toContents(data)

  /** Converts a `List[(Position[P], Content)]` to a `IndexedContents`. */
  implicit def slistToIndexed[
    P <: HList
  ](
    data: Context.U[(Position[P], Content)]
  )(implicit
    ctx: Context
  ): IndexedContents[P] = ctx.implicits.content.toIndexed(data)

  /** Conversion from `List[Cell[P]]` to a `Matrix`. */
  implicit def slistToMatrix[
    P <: HList
  ](
    data: Context.U[Cell[P]]
  )(implicit
    ctx: Context
  ): Matrix[P] = ctx.implicits.matrix.toMatrix(data)

  /** Conversion from `List[Cell[V1 :: HNil]]` to a `Matrix1D`. */
  implicit def slistToMatrix1D[
    V1 <: Value[_]
  ](
    data: Context.U[Cell[V1 :: HNil]]
  )(implicit
    ctx: Context,
    ev1: Position.IndexConstraints.Aux[V1 :: HNil, _0, V1]
  ): Matrix1D[V1] = ctx.implicits.matrix.toMatrix1D(data)

  /** Conversion from `List[Cell[V1 :: V2 :: HNil]]` to a `Matrix2D`. */
  implicit def slistToMatrix2D[
    V1 <: Value[_],
    V2 <: Value[_]
  ](
    data: Context.U[Cell[V1 :: V2 :: HNil]]
  )(implicit
    ctx: Context,
    ev1: Position.IndexConstraints.Aux[V1 :: V2 :: HNil, _0, V1],
    ev2: Position.IndexConstraints.Aux[V1 :: V2 :: HNil, _1, V2]
  ): Matrix2D[V1, V2] = ctx.implicits.matrix.toMatrix2D(data)

  /** Conversion from `List[Cell[V1 :: V2 :: V3 :: HNil]]` to a `Matrix3D`. */
  implicit def slistToMatrix3D[
    V1 <: Value[_],
    V2 <: Value[_],
    V3 <: Value[_]
  ](
    data: Context.U[Cell[V1 :: V2 :: V3 :: HNil]]
  )(implicit
    ctx: Context,
    ev1: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: HNil, _0, V1],
    ev2: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: HNil, _1, V2],
    ev3: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: HNil, _2, V3]
  ): Matrix3D[V1, V2, V3] = ctx.implicits.matrix.toMatrix3D(data)

  /** Conversion from `List[Cell[V1 :: V2 :: V3 :: V4 :: HNil]]` to a `Matrix4D`. */
  implicit def slistToMatrix4D[
    V1 <: Value[_],
    V2 <: Value[_],
    V3 <: Value[_],
    V4 <: Value[_]
  ](
    data: Context.U[Cell[V1 :: V2 :: V3 :: V4 :: HNil]]
  )(implicit
    ctx: Context,
    ev1: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: HNil, _0, V1],
    ev2: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: HNil, _1, V2],
    ev3: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: HNil, _2, V3],
    ev4: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: HNil, _3, V4]
  ): Matrix4D[V1, V2, V3, V4] = ctx.implicits.matrix.toMatrix4D(data)

  /** Conversion from `List[Cell[V1 :: V2 :: V3 :: V4 :: V5 :: HNil]]` to a `Matrix5D`. */
  implicit def slistToMatrix5D[
    V1 <: Value[_],
    V2 <: Value[_],
    V3 <: Value[_],
    V4 <: Value[_],
    V5 <: Value[_]
  ](
    data: Context.U[Cell[V1 :: V2 :: V3 :: V4 :: V5 :: HNil]]
  )(implicit
    ctx: Context,
    ev1: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: V5 :: HNil, _0, V1],
    ev2: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: V5 :: HNil, _1, V2],
    ev3: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: V5 :: HNil, _2, V3],
    ev4: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: V5 :: HNil, _3, V4],
    ev5: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: V5 :: HNil, _4, V5]
  ): Matrix5D[V1, V2, V3, V4, V5] = ctx.implicits.matrix.toMatrix5D(data)

  /** Conversion from `List[Cell[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: HNil]]` to a `Matrix6D`. */
  implicit def slistToMatrix6D[
    V1 <: Value[_],
    V2 <: Value[_],
    V3 <: Value[_],
    V4 <: Value[_],
    V5 <: Value[_],
    V6 <: Value[_]
  ](
    data: Context.U[Cell[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: HNil]]
  )(implicit
    ctx: Context,
    ev1: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: HNil, _0, V1],
    ev2: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: HNil, _1, V2],
    ev3: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: HNil, _2, V3],
    ev4: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: HNil, _3, V4],
    ev5: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: HNil, _4, V5],
    ev6: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: HNil, _5, V6]
  ): Matrix6D[V1, V2, V3, V4, V5, V6] = ctx.implicits.matrix.toMatrix6D(data)

  /** Conversion from `List[Cell[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: HNil]]` to a `Matrix7D`. */
  implicit def slistToMatrix7D[
    V1 <: Value[_],
    V2 <: Value[_],
    V3 <: Value[_],
    V4 <: Value[_],
    V5 <: Value[_],
    V6 <: Value[_],
    V7 <: Value[_]
  ](
    data: Context.U[Cell[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: HNil]]
  )(implicit
    ctx: Context,
    ev1: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: HNil, _0, V1],
    ev2: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: HNil, _1, V2],
    ev3: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: HNil, _2, V3],
    ev4: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: HNil, _3, V4],
    ev5: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: HNil, _4, V5],
    ev6: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: HNil, _5, V6],
    ev7: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: HNil, _6, V7]
  ): Matrix7D[V1, V2, V3, V4, V5, V6, V7] = ctx.implicits.matrix.toMatrix7D(data)

  /** Conversion from `List[Cell[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: HNil]]` to a `Matrix8D`. */
  implicit def slistToMatrix8D[
    V1 <: Value[_],
    V2 <: Value[_],
    V3 <: Value[_],
    V4 <: Value[_],
    V5 <: Value[_],
    V6 <: Value[_],
    V7 <: Value[_],
    V8 <: Value[_]
  ](
    data: Context.U[Cell[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: HNil]]
  )(implicit
    ctx: Context,
    ev1: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: HNil, _0, V1],
    ev2: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: HNil, _1, V2],
    ev3: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: HNil, _2, V3],
    ev4: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: HNil, _3, V4],
    ev5: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: HNil, _4, V5],
    ev6: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: HNil, _5, V6],
    ev7: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: HNil, _6, V7],
    ev8: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: HNil, _7, V8]
  ): Matrix8D[V1, V2, V3, V4, V5, V6, V7, V8] = ctx.implicits.matrix.toMatrix8D(data)

  /** Conversion from `List[Cell[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: V9 :: HNil]]` to a `Matrix9D`. */
  implicit def slistToMatrix9D[
    V1 <: Value[_],
    V2 <: Value[_],
    V3 <: Value[_],
    V4 <: Value[_],
    V5 <: Value[_],
    V6 <: Value[_],
    V7 <: Value[_],
    V8 <: Value[_],
    V9 <: Value[_]
  ](
    data: Context.U[Cell[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: V9 :: HNil]]
  )(implicit
    ctx: Context,
    ev1: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: V9 :: HNil, _0, V1],
    ev2: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: V9 :: HNil, _1, V2],
    ev3: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: V9 :: HNil, _2, V3],
    ev4: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: V9 :: HNil, _3, V4],
    ev5: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: V9 :: HNil, _4, V5],
    ev6: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: V9 :: HNil, _5, V6],
    ev7: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: V9 :: HNil, _6, V7],
    ev8: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: V9 :: HNil, _7, V8],
    ev9: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: V9 :: HNil, _8, V9]
  ): Matrix9D[V1, V2, V3, V4, V5, V6, V7, V8, V9] = ctx.implicits.matrix.toMatrix9D(data)

  /** Conversion from `List[Cell[P]]` to a `MultiDimensionMatrix`. */
  implicit def slistToMultiDimensionMatrix[
    P <: HList
  ](
    data: Context.U[Cell[P]]
  )(implicit
    ctx: Context,
    ev: Position.IsMultiDimensionalConstraints[P]
  ): MultiDimensionMatrix[P] = ctx.implicits.matrix.toMultiDimensionMatrix(data)

  /** Conversion from `List[(T1, Content)]` to a `Matrix`. */
  implicit def tuple1ToSListMatrix[
    T1 <% Value[T1]
  ](
    list: List[(T1, Content)]
  )(implicit
    ctx: Context
  ): Matrix[Coordinates1[T1]] = ctx.implicits.matrix.tuple1ToMatrix(list)

  /** Conversion from `List[(T1, Content)]` to a `Matrix1D`. */
  implicit def tuple1ToSListMatrix1D[
    T1 <% Value[T1]
  ](
    list: List[(T1, Content)]
  )(implicit
    ctx: Context,
    ev1: Position.IndexConstraints.Aux[Coordinates1[T1], _0, Value[T1]]
  ): Matrix1D[Value[T1]] = ctx.implicits.matrix.tuple1ToMatrix1D(list)

  /** Conversion from `List[(T1, T2, Content)]` to a `Matrix`. */
  implicit def tuple2ToSListMatrix[
    T1 <% Value[T1],
    T2 <% Value[T2]
  ](
    list: List[(T1, T2, Content)]
  )(implicit
    ctx: Context
  ): Matrix[Coordinates2[T1, T2]] = ctx.implicits.matrix.tuple2ToMatrix(list)

  /** Conversion from `List[(T1, T2, Content)]` to a `Matrix2D`. */
  implicit def tuple2ToSListMatrix2D[
    T1 <% Value[T1],
    T2 <% Value[T2]
  ](
    list: List[(T1, T2, Content)]
  )(implicit
    ctx: Context,
    ev1: Position.IndexConstraints.Aux[Coordinates2[T1, T2], _0, Value[T1]],
    ev2: Position.IndexConstraints.Aux[Coordinates2[T1, T2], _1, Value[T2]]
  ): Matrix2D[Value[T1], Value[T2]] = ctx.implicits.matrix.tuple2ToMatrix2D(list)

  /** Conversion from `List[(T1, T2, Content)]` to a `MultiDimensionMatrix`. */
  implicit def tuple2ToSListMultiDimensionMatrix[
    T1 <% Value[T1],
    T2 <% Value[T2]
  ](
    list: List[(T1, T2, Content)]
  )(implicit
    ctx: Context
  ): MultiDimensionMatrix[Coordinates2[T1, T2]] = ctx.implicits.matrix.tuple2ToMultiDimensionMatrix(list)

  /** Conversion from `List[(T1, T2, T3, Content)]` to a `Matrix`. */
  implicit def tuple3ToSListMatrix[
    T1 <% Value[T1],
    T2 <% Value[T2],
    T3 <% Value[T3]
  ](
    list: List[(T1, T2, T3, Content)]
  )(implicit
    ctx: Context
  ): Matrix[Coordinates3[T1, T2, T3]] = ctx.implicits.matrix.tuple3ToMatrix(list)

  /** Conversion from `List[(T1, T2, T3, Content)]` to a `Matrix3D`. */
  implicit def tuple3ToSListMatrix3D[
    T1 <% Value[T1],
    T2 <% Value[T2],
    T3 <% Value[T3]
  ](
    list: List[(T1, T2, T3, Content)]
  )(implicit
    ctx: Context,
    ev1: Position.IndexConstraints.Aux[Coordinates3[T1, T2, T3], _0, Value[T1]],
    ev2: Position.IndexConstraints.Aux[Coordinates3[T1, T2, T3], _1, Value[T2]],
    ev3: Position.IndexConstraints.Aux[Coordinates3[T1, T2, T3], _2, Value[T3]]
  ): Matrix3D[Value[T1], Value[T2], Value[T3]] = ctx.implicits.matrix.tuple3ToMatrix3D(list)

  /** Conversion from `List[(T1, T2, T3, Content)]` to a `MultiDimensionMatrix`. */
  implicit def tuple3ToSListMultiDimensionMatrix[
    T1 <% Value[T1],
    T2 <% Value[T2],
    T3 <% Value[T3]
  ](
    list: List[(T1, T2, T3, Content)]
  )(implicit
    ctx: Context
  ): MultiDimensionMatrix[Coordinates3[T1, T2, T3]] = ctx.implicits.matrix.tuple3ToMultiDimensionMatrix(list)

  /** Conversion from `List[(T1, T2, T3, T4, Content)]` to a `Matrix`. */
  implicit def tuple4ToSListMatrix[
    T1 <% Value[T1],
    T2 <% Value[T2],
    T3 <% Value[T3],
    T4 <% Value[T4]
  ](
    list: List[(T1, T2, T3, T4, Content)]
  )(implicit
    ctx: Context
  ): Matrix[Coordinates4[T1, T2, T3, T4]] = ctx.implicits.matrix.tuple4ToMatrix(list)

  /** Conversion from `List[(T1, T2, T3, T4, Content)]` to a `Matrix4D`. */
  implicit def tuple4ToSListMatrix4D[
    T1 <% Value[T1],
    T2 <% Value[T2],
    T3 <% Value[T3],
    T4 <% Value[T4]
  ](
    list: List[(T1, T2, T3, T4, Content)]
  )(implicit
    ctx: Context,
    ev1: Position.IndexConstraints.Aux[Coordinates4[T1, T2, T3, T4], _0, Value[T1]],
    ev2: Position.IndexConstraints.Aux[Coordinates4[T1, T2, T3, T4], _1, Value[T2]],
    ev3: Position.IndexConstraints.Aux[Coordinates4[T1, T2, T3, T4], _2, Value[T3]],
    ev4: Position.IndexConstraints.Aux[Coordinates4[T1, T2, T3, T4], _3, Value[T4]]
  ): Matrix4D[Value[T1], Value[T2], Value[T3], Value[T4]] = ctx.implicits.matrix.tuple4ToMatrix4D(list)

  /** Conversion from `List[(T1, T2, T3, T4, Content)]` to a `MultiDimensionMatrix`. */
  implicit def tuple4ToSListMultiDimensionMatrix[
    T1 <% Value[T1],
    T2 <% Value[T2],
    T3 <% Value[T3],
    T4 <% Value[T4]
  ](
    list: List[(T1, T2, T3, T4, Content)]
  )(implicit
    ctx: Context
  ): MultiDimensionMatrix[Coordinates4[T1, T2, T3, T4]] = ctx
    .implicits
    .matrix
    .tuple4ToMultiDimensionMatrix(list)

  /** Conversion from `List[(T1, T2, T3, T4, T5, Content)]` to a `Matrix`. */
  implicit def tuple5ToSListMatrix[
    T1 <% Value[T1],
    T2 <% Value[T2],
    T3 <% Value[T3],
    T4 <% Value[T4],
    T5 <% Value[T5]
  ](
    list: List[(T1, T2, T3, T4, T5, Content)]
  )(implicit
    ctx: Context
  ): Matrix[Coordinates5[T1, T2, T3, T4, T5]] = ctx.implicits.matrix.tuple5ToMatrix(list)

  /** Conversion from `List[(T1, T2, T3, T4, T5, Content)]` to a `Matrix5D`. */
  implicit def tuple5ToSListMatrix5D[
    T1 <% Value[T1],
    T2 <% Value[T2],
    T3 <% Value[T3],
    T4 <% Value[T4],
    T5 <% Value[T5]
  ](
    list: List[(T1, T2, T3, T4, T5, Content)]
  )(implicit
    ctx: Context,
    ev1: Position.IndexConstraints.Aux[Coordinates5[T1, T2, T3, T4, T5], _0, Value[T1]],
    ev2: Position.IndexConstraints.Aux[Coordinates5[T1, T2, T3, T4, T5], _1, Value[T2]],
    ev3: Position.IndexConstraints.Aux[Coordinates5[T1, T2, T3, T4, T5], _2, Value[T3]],
    ev4: Position.IndexConstraints.Aux[Coordinates5[T1, T2, T3, T4, T5], _3, Value[T4]],
    ev5: Position.IndexConstraints.Aux[Coordinates5[T1, T2, T3, T4, T5], _4, Value[T5]]
  ): Matrix5D[Value[T1], Value[T2], Value[T3], Value[T4], Value[T5]] = ctx.implicits.matrix.tuple5ToMatrix5D(list)

  /** Conversion from `List[(T1, T2, T3, T4, T5, Content)]` to a `MultiDimensionMatrix`. */
  implicit def tuple5ToSListMultiDimensionMatrix[
    T1 <% Value[T1],
    T2 <% Value[T2],
    T3 <% Value[T3],
    T4 <% Value[T4],
    T5 <% Value[T5]
  ](
    list: List[(T1, T2, T3, T4, T5, Content)]
  )(implicit
    ctx: Context
  ): MultiDimensionMatrix[Coordinates5[T1, T2, T3, T4, T5]] = ctx
    .implicits
    .matrix
    .tuple5ToMultiDimensionMatrix(list)

  /** Conversion from `List[(T1, T2, T3, T4, T5, T6, Content)]` to a `Matrix`. */
  implicit def tuple6ToSListMatrix[
    T1 <% Value[T1],
    T2 <% Value[T2],
    T3 <% Value[T3],
    T4 <% Value[T4],
    T5 <% Value[T5],
    T6 <% Value[T6]
  ](
    list: List[(T1, T2, T3, T4, T5, T6, Content)]
  )(implicit
    ctx: Context
  ): Matrix[Coordinates6[T1, T2, T3, T4, T5, T6]] = ctx.implicits.matrix.tuple6ToMatrix(list)

  /** Conversion from `List[(T1, T2, T3, T4, T5, T6, Content)]` to a `Matrix6D`. */
  implicit def tuple6ToSListMatrix6D[
    T1 <% Value[T1],
    T2 <% Value[T2],
    T3 <% Value[T3],
    T4 <% Value[T4],
    T5 <% Value[T5],
    T6 <% Value[T6]
  ](
    list: List[(T1, T2, T3, T4, T5, T6, Content)]
  )(implicit
    ctx: Context,
    ev1: Position.IndexConstraints.Aux[Coordinates6[T1, T2, T3, T4, T5, T6], _0, Value[T1]],
    ev2: Position.IndexConstraints.Aux[Coordinates6[T1, T2, T3, T4, T5, T6], _1, Value[T2]],
    ev3: Position.IndexConstraints.Aux[Coordinates6[T1, T2, T3, T4, T5, T6], _2, Value[T3]],
    ev4: Position.IndexConstraints.Aux[Coordinates6[T1, T2, T3, T4, T5, T6], _3, Value[T4]],
    ev5: Position.IndexConstraints.Aux[Coordinates6[T1, T2, T3, T4, T5, T6], _4, Value[T5]],
    ev6: Position.IndexConstraints.Aux[Coordinates6[T1, T2, T3, T4, T5, T6], _5, Value[T6]]
  ): Matrix6D[Value[T1], Value[T2], Value[T3], Value[T4], Value[T5], Value[T6]] = ctx
    .implicits
    .matrix
    .tuple6ToMatrix6D(list)

  /** Conversion from `List[(T1, T2, T3, T4, T5, T6, Content)]` to a `MultiDimensionMatrix`. */
  implicit def tuple6ToSListMultiDimensionMatrix[
    T1 <% Value[T1],
    T2 <% Value[T2],
    T3 <% Value[T3],
    T4 <% Value[T4],
    T5 <% Value[T5],
    T6 <% Value[T6]
  ](
    list: List[(T1, T2, T3, T4, T5, T6, Content)]
  )(implicit
    ctx: Context
  ): MultiDimensionMatrix[Coordinates6[T1, T2, T3, T4, T5, T6]] = ctx
    .implicits
    .matrix
    .tuple6ToMultiDimensionMatrix(list)

  /** Conversion from `List[(T1, T2, T3, T4, T5, T6, T7, Content)]` to a `Matrix`. */
  implicit def tuple7ToSListMatrix[
    T1 <% Value[T1],
    T2 <% Value[T2],
    T3 <% Value[T3],
    T4 <% Value[T4],
    T5 <% Value[T5],
    T6 <% Value[T6],
    T7 <% Value[T7]
  ](
    list: List[(T1, T2, T3, T4, T5, T6, T7, Content)]
  )(implicit
    ctx: Context
  ): Matrix[Coordinates7[T1, T2, T3, T4, T5, T6, T7]] = ctx.implicits.matrix.tuple7ToMatrix(list)

  /** Conversion from `List[(T1, T2, T3, T4, T5, T6, T7, Content)]` to a `Matrix7D`. */
  implicit def tuple7ToSListMatrix7D[
    T1 <% Value[T1],
    T2 <% Value[T2],
    T3 <% Value[T3],
    T4 <% Value[T4],
    T5 <% Value[T5],
    T6 <% Value[T6],
    T7 <% Value[T7]
  ](
    list: List[(T1, T2, T3, T4, T5, T6, T7, Content)]
  )(implicit
    ctx: Context,
    ev1: Position.IndexConstraints.Aux[Coordinates7[T1, T2, T3, T4, T5, T6, T7], _0, Value[T1]],
    ev2: Position.IndexConstraints.Aux[Coordinates7[T1, T2, T3, T4, T5, T6, T7], _1, Value[T2]],
    ev3: Position.IndexConstraints.Aux[Coordinates7[T1, T2, T3, T4, T5, T6, T7], _2, Value[T3]],
    ev4: Position.IndexConstraints.Aux[Coordinates7[T1, T2, T3, T4, T5, T6, T7], _3, Value[T4]],
    ev5: Position.IndexConstraints.Aux[Coordinates7[T1, T2, T3, T4, T5, T6, T7], _4, Value[T5]],
    ev6: Position.IndexConstraints.Aux[Coordinates7[T1, T2, T3, T4, T5, T6, T7], _5, Value[T6]],
    ev7: Position.IndexConstraints.Aux[Coordinates7[T1, T2, T3, T4, T5, T6, T7], _6, Value[T7]]
  ): Matrix7D[Value[T1], Value[T2], Value[T3], Value[T4], Value[T5], Value[T6], Value[T7]] = ctx
    .implicits
    .matrix
    .tuple7ToMatrix7D(list)

  /** Conversion from `List[(T1, T2, T3, T4, T5, T6, T7, Content)]` to a `MultiDimensionMatrix`. */
  implicit def tuple7ToSListMultiDimensionMatrix[
    T1 <% Value[T1],
    T2 <% Value[T2],
    T3 <% Value[T3],
    T4 <% Value[T4],
    T5 <% Value[T5],
    T6 <% Value[T6],
    T7 <% Value[T7]
  ](
    list: List[(T1, T2, T3, T4, T5, T6, T7, Content)]
  )(implicit
    ctx: Context
  ): MultiDimensionMatrix[Coordinates7[T1, T2, T3, T4, T5, T6, T7]] = ctx
    .implicits
    .matrix
    .tuple7ToMultiDimensionMatrix(list)

  /** Conversion from `List[(T1, T2, T3, T4, T5, T6, T7, T8, Content)]` to a `Matrix`. */
  implicit def tuple8ToSListMatrix[
    T1 <% Value[T1],
    T2 <% Value[T2],
    T3 <% Value[T3],
    T4 <% Value[T4],
    T5 <% Value[T5],
    T6 <% Value[T6],
    T7 <% Value[T7],
    T8 <% Value[T8]
  ](
    list: List[(T1, T2, T3, T4, T5, T6, T7, T8, Content)]
  )(implicit
    ctx: Context
  ): Matrix[Coordinates8[T1, T2, T3, T4, T5, T6, T7, T8]] = ctx.implicits.matrix.tuple8ToMatrix(list)

  /** Conversion from `List[(T1, T2, T3, T4, T5, T6, T7, T8, Content)]` to a `Matrix8D`. */
  implicit def tuple8ToSListMatrix8D[
    T1 <% Value[T1],
    T2 <% Value[T2],
    T3 <% Value[T3],
    T4 <% Value[T4],
    T5 <% Value[T5],
    T6 <% Value[T6],
    T7 <% Value[T7],
    T8 <% Value[T8]
  ](
    list: List[(T1, T2, T3, T4, T5, T6, T7, T8, Content)]
  )(implicit
    ctx: Context,
    ev1: Position.IndexConstraints.Aux[Coordinates8[T1, T2, T3, T4, T5, T6, T7, T8], _0, Value[T1]],
    ev2: Position.IndexConstraints.Aux[Coordinates8[T1, T2, T3, T4, T5, T6, T7, T8], _1, Value[T2]],
    ev3: Position.IndexConstraints.Aux[Coordinates8[T1, T2, T3, T4, T5, T6, T7, T8], _2, Value[T3]],
    ev4: Position.IndexConstraints.Aux[Coordinates8[T1, T2, T3, T4, T5, T6, T7, T8], _3, Value[T4]],
    ev5: Position.IndexConstraints.Aux[Coordinates8[T1, T2, T3, T4, T5, T6, T7, T8], _4, Value[T5]],
    ev6: Position.IndexConstraints.Aux[Coordinates8[T1, T2, T3, T4, T5, T6, T7, T8], _5, Value[T6]],
    ev7: Position.IndexConstraints.Aux[Coordinates8[T1, T2, T3, T4, T5, T6, T7, T8], _6, Value[T7]],
    ev8: Position.IndexConstraints.Aux[Coordinates8[T1, T2, T3, T4, T5, T6, T7, T8], _7, Value[T8]]
  ): Matrix8D[Value[T1], Value[T2], Value[T3], Value[T4], Value[T5], Value[T6], Value[T7], Value[T8]] = ctx
    .implicits
    .matrix
    .tuple8ToMatrix8D(list)

  /** Conversion from `List[(T1, T2, T3, T4, T5, T6, T7, T8, Content)]` to a `MultiDimensionMatrix`. */
  implicit def tuple8ToSListMultiDimensionMatrix[
    T1 <% Value[T1],
    T2 <% Value[T2],
    T3 <% Value[T3],
    T4 <% Value[T4],
    T5 <% Value[T5],
    T6 <% Value[T6],
    T7 <% Value[T7],
    T8 <% Value[T8]
  ](
    list: List[(T1, T2, T3, T4, T5, T6, T7, T8, Content)]
  )(implicit
    ctx: Context
  ): MultiDimensionMatrix[Coordinates8[T1, T2, T3, T4, T5, T6, T7, T8]] = ctx
    .implicits
    .matrix
    .tuple8ToMultiDimensionMatrix(list)

  /** Conversion from `List[(T1, T2, T3, T4, T5, T6, T7, T8, T9, Content)]` to a `Matrix`. */
  implicit def tuple9ToSListMatrix[
    T1 <% Value[T1],
    T2 <% Value[T2],
    T3 <% Value[T3],
    T4 <% Value[T4],
    T5 <% Value[T5],
    T6 <% Value[T6],
    T7 <% Value[T7],
    T8 <% Value[T8],
    T9 <% Value[T9]
  ](
    list: List[(T1, T2, T3, T4, T5, T6, T7, T8, T9, Content)]
  )(implicit
    ctx: Context
  ): Matrix[Coordinates9[T1, T2, T3, T4, T5, T6, T7, T8, T9]] = ctx.implicits.matrix.tuple9ToMatrix(list)

  /** Conversion from `List[(T1, T2, T3, T4, T5, T6, T7, T8, T9, Content)]` to a `Matrix9D`. */
  implicit def tuple9ToSListMatrix9D[
    T1 <% Value[T1],
    T2 <% Value[T2],
    T3 <% Value[T3],
    T4 <% Value[T4],
    T5 <% Value[T5],
    T6 <% Value[T6],
    T7 <% Value[T7],
    T8 <% Value[T8],
    T9 <% Value[T9]
  ](
    list: List[(T1, T2, T3, T4, T5, T6, T7, T8, T9, Content)]
  )(implicit
    ctx: Context,
    ev1: Position.IndexConstraints.Aux[Coordinates9[T1, T2, T3, T4, T5, T6, T7, T8, T9], _0, Value[T1]],
    ev2: Position.IndexConstraints.Aux[Coordinates9[T1, T2, T3, T4, T5, T6, T7, T8, T9], _1, Value[T2]],
    ev3: Position.IndexConstraints.Aux[Coordinates9[T1, T2, T3, T4, T5, T6, T7, T8, T9], _2, Value[T3]],
    ev4: Position.IndexConstraints.Aux[Coordinates9[T1, T2, T3, T4, T5, T6, T7, T8, T9], _3, Value[T4]],
    ev5: Position.IndexConstraints.Aux[Coordinates9[T1, T2, T3, T4, T5, T6, T7, T8, T9], _4, Value[T5]],
    ev6: Position.IndexConstraints.Aux[Coordinates9[T1, T2, T3, T4, T5, T6, T7, T8, T9], _5, Value[T6]],
    ev7: Position.IndexConstraints.Aux[Coordinates9[T1, T2, T3, T4, T5, T6, T7, T8, T9], _6, Value[T7]],
    ev8: Position.IndexConstraints.Aux[Coordinates9[T1, T2, T3, T4, T5, T6, T7, T8, T9], _7, Value[T8]],
    ev9: Position.IndexConstraints.Aux[Coordinates9[T1, T2, T3, T4, T5, T6, T7, T8, T9], _8, Value[T9]]
  ): Matrix9D[Value[T1], Value[T2], Value[T3], Value[T4], Value[T5], Value[T6], Value[T7], Value[T8], Value[T9]] = ctx
    .implicits
    .matrix
    .tuple9ToMatrix9D(list)

  /** Conversion from `List[(T1, T2, T3, T4, T5, T6, T7, T8, T9, Content)]` to a `MultiDimensionMatrix`.  */
  implicit def tuple9ToSListMultiDimensionMatrix[
    T1 <% Value[T1],
    T2 <% Value[T2],
    T3 <% Value[T3],
    T4 <% Value[T4],
    T5 <% Value[T5],
    T6 <% Value[T6],
    T7 <% Value[T7],
    T8 <% Value[T8],
    T9 <% Value[T9]
  ](
    list: List[(T1, T2, T3, T4, T5, T6, T7, T8, T9, Content)]
  )(implicit
    ctx: Context
  ): MultiDimensionMatrix[Coordinates9[T1, T2, T3, T4, T5, T6, T7, T8, T9]] = ctx
    .implicits
    .matrix
    .tuple9ToMultiDimensionMatrix(list)

  /** Conversion from matrix with errors tuple to `MatrixWithParseErrors`. */
  implicit def tupleToSListParseErrors[
    P <: HList
  ](
    t: (Context.U[Cell[P]], Context.U[Throwable])
  )(implicit
    ctx: Context
  ): MatrixWithParseErrors[P, Context.U] = ctx.implicits.matrix.tupleToParseErrors(t)

  /** Conversion from `List[(I, Cell[P])]` to a `Partitions`. */
  implicit def slistToPartitions[
    P <: HList,
    I : Ordering
  ](
    data: Context.U[(I, Cell[P])]
  )(implicit
    ctx: Context
  ): Partitions[P, I] = ctx.implicits.partition.toPartitions(data)

  /** Converts a `T` to a `List[Position[Coordinates1[T]]]`. */
  implicit def tToSList[
    T <% Value[T]
  ](
    t: T
  )(implicit
    ctx: Context
  ): Context.U[Position[Coordinates1[T]]] = ctx.implicits.position.tToU(t)

  /** Converts a `List[T]` to a `List[Position[Coordinates1[T]]]`. */
  implicit def listTToSList[
    T <% Value[T]
  ](
    l: List[T]
  )(implicit
    ctx: Context
  ): Context.U[Position[Coordinates1[T]]] = ctx.implicits.position.listTToU(l)

  /** Converts a `V` to a `List[Position[V :: HNil]]`. */
  implicit def valueToSList[
    V <: Value[_]
  ](
    v: V
  )(implicit
    ctx: Context
  ): Context.U[Position[V :: HNil]] = ctx.implicits.position.valueToU(v)

  /** Converts a `List[V]` to a `List[Position[V :: HNil]]`. */
  implicit def listValueToSList[
    V <: Value[_]
  ](
    l: List[V]
  )(implicit
    ctx: Context
  ): Context.U[Position[V :: HNil]] = ctx.implicits.position.listValueToU(l)

  /** Converts a `Position[P]` to a `List[Position[P]]`. */
  implicit def positionToSList[
    P <: HList
  ](
    p: Position[P]
  )(implicit
    ctx: Context
  ): Context.U[Position[P]] = ctx.implicits.position.positionToU(p)

  /** Converts a `List[Position[P]]` to a `Positions`. */
  implicit def slistToPositions[
    P <: HList
  ](
    data: Context.U[Position[P]]
  )(implicit
    ctx: Context
  ): Positions[P] = ctx.implicits.position.toPositions(data)

  /** Converts a `(T, Cell.Predicate[P])` to a `List[(List[Position[S]], Cell.Predicate[P])]`. */
  implicit def predicateToSListList[
    P <: HList,
    S <: HList,
    T <% Context.U[Position[S]]
  ](
    t: (T, Cell.Predicate[P])
  )(implicit
    ctx: Context
  ): List[(Context.U[Position[S]], Cell.Predicate[P])] = ctx.implicits.position.predicateToU(t)

  /** Converts a `List[(T, Cell.Predicate[P])]` to a `List[(List[Position[S]], Cell.Predicate[P])]`. */
  implicit def listPredicateToSListList[
    P <: HList,
    S <: HList,
    T <% Context.U[Position[S]]
  ](
    l: List[(T, Cell.Predicate[P])]
  )(implicit
    ctx: Context
  ): List[(Context.U[Position[S]], Cell.Predicate[P])] = ctx.implicits.position.listPredicateToU(l)

  /** Converts a `List[String]` to a `SaveStringsAsText`. */
  implicit def saveSListStringsAsText(
    data: Context.U[String]
  )(implicit
    ctx: Context
  ): SaveStringsAsText = ctx.implicits.environment.saveStringsAsText(data)
}

