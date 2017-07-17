// Copyright 2017 Commonwealth Bank of Australia
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

import commbank.grimlock.framework.{ Cell, MatrixWithParseErrors }
import commbank.grimlock.framework.content.Content
import commbank.grimlock.framework.distance.PairwiseDistance.{ CorrelationTuners, MutualInformationTuners }
import commbank.grimlock.framework.distribution.ApproximateDistribution.{
  CountMapQuantilesTuners,
  HistogramTuners,
  QuantilesTuners,
  TDigestQuantilesTuners,
  UniformQuantilesTuners
}
import commbank.grimlock.framework.encoding.Value
import commbank.grimlock.framework.environment.tuner.{
  Binary,
  Default,
  InMemory,
  NoParameters,
  Redistribute,
  Reducers,
  Ternary,
  Tuner,
  Unbalanced
}
import commbank.grimlock.framework.Matrix.{
  ChangeTuners,
  CompactTuners,
  DomainTuners,
  FillHeterogeneousTuners,
  FillHomogeneousTuners,
  GetTuners,
  JoinTuners,
  MaterialiseTuners,
  PairwiseTuners,
  ReshapeTuners,
  SaveAsCSVTuners,
  SaveAsIVTuners,
  SaveAsVWTuners,
  ShapeTuners,
  SetTuners,
  SizeTuners,
  SliceTuners,
  SlideTuners,
  SquashTuners,
  StreamTuners,
  SummariseTuners,
  TypesTuners,
  UniqueTuners,
  WhichTuners
}
import commbank.grimlock.framework.partition.Partitions.{ ForAllTuners, IdsTuners }
import commbank.grimlock.framework.Persist.SaveAsTextTuners
import commbank.grimlock.framework.position.Position
import commbank.grimlock.framework.position.Positions.NamesTuners
import commbank.grimlock.framework.statistics.Statistics.{
  CountsTuners,
  DistinctCountsTuners,
  KurtosisTuners,
  MaximumAbsoluteTuners,
  MaximumTuners,
  MeanTuners,
  MinimumTuners,
  PredicateCountsTuners,
  StandardDeviationTuners,
  SkewnessTuners,
  SumsTuners
}
import commbank.grimlock.framework.utility.UnionTypes.{ In, Is, OneOf }

import commbank.grimlock.scalding.content.{ Contents, IndexedContents }
import commbank.grimlock.scalding.partition.Partitions
import commbank.grimlock.scalding.position.Positions

import shapeless.Nat
import shapeless.nat.{ _1, _2, _3, _4, _5, _6, _7, _8, _9 }
import shapeless.ops.nat.Diff

package object environment {
  /** Type alias for all default tuners. */
  type PipeDefaultTuners[T] = T In OneOf[Default[NoParameters]]#Or[Default[Reducers]]

  /** Type alias for all common tuners. */
  type PipeAllTuners[T] = T In OneOf[InMemory[NoParameters]]#
    Or[Default[NoParameters]]#
    Or[Default[Reducers]]#
    Or[Unbalanced[Reducers]]

  // *** Matrix/Position shared tuners

  /** Implicit for checking tuners on a call to `names`. */
  implicit def pipeNamesTuners[T <: Tuner : PipeDefaultTuners] = new NamesTuners[Context.U, T] { }

  //  *** Persist tuners

  /** Type alias for all saveAsText tuners. */
  type PipeSaveAsTextTuners[T] = T In OneOf[Default[NoParameters]]#Or[Redistribute]

  /** Implicit for checking tuners on a call to `saveAstext`. */
  implicit def pipeSaveAsTextTuners[T <: Tuner : PipeSaveAsTextTuners] = new SaveAsTextTuners[Context.U, T] { }

  // *** Pairwise distance tuners

  /** Type alias for all pairwise distance tuners. */
  type PipeDistanceTuners[T] = T In OneOf[InMemory[NoParameters]]# // Def[NoParam], InMem[NoParam], Def[NoParam]
    Or[Default[NoParameters]]#                                     // Def[NoParam], Def[NoParam],   Def[NoParam]
    Or[Ternary[Default[Reducers], InMemory[NoParameters], Default[NoParameters]]]#
    Or[Ternary[Default[Reducers], InMemory[NoParameters], Default[Reducers]]]#
    Or[Ternary[Default[Reducers], InMemory[NoParameters], Unbalanced[Reducers]]]#
    Or[Ternary[Default[Reducers], InMemory[Reducers], Default[Reducers]]]#
    Or[Ternary[Default[Reducers], InMemory[Reducers], Unbalanced[Reducers]]]#
    Or[Ternary[Default[Reducers], Default[NoParameters], Default[NoParameters]]]#
    Or[Ternary[Default[Reducers], Default[NoParameters], Default[Reducers]]]#
    Or[Ternary[Default[Reducers], Default[NoParameters], Unbalanced[Reducers]]]#
    Or[Ternary[Default[Reducers], Default[Reducers], Default[Reducers]]]#
    Or[Ternary[Default[Reducers], Default[Reducers], Unbalanced[Reducers]]]#
    Or[Ternary[Default[Reducers], Unbalanced[Reducers], Default[Reducers]]]#
    Or[Ternary[Default[Reducers], Unbalanced[Reducers], Unbalanced[Reducers]]]

  /** Implicit for checking tuners on a call to `corrrelation`. */
  implicit def pipeCorrelationTuners[T <: Tuner : PipeDistanceTuners] = new CorrelationTuners[Context.U, T] { }

  /** Implicit for checking tuners on a call to `mutualInformation`. */
  implicit def pipeMutualInformationTuners[
    T <: Tuner : PipeDistanceTuners
  ] = new MutualInformationTuners[Context.U, T] { }

  // *** Distribution tuners

  /** Type alias for quantiles tuners. */
  type PipeQuantilesTuners[T] = T In OneOf[InMemory[NoParameters]]#
    Or[InMemory[Reducers]]#
    Or[Default[NoParameters]]#
    Or[Default[Reducers]]#
    Or[Unbalanced[Reducers]]

  /** Implicit for checking tuners on a call to `histogram`. */
  implicit def pipeHistogramTuners[T <: Tuner : PipeDefaultTuners] = new HistogramTuners[Context.U, T] { }

  /** Implicit for checking tuners on a call to `quantiles`. */
  implicit def pipeQuantilesTuners[T <: Tuner : PipeQuantilesTuners] = new QuantilesTuners[Context.U, T] { }

  /** Implicit for checking tuners on a call to `countMapQuantiles`. */
  implicit def pipeCountMapQuantilesTuners[
    T <: Tuner : PipeDefaultTuners
  ] = new CountMapQuantilesTuners[Context.U, T] { }

  /** Implicit for checking tuners on a call to `tDigestQuantiles`. */
  implicit def pipeTDigestQuantilesTuners[T <: Tuner : PipeDefaultTuners] = new TDigestQuantilesTuners[Context.U, T] { }

  /** Implicit for checking tuners on a call to `uniformQuantiles`. */
  implicit def pipeUniformQuantilesTuners[T <: Tuner : PipeDefaultTuners] = new UniformQuantilesTuners[Context.U, T] { }

  // *** Partition tuners

  /** Implicit for checking tuners on a call to `forAll`. */
  implicit def pipeForAllTuners[T <: Tuner : PipeDefaultTuners] = new ForAllTuners[Context.U, T] { }

  /** Implicit for checking tuners on a call to `ids`. */
  implicit def pipeIdsTuners[T <: Tuner : PipeDefaultTuners] = new IdsTuners[Context.U, T] { }

  // *** Statistics tuners

  /** Implicit for checking tuners on a call to `counts`. */
  implicit def pipeCountsTuners[T <: Tuner : PipeDefaultTuners] = new CountsTuners[Context.U, T] { }

  /** Implicit for checking tuners on a call to `distinctCounts`. */
  implicit def pipeDistinctCountsTuners[T <: Tuner : PipeDefaultTuners] = new DistinctCountsTuners[Context.U, T] { }

  /** Implicit for checking tuners on a call to `predicateCounts`. */
  implicit def pipePredicateCountsTuners[T <: Tuner : PipeDefaultTuners] = new PredicateCountsTuners[Context.U, T] { }

  /** Implicit for checking tuners on a call to `mean`. */
  implicit def pipeMeanTuners[T <: Tuner : PipeDefaultTuners] = new MeanTuners[Context.U, T] { }

  /** Implicit for checking tuners on a call to `standardDeviation`. */
  implicit def pipeStandardDeviationTuners[
    T <: Tuner : PipeDefaultTuners
  ] = new StandardDeviationTuners[Context.U, T] { }

  /** Implicit for checking tuners on a call to `skewness`. */
  implicit def pipeSkewnesTuners[T <: Tuner : PipeDefaultTuners] = new SkewnessTuners[Context.U, T] { }

  /** Implicit for checking tuners on a call to `kurtosis`. */
  implicit def pipeKurtosisTuners[T <: Tuner : PipeDefaultTuners] = new KurtosisTuners[Context.U, T] { }

  /** Implicit for checking tuners on a call to `minimum`. */
  implicit def pipeMinimumTuners[T <: Tuner : PipeDefaultTuners] = new MinimumTuners[Context.U, T] { }

  /** Implicit for checking tuners on a call to `maximum`. */
  implicit def pipeMaximumTuners[T <: Tuner : PipeDefaultTuners] = new MaximumTuners[Context.U, T] { }

  /** Implicit for checking tuners on a call to `maximumAbsolute`. */
  implicit def pipeMaximumAbsoluteTuners[T <: Tuner : PipeDefaultTuners] = new MaximumAbsoluteTuners[Context.U, T] { }

  /** Implicit for checking tuners on a call to `sums`. */
  implicit def pipeSumsTuners[T <: Tuner : PipeDefaultTuners] = new SumsTuners[Context.U, T] { }

  // *** Matrix tuners

  /** Type alias for domain tuners. */
  type PipeDomainTuners[T] = T In OneOf[InMemory[NoParameters]]#
    Or[InMemory[Reducers]]#
    Or[Default[NoParameters]]#
    Or[Default[Reducers]]

  /** Type alias for fillHeterogeneous tuners. */
  type PipeFillHeterogeneousTuners[T] = T In OneOf[Default[NoParameters]]# // Default[NoParam] x 3
    Or[Ternary[InMemory[NoParameters], InMemory[NoParameters], Default[NoParameters]]]#
    Or[Ternary[InMemory[NoParameters], Default[NoParameters], Default[NoParameters]]]#
    Or[Ternary[InMemory[NoParameters], InMemory[NoParameters], Default[Reducers]]]#
    Or[Ternary[InMemory[NoParameters], Default[NoParameters], Default[Reducers]]]#
    Or[Ternary[InMemory[NoParameters], Default[Reducers], Default[Reducers]]]#
    Or[Ternary[Default[NoParameters], Default[NoParameters], Default[Reducers]]]#
    Or[Ternary[Default[NoParameters], Default[Reducers], Default[Reducers]]]#
    Or[Ternary[Default[Reducers], Default[Reducers], Default[Reducers]]]

  /** Type alias for fillHomogeneous tuners. */
  type PipeFillHomogeneousTuners[T] = T In OneOf[Default[NoParameters]]# // Default[NoParam] x 2
    Or[Binary[InMemory[NoParameters], Default[NoParameters]]]#
    Or[Binary[InMemory[NoParameters], Default[Reducers]]]#
    Or[Binary[InMemory[Reducers], Default[NoParameters]]]#
    Or[Binary[InMemory[Reducers], Default[Reducers]]]#
    Or[Binary[Default[NoParameters], Default[Reducers]]]#
    Or[Binary[Default[Reducers], Default[NoParameters]]]#
    Or[Binary[Default[Reducers], Default[Reducers]]]

  /** Type alias for join tuners. */
  type PipeJoinTuners[T] = T In OneOf[InMemory[NoParameters]]# // InMemory[NoParam], InMemory[NoParam]
    Or[Default[NoParameters]]#                                 // Default[NoParam],  Default[NoParam]
    Or[Binary[InMemory[NoParameters], Default[NoParameters]]]#
    Or[Binary[InMemory[NoParameters], Default[Reducers]]]#
    Or[Binary[InMemory[NoParameters], Unbalanced[Reducers]]]#
    Or[Binary[InMemory[Reducers], Default[Reducers]]]#
    Or[Binary[InMemory[Reducers], Unbalanced[Reducers]]]#
    Or[Binary[Default[NoParameters], Default[Reducers]]]#
    Or[Binary[Default[NoParameters], Unbalanced[Reducers]]]#
    Or[Binary[Default[Reducers], Default[Reducers]]]#
    Or[Binary[Default[Reducers], Unbalanced[Reducers]]]

  /** Type alias for materialise tuners. */
  type PipeMaterialiseTuners[T] = T Is Default[NoParameters]

  /** Type alias for pairwise tuners. */
  type PipePairwiseTuners[T] = T In OneOf[InMemory[NoParameters]]#
    Or[Default[NoParameters]]#
    Or[Ternary[InMemory[NoParameters], Default[Reducers], Default[Reducers]]]#
    Or[Ternary[InMemory[NoParameters], Default[Reducers], Unbalanced[Reducers]]]#
    Or[Ternary[InMemory[NoParameters], Unbalanced[Reducers], Default[Reducers]]]#
    Or[Ternary[InMemory[NoParameters], Unbalanced[Reducers], Unbalanced[Reducers]]]#
    Or[Ternary[Default[Reducers], Default[Reducers], Default[Reducers]]]#
    Or[Ternary[Default[Reducers], Default[Reducers], Unbalanced[Reducers]]]#
    Or[Ternary[Default[Reducers], Unbalanced[Reducers], Default[Reducers]]]#
    Or[Ternary[Default[Reducers], Unbalanced[Reducers], Unbalanced[Reducers]]]

  /** Type alias for saveAsIV tuners. */
  type PipeSaveAsIVTuners[T] = T In OneOf[Default[NoParameters]]#  // Default[NoParam] x 2
    Or[Binary[InMemory[NoParameters], Default[NoParameters]]]#
    Or[Binary[InMemory[NoParameters], Redistribute]]#
    Or[Binary[Default[NoParameters], Redistribute]]#
    Or[Binary[Default[Reducers], Default[NoParameters]]]#
    Or[Binary[Default[Reducers], Redistribute]]#
    Or[Binary[Unbalanced[Reducers], Default[NoParameters]]]#
    Or[Binary[Unbalanced[Reducers], Redistribute]]

  /** Type alias for slide tuners. */
  type PipeSlideTuners[T] = T In OneOf[Default[NoParameters]]#Or[Default[Reducers]]#Or[Redistribute]

  /** Type alias for stream tuners. */
  type PipeStreamTuners[T] = T Is Default[Reducers]

  /** Type alias for saveAsCSV tuners. */
  type PipeSaveAsCSVTuners[T] = T In OneOf[Default[NoParameters]]# // Default[NoParameters], Default[NoParameters]
    Or[Default[Reducers]]#                                         // Default[Reducers],     Default[NoParameters]
    Or[Redistribute]#                                              // Default[NoParameters], Redistribute
    Or[Binary[Default[Reducers], Redistribute]]

  /** Type alias for saveAsVW tuners. */
  type PipeSaveAsVWTuners[T] = T In OneOf[Default[NoParameters]]#  // Def[NoParam], Def[NoParam],   Def[NoParam]
    Or[Default[Reducers]]#                                         // Def[Red],     Def[Red],       Def[NoParam]
    Or[Binary[Default[Reducers], Redistribute]]#                   // Def[Red],     Def[Red],       Redis
    Or[Binary[Default[NoParameters], Redistribute]]#               // Def[NoParam], Def[NoParam],   Redis
    Or[Binary[Default[NoParameters], InMemory[NoParameters]]]#     // Def[NoParam], InMem[NoParam], Def[NoParam]
    Or[Binary[Default[NoParameters], Default[Reducers]]]#          // Def[NoParam], Def[Red],       Def[NoParam]
    Or[Ternary[Default[NoParameters], InMemory[NoParameters], Redistribute]]#
    Or[Ternary[Default[NoParameters], Default[Reducers], Redistribute]]#
    Or[Ternary[Default[NoParameters], Unbalanced[Reducers], Default[NoParameters]]]#
    Or[Ternary[Default[NoParameters], Unbalanced[Reducers], Redistribute]]#
    Or[Ternary[Default[Reducers], InMemory[NoParameters], Default[NoParameters]]]#
    Or[Ternary[Default[Reducers], InMemory[NoParameters], Redistribute]]#
    Or[Ternary[Default[Reducers], Unbalanced[Reducers], Default[NoParameters]]]#
    Or[Ternary[Default[Reducers], Unbalanced[Reducers], Redistribute]]

  /** Implicit for checking tuners on a call to `change`. */
  implicit def pipChangeTuners[T <: Tuner : PipeAllTuners] = new ChangeTuners[Context.U, T] { }

  /** Implicit for checking tuners on a call to `compact`. */
  implicit def pipeCompactTuners[T <: Tuner : PipeDefaultTuners] = new CompactTuners[Context.U, T] { }

  /** Implicit for checking tuners on a call to `domain`. */
  implicit def pipeDomainTuners[T <: Tuner : PipeDomainTuners] = new DomainTuners[Context.U, T] { }

  /** Implicit for checking tuners on a call to `fillHeterogeneous`. */
  implicit def pipeFillHeterogeneousTuners[
    T <: Tuner : PipeFillHeterogeneousTuners
  ] = new FillHeterogeneousTuners[Context.U, T] { }

  /** Implicit for checking tuners on a call to `fillHomogeneous`. */
  implicit def pipeFillHomogeneousTuners[
    T <: Tuner : PipeFillHomogeneousTuners
  ] = new FillHomogeneousTuners[Context.U, T] { }

  /** Implicit for checking tuners on a call to `get`. */
  implicit def pipeGetTuners[T <: Tuner : PipeAllTuners] = new GetTuners[Context.U, T] { }

  /** Implicit for checking tuners on a call to `join`. */
  implicit def pipeJoinTuners[T <: Tuner : PipeJoinTuners] = new JoinTuners[Context.U, T] { }

  /** Implicit for checking tuners on a call to `materialise`. */
  implicit def pipeMaterialiseTuners[T <: Tuner : PipeMaterialiseTuners] = new  MaterialiseTuners[Context.U, T] { }

  /** Implicit for checking tuners on a call to `pairwise*`. */
  implicit def pipePairwiseTuners[T <: Tuner : PipePairwiseTuners] = new PairwiseTuners[Context.U, T] { }

  /** Implicit for checking tuners on a call to `saveAsIV`. */
  implicit def pipeSaveAsIVTuners[T <: Tuner : PipeSaveAsIVTuners] = new SaveAsIVTuners[Context.U, T] { }

  /** Implicit for checking tuners on a call to `set`. */
  implicit def pipeSetTuners[T <: Tuner : PipeDefaultTuners] = new SetTuners[Context.U, T] { }

  /** Implicit for checking tuners on a call to `shape`. */
  implicit def pipeShapeTuners[T <: Tuner : PipeDefaultTuners] = new ShapeTuners[Context.U, T] { }

  /** Implicit for checking tuners on a call to `size`. */
  implicit def pipeSizeTuners[T <: Tuner : PipeDefaultTuners] = new SizeTuners[Context.U, T] { }

  /** Implicit for checking tuners on a call to `slice`. */
  implicit def pipeSliceTuners[T <: Tuner : PipeAllTuners] = new SliceTuners[Context.U, T] { }

  /** Implicit for checking tuners on a call to `slide`. */
  implicit def pipeSlideTuners[T <: Tuner : PipeSlideTuners] = new SlideTuners[Context.U, T] { }

  /** Implicit for checking tuners on a call to `stream`. */
  implicit def pipeStreamTuners[T <: Tuner : PipeStreamTuners] = new StreamTuners[Context.U, T] { }

  /** Implicit for checking tuners on a call to `summmarise`. */
  implicit def pipeSummariseTuners[T <: Tuner : PipeDefaultTuners] = new SummariseTuners[Context.U, T] { }

  /** Implicit for checking tuners on a call to `types`. */
  implicit def pipeTypesTuners[T <: Tuner : PipeDefaultTuners] = new TypesTuners[Context.U, T] { }

  /** Implicit for checking tuners on a call to `unique`. */
  implicit def pipeUniqueTuners[T <: Tuner : PipeDefaultTuners] = new UniqueTuners[Context.U, T] { }

  /** Implicit for checking tuners on a call to `which`. */
  implicit def pipeWhichTuners[T <: Tuner : PipeAllTuners] = new WhichTuners[Context.U, T] { }

  /** Implicit for checking tuners on a call to `squash`. */
  implicit def pipeSquashTuners[T <: Tuner : PipeDefaultTuners] = new SquashTuners[Context.U, T] { }

  /** Implicit for checking tuners on a call to `reshape`. */
  implicit def pipeReshapeTuners[T <: Tuner : PipeAllTuners] = new ReshapeTuners[Context.U, T] { }

  /** Implicit for checking tuners on a call to `saveAsCSV`. */
  implicit def pipeSaveAsCSVTuners[T <: Tuner : PipeSaveAsCSVTuners] = new SaveAsCSVTuners[Context.U, T] { }

  /** Implicit for checking tuners on a call to `saveAsVW`. */
  implicit def pipeSaveAsVWTuners[T <: Tuner : PipeSaveAsVWTuners] = new SaveAsVWTuners[Context.U, T] { }

  /** Converts a `Cell[P]` into a `TypedPipe[Cell[P]]`. */
  implicit def cellToPipe[
    P <: Nat
  ](
    t: Cell[P]
  )(implicit
    ctx: Context
  ): Context.U[Cell[P]] = ctx.implicits.cell.cellToU(t)

  /** Converts a `List[Cell[P]]` into a `TypedPipe[Cell[P]]`. */
  implicit def listCellToPipe[
    P <: Nat
  ](
    t: List[Cell[P]]
  )(implicit
    ctx: Context
  ): Context.U[Cell[P]] = ctx.implicits.cell.listCellToU(t)

  /** Converts a `TypedPipe[Content]` to a `Contents`. */
  implicit def pipeToContents(
    data: Context.U[Content]
  )(implicit
    ctx: Context
  ): Contents = ctx.implicits.content.toContents(data)

  /** Converts a `TypedPipe[(Position[P], Content)]` to a `IndexedContents`. */
  implicit def pipeToIndexed[
    P <: Nat
  ](
    data: Context.U[(Position[P], Content)]
  )(implicit
    ctx: Context
  ): IndexedContents[P] = ctx.implicits.content.toIndexed(data)

  /** Conversion from `TypedPipe[Cell[_1]]` to a `Matrix1D`. */
  implicit def pipeToMatrix1(
    data: Context.U[Cell[_1]]
  )(implicit
    ctx: Context
  ): Matrix1D = ctx.implicits.matrix.toMatrix1D(data)

  /** Conversion from `TypedPipe[Cell[_2]]` to a `Matrix2D`. */
  implicit def pipeToMatrix2(
    data: Context.U[Cell[_2]]
  )(implicit
    ctx: Context
  ): Matrix2D = ctx.implicits.matrix.toMatrix2D(data)

  /** Conversion from `TypedPipe[Cell[_3]]` to a `Matrix3D`. */
  implicit def pipeToMatrix3(
    data: Context.U[Cell[_3]]
  )(implicit
    ctx: Context
  ): Matrix3D = ctx.implicits.matrix.toMatrix3D(data)

  /** Conversion from `TypedPipe[Cell[_4]]` to a `Matrix4D`. */
  implicit def pipeToMatrix4(
    data: Context.U[Cell[_4]]
  )(implicit
    ctx: Context
  ): Matrix4D = ctx.implicits.matrix.toMatrix4D(data)

  /** Conversion from `TypedPipe[Cell[_5]]` to a `Matrix5D`. */
  implicit def pipeToMatrix5(
    data: Context.U[Cell[_5]]
  )(implicit
    ctx: Context
  ): Matrix5D = ctx.implicits.matrix.toMatrix5D(data)

  /** Conversion from `TypedPipe[Cell[_6]]` to a `Matrix6D`. */
  implicit def pipeToMatrix6(
    data: Context.U[Cell[_6]]
  )(implicit
    ctx: Context
  ): Matrix6D = ctx.implicits.matrix.toMatrix6D(data)

  /** Conversion from `TypedPipe[Cell[_7]]` to a `Matrix7D`. */
  implicit def pipeToMatrix7(
    data: Context.U[Cell[_7]]
  )(implicit
    ctx: Context
  ): Matrix7D = ctx.implicits.matrix.toMatrix7D(data)

  /** Conversion from `TypedPipe[Cell[_8]]` to a `Matrix8D`. */
  implicit def pipeToMatrix8(
    data: Context.U[Cell[_8]]
  )(implicit
    ctx: Context
  ): Matrix8D = ctx.implicits.matrix.toMatrix8D(data)

  /** Conversion from `TypedPipe[Cell[_9]]` to a `Matrix9D`. */
  implicit def pipeToMatrix9(
    data: Context.U[Cell[_9]]
  )(implicit
    ctx: Context
  ): Matrix9D = ctx.implicits.matrix.toMatrix9D(data)

  /** Conversion from `List[Cell[_1]]` to a `Matrix1D`. */
  implicit def listToPipeMatrix1(
    data: List[Cell[_1]]
  )(implicit
    ctx: Context
  ): Matrix1D = ctx.implicits.matrix.listToMatrix1D(data)

  /** Conversion from `List[Cell[_2]]` to a `Matrix2D`. */
  implicit def listToPipeMatrix2(
    data: List[Cell[_2]]
  )(implicit
    ctx: Context
  ): Matrix2D = ctx.implicits.matrix.listToMatrix2D(data)

  /** Conversion from `List[Cell[_3]]` to a `Matrix3D`. */
  implicit def listToPipeMatrix3(
    data: List[Cell[_3]]
  )(implicit
    ctx: Context
  ): Matrix3D = ctx.implicits.matrix.listToMatrix3D(data)

  /** Conversion from `List[Cell[_4]]` to a `Matrix4D`. */
  implicit def listToPipeMatrix4(
    data: List[Cell[_4]]
  )(implicit
    ctx: Context
  ): Matrix4D = ctx.implicits.matrix.listToMatrix4D(data)

  /** Conversion from `List[Cell[_5]]` to a `Matrix5D`. */
  implicit def listToPipeMatrix5(
    data: List[Cell[_5]]
  )(implicit
    ctx: Context
  ): Matrix5D = ctx.implicits.matrix.listToMatrix5D(data)

  /** Conversion from `List[Cell[_6]]` to a `Matrix6D`. */
  implicit def listToPipeMatrix6(
    data: List[Cell[_6]]
  )(implicit
    ctx: Context
  ): Matrix6D = ctx.implicits.matrix.listToMatrix6D(data)

  /** Conversion from `List[Cell[_7]]` to a `Matrix7D`. */
  implicit def listToPipeMatrix7(
    data: List[Cell[_7]]
  )(implicit
    ctx: Context
  ): Matrix7D = ctx.implicits.matrix.listToMatrix7D(data)

  /** Conversion from `List[Cell[_8]]` to a `Matrix8D`. */
  implicit def listToPipeMatrix8(
    data: List[Cell[_8]]
  )(implicit
    ctx: Context
  ): Matrix8D = ctx.implicits.matrix.listToMatrix8D(data)

  /** Conversion from `List[Cell[_9]]` to a `Matrix9D`. */
  implicit def listToPipeMatrix9(
    data: List[Cell[_9]]
  )(implicit
    ctx: Context
  ): Matrix9D = ctx.implicits.matrix.listToMatrix9D(data)

  /** Conversion from `List[(Value, Content)]` to a `Matrix1D`. */
  implicit def tupleToPipeMatrix1[
    V <% Value
  ](
    list: List[(V, Content)]
  )(implicit
    ctx: Context
  ): Matrix1D = ctx.implicits.matrix.tupleToMatrix1D(list)

  /** Conversion from `List[(Value, Value, Content)]` to a `Matrix2D`. */
  implicit def tupleToPipeMatrix2[
    V <% Value,
    W <% Value
  ](
    list: List[(V, W, Content)]
  )(implicit
    ctx: Context
  ): Matrix2D = ctx.implicits.matrix.tupleToMatrix2D(list)

  /** Conversion from `List[(Value, Value, Value, Content)]` to a `Matrix3D`. */
  implicit def tupleToPipeMatrix3[
    V <% Value,
    W <% Value,
    X <% Value
  ](
    list: List[(V, W, X, Content)]
  )(implicit
    ctx: Context
  ): Matrix3D = ctx.implicits.matrix.tupleToMatrix3D(list)

  /** Conversion from `List[(Value, Value, Value, Value, Content)]` to a `Matrix4D`. */
  implicit def tupleToPipeMatrix4[
    V <% Value,
    W <% Value,
    X <% Value,
    Y <% Value
  ](
    list: List[(V, W, X, Y, Content)]
  )(implicit
    ctx: Context
  ): Matrix4D = ctx.implicits.matrix.tupleToMatrix4D(list)

  /** Conversion from `List[(Value, Value, Value, Value, Value, Content)]` to a `Matrix5D`. */
  implicit def tupleToPipeMatrix5[
    V <% Value,
    W <% Value,
    X <% Value,
    Y <% Value,
    Z <% Value
  ](
    list: List[(V, W, X, Y, Z, Content)]
  )(implicit
    ctx: Context
  ): Matrix5D = ctx.implicits.matrix.tupleToMatrix5D(list)

  /** Conversion from `List[(Value, Value, Value, Value, Value, Value, Content)]` to a `Matrix6D`. */
  implicit def tupleToPipeMatrix6[
    T <% Value,
    V <% Value,
    W <% Value,
    X <% Value,
    Y <% Value,
    Z <% Value
  ](
    list: List[(T, V, W, X, Y, Z, Content)]
  )(implicit
    ctx: Context
  ): Matrix6D = ctx.implicits.matrix.tupleToMatrix6D(list)

  /** Conversion from `List[(Value, Value, Value, Value, Value, Value, Value, Content)]` to a `Matrix7D`. */
  implicit def tupleToPipeMatrix7[
    S <% Value,
    T <% Value,
    V <% Value,
    W <% Value,
    X <% Value,
    Y <% Value,
    Z <% Value
  ](
    list: List[(S, T, V, W, X, Y, Z, Content)]
  )(implicit
    ctx: Context
  ): Matrix7D = ctx.implicits.matrix.tupleToMatrix7D(list)

  /** Conversion from `List[(Value, Value, Value, Value, Value, Value, Value, Value, Content)]` to a `Matrix8D`. */
  implicit def tupleToPipeMatrix8[
    R <% Value,
    S <% Value,
    T <% Value,
    V <% Value,
    W <% Value,
    X <% Value,
    Y <% Value,
    Z <% Value
  ](
    list: List[(R, S, T, V, W, X, Y, Z, Content)]
  )(implicit
    ctx: Context
  ): Matrix8D = ctx.implicits.matrix.tupleToMatrix8D(list)

  /**
   * Conversion from `List[(Value, Value, Value, Value, Value, Value, Value, Value, Value, Content)]` to a `Matrix9D`.
   */
  implicit def tupleToPipeMatrix9[
    Q <% Value,
    R <% Value,
    S <% Value,
    T <% Value,
    V <% Value,
    W <% Value,
    X <% Value,
    Y <% Value,
    Z <% Value
  ](
    list: List[(Q, R, S, T, V, W, X, Y, Z, Content)]
  )(implicit
    ctx: Context
  ): Matrix9D = ctx.implicits.matrix.tupleToMatrix9D(list)

  /** Conversion from matrix with errors tuple to `MatrixWithParseErrors`. */
  implicit def tupleToPipeParseErrors[
    P <: Nat
  ](
    t: (Context.U[Cell[P]], Context.U[String])
  )(implicit
    ctx: Context
  ): MatrixWithParseErrors[P, Context.U] = ctx.implicits.matrix.tupleToParseErrors(t)

  /** Conversion from `TypedPipe[(I, Cell[P])]` to a `Partitions`. */
  implicit def pipeToPartitions[
    P <: Nat,
    I : Ordering
  ](
    data: Context.U[(I, Cell[P])]
  )(implicit
    ctx: Context
  ): Partitions[P, I] = ctx.implicits.partition.toPartitions(data)

  /** Converts a `Value` to a `TypedPipe[Position[_1]]`. */
  implicit def valueToPipe[
    T <% Value
  ](
    t: T
  )(implicit
    ctx: Context
  ): Context.U[Position[_1]] = ctx.implicits.position.valueToU(t)

  /** Converts a `List[Value]` to a `TypedPipe[Position[_1]]`. */
  implicit def listValueToPipe[
    T <% Value
  ](
    t: List[T]
  )(implicit
    ctx: Context
  ): Context.U[Position[_1]] = ctx.implicits.position.listValueToU(t)

  /** Converts a `Position[T]` to a `TypedPipe[Position[T]]`. */
  implicit def positionToPipe[
    T <: Nat
  ](
    t: Position[T]
  )(implicit
    ctx: Context
  ): Context.U[Position[T]] = ctx.implicits.position.positionToU(t)

  /** Converts a `List[Position[T]]` to a `TypedPipe[Position[T]]`. */
  implicit def listPositionToPipe[
    T <: Nat
  ](
    t: List[Position[T]]
  )(implicit
    ctx: Context
  ): Context.U[Position[T]] = ctx.implicits.position.listPositionToU(t)

  /** Converts a `TypedPipe[Position[P]]` to a `Positions`. */
  implicit def pipeToPositions[
    L <: Nat,
    P <: Nat
  ](
    data: Context.U[Position[P]]
  )(implicit
    ctx: Context,
    ev: Diff.Aux[P, _1, L]
  ): Positions[L, P] = ctx.implicits.position.toPositions(data)

  /** Converts a `(T, Cell.Predicate[P])` to a `List[(TypedPipe[Position[S]], Cell.Predicate[P])]`. */
  implicit def predicateToPipeList[
    P <: Nat,
    S <: Nat,
    T <% Context.U[Position[S]]
  ](
    t: (T, Cell.Predicate[P])
  )(implicit
    ctx: Context
  ): List[(Context.U[Position[S]], Cell.Predicate[P])] = ctx.implicits.position.predicateToU(t)

  /** Converts a `List[(T, Cell.Predicate[P])]` to a `List[(TypedPipe[Position[S]], Cell.Predicate[P])]`. */
  implicit def listPredicateToPipeList[
    P <: Nat,
    S <: Nat,
    T <% Context.U[Position[S]]
  ](
    t: List[(T, Cell.Predicate[P])]
  )(implicit
    ctx: Context
  ): List[(Context.U[Position[S]], Cell.Predicate[P])] = ctx.implicits.position.listPredicateToU(t)

  /** Converts a `TypedPipe[String]` to a `SaveStringsAsText`. */
  implicit def savePipeStringsAsText(
    data: Context.U[String]
  )(implicit
    ctx: Context
  ): SaveStringsAsText = ctx.implicits.environment.saveStringsAsText(data)
}

