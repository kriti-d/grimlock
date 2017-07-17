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

package commbank.grimlock.spark

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
  Tuner
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

import commbank.grimlock.spark.content.{ Contents, IndexedContents }
import commbank.grimlock.spark.partition.Partitions
import commbank.grimlock.spark.position.Positions

import shapeless.Nat
import shapeless.nat.{ _1, _2, _3, _4, _5, _6, _7, _8, _9 }
import shapeless.ops.nat.Diff

package object environment {
  /** Type alias for all default tuners. */
  type RDDDefaultTuners[T] = T In OneOf[Default[NoParameters]]#Or[Default[Reducers]]

  /** Type alias for default or in memory tuners. */
  type RDDDefaultInMemoryTuners[T] = T In OneOf[InMemory[NoParameters]]#
    Or[Default[NoParameters]]#
    Or[Default[Reducers]]

  // *** Matrix/Position shared tuners

  /** Implicit for checking tuners on a call to `names`. */
  implicit def rddNamesTuners[T <: Tuner : RDDDefaultTuners] = new NamesTuners[Context.U, T] { }

  // *** Persist tuners

  /** Type alias for all saveAsText tuners. */
  type RDDSaveAsTextTuners[T] = T In OneOf[Default[NoParameters]]#Or[Redistribute]

  /** Implicit for checking tuners on a call to `saveAstext`. */
  implicit def rddSaveAsTextTuners[T <: Tuner : RDDSaveAsTextTuners] = new SaveAsTextTuners[Context.U, T] { }

  // *** Pairwise distance tuners

  /** Type alias for all pairwise distance tuners. */
  type RDDDistanceTuners[T] = T In OneOf[InMemory[NoParameters]]# // Def[NoParam], InMem[NoParam], Def[NoParam]
    Or[Default[NoParameters]]#                                    // Def[NoParam], Def[NoParam],  Def[NoParam]
    Or[Ternary[Default[Reducers], InMemory[NoParameters], Default[NoParameters]]]#
    Or[Ternary[Default[Reducers], InMemory[NoParameters], Default[Reducers]]]#
    Or[Ternary[Default[Reducers], InMemory[Reducers], Default[Reducers]]]#
    Or[Ternary[Default[Reducers], Default[NoParameters], Default[NoParameters]]]#
    Or[Ternary[Default[Reducers], Default[NoParameters], Default[Reducers]]]#
    Or[Ternary[Default[Reducers], Default[Reducers], Default[Reducers]]]

  /** Implicit for checking tuners on a call to `corrrelation`. */
  implicit def rddCorrelationTuners[T <: Tuner : RDDDistanceTuners] = new CorrelationTuners[Context.U, T] { }

  /** Implicit for checking tuners on a call to `mutualInformation`. */
  implicit def rddMutualInformationTuners[
    T <: Tuner : RDDDistanceTuners
  ] = new MutualInformationTuners[Context.U, T] { }

  // *** Distribution tuners

  /** Type alias for quantiles tuners. */
  type RDDQuantilesTuners[T] = T In OneOf[InMemory[NoParameters]]#
    Or[InMemory[Reducers]]#
    Or[Default[NoParameters]]#
    Or[Default[Reducers]]

  /** Implicit for checking tuners on a call to `histogram`. */
  implicit def rddHistogramTuners[T <: Tuner : RDDDefaultTuners] = new HistogramTuners[Context.U, T] { }

  /** Implicit for checking tuners on a call to `quantiles`. */
  implicit def rddQuantilesTuners[T <: Tuner : RDDQuantilesTuners] = new QuantilesTuners[Context.U, T] { }

  /** Implicit for checking tuners on a call to `countMapQuantiles`. */
  implicit def rddCountMapQuantilesTuners[T <: Tuner : RDDDefaultTuners] = new CountMapQuantilesTuners[Context.U, T] { }

  /** Implicit for checking tuners on a call to `tDigestQuantiles`. */
  implicit def rddTDigestQuantilesTuners[T <: Tuner : RDDDefaultTuners] = new TDigestQuantilesTuners[Context.U, T] { }

  /** Implicit for checking tuners on a call to `uniformQuantiles`. */
  implicit def rddUniformQuantilesTuners[T <: Tuner : RDDDefaultTuners] = new UniformQuantilesTuners[Context.U, T] { }

  // *** Partition tuners

  /** Implicit for checking tuners on a call to `forAll`. */
  implicit def rddForAllTuners[T <: Tuner : RDDDefaultTuners] = new ForAllTuners[Context.U, T] { }

  /** Implicit for checking tuners on a call to `ids`. */
  implicit def rddIdsTuners[T <: Tuner : RDDDefaultTuners] = new IdsTuners[Context.U, T] { }

  // *** Statistics tuners

  /** Implicit for checking tuners on a call to `counts`. */
  implicit def rddCountsTuners[T <: Tuner : RDDDefaultTuners] = new CountsTuners[Context.U, T] { }

  /** Implicit for checking tuners on a call to `distinctCounts`. */
  implicit def rddDistinctCountsTuners[T <: Tuner : RDDDefaultTuners] = new DistinctCountsTuners[Context.U, T] { }

  /** Implicit for checking tuners on a call to `predicateCounts`. */
  implicit def rddPredicateCountsTuners[T <: Tuner : RDDDefaultTuners] = new PredicateCountsTuners[Context.U, T] { }

  /** Implicit for checking tuners on a call to `mean`. */
  implicit def rddMeanTuners[T <: Tuner : RDDDefaultTuners] = new MeanTuners[Context.U, T] { }

  /** Implicit for checking tuners on a call to `standardDeviation`. */
  implicit def rddStandardDeviationTuners[T <: Tuner : RDDDefaultTuners] = new StandardDeviationTuners[Context.U, T] { }

  /** Implicit for checking tuners on a call to `skewness`. */
  implicit def rddSkewnesTuners[T <: Tuner : RDDDefaultTuners] = new SkewnessTuners[Context.U, T] { }

  /** Implicit for checking tuners on a call to `kurtosis`. */
  implicit def rddKurtosisTuners[T <: Tuner : RDDDefaultTuners] = new KurtosisTuners[Context.U, T] { }

  /** Implicit for checking tuners on a call to `minimum`. */
  implicit def rddMinimumTuners[T <: Tuner : RDDDefaultTuners] = new MinimumTuners[Context.U, T] { }

  /** Implicit for checking tuners on a call to `maximum`. */
  implicit def rddMaximumTuners[T <: Tuner : RDDDefaultTuners] = new MaximumTuners[Context.U, T] { }

  /** Implicit for checking tuners on a call to `maximumAbsolute`. */
  implicit def rddMaximumAbsoluteTuners[T <: Tuner : RDDDefaultTuners] = new MaximumAbsoluteTuners[Context.U, T] { }

  /** Implicit for checking tuners on a call to `sums`. */
  implicit def rddSumsTuners[T <: Tuner : RDDDefaultTuners] = new SumsTuners[Context.U, T] { }

  // *** Matrix tuners

  /** Type alias for fillHeterogeneous tuners. */
  type RDDFillHeterogeneousTuners[T] = T In OneOf[Default[NoParameters]]# // Default[NoParam] x 3
    Or[Ternary[InMemory[NoParameters], InMemory[NoParameters], Default[NoParameters]]]#
    Or[Ternary[InMemory[NoParameters], Default[NoParameters], Default[NoParameters]]]#
    Or[Ternary[InMemory[NoParameters], InMemory[NoParameters], Default[Reducers]]]#
    Or[Ternary[InMemory[NoParameters], Default[NoParameters], Default[Reducers]]]#
    Or[Ternary[InMemory[NoParameters], Default[Reducers], Default[Reducers]]]#
    Or[Ternary[Default[NoParameters], Default[NoParameters], Default[Reducers]]]#
    Or[Ternary[Default[NoParameters], Default[Reducers], Default[Reducers]]]#
    Or[Ternary[Default[Reducers], Default[Reducers], Default[Reducers]]]

  /** Type alias for fillHomogeneous tuners. */
  type RDDFillHomogeneousTuners[T] = T In OneOf[Default[NoParameters]]# // Default[NoParameters] x 2
    Or[Binary[InMemory[NoParameters], Default[NoParameters]]]#
    Or[Binary[InMemory[NoParameters], Default[Reducers]]]#
    Or[Binary[InMemory[Reducers], Default[NoParameters]]]#
    Or[Binary[InMemory[Reducers], Default[Reducers]]]#
    Or[Binary[Default[NoParameters], Default[Reducers]]]#
    Or[Binary[Default[Reducers], Default[NoParameters]]]#
    Or[Binary[Default[Reducers], Default[Reducers]]]

  /** Type alias for join tuners. */
  type RDDJoinTuners[T] = T In OneOf[InMemory[NoParameters]]# // InMemory[NoParam], InMemory[NoParam]
    Or[Default[NoParameters]]#                                // Default[NoParam],  Default[NoParam]
    Or[Binary[InMemory[NoParameters], Default[NoParameters]]]#
    Or[Binary[InMemory[NoParameters], Default[Reducers]]]#
    Or[Binary[InMemory[Reducers], Default[Reducers]]]#
    Or[Binary[Default[NoParameters], Default[Reducers]]]#
    Or[Binary[Default[Reducers], Default[Reducers]]]

  /** Type alias for materialise tuners. */
  type RDDMaterialiseTuners[T] = T Is Default[NoParameters]

  /** Type alias for pairwise tuners. */
  type RDDPairwiseTuners[T] = T In OneOf[InMemory[NoParameters]]#
    Or[Default[NoParameters]]#
    Or[Ternary[InMemory[NoParameters], Default[Reducers], Default[Reducers]]]#
    Or[Ternary[Default[Reducers], Default[Reducers], Default[Reducers]]]

  /** Type alias for saveAsIV tuners. */
  type RDDSaveAsIVTuners[T] = T In OneOf[Default[NoParameters]]#  // Default[NoParam] x 2
    Or[Binary[InMemory[NoParameters], Default[NoParameters]]]#
    Or[Binary[InMemory[NoParameters], Redistribute]]#
    Or[Binary[Default[NoParameters], Redistribute]]#
    Or[Binary[Default[Reducers], Default[NoParameters]]]#
    Or[Binary[Default[Reducers], Redistribute]]

  /** Type alias for stream tuners. */
  type RDDStreamTuners[T] = T Is Default[Reducers]

  /** Type alias for saveAsCSV tuners. */
  type RDDSaveAsCSVTuners[T] = T In OneOf[Default[NoParameters]]# // Default[NoParam], Default[NoParam]
    Or[Default[Reducers]]#                                        // Default[Red],     Default[NoParam]
    Or[Redistribute]#                                             // Default[NoParam], Redistribute
    Or[Binary[Default[Reducers], Redistribute]]

  /** Type alias for saveAsVW tuners. */
  type RDDSaveAsVWTuners[T] = T In OneOf[Default[NoParameters]]# // Def[NoParam], Def[NoParam],   Def[NoParam]
    Or[Default[Reducers]]#                                       // Def[Red],     Def[Red],       Def[NoParam]
    Or[Binary[Default[Reducers], Redistribute]]#                 // Def[Red],     Def[Red],       Redis
    Or[Binary[Default[NoParameters], Redistribute]]#             // Def[NoParam], Def[NoParam],   Redis
    Or[Binary[Default[NoParameters], InMemory[NoParameters]]]#   // Def[NoParam], InMem[NoParam], Def[NoParam]
    Or[Binary[Default[NoParameters], Default[Reducers]]]#        // Def[NoParam], Def[Red],       Def[NoParam]
    Or[Ternary[Default[NoParameters], InMemory[NoParameters], Redistribute]]#
    Or[Ternary[Default[NoParameters], Default[Reducers], Redistribute]]#
    Or[Ternary[Default[Reducers], InMemory[NoParameters], Default[NoParameters]]]#
    Or[Ternary[Default[Reducers], InMemory[NoParameters], Redistribute]]

  /** Implicit for checking tuners on a call to `change`. */
  implicit def rddChangeTuners[T <: Tuner : RDDDefaultInMemoryTuners] = new ChangeTuners[Context.U, T] { }

  /** Implicit for checking tuners on a call to `compact`. */
  implicit def rddCompactTuners[T <: Tuner : RDDDefaultTuners] = new CompactTuners[Context.U, T] { }

  /** Implicit for checking tuners on a call to `domain`. */
  implicit def rddDomainTuners[T <: Tuner : RDDDefaultTuners] = new DomainTuners[Context.U, T] { }

  /** Implicit for checking tuners on a call to `fillHeterogeneous`. */
  implicit def rddFillHeterogeneousTuners[
    T <: Tuner : RDDFillHeterogeneousTuners
  ] = new FillHeterogeneousTuners[Context.U, T] { }

  /** Implicit for checking tuners on a call to `fillHomogeneous`. */
  implicit def rddFillHomogeneousTuners[
    T <: Tuner : RDDFillHomogeneousTuners
  ] = new FillHomogeneousTuners[Context.U, T] { }

  /** Implicit for checking tuners on a call to `get`. */
  implicit def rddGetTuners[T <: Tuner : RDDDefaultInMemoryTuners] = new GetTuners[Context.U, T] { }

  /** Implicit for checking tuners on a call to `join`. */
  implicit def rddJoinTuners[T <: Tuner : RDDJoinTuners] = new JoinTuners[Context.U, T] { }

  /** Implicit for checking tuners on a call to `materialise`. */
  implicit def rddMaterialiseTuners[T <: Tuner : RDDMaterialiseTuners] = new MaterialiseTuners[Context.U, T] { }

  /** Implicit for checking tuners on a call to `pairwise*`. */
  implicit def rddPairwiseTuners[T <: Tuner : RDDPairwiseTuners] = new PairwiseTuners[Context.U, T] { }

  /** Implicit for checking tuners on a call to `saveAsIV`. */
  implicit def rddSaveAsIVTuners[T <: Tuner : RDDSaveAsIVTuners] = new SaveAsIVTuners[Context.U, T] { }

  /** Implicit for checking tuners on a call to `set`. */
  implicit def rddSetTuners[T <: Tuner : RDDDefaultTuners] = new SetTuners[Context.U, T] { }

  /** Implicit for checking tuners on a call to `shape`. */
  implicit def rddShapeTuners[T <: Tuner: RDDDefaultTuners] = new ShapeTuners[Context.U, T] { }

  /** Implicit for checking tuners on a call to `size`. */
  implicit def rddSizeTuners[T <: Tuner : RDDDefaultTuners] = new SizeTuners[Context.U, T] { }

  /** Implicit for checking tuners on a call to `slice`. */
  implicit def rddSliceTuners[T <: Tuner : RDDDefaultInMemoryTuners] = new SliceTuners[Context.U, T] { }

  /** Implicit for checking tuners on a call to `slide`. */
  implicit def rddSlideTuners[T <: Tuner : RDDDefaultTuners] = new SlideTuners[Context.U, T] { }

  /** Implicit for checking tuners on a call to `stream`. */
  implicit def rddStreamTuners[T <: Tuner : RDDStreamTuners] = new StreamTuners[Context.U, T] { }

  /** Implicit for checking tuners on a call to `summmarise`. */
  implicit def rddSummariseTuners[T <: Tuner : RDDDefaultTuners] = new SummariseTuners[Context.U, T] { }

  /** Implicit for checking tuners on a call to `types`. */
  implicit def rddTypesTuners[T <: Tuner : RDDDefaultTuners] = new TypesTuners[Context.U, T] { }

  /** Implicit for checking tuners on a call to `unique`. */
  implicit def rddUniqueTuners[T <: Tuner : RDDDefaultTuners] = new UniqueTuners[Context.U, T] { }

  /** Implicit for checking tuners on a call to `which`. */
  implicit def rddWhichTuners[T <: Tuner : RDDDefaultInMemoryTuners] = new WhichTuners[Context.U, T] { }

  /** Implicit for checking tuners on a call to `squash`. */
  implicit def rddSquashTuners[T <: Tuner : RDDDefaultTuners] = new SquashTuners[Context.U, T] { }

  /** Implicit for checking tuners on a call to `reshape`. */
  implicit def rddReshapeTuners[T <: Tuner : RDDDefaultInMemoryTuners] = new ReshapeTuners[Context.U, T] { }

  /** Implicit for checking tuners on a call to `saveAsCSV`. */
  implicit def rddSaveAsCSVTuners[T <: Tuner : RDDSaveAsCSVTuners] = new SaveAsCSVTuners[Context.U, T] { }

  /** Implicit for checking tuners on a call to `saveAsVW`. */
  implicit def rddSaveAsVWTuners[T <: Tuner : RDDSaveAsVWTuners] = new SaveAsVWTuners[Context.U, T] { }

  /** Converts a `Cell[P]` into a `RDD[Cell[P]]`. */
  implicit def cellToRDD[
    P <: Nat
  ](
    t: Cell[P]
  )(implicit
    ctx: Context
  ): Context.U[Cell[P]] = ctx.implicits.cell.cellToU(t)

  /** Converts a `List[Cell[P]]` into a `RDD[Cell[P]]`. */
  implicit def listCellToRDD[
    P <: Nat
  ](
    t: List[Cell[P]]
  )(implicit
    ctx: Context
  ): Context.U[Cell[P]] = ctx.implicits.cell.listCellToU(t)

  /** Converts a `RDD[Content]` to a `Contents`. */
  implicit def rddToContents(
    data: Context.U[Content]
  )(implicit
    ctx: Context
  ): Contents = ctx.implicits.content.toContents(data)

  /** Converts a `RDD[(Position[P], Content)]` to a `IndexedContents`. */
  implicit def rddToIndexed[
    P <: Nat
  ](
    data: Context.U[(Position[P], Content)]
  )(implicit
    ctx: Context
  ): IndexedContents[P] = ctx.implicits.content.toIndexed(data)

  /** Conversion from `RDD[Cell[_1]]` to a `Matrix1D`. */
  implicit def rddToMatrix1(
    data: Context.U[Cell[_1]]
  )(implicit
    ctx: Context
  ): Matrix1D = ctx.implicits.matrix.toMatrix1D(data)

  /** Conversion from `RDD[Cell[_2]]` to a `Matrix2D`. */
  implicit def rddToMatrix2(
    data: Context.U[Cell[_2]]
  )(implicit
    ctx: Context
  ): Matrix2D = ctx.implicits.matrix.toMatrix2D(data)

  /** Conversion from `RDD[Cell[_3]]` to a `Matrix3D`. */
  implicit def rddToMatrix3(
    data: Context.U[Cell[_3]]
  )(implicit
    ctx: Context
  ): Matrix3D = ctx.implicits.matrix.toMatrix3D(data)

  /** Conversion from `RDD[Cell[_4]]` to a `Matrix4D`. */
  implicit def rddToMatrix4(
    data: Context.U[Cell[_4]]
  )(implicit
    ctx: Context
  ): Matrix4D = ctx.implicits.matrix.toMatrix4D(data)

  /** Conversion from `RDD[Cell[_5]]` to a `Matrix5D`. */
  implicit def rddToMatrix5(
    data: Context.U[Cell[_5]]
  )(implicit
    ctx: Context
  ): Matrix5D = ctx.implicits.matrix.toMatrix5D(data)

  /** Conversion from `RDD[Cell[_6]]` to a `Matrix6D`. */
  implicit def rddToMatrix6(
    data: Context.U[Cell[_6]]
  )(implicit
    ctx: Context
  ): Matrix6D = ctx.implicits.matrix.toMatrix6D(data)

  /** Conversion from `RDD[Cell[_7]]` to a `Matrix7D`. */
  implicit def rddToMatrix7(
    data: Context.U[Cell[_7]]
  )(implicit
    ctx: Context
  ): Matrix7D = ctx.implicits.matrix.toMatrix7D(data)

  /** Conversion from `RDD[Cell[_8]]` to a `Matrix8D`. */
  implicit def rddToMatrix8(
    data: Context.U[Cell[_8]]
  )(implicit
    ctx: Context
  ): Matrix8D = ctx.implicits.matrix.toMatrix8D(data)

  /** Conversion from `RDD[Cell[_9]]` to a `Matrix9D`. */
  implicit def rddToMatrix9(
    data: Context.U[Cell[_9]]
  )(implicit
    ctx: Context
  ): Matrix9D = ctx.implicits.matrix.toMatrix9D(data)

  /** Conversion from `List[Cell[_1]]` to a `Matrix1D`. */
  implicit def listToRDDMatrix1(
    data: List[Cell[_1]]
  )(implicit
    ctx: Context
  ): Matrix1D = ctx.implicits.matrix.listToMatrix1D(data)

  /** Conversion from `List[Cell[_2]]` to a `Matrix2D`. */
  implicit def listToRDDMatrix2(
    data: List[Cell[_2]]
  )(implicit
    ctx: Context
  ): Matrix2D = ctx.implicits.matrix.listToMatrix2D(data)

  /** Conversion from `List[Cell[_3]]` to a `Matrix3D`. */
  implicit def listToRDDMatrix3(
    data: List[Cell[_3]]
  )(implicit
    ctx: Context
  ): Matrix3D = ctx.implicits.matrix.listToMatrix3D(data)

  /** Conversion from `List[Cell[_4]]` to a `Matrix4D`. */
  implicit def listToRDDMatrix4(
    data: List[Cell[_4]]
  )(implicit
    ctx: Context
  ): Matrix4D = ctx.implicits.matrix.listToMatrix4D(data)

  /** Conversion from `List[Cell[_5]]` to a `Matrix5D`. */
  implicit def listToRDDMatrix5(
    data: List[Cell[_5]]
  )(implicit
    ctx: Context
  ): Matrix5D = ctx.implicits.matrix.listToMatrix5D(data)

  /** Conversion from `List[Cell[_6]]` to a `Matrix6D`. */
  implicit def listToRDDMatrix6(
    data: List[Cell[_6]]
  )(implicit
    ctx: Context
  ): Matrix6D = ctx.implicits.matrix.listToMatrix6D(data)

  /** Conversion from `List[Cell[_7]]` to a `Matrix7D`. */
  implicit def listToRDDMatrix7(
    data: List[Cell[_7]]
  )(implicit
    ctx: Context
  ): Matrix7D = ctx.implicits.matrix.listToMatrix7D(data)

  /** Conversion from `List[Cell[_8]]` to a `Matrix8D`. */
  implicit def listToRDDMatrix8(
    data: List[Cell[_8]]
  )(implicit
    ctx: Context
  ): Matrix8D = ctx.implicits.matrix.listToMatrix8D(data)

  /** Conversion from `List[Cell[_9]]` to a `Matrix9D`. */
  implicit def listToRDDMatrix9(
    data: List[Cell[_9]]
  )(implicit
    ctx: Context
  ): Matrix9D = ctx.implicits.matrix.listToMatrix9D(data)

  /** Conversion from `List[(Value, Content)]` to a `Matrix1D`. */
  implicit def tupleToRDDMatrix1[
    V <% Value
  ](
    list: List[(V, Content)]
  )(implicit
    ctx: Context
  ): Matrix1D = ctx.implicits.matrix.tupleToMatrix1D(list)

  /** Conversion from `List[(Value, Value, Content)]` to a `Matrix2D`. */
  implicit def tupleToRDDMatrix2[
    V <% Value,
    W <% Value
  ](
    list: List[(V, W, Content)]
  )(implicit
    ctx: Context
  ): Matrix2D = ctx.implicits.matrix.tupleToMatrix2D(list)

  /** Conversion from `List[(Value, Value, Value, Content)]` to a `Matrix3D`. */
  implicit def tupleToRDDMatrix3[
    V <% Value,
    W <% Value,
    X <% Value
  ](
    list: List[(V, W, X, Content)]
  )(implicit
    ctx: Context
  ): Matrix3D = ctx.implicits.matrix.tupleToMatrix3D(list)

  /** Conversion from `List[(Value, Value, Value, Value, Content)]` to a `Matrix4D`. */
  implicit def tupleToRDDMatrix4[
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
  implicit def tupleToRDDMatrix5[
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
  implicit def tupleToRDDMatrix6[
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
  implicit def tupleToRDDMatrix7[
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
  implicit def tupleToRDDMatrix8[
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
  implicit def tupleToRDDMatrix9[
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
  implicit def tupleToRDDParseErrors[
    P <: Nat
  ](
    t: (Context.U[Cell[P]], Context.U[String])
  )(implicit
    ctx: Context
  ): MatrixWithParseErrors[P, Context.U] = ctx.implicits.matrix.tupleToParseErrors(t)

  /** Conversion from `RDD[(I, Cell[P])]` to a `Partitions`. */
  implicit def rddToPartitions[
    P <: Nat,
    I : Ordering
  ](
    data: Context.U[(I, Cell[P])]
  )(implicit
    ctx: Context
  ): Partitions[P, I] = ctx.implicits.partition.toPartitions(data)

  /** Converts a `Value` to a `RDD[Position[_1]]`. */
  implicit def valueToRDD[
    T <% Value
  ](
    t: T
  )(implicit
    ctx: Context
  ): Context.U[Position[_1]] = ctx.implicits.position.valueToU(t)

  /** Converts a `List[Value]` to a `RDD[Position[_1]]`. */
  implicit def listValueToRDD[
    T <% Value
  ](
    t: List[T]
  )(implicit
    ctx: Context
  ): Context.U[Position[_1]] = ctx.implicits.position.listValueToU(t)

  /** Converts a `Position[T]` to a `RDD[Position[T]]`. */
  implicit def positionToRDD[
    T <: Nat
  ](
    t: Position[T]
  )(implicit
    ctx: Context
  ): Context.U[Position[T]] = ctx.implicits.position.positionToU(t)

  /** Converts a `List[Position[T]]` to a `RDD[Position[T]]`. */
  implicit def listPositionToRDD[
    T <: Nat
  ](
    t: List[Position[T]]
  )(implicit
    ctx: Context
  ): Context.U[Position[T]] = ctx.implicits.position.listPositionToU(t)

  /** Converts a `RDD[Position[P]]` to a `Positions`. */
  implicit def rddToPositions[
    L <: Nat,
    P <: Nat
  ](
    data: Context.U[Position[P]]
  )(implicit
    ctx: Context,
    ev: Diff.Aux[P, _1, L]
  ): Positions[L, P] = ctx.implicits.position.toPositions(data)

  /** Converts a `(T, Cell.Predicate[P])` to a `List[(RDD[Position[S]], Cell.Predicate[P])]`. */
  implicit def predicateToRDDList[
    P <: Nat,
    S <: Nat,
    T <% Context.U[Position[S]]
  ](
    t: (T, Cell.Predicate[P])
  )(implicit
    ctx: Context
  ): List[(Context.U[Position[S]], Cell.Predicate[P])] = ctx.implicits.position.predicateToU(t)

  /**
   * Converts a `List[(T, Cell.Predicate[P])]` to a `List[(RDD[Position[S]], Cell.Predicate[P])]`.
   */
  implicit def listPredicateToScaldingList[
    P <: Nat,
    S <: Nat,
    T <% Context.U[Position[S]]
  ](
    t: List[(T, Cell.Predicate[P])]
  )(implicit
    ctx: Context
  ): List[(Context.U[Position[S]], Cell.Predicate[P])] = ctx.implicits.position.listPredicateToU(t)

  /** Converts a `RDD[String]` to a `SaveStringsAsText`. */
  implicit def savePipeStringsAsText(
    data: Context.U[String]
  )(implicit
    ctx: Context
  ): SaveStringsAsText = ctx.implicits.environment.saveStringsAsText(data)
}

