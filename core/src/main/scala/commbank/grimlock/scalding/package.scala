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
import commbank.grimlock.framework.distance.PairwiseDistance.{ CorrelationTuner, MutualInformationTuner }
import commbank.grimlock.framework.distribution.ApproximateDistribution.{
  CountMapQuantilesTuner,
  HistogramTuner,
  QuantilesTuner,
  TDigestQuantilesTuner,
  UniformQuantilesTuner
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
  Unbalanced
}
import commbank.grimlock.framework.Matrix.{
  ChangeTuner,
  CompactTuner,
  DomainTuner,
  FillHeterogeneousTuner,
  FillHomogeneousTuner,
  GetTuner,
  JoinTuner,
  PairwiseTuner,
  ReshapeTuner,
  SaveAsCSVTuner,
  SaveAsIVTuner,
  SaveAsVWTuner,
  ShapeTuner,
  SetTuner,
  SizeTuner,
  SliceTuner,
  SlideTuner,
  SquashTuner,
  SummariseTuner,
  TypesTuner,
  UniqueTuner,
  WhichTuner
}
import commbank.grimlock.framework.partition.Partitions.{ ForAllTuner, IdsTuner }
import commbank.grimlock.framework.Persist.SaveAsTextTuner
import commbank.grimlock.framework.position.Position
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

import commbank.grimlock.scalding.content.{ Contents, IndexedContents }
import commbank.grimlock.scalding.partition.Partitions
import commbank.grimlock.scalding.position.Positions

import shapeless.Nat
import shapeless.nat.{ _1, _2, _3, _4, _5, _6, _7, _8, _9 }
import shapeless.ops.nat.GT

package object environment {
  // *** Matrix/Position shared tuners

  /** Implicits for checking tuners on a call to `names`. */
  implicit def pipeNamesTunerDn = new NamesTuner[Context.U, Default[NoParameters]] { }
  implicit def pipeNamesTunerDr = new NamesTuner[Context.U, Default[Reducers]] { }

  //  *** Persist tuners

  /** Implicits for checking tuners on a call to `saveAstext`. */
  implicit def pipeSaveAsTextTunerDn = new SaveAsTextTuner[Context.U, Default[NoParameters]] { }
  implicit def pipeSaveAsTextTunerRr = new SaveAsTextTuner[Context.U, Redistribute] { }

  // *** Pairwise distance tuners

  /** Implicits for checking tuners on a call to `corrrelation`. */
  implicit def pipeCorrelationTunerDnInDn = new CorrelationTuner[Context.U, InMemory[NoParameters]] { }
  implicit def pipeCorrelationTunerDnDnDn = new CorrelationTuner[Context.U, Default[NoParameters]] { }
  implicit def pipeCorrelationTunerDrInDn = new CorrelationTuner[
    Context.U,
    Ternary[Default[Reducers], InMemory[NoParameters], Default[NoParameters]]
  ] { }
  implicit def pipeCorrelationTunerDrInDr = new CorrelationTuner[
    Context.U,
    Ternary[Default[Reducers], InMemory[NoParameters], Default[Reducers]]
  ] { }
  implicit def pipeCorrelationTunerDrInUr = new CorrelationTuner[
    Context.U,
    Ternary[Default[Reducers], InMemory[NoParameters], Unbalanced[Reducers]]
  ] { }
  implicit def pipeCorrelationTunerDrIrDr = new CorrelationTuner[
    Context.U,
    Ternary[Default[Reducers], InMemory[Reducers], Default[Reducers]]
  ] { }
  implicit def pipeCorrelationTunerDrIrUr = new CorrelationTuner[
    Context.U,
    Ternary[Default[Reducers], InMemory[Reducers], Unbalanced[Reducers]]
  ] { }
  implicit def pipeCorrelationTunerDrDnDn = new CorrelationTuner[
    Context.U,
    Ternary[Default[Reducers], Default[NoParameters], Default[NoParameters]]
  ] { }
  implicit def pipeCorrelationTunerDrDnDr = new CorrelationTuner[
    Context.U,
    Ternary[Default[Reducers], Default[NoParameters], Default[Reducers]]
  ] { }
  implicit def pipeCorrelationTunerDrDnUr = new CorrelationTuner[
    Context.U,
    Ternary[Default[Reducers], Default[NoParameters], Unbalanced[Reducers]]
  ] { }
  implicit def pipeCorrelationTunerDrDrDr = new CorrelationTuner[
    Context.U,
    Ternary[Default[Reducers], Default[Reducers], Default[Reducers]]
  ] { }
  implicit def pipeCorrelationTunerDrDrUr = new CorrelationTuner[
    Context.U,
    Ternary[Default[Reducers], Default[Reducers], Unbalanced[Reducers]]
  ] { }
  implicit def pipeCorrelationTunerDrUrDr = new CorrelationTuner[
    Context.U,
    Ternary[Default[Reducers], Unbalanced[Reducers], Default[Reducers]]
  ] { }
  implicit def pipeCorrelationTunerDrUrUr = new CorrelationTuner[
    Context.U,
    Ternary[Default[Reducers], Unbalanced[Reducers], Unbalanced[Reducers]]
  ] { }

  /** Implicits for checking tuners on a call to `mutualInformation`. */
  implicit def pipeMutualInformationTunerDnInDn = new MutualInformationTuner[Context.U, InMemory[NoParameters]] { }
  implicit def pipeMutualInformationTunerDnDnDn = new MutualInformationTuner[Context.U, Default[NoParameters]] { }
  implicit def pipeMutualInformationTunerDrInDn = new MutualInformationTuner[
    Context.U,
    Ternary[Default[Reducers], InMemory[NoParameters], Default[NoParameters]]
  ] { }
  implicit def pipeMutualInformationTunerDrInDr = new MutualInformationTuner[
    Context.U,
    Ternary[Default[Reducers], InMemory[NoParameters], Default[Reducers]]
  ] { }
  implicit def pipeMutualInformationTunerDrInUr = new MutualInformationTuner[
    Context.U,
    Ternary[Default[Reducers], InMemory[NoParameters], Unbalanced[Reducers]]
  ] { }
  implicit def pipeMutualInformationTunerDrIrDr = new MutualInformationTuner[
    Context.U,
    Ternary[Default[Reducers], InMemory[Reducers], Default[Reducers]]
  ] { }
  implicit def pipeMutualInformationTunerDrIrUr = new MutualInformationTuner[
    Context.U,
    Ternary[Default[Reducers], InMemory[Reducers], Unbalanced[Reducers]]
  ] { }
  implicit def pipeMutualInformationTunerDrDnDn = new MutualInformationTuner[
    Context.U,
    Ternary[Default[Reducers], Default[NoParameters], Default[NoParameters]]
  ] { }
  implicit def pipeMutualInformationTunerDrDnDr = new MutualInformationTuner[
    Context.U,
    Ternary[Default[Reducers], Default[NoParameters], Default[Reducers]]
  ] { }
  implicit def pipeMutualInformationTunerDrDnUr = new MutualInformationTuner[
    Context.U,
    Ternary[Default[Reducers], Default[NoParameters], Unbalanced[Reducers]]
  ] { }
  implicit def pipeMutualInformationTunerDrDrDr = new MutualInformationTuner[
    Context.U,
    Ternary[Default[Reducers], Default[Reducers], Default[Reducers]]
  ] { }
  implicit def pipeMutualInformationTunerDrDrUr = new MutualInformationTuner[
    Context.U,
    Ternary[Default[Reducers], Default[Reducers], Unbalanced[Reducers]]
  ] { }
  implicit def pipeMutualInformationTunerDrUrDr = new MutualInformationTuner[
    Context.U,
    Ternary[Default[Reducers], Unbalanced[Reducers], Default[Reducers]]
  ] { }
  implicit def pipeMutualInformationTunerDrUrUr = new MutualInformationTuner[
    Context.U,
    Ternary[Default[Reducers], Unbalanced[Reducers], Unbalanced[Reducers]]
  ] { }

  // *** Distribution tuners

  /** Implicits for checking tuners on a call to `histogram`. */
  implicit def pipeHistogramTunerDn = new HistogramTuner[Context.U, Default[NoParameters]] { }
  implicit def pipeHistogramTunerDr = new HistogramTuner[Context.U, Default[Reducers]] { }

  /** Implicits for checking tuners on a call to `quantiles`. */
  implicit def pipeQuantilesTunerIn = new QuantilesTuner[Context.U, InMemory[NoParameters]] { }
  implicit def pipeQuantilesTunerIr = new QuantilesTuner[Context.U, InMemory[Reducers]] { }
  implicit def pipeQuantilesTunerDn = new QuantilesTuner[Context.U, Default[NoParameters]] { }
  implicit def pipeQuantilesTunerDr = new QuantilesTuner[Context.U, Default[Reducers]] { }
  implicit def pipeQuantilesTunerUr = new QuantilesTuner[Context.U, Unbalanced[Reducers]] { }

  /** Implicits for checking tuners on a call to `countMapQuantiles`. */
  implicit def pipeCountMapQuantilesTunerDn = new CountMapQuantilesTuner[Context.U, Default[NoParameters]] { }
  implicit def pipeCountMapQuantilesTunerDr = new CountMapQuantilesTuner[Context.U, Default[Reducers]] { }

  /** Implicits for checking tuners on a call to `tDigestQuantiles`. */
  implicit def pipeTDigestQuantilesTunerDn = new TDigestQuantilesTuner[Context.U, Default[NoParameters]] { }
  implicit def pipeTDigestQuantilesTunerDr = new TDigestQuantilesTuner[Context.U, Default[Reducers]] { }

  /** Implicits for checking tuners on a call to `uniformQuantiles`. */
  implicit def pipeUniformQuantilesTunerDn = new UniformQuantilesTuner[Context.U, Default[NoParameters]] { }
  implicit def pipeUniformQuantilesTunerDr = new UniformQuantilesTuner[Context.U, Default[Reducers]] { }

  // *** Partition tuners

  /** Implicits for checking tuners on a call to `forAll`. */
  implicit def pipeForAllTunerDn = new ForAllTuner[Context.U, Default[NoParameters]] { }
  implicit def pipeForAllTunerDr = new ForAllTuner[Context.U, Default[Reducers]] { }

  /** Implicits for checking tuners on a call to `ids`. */
  implicit def pipeIdsTunerDn = new IdsTuner[Context.U, Default[NoParameters]] { }
  implicit def pipeIdsTunerDr = new IdsTuner[Context.U, Default[Reducers]] { }

  // *** Statistics tuners

  /** Implicits for checking tuners on a call to `counts`. */
  implicit def pipeCountsTunerDn = new CountsTuner[Context.U, Default[NoParameters]] { }
  implicit def pipeCountsTunerDr = new CountsTuner[Context.U, Default[Reducers]] { }

  /** Implicits for checking tuners on a call to `distinctCounts`. */
  implicit def pipeDistinctCountsTunerDn = new DistinctCountsTuner[Context.U, Default[NoParameters]] { }
  implicit def pipeDistinctCountsTunerDr = new DistinctCountsTuner[Context.U, Default[Reducers]] { }

  /** Implicits for checking tuners on a call to `predicateCounts`. */
  implicit def pipePredicateCountsTunerDn = new PredicateCountsTuner[Context.U, Default[NoParameters]] { }
  implicit def pipePredicateCountsTunerDr = new PredicateCountsTuner[Context.U, Default[Reducers]] { }

  /** Implicits for checking tuners on a call to `mean`. */
  implicit def pipeMeanTunerDn = new MeanTuner[Context.U, Default[NoParameters]] { }
  implicit def pipeMeanTunerDr = new MeanTuner[Context.U, Default[Reducers]] { }

  /** Implicits for checking tuners on a call to `standardDeviation`. */
  implicit def pipeStandardDeviationTunerDn = new StandardDeviationTuner[Context.U, Default[NoParameters]] { }
  implicit def pipeStandardDeviationTunerDr = new StandardDeviationTuner[Context.U, Default[Reducers]] { }

  /** Implicits for checking tuners on a call to `skewness`. */
  implicit def pipeSkewnessTunerDn = new SkewnessTuner[Context.U, Default[NoParameters]] { }
  implicit def pipeSkewnessTunerDr = new SkewnessTuner[Context.U, Default[Reducers]] { }

  /** Implicits for checking tuners on a call to `kurtosis`. */
  implicit def pipeKurtosisTunerDn = new KurtosisTuner[Context.U, Default[NoParameters]] { }
  implicit def pipeKurtosisTunerDr = new KurtosisTuner[Context.U, Default[Reducers]] { }

  /** Implicits for checking tuners on a call to `minimum`. */
  implicit def pipeMinimumTunerDn = new MinimumTuner[Context.U, Default[NoParameters]] { }
  implicit def pipeMinimumTunerDr = new MinimumTuner[Context.U, Default[Reducers]] { }

  /** Implicits for checking tuners on a call to `maximum`. */
  implicit def pipeMaximumTunerDn = new MaximumTuner[Context.U, Default[NoParameters]] { }
  implicit def pipeMaximumTunerDr = new MaximumTuner[Context.U, Default[Reducers]] { }

  /** Implicits for checking tuners on a call to `maximumAbsolute`. */
  implicit def pipeMaximumAbsoluteTunerDn = new MaximumAbsoluteTuner[Context.U, Default[NoParameters]] { }
  implicit def pipeMaximumAbsoluteTunerDr = new MaximumAbsoluteTuner[Context.U, Default[Reducers]] { }

  /** Implicits for checking tuners on a call to `sums`. */
  implicit def pipeSumsTunerDn = new SumsTuner[Context.U, Default[NoParameters]] { }
  implicit def pipeSumsTunerDr = new SumsTuner[Context.U, Default[Reducers]] { }

  // *** Matrix tuners

  /** Implicits for checking tuners on a call to `change`. */
  implicit def pipeChangeTunerIn = new ChangeTuner[Context.U, InMemory[NoParameters]] { }
  implicit def pipeChangeTunerDn = new ChangeTuner[Context.U, Default[NoParameters]] { }
  implicit def pipeChangeTunerDr = new ChangeTuner[Context.U, Default[Reducers]] { }
  implicit def pipeChangeTunerUr = new ChangeTuner[Context.U, Unbalanced[Reducers]] { }

  /** Implicits for checking tuners on a call to `compact`. */
  implicit def pipeCompactTunerDn = new CompactTuner[Context.U, Default[NoParameters]] { }
  implicit def pipeCompactTunerDr = new CompactTuner[Context.U, Default[Reducers]] { }

  /** Implicits for checking tuners on a call to `domain`. */
  implicit def pipeDomainTunerIn = new DomainTuner[Context.U, InMemory[NoParameters]] { }
  implicit def pipeDomainTunerIr = new DomainTuner[Context.U, InMemory[Reducers]] { }
  implicit def pipeDomainTunerDn = new DomainTuner[Context.U, Default[NoParameters]] { }
  implicit def pipeDomainTunerDr = new DomainTuner[Context.U, Default[Reducers]] { }

  /** Implicits for checking tuners on a call to `fillHeterogeneous`. */
  implicit def pipeFillHeterogeneousTunerDnDnDn = new FillHeterogeneousTuner[Context.U, Default[NoParameters]] { }
  implicit def pipeFillHeterogeneousTunerInInDn = new FillHeterogeneousTuner[
    Context.U,
    Ternary[InMemory[NoParameters], InMemory[NoParameters], Default[NoParameters]]
  ] { }
  implicit def pipeFillHeterogeneousTunerInDnDn = new FillHeterogeneousTuner[
    Context.U,
    Ternary[InMemory[NoParameters], Default[NoParameters], Default[NoParameters]]
  ] { }
  implicit def pipeFillHeterogeneousTunerInInDr = new FillHeterogeneousTuner[
    Context.U,
    Ternary[InMemory[NoParameters], InMemory[NoParameters], Default[Reducers]]
  ] { }
  implicit def pipeFillHeterogeneousTunerInDnDr = new FillHeterogeneousTuner[
    Context.U,
    Ternary[InMemory[NoParameters], Default[NoParameters], Default[Reducers]]
  ] { }
  implicit def pipeFillHeterogeneousTunerInDrDr = new FillHeterogeneousTuner[
    Context.U,
    Ternary[InMemory[NoParameters], Default[Reducers], Default[Reducers]]
  ] { }
  implicit def pipeFillHeterogeneousTunerDnDnDr = new FillHeterogeneousTuner[
    Context.U,
    Ternary[Default[NoParameters], Default[NoParameters], Default[Reducers]]
  ] { }
  implicit def pipeFillHeterogeneousTunerDnDrDr = new FillHeterogeneousTuner[
    Context.U,
    Ternary[Default[NoParameters], Default[Reducers], Default[Reducers]]
  ] { }
  implicit def pipeFillHeterogeneousTunerDrDrDr = new FillHeterogeneousTuner[
    Context.U,
    Ternary[Default[Reducers], Default[Reducers], Default[Reducers]]
  ] { }

  /** Implicits for checking tuners on a call to `fillHomogeneous`. */
  implicit def pipeFillHomogeneousTunerDnDn = new FillHomogeneousTuner[Context.U, Default[NoParameters]] { }
  implicit def pipeFillHomogeneousTunerInDn = new FillHomogeneousTuner[
    Context.U,
    Binary[InMemory[NoParameters], Default[NoParameters]]
  ] { }
  implicit def pipeFillHomogeneousTunerInDr = new FillHomogeneousTuner[
    Context.U,
    Binary[InMemory[NoParameters], Default[Reducers]]
  ] { }
  implicit def pipeFillHomogeneousTunerIrDn = new FillHomogeneousTuner[
    Context.U,
    Binary[InMemory[Reducers], Default[NoParameters]]
  ] { }
  implicit def pipeFillHomogeneousTunerIrDr = new FillHomogeneousTuner[
    Context.U,
    Binary[InMemory[Reducers], Default[Reducers]]
  ] { }
  implicit def pipeFillHomogeneousTunerDnDr = new FillHomogeneousTuner[
    Context.U,
    Binary[Default[NoParameters], Default[Reducers]]
  ] { }
  implicit def pipeFillHomogeneousTunerDrDn = new FillHomogeneousTuner[
    Context.U,
    Binary[Default[Reducers], Default[NoParameters]]
  ] { }
  implicit def pipeFillHomogeneousTunerDrDr = new FillHomogeneousTuner[
    Context.U,
    Binary[Default[Reducers], Default[Reducers]]
  ] { }

  /** Implicits for checking tuners on a call to `get`. */
  implicit def pipeGetTunerIn = new GetTuner[Context.U, InMemory[NoParameters]] { }
  implicit def pipeGetTunerDn = new GetTuner[Context.U, Default[NoParameters]] { }
  implicit def pipeGetTunerDr = new GetTuner[Context.U, Default[Reducers]] { }
  implicit def pipeGetTunerUr = new GetTuner[Context.U, Unbalanced[Reducers]] { }

  /** Implicits for checking tuners on a call to `join`. */
  implicit def pipeJoinTunerInIn = new JoinTuner[Context.U, InMemory[NoParameters]] { }
  implicit def pipeJoinTunerDnDn = new JoinTuner[Context.U, Default[NoParameters]] { }
  implicit def pipeJoinTunerInDn = new JoinTuner[Context.U, Binary[InMemory[NoParameters], Default[NoParameters]]] { }
  implicit def pipeJoinTunerInDr = new JoinTuner[Context.U, Binary[InMemory[NoParameters], Default[Reducers]]] { }
  implicit def pipeJoinTunerInUr = new JoinTuner[Context.U, Binary[InMemory[NoParameters], Unbalanced[Reducers]]] { }
  implicit def pipeJoinTunerIrDr = new JoinTuner[Context.U, Binary[InMemory[Reducers], Default[Reducers]]] { }
  implicit def pipeJoinTunerIrUr = new JoinTuner[Context.U, Binary[InMemory[Reducers], Unbalanced[Reducers]]] { }
  implicit def pipeJoinTunerDnDr = new JoinTuner[Context.U, Binary[Default[NoParameters], Default[Reducers]]] { }
  implicit def pipeJoinTunerDnUr = new JoinTuner[Context.U, Binary[Default[NoParameters], Unbalanced[Reducers]]] { }
  implicit def pipeJoinTunerDrDr = new JoinTuner[Context.U, Binary[Default[Reducers], Default[Reducers]]] { }
  implicit def pipeJoinTunerDrUr = new JoinTuner[Context.U, Binary[Default[Reducers], Unbalanced[Reducers]]] { }

  /** Implicits for checking tuners on a call to `pairwise*`. */
  implicit def pipePairwiseTunerIn = new PairwiseTuner[Context.U, InMemory[NoParameters]] { }
  implicit def pipePairwiseTunerDnDnDn = new PairwiseTuner[Context.U, Default[NoParameters]] { }
  implicit def pipePairwiseTunerInDrDr = new PairwiseTuner[
    Context.U,
    Ternary[InMemory[NoParameters], Default[Reducers], Default[Reducers]]
  ] { }
  implicit def pipePairwiseTunerInDrUr = new PairwiseTuner[
    Context.U,
    Ternary[InMemory[NoParameters], Default[Reducers], Unbalanced[Reducers]]
  ] { }
  implicit def pipePairwiseTunerInUrDr = new PairwiseTuner[
    Context.U,
    Ternary[InMemory[NoParameters], Unbalanced[Reducers], Default[Reducers]]
  ] { }
  implicit def pipePairwiseTunerInUrUr = new PairwiseTuner[
    Context.U,
    Ternary[InMemory[NoParameters], Unbalanced[Reducers], Unbalanced[Reducers]]
  ] { }
  implicit def pipePairwiseTunerDrDrDr = new PairwiseTuner[
    Context.U,
    Ternary[Default[Reducers], Default[Reducers], Default[Reducers]]
  ] { }
  implicit def pipePairwiseTunerDrDrUr = new PairwiseTuner[
    Context.U,
    Ternary[Default[Reducers], Default[Reducers], Unbalanced[Reducers]]
  ] { }
  implicit def pipePairwiseTunerDrUrDr = new PairwiseTuner[
    Context.U,
    Ternary[Default[Reducers], Unbalanced[Reducers], Default[Reducers]]
  ] { }
  implicit def pipePairwiseTunerDrUrUr = new PairwiseTuner[
    Context.U,
    Ternary[Default[Reducers], Unbalanced[Reducers], Unbalanced[Reducers]]
  ] { }

  /** Implicits for checking tuners on a call to `saveAsIV`. */
  implicit def pipeSaveAsIVTunerDnDn = new SaveAsIVTuner[Context.U, Default[NoParameters]] { }
  implicit def pipeSaveAsIVTunerInDn = new SaveAsIVTuner[
    Context.U,
    Binary[InMemory[NoParameters], Default[NoParameters]]
  ] { }
  implicit def pipeSaveAsIVTunerInRr = new SaveAsIVTuner[Context.U, Binary[InMemory[NoParameters], Redistribute]] { }
  implicit def pipeSaveAsIVTunerDnRr = new SaveAsIVTuner[Context.U, Binary[Default[NoParameters], Redistribute]] { }
  implicit def pipeSaveAsIVTunerDrDn = new SaveAsIVTuner[
    Context.U,
    Binary[Default[Reducers], Default[NoParameters]]
  ] { }
  implicit def pipeSaveAsIVTunerDrRr = new SaveAsIVTuner[Context.U, Binary[Default[Reducers], Redistribute]] { }
  implicit def pipeSaveAsIVTunerUrDn = new SaveAsIVTuner[
    Context.U,
    Binary[Unbalanced[Reducers], Default[NoParameters]]
  ] { }
  implicit def pipeSaveAsIVTunerUrRr = new SaveAsIVTuner[Context.U, Binary[Unbalanced[Reducers], Redistribute]] { }

  /** Implicits for checking tuners on a call to `set`. */
  implicit def pipeSetTunerDn = new SetTuner[Context.U, Default[NoParameters]] { }
  implicit def pipeSetTunerDr = new SetTuner[Context.U, Default[Reducers]] { }

  /** Implicits for checking tuners on a call to `shape`. */
  implicit def pipeShapeTunerDn = new ShapeTuner[Context.U, Default[NoParameters]] { }
  implicit def pipeShapeTunerDr = new ShapeTuner[Context.U, Default[Reducers]] { }

  /** Implicits for checking tuners on a call to `size`. */
  implicit def pipeSizeTunerDn = new SizeTuner[Context.U, Default[NoParameters]] { }
  implicit def pipeSizeTunerDr = new SizeTuner[Context.U, Default[Reducers]] { }

  /** Implicits for checking tuners on a call to `slice`. */
  implicit def pipeSliceTunerIn = new SliceTuner[Context.U, InMemory[NoParameters]] { }
  implicit def pipeSliceTunerDn = new SliceTuner[Context.U, Default[NoParameters]] { }
  implicit def pipeSliceTunerDr = new SliceTuner[Context.U, Default[Reducers]] { }
  implicit def pipeSliceTunerUr = new SliceTuner[Context.U, Unbalanced[Reducers]] { }

  /** Implicits for checking tuners on a call to `slide`. */
  implicit def pipeSlideTunerDn = new SlideTuner[Context.U, Default[NoParameters]] { }
  implicit def pipeSlideTunerDr = new SlideTuner[Context.U, Default[Reducers]] { }
  implicit def pipeSlideTunerRr = new SlideTuner[Context.U, Redistribute] { }

  /** Implicits for checking tuners on a call to `summmarise`. */
  implicit def pipeSummariseTunerDn = new SummariseTuner[Context.U, Default[NoParameters]] { }
  implicit def pipeSummariseTunerDr = new SummariseTuner[Context.U, Default[Reducers]] { }

  /** Implicits for checking tuners on a call to `types`. */
  implicit def pipeTypesTunerDn = new TypesTuner[Context.U, Default[NoParameters]] { }
  implicit def pipeTypesTunerDr = new TypesTuner[Context.U, Default[Reducers]] { }

  /** Implicits for checking tuners on a call to `unique`. */
  implicit def pipeUniqueTunerDn = new UniqueTuner[Context.U, Default[NoParameters]] { }
  implicit def pipeUniqueTunerDr = new UniqueTuner[Context.U, Default[Reducers]] { }

  /** Implicits for checking tuners on a call to `which`. */
  implicit def pipeWhichTunerIn = new WhichTuner[Context.U, InMemory[NoParameters]] { }
  implicit def pipeWhichTunerDn = new WhichTuner[Context.U, Default[NoParameters]] { }
  implicit def pipeWhichTunerDr = new WhichTuner[Context.U, Default[Reducers]] { }
  implicit def pipeWhichTunerUr = new WhichTuner[Context.U, Unbalanced[Reducers]] { }

  /** Implicits for checking tuners on a call to `squash`. */
  implicit def pipeSquashTunerDn = new SquashTuner[Context.U, Default[NoParameters]] { }
  implicit def pipeSquashTunerDr = new SquashTuner[Context.U, Default[Reducers]] { }

  /** Implicits for checking tuners on a call to `reshape`. */
  implicit def pipeReshapeTunerIn = new ReshapeTuner[Context.U, InMemory[NoParameters]] { }
  implicit def pipeReshapeTunerDn = new ReshapeTuner[Context.U, Default[NoParameters]] { }
  implicit def pipeReshapeTunerDr = new ReshapeTuner[Context.U, Default[Reducers]] { }
  implicit def pipeReshapeTunerUr = new ReshapeTuner[Context.U, Unbalanced[Reducers]] { }

  /** Implicits for checking tuners on a call to `saveAsCSV`. */
  implicit def pipeSaveAsCSVDnDnTuner = new SaveAsCSVTuner[Context.U, Default[NoParameters]] { }
  implicit def pipeSaveAsCSVDrDnTuner = new SaveAsCSVTuner[Context.U, Default[Reducers]] { }
  implicit def pipeSaveAsCSVDnRrTuner = new SaveAsCSVTuner[Context.U, Redistribute] { }
  implicit def pipeSaveAsCSVDrRrTuner = new SaveAsCSVTuner[Context.U, Binary[Default[Reducers], Redistribute]] { }

  /** Implicits for checking tuners on a call to `saveAsVW`. */
  implicit def pipeSaveAsVWTunerDnDnDn = new SaveAsVWTuner[Context.U, Default[NoParameters]] { }
  implicit def pipeSaveAsVWTunerDrDrDn = new SaveAsVWTuner[Context.U, Default[Reducers]] { }
  implicit def pipeSaveAsVWTunerDnDnRr = new SaveAsVWTuner[Context.U, Binary[Default[NoParameters], Redistribute]] { }
  implicit def pipeSaveAsVWTunerDrDrRr = new SaveAsVWTuner[Context.U, Binary[Default[Reducers], Redistribute]] { }
  implicit def pipeSaveAsVWTunerDnInDn = new SaveAsVWTuner[
    Context.U,
    Binary[Default[NoParameters], InMemory[NoParameters]]
  ] { }
  implicit def pipeSaveAsVWTunerDnDrDn = new SaveAsVWTuner[
    Context.U,
    Binary[Default[NoParameters], Default[Reducers]]
  ] { }
  implicit def pipeSaveAsVWTunerDnInRr = new SaveAsVWTuner[
    Context.U,
    Ternary[Default[NoParameters], InMemory[NoParameters], Redistribute]
  ] { }
  implicit def pipeSaveAsVWTunerDnDrRr = new SaveAsVWTuner[
    Context.U,
    Ternary[Default[NoParameters], Default[Reducers], Redistribute]
  ] { }
  implicit def pipeSaveAsVWTunerDnUrDn = new SaveAsVWTuner[
    Context.U,
    Ternary[Default[NoParameters], Unbalanced[Reducers], Default[NoParameters]]
  ] { }
  implicit def pipeSaveAsVWTunerDnUrRr = new SaveAsVWTuner[
    Context.U,
    Ternary[Default[NoParameters], Unbalanced[Reducers], Redistribute]
  ] { }
  implicit def pipeSaveAsVWTunerDrInDn = new SaveAsVWTuner[
    Context.U,
    Ternary[Default[Reducers], InMemory[NoParameters], Default[NoParameters]]
  ] { }
  implicit def pipeSaveAsVWTunerDrInRr = new SaveAsVWTuner[
    Context.U,
    Ternary[Default[Reducers], InMemory[NoParameters], Redistribute]
  ] { }
  implicit def pipeSaveAsVWTunerDrUrDn = new SaveAsVWTuner[
    Context.U,
    Ternary[Default[Reducers], Unbalanced[Reducers], Default[NoParameters]]
  ] { }
  implicit def pipeSaveAsVWTunerDrUrRr = new SaveAsVWTuner[
    Context.U,
    Ternary[Default[Reducers], Unbalanced[Reducers], Redistribute]
  ] { }

  /** Converts a `Cell[P]` into a `TypedPipe[Cell[P]]`. */
  implicit def cellToPipe[
    P <: Nat
  ](
    c: Cell[P]
  )(implicit
    ctx: Context
  ): Context.U[Cell[P]] = ctx.implicits.cell.cellToU(c)

  /** Converts a `List[Cell[P]]` into a `TypedPipe[Cell[P]]`. */
  implicit def listCellToPipe[
    P <: Nat
  ](
    l: List[Cell[P]]
  )(implicit
    ctx: Context
  ): Context.U[Cell[P]] = ctx.implicits.cell.listCellToU(l)

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

  /** Conversion from `TypedPipe[Cell[P]]` to a `Matrix`. */
  implicit def pipeToMatrix[
    P <: Nat
  ](
    data: Context.U[Cell[P]]
  )(implicit
    ctx: Context
  ): Matrix[P] = ctx.implicits.matrix.toMatrix(data)

  /** Conversion from `TypedPipe[Cell[_1]]` to a `Matrix1D`. */
  implicit def pipeToMatrix1D(
    data: Context.U[Cell[_1]]
  )(implicit
    ctx: Context
  ): Matrix1D = ctx.implicits.matrix.toMatrix1D(data)

  /** Conversion from `TypedPipe[Cell[_2]]` to a `Matrix2D`. */
  implicit def pipeToMatrix2D(
    data: Context.U[Cell[_2]]
  )(implicit
    ctx: Context
  ): Matrix2D = ctx.implicits.matrix.toMatrix2D(data)

  /** Conversion from `TypedPipe[Cell[_3]]` to a `Matrix3D`. */
  implicit def pipeToMatrix3D(
    data: Context.U[Cell[_3]]
  )(implicit
    ctx: Context
  ): Matrix3D = ctx.implicits.matrix.toMatrix3D(data)

  /** Conversion from `TypedPipe[Cell[_4]]` to a `Matrix4D`. */
  implicit def pipeToMatrix4D(
    data: Context.U[Cell[_4]]
  )(implicit
    ctx: Context
  ): Matrix4D = ctx.implicits.matrix.toMatrix4D(data)

  /** Conversion from `TypedPipe[Cell[_5]]` to a `Matrix5D`. */
  implicit def pipeToMatrix5D(
    data: Context.U[Cell[_5]]
  )(implicit
    ctx: Context
  ): Matrix5D = ctx.implicits.matrix.toMatrix5D(data)

  /** Conversion from `TypedPipe[Cell[_6]]` to a `Matrix6D`. */
  implicit def pipeToMatrix6D(
    data: Context.U[Cell[_6]]
  )(implicit
    ctx: Context
  ): Matrix6D = ctx.implicits.matrix.toMatrix6D(data)

  /** Conversion from `TypedPipe[Cell[_7]]` to a `Matrix7D`. */
  implicit def pipeToMatrix7D(
    data: Context.U[Cell[_7]]
  )(implicit
    ctx: Context
  ): Matrix7D = ctx.implicits.matrix.toMatrix7D(data)

  /** Conversion from `TypedPipe[Cell[_8]]` to a `Matrix8D`. */
  implicit def pipeToMatrix8D(
    data: Context.U[Cell[_8]]
  )(implicit
    ctx: Context
  ): Matrix8D = ctx.implicits.matrix.toMatrix8D(data)

  /** Conversion from `TypedPipe[Cell[_9]]` to a `Matrix9D`. */
  implicit def pipeToMatrix9D(
    data: Context.U[Cell[_9]]
  )(implicit
    ctx: Context
  ): Matrix9D = ctx.implicits.matrix.toMatrix9D(data)

  /** Conversion from `TypedPipe[Cell[P]]` to a `MultiDimensionMatrix`. */
  implicit def pipeToMultiDimensionMatrix[
    P <: Nat
  ](
    data: Context.U[Cell[P]]
  )(implicit
    ctx: Context,
    ev: GT[P, _1]
  ): MultiDimensionMatrix[P] = ctx.implicits.matrix.toMultiDimensionMatrix(data)

  /** Conversion from `List[Cell[P]]` to a `Matrix`. */
  implicit def listToPipeMatrix[
    P <: Nat
  ](
    data: List[Cell[P]]
  )(implicit
    ctx: Context
  ): Matrix[P] = ctx.implicits.matrix.listToMatrix(data)

  /** Conversion from `List[Cell[_1]]` to a `Matrix1D`. */
  implicit def listToPipeMatrix1D(
    data: List[Cell[_1]]
  )(implicit
    ctx: Context
  ): Matrix1D = ctx.implicits.matrix.listToMatrix1D(data)

  /** Conversion from `List[Cell[_2]]` to a `Matrix2D`. */
  implicit def listToPipeMatrix2D(
    data: List[Cell[_2]]
  )(implicit
    ctx: Context
  ): Matrix2D = ctx.implicits.matrix.listToMatrix2D(data)

  /** Conversion from `List[Cell[_3]]` to a `Matrix3D`. */
  implicit def listToPipeMatrix3D(
    data: List[Cell[_3]]
  )(implicit
    ctx: Context
  ): Matrix3D = ctx.implicits.matrix.listToMatrix3D(data)

  /** Conversion from `List[Cell[_4]]` to a `Matrix4D`. */
  implicit def listToPipeMatrix4D(
    data: List[Cell[_4]]
  )(implicit
    ctx: Context
  ): Matrix4D = ctx.implicits.matrix.listToMatrix4D(data)

  /** Conversion from `List[Cell[_5]]` to a `Matrix5D`. */
  implicit def listToPipeMatrix5D(
    data: List[Cell[_5]]
  )(implicit
    ctx: Context
  ): Matrix5D = ctx.implicits.matrix.listToMatrix5D(data)

  /** Conversion from `List[Cell[_6]]` to a `Matrix6D`. */
  implicit def listToPipeMatrix6D(
    data: List[Cell[_6]]
  )(implicit
    ctx: Context
  ): Matrix6D = ctx.implicits.matrix.listToMatrix6D(data)

  /** Conversion from `List[Cell[_7]]` to a `Matrix7D`. */
  implicit def listToPipeMatrix7D(
    data: List[Cell[_7]]
  )(implicit
    ctx: Context
  ): Matrix7D = ctx.implicits.matrix.listToMatrix7D(data)

  /** Conversion from `List[Cell[_8]]` to a `Matrix8D`. */
  implicit def listToPipeMatrix8D(
    data: List[Cell[_8]]
  )(implicit
    ctx: Context
  ): Matrix8D = ctx.implicits.matrix.listToMatrix8D(data)

  /** Conversion from `List[Cell[_9]]` to a `Matrix9D`. */
  implicit def listToPipeMatrix9D(
    data: List[Cell[_9]]
  )(implicit
    ctx: Context
  ): Matrix9D = ctx.implicits.matrix.listToMatrix9D(data)

  /** Conversion from `List[Cell[P]]` to a `MultiDimensionMatrix`. */
  implicit def listToPipeMultiDimensionMatrix[
    P <: Nat
  ](
    data: List[Cell[P]]
  )(implicit
    ctx: Context,
    ev: GT[P, _1]
  ): MultiDimensionMatrix[P] = ctx.implicits.matrix.listToMultiDimensionMatrix(data)

  /** Conversion from `List[(Value, Content)]` to a `Matrix`. */
  implicit def tuple1ToPipeMatrix[
    V <% Value
  ](
    list: List[(V, Content)]
  )(implicit
    ctx: Context
  ): Matrix[_1] = ctx.implicits.matrix.tuple1ToMatrix(list)

  /** Conversion from `List[(Value, Content)]` to a `Matrix1D`. */
  implicit def tuple1ToPipeMatrix1D[
    V <% Value
  ](
    list: List[(V, Content)]
  )(implicit
    ctx: Context
  ): Matrix1D = ctx.implicits.matrix.tuple1ToMatrix1D(list)

  /** Conversion from `List[(Value, Value, Content)]` to a `Matrix`. */
  implicit def tuple2ToPipeMatrix[
    V1 <% Value,
    V2 <% Value
  ](
    list: List[(V1, V2, Content)]
  )(implicit
    ctx: Context
  ): Matrix[_2] = ctx.implicits.matrix.tuple2ToMatrix(list)

  /** Conversion from `List[(Value, Value, Content)]` to a `Matrix2D`. */
  implicit def tuple2ToPipeMatrix2D[
    V1 <% Value,
    V2 <% Value
  ](
    list: List[(V1, V2, Content)]
  )(implicit
    ctx: Context
  ): Matrix2D = ctx.implicits.matrix.tuple2ToMatrix2D(list)

  /** Conversion from `List[(Value, Value, Content)]` to a `MultiDimensionMatrix`. */
  implicit def tuple2ToPipeMultiDimensionMatrix[
    V1 <% Value,
    V2 <% Value
  ](
    list: List[(V1, V2, Content)]
  )(implicit
    ctx: Context
  ): MultiDimensionMatrix[_2] = ctx.implicits.matrix.tuple2ToMultiDimensionMatrix(list)

  /** Conversion from `List[(Value, Value, Value, Content)]` to a `Matrix`. */
  implicit def tuple3ToPipeMatrix[
    V1 <% Value,
    V2 <% Value,
    V3 <% Value
  ](
    list: List[(V1, V2, V3, Content)]
  )(implicit
    ctx: Context
  ): Matrix[_3] = ctx.implicits.matrix.tuple3ToMatrix(list)

  /** Conversion from `List[(Value, Value, Value, Content)]` to a `Matrix3D`. */
  implicit def tuple3ToPipeMatrix3D[
    V1 <% Value,
    V2 <% Value,
    V3 <% Value
  ](
    list: List[(V1, V2, V3, Content)]
  )(implicit
    ctx: Context
  ): Matrix3D = ctx.implicits.matrix.tuple3ToMatrix3D(list)

  /** Conversion from `List[(Value, Value, Value, Content)]` to a `MultiDimensionMatrix`. */
  implicit def tuple3ToPipeMultiDimensionMatrix[
    V1 <% Value,
    V2 <% Value,
    V3 <% Value
  ](
    list: List[(V1, V2, V3, Content)]
  )(implicit
    ctx: Context
  ): MultiDimensionMatrix[_3] = ctx.implicits.matrix.tuple3ToMultiDimensionMatrix(list)

  /** Conversion from `List[(Value, Value, Value, Value, Content)]` to a `Matrix`. */
  implicit def tuple4ToPipeMatrix[
    V1 <% Value,
    V2 <% Value,
    V3 <% Value,
    V4 <% Value
  ](
    list: List[(V1, V2, V3, V4, Content)]
  )(implicit
    ctx: Context
  ): Matrix[_4] = ctx.implicits.matrix.tuple4ToMatrix(list)

  /** Conversion from `List[(Value, Value, Value, Value, Content)]` to a `Matrix4D`. */
  implicit def tuple4ToPipeMatrix4D[
    V1 <% Value,
    V2 <% Value,
    V3 <% Value,
    V4 <% Value
  ](
    list: List[(V1, V2, V3, V4, Content)]
  )(implicit
    ctx: Context
  ): Matrix4D = ctx.implicits.matrix.tuple4ToMatrix4D(list)

  /** Conversion from `List[(Value, Value, Value, Value, Content)]` to a `MultiDimensionMatrix`. */
  implicit def tuple4ToPipeMultiDimensionMatrix[
    V1 <% Value,
    V2 <% Value,
    V3 <% Value,
    V4 <% Value
  ](
    list: List[(V1, V2, V3, V4, Content)]
  )(implicit
    ctx: Context
  ): MultiDimensionMatrix[_4] = ctx.implicits.matrix.tuple4ToMultiDimensionMatrix(list)

  /** Conversion from `List[(Value, Value, Value, Value, Value, Content)]` to a `Matrix`. */
  implicit def tuple5ToPipeMatrix[
    V1 <% Value,
    V2 <% Value,
    V3 <% Value,
    V4 <% Value,
    V5 <% Value
  ](
    list: List[(V1, V2, V3, V4, V5, Content)]
  )(implicit
    ctx: Context
  ): Matrix[_5] = ctx.implicits.matrix.tuple5ToMatrix(list)

  /** Conversion from `List[(Value, Value, Value, Value, Value, Content)]` to a `Matrix5D`. */
  implicit def tuple5ToPipeMatrix5D[
    V1 <% Value,
    V2 <% Value,
    V3 <% Value,
    V4 <% Value,
    V5 <% Value
  ](
    list: List[(V1, V2, V3, V4, V5, Content)]
  )(implicit
    ctx: Context
  ): Matrix5D = ctx.implicits.matrix.tuple5ToMatrix5D(list)

  /** Conversion from `List[(Value, Value, Value, Value, Value, Content)]` to a `MultiDimensionMatrix`. */
  implicit def tuple5ToPipeMultiDimensionMatrix[
    V1 <% Value,
    V2 <% Value,
    V3 <% Value,
    V4 <% Value,
    V5 <% Value
  ](
    list: List[(V1, V2, V3, V4, V5, Content)]
  )(implicit
    ctx: Context
  ): MultiDimensionMatrix[_5] = ctx.implicits.matrix.tuple5ToMultiDimensionMatrix(list)

  /** Conversion from `List[(Value, Value, Value, Value, Value, Value, Content)]` to a `Matrix`. */
  implicit def tuple6ToPipeMatrix[
    V1 <% Value,
    V2 <% Value,
    V3 <% Value,
    V4 <% Value,
    V5 <% Value,
    V6 <% Value
  ](
    list: List[(V1, V2, V3, V4, V5, V6, Content)]
  )(implicit
    ctx: Context
  ): Matrix[_6] = ctx.implicits.matrix.tuple6ToMatrix(list)

  /** Conversion from `List[(Value, Value, Value, Value, Value, Value, Content)]` to a `Matrix6D`. */
  implicit def tuple6ToPipeMatrix6D[
    V1 <% Value,
    V2 <% Value,
    V3 <% Value,
    V4 <% Value,
    V5 <% Value,
    V6 <% Value
  ](
    list: List[(V1, V2, V3, V4, V5, V6, Content)]
  )(implicit
    ctx: Context
  ): Matrix6D = ctx.implicits.matrix.tuple6ToMatrix6D(list)

  /** Conversion from `List[(Value, Value, Value, Value, Value, Value, Content)]` to a `MultiDimensionMatrix`. */
  implicit def tuple6ToPipeMultiDimensionMatrix[
    V1 <% Value,
    V2 <% Value,
    V3 <% Value,
    V4 <% Value,
    V5 <% Value,
    V6 <% Value
  ](
    list: List[(V1, V2, V3, V4, V5, V6, Content)]
  )(implicit
    ctx: Context
  ): MultiDimensionMatrix[_6] = ctx.implicits.matrix.tuple6ToMultiDimensionMatrix(list)

  /** Conversion from `List[(Value, Value, Value, Value, Value, Value, Value, Content)]` to a `Matrix`. */
  implicit def tuple7ToPipeMatrix[
    V1 <% Value,
    V2 <% Value,
    V3 <% Value,
    V4 <% Value,
    V5 <% Value,
    V6 <% Value,
    V7 <% Value
  ](
    list: List[(V1, V2, V3, V4, V5, V6, V7, Content)]
  )(implicit
    ctx: Context
  ): Matrix[_7] = ctx.implicits.matrix.tuple7ToMatrix(list)

  /** Conversion from `List[(Value, Value, Value, Value, Value, Value, Value, Content)]` to a `Matrix7D`. */
  implicit def tuple7ToPipeMatrix7D[
    V1 <% Value,
    V2 <% Value,
    V3 <% Value,
    V4 <% Value,
    V5 <% Value,
    V6 <% Value,
    V7 <% Value
  ](
    list: List[(V1, V2, V3, V4, V5, V6, V7, Content)]
  )(implicit
    ctx: Context
  ): Matrix7D = ctx.implicits.matrix.tuple7ToMatrix7D(list)

  /**
   * Conversion from `List[(Value, Value, Value, Value, Value, Value, Value, Content)]` to a `MultiDimensionMatrix`.
   */
  implicit def tuple7ToPipeMultiDimensionMatrix[
    V1 <% Value,
    V2 <% Value,
    V3 <% Value,
    V4 <% Value,
    V5 <% Value,
    V6 <% Value,
    V7 <% Value
  ](
    list: List[(V1, V2, V3, V4, V5, V6, V7, Content)]
  )(implicit
    ctx: Context
  ): MultiDimensionMatrix[_7] = ctx.implicits.matrix.tuple7ToMultiDimensionMatrix(list)

  /** Conversion from `List[(Value, Value, Value, Value, Value, Value, Value, Value, Content)]` to a `Matrix`. */
  implicit def tuple8ToPipeMatrix[
    V1 <% Value,
    V2 <% Value,
    V3 <% Value,
    V4 <% Value,
    V5 <% Value,
    V6 <% Value,
    V7 <% Value,
    V8 <% Value
  ](
    list: List[(V1, V2, V3, V4, V5, V6, V7, V8, Content)]
  )(implicit
    ctx: Context
  ): Matrix[_8] = ctx.implicits.matrix.tuple8ToMatrix(list)

  /** Conversion from `List[(Value, Value, Value, Value, Value, Value, Value, Value, Content)]` to a `Matrix8D`. */
  implicit def tuple8ToPipeMatrix8D[
    V1 <% Value,
    V2 <% Value,
    V3 <% Value,
    V4 <% Value,
    V5 <% Value,
    V6 <% Value,
    V7 <% Value,
    V8 <% Value
  ](
    list: List[(V1, V2, V3, V4, V5, V6, V7, V8, Content)]
  )(implicit
    ctx: Context
  ): Matrix8D = ctx.implicits.matrix.tuple8ToMatrix8D(list)

  /**
   * Conversion from `List[(Value, Value, Value, Value, Value, Value, Value, Value, Content)]` to a
   * `MultiDimensionMatrix`.
   */
  implicit def tuple8ToPipeMultiDimensionMatrix[
    V1 <% Value,
    V2 <% Value,
    V3 <% Value,
    V4 <% Value,
    V5 <% Value,
    V6 <% Value,
    V7 <% Value,
    V8 <% Value
  ](
    list: List[(V1, V2, V3, V4, V5, V6, V7, V8, Content)]
  )(implicit
    ctx: Context
  ): MultiDimensionMatrix[_8] = ctx.implicits.matrix.tuple8ToMultiDimensionMatrix(list)

  /**
   * Conversion from `List[(Value, Value, Value, Value, Value, Value, Value, Value, Value, Content)]` to a `Matrix`.
   */
  implicit def tuple9ToPipeMatrix[
    V1 <% Value,
    V2 <% Value,
    V3 <% Value,
    V4 <% Value,
    V5 <% Value,
    V6 <% Value,
    V7 <% Value,
    V8 <% Value,
    V9 <% Value
  ](
    list: List[(V1, V2, V3, V4, V5, V6, V7, V8, V9, Content)]
  )(implicit
    ctx: Context
  ): Matrix[_9] = ctx.implicits.matrix.tuple9ToMatrix(list)

  /**
   * Conversion from `List[(Value, Value, Value, Value, Value, Value, Value, Value, Value, Content)]` to a `Matrix9D`.
   */
  implicit def tuple9ToPipeMatrix9D[
    V1 <% Value,
    V2 <% Value,
    V3 <% Value,
    V4 <% Value,
    V5 <% Value,
    V6 <% Value,
    V7 <% Value,
    V8 <% Value,
    V9 <% Value
  ](
    list: List[(V1, V2, V3, V4, V5, V6, V7, V8, V9, Content)]
  )(implicit
    ctx: Context
  ): Matrix9D = ctx.implicits.matrix.tuple9ToMatrix9D(list)

  /**
   * Conversion from `List[(Value, Value, Value, Value, Value, Value, Value, Value, Value, Content)]` to a
   * `MultiDimensionMatrix`.
   */
  implicit def tuple9ToPipeMultiDimensionMatrix[
    V1 <% Value,
    V2 <% Value,
    V3 <% Value,
    V4 <% Value,
    V5 <% Value,
    V6 <% Value,
    V7 <% Value,
    V8 <% Value,
    V9 <% Value
  ](
    list: List[(V1, V2, V3, V4, V5, V6, V7, V8, V9, Content)]
  )(implicit
    ctx: Context
  ): MultiDimensionMatrix[_9] = ctx.implicits.matrix.tuple9ToMultiDimensionMatrix(list)

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
    V <% Value
  ](
    v: V
  )(implicit
    ctx: Context
  ): Context.U[Position[_1]] = ctx.implicits.position.valueToU(v)

  /** Converts a `List[Value]` to a `TypedPipe[Position[_1]]`. */
  implicit def listValueToPipe[
    V <% Value
  ](
    l: List[V]
  )(implicit
    ctx: Context
  ): Context.U[Position[_1]] = ctx.implicits.position.listValueToU(l)

  /** Converts a `Position[T]` to a `TypedPipe[Position[T]]`. */
  implicit def positionToPipe[
    P <: Nat
  ](
    p: Position[P]
  )(implicit
    ctx: Context
  ): Context.U[Position[P]] = ctx.implicits.position.positionToU(p)

  /** Converts a `List[Position[T]]` to a `TypedPipe[Position[T]]`. */
  implicit def listPositionToPipe[
    P <: Nat
  ](
    l: List[Position[P]]
  )(implicit
    ctx: Context
  ): Context.U[Position[P]] = ctx.implicits.position.listPositionToU(l)

  /** Converts a `TypedPipe[Position[P]]` to a `Positions`. */
  implicit def pipeToPositions[
    P <: Nat
  ](
    data: Context.U[Position[P]]
  )(implicit
    ctx: Context
  ): Positions[P] = ctx.implicits.position.toPositions(data)

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
    l: List[(T, Cell.Predicate[P])]
  )(implicit
    ctx: Context
  ): List[(Context.U[Position[S]], Cell.Predicate[P])] = ctx.implicits.position.listPredicateToU(l)

  /** Converts a `TypedPipe[String]` to a `SaveStringsAsText`. */
  implicit def savePipeStringsAsText(
    data: Context.U[String]
  )(implicit
    ctx: Context
  ): SaveStringsAsText = ctx.implicits.environment.saveStringsAsText(data)
}

