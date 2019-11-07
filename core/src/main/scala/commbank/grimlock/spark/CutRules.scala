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

package commbank.grimlock.spark.transform

import commbank.grimlock.framework.encoding.Value
import commbank.grimlock.framework.position.{ Coordinates1, Position }

import commbank.grimlock.spark.environment.Context

import commbank.grimlock.library.transform.{ CutRules => FwCutRules }

import shapeless.HList

/** Implement cut rules for Spark. */
case object CutRules extends FwCutRules[Context.E] {
  def fixed[
    K <: HList,
    V <: HList
  ](
    ext: Context.E[Stats[K, V]],
    min: Position[V],
    max: Position[V],
    k: Long
  ): Context.E[Map[Position[K], List[Double]]] = fixedFromStats(ext, min, max, k)

  def squareRootChoice[
    K <: HList,
    V <: HList
  ](
    ext: Context.E[Stats[K, V]],
    count: Position[V],
    min: Position[V],
    max: Position[V]
  ): Context.E[Map[Position[K], List[Double]]] = squareRootChoiceFromStats(ext, count, min, max)

  def sturgesFormula[
    K <: HList,
    V <: HList
  ](
    ext: Context.E[Stats[K, V]],
    count: Position[V],
    min: Position[V],
    max: Position[V]
  ): Context.E[Map[Position[K], List[Double]]] = sturgesFormulaFromStats(ext, count, min, max)

  def riceRule[
    K <: HList,
    V <: HList
  ](
    ext: Context.E[Stats[K, V]],
    count: Position[V],
    min: Position[V],
    max: Position[V]
  ): Context.E[Map[Position[K], List[Double]]] = riceRuleFromStats(ext, count, min, max)

  def doanesFormula[
    K <: HList,
    V <: HList
  ](
    ext: Context.E[Stats[K, V]],
    count: Position[V],
    min: Position[V],
    max: Position[V],
    skewness: Position[V]
  ): Context.E[Map[Position[K], List[Double]]] = doanesFormulaFromStats(ext, count, min, max, skewness)

  def scottsNormalReferenceRule[
    K <: HList,
    V <: HList
  ](
    ext: Context.E[Stats[K, V]],
    count: Position[V],
    min: Position[V],
    max: Position[V],
    sd: Position[V]
  ): Context.E[Map[Position[K], List[Double]]] = scottsNormalReferenceRuleFromStats(ext, count, min, max, sd)

  def breaks[
    T <% Value[T]
  ](
    range: Map[T, List[Double]]
  ): Context.E[Map[Position[Coordinates1[T]], List[Double]]] = breaksFromMap(range)
}

