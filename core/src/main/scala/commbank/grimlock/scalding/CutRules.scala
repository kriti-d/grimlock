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

package commbank.grimlock.scalding.transform

import commbank.grimlock.framework.position.Position

import commbank.grimlock.scalding.environment.UserData

import commbank.grimlock.library.transform.{ CutRules => FwCutRules }

import com.twitter.scalding.typed.LiteralValue

import shapeless.nat._1

/** Implement cut rules using Scalding. */
object CutRules extends FwCutRules with UserData {
  def fixed(ext: E[Stats], min: Position[_1], max: Position[_1], k: Long): E[Map[Position[_1], List[Double]]] = ext
    .map(stats => fixedFromStats(stats, min, max, k))

  def squareRootChoice(
    ext: E[Stats],
    count: Position[_1],
    min: Position[_1],
    max: Position[_1]
  ): E[Map[Position[_1], List[Double]]] = ext.map(stats => squareRootChoiceFromStats(stats, count, min, max))

  def sturgesFormula(
    ext: E[Stats],
    count: Position[_1],
    min: Position[_1],
    max: Position[_1]
  ): E[Map[Position[_1], List[Double]]] = ext.map(stats => sturgesFormulaFromStats(stats, count, min, max))

  def riceRule(
    ext: E[Stats],
    count: Position[_1],
    min: Position[_1],
    max: Position[_1]
  ): E[Map[Position[_1], List[Double]]] = ext.map(stats => riceRuleFromStats(stats, count, min, max))

  def doanesFormula(
    ext: E[Stats],
    count: Position[_1],
    min: Position[_1],
    max: Position[_1],
    skewness: Position[_1]
  ): E[Map[Position[_1], List[Double]]] = ext.map(stats => doanesFormulaFromStats(stats, count, min, max, skewness))

  def scottsNormalReferenceRule(
    ext: E[Stats],
    count: Position[_1],
    min: Position[_1],
    max: Position[_1],
    sd: Position[_1]
  ): E[Map[Position[_1], List[Double]]] = ext
    .map(stats => scottsNormalReferenceRuleFromStats(stats, count, min, max, sd))

  def breaks[P <% Position[_1]](range: Map[P, List[Double]]): E[Map[Position[_1], List[Double]]] = new LiteralValue(
    breaksFromMap(range)
  )
}

