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

import commbank.grimlock.framework.{ Type, Types => FwTypes }
import commbank.grimlock.framework.position.Position

import org.apache.spark.rdd.RDD

import shapeless.Nat

/**
 * Rich wrapper around a `RDD[(Position[P], Type)]`.
 *
 * @param data `RDD[(Position[P], Type)]`.
 *
 * @note This class represents the variable type along the dimensions of a matrix.
 */
case class Types[P <: Nat](data: RDD[(Position[P], Type)]) extends FwTypes[P] with Persist[(Position[P], Type)] {
  def saveAsText(ctx: C, file: String, writer: TextWriter): U[(Position[P], Type)] = saveText(ctx, file, writer)
}

