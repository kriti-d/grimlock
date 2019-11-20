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

import commbank.grimlock.framework.Cell
import commbank.grimlock.framework.environment.{
  CellImplicits => FwCellImplicits,
  ContentImplicits => FwContentImplicits,
  EnvironmentImplicits => FwEnvironmentImplicits,
  Implicits => FwImplicits,
  MatrixImplicits => FwMatrixImplicits,
  NativeOperations => FwNativeOperations,
  PartitionImplicits => FwPartitionImplicits,
  PositionImplicits => FwPositionImplicits,
  ValueOperations => FwValueOperations
}
import commbank.grimlock.framework.content.Content
import commbank.grimlock.framework.encoding.Value
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

import scala.reflect.ClassTag

import shapeless.{ ::, =:!=, HList, HNil }
import shapeless.nat.{ _0, _1, _2, _3, _4, _5, _6, _7, _8 }

/** Implements all implicits. */
case object Implicits extends FwImplicits[Context] {
  val cell = CellImplicits
  val content = ContentImplicits
  val environment = EnvironmentImplicits
  val matrix = MatrixImplicits
  val partition = PartitionImplicits
  val position = PositionImplicits
}

/** Implements all cell implicits. */
case object CellImplicits extends FwCellImplicits[Context] {
  implicit def cellToU[P <: HList](c: Cell[P])(implicit ctx: Context): Context.U[Cell[P]] = List(c)

  implicit def listCellToU[P <: HList](l: List[Cell[P]])(implicit ctx: Context): Context.U[Cell[P]] = l
}

/** Implements all content implicits. */
case object ContentImplicits extends FwContentImplicits[Context] {
  implicit def toContents(data: Context.U[Content]): Contents = Contents(data)

  implicit def toIndexed[
    P <: HList
  ](
    data: Context.U[(Position[P], Content)]
  ): IndexedContents[P] = IndexedContents(data)
}

/** Implements all environment implicits. */
case object EnvironmentImplicits extends FwEnvironmentImplicits[Context]  {
  implicit def saveStringsAsText(data: Context.U[String]): SaveStringsAsText = SaveStringsAsText(data)

  implicit def nativeFunctions[X](data: Context.U[X]): NativeOperations[X] = NativeOperations(data)

  implicit def valueFunctions[X](value: Context.E[X]): ValueOperations[X] = ValueOperations(value)

  implicit def eToU[X : ClassTag](value: Context.E[X])(implicit ctx: Context): Context.U[X] = List(value)
}

/** Implements all matrix implicits. */
case object MatrixImplicits extends FwMatrixImplicits[Context] {
  implicit def toMatrix[P <: HList](data: Context.U[Cell[P]]): Matrix[P] = Matrix(data)

  implicit def toMatrix1D[
    V1 <: Value[_]
  ](
    data: Context.U[Cell[V1 :: HNil]]
  )(implicit
    ev1: Position.IndexConstraints.Aux[V1 :: HNil, _0, V1]
  ): Matrix1D[V1] = Matrix1D(data)

  implicit def toMatrix2D[
    V1 <: Value[_],
    V2 <: Value[_]
  ](
    data: Context.U[Cell[V1 :: V2 :: HNil]]
  )(implicit
    ev1: Position.IndexConstraints.Aux[V1 :: V2 :: HNil, _0, V1],
    ev2: Position.IndexConstraints.Aux[V1 :: V2 :: HNil, _1, V2]
  ): Matrix2D[V1, V2] = Matrix2D(data)

  implicit def toMatrix3D[
    V1 <: Value[_],
    V2 <: Value[_],
    V3 <: Value[_]
  ](
    data: Context.U[Cell[V1 :: V2 :: V3 :: HNil]]
  )(implicit
    ev1: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: HNil, _0, V1],
    ev2: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: HNil, _1, V2],
    ev3: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: HNil, _2, V3]
  ): Matrix3D[V1, V2, V3] = Matrix3D(data)

  implicit def toMatrix4D[
    V1 <: Value[_],
    V2 <: Value[_],
    V3 <: Value[_],
    V4 <: Value[_]
  ](
    data: Context.U[Cell[V1 :: V2 :: V3 :: V4 :: HNil]]
  )(implicit
    ev1: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: HNil, _0, V1],
    ev2: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: HNil, _1, V2],
    ev3: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: HNil, _2, V3],
    ev4: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: HNil, _3, V4]
  ): Matrix4D[V1, V2, V3, V4] = Matrix4D(data)

  implicit def toMatrix5D[
    V1 <: Value[_],
    V2 <: Value[_],
    V3 <: Value[_],
    V4 <: Value[_],
    V5 <: Value[_]
  ](
    data: Context.U[Cell[V1 :: V2 :: V3 :: V4 :: V5 :: HNil]]
  )(implicit
    ev1: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: V5 :: HNil, _0, V1],
    ev2: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: V5 :: HNil, _1, V2],
    ev3: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: V5 :: HNil, _2, V3],
    ev4: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: V5 :: HNil, _3, V4],
    ev5: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: V5 :: HNil, _4, V5]
  ): Matrix5D[V1, V2, V3, V4, V5] = Matrix5D(data)

  implicit def toMatrix6D[
    V1 <: Value[_],
    V2 <: Value[_],
    V3 <: Value[_],
    V4 <: Value[_],
    V5 <: Value[_],
    V6 <: Value[_]
  ](
    data: Context.U[Cell[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: HNil]]
  )(implicit
    ev1: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: HNil, _0, V1],
    ev2: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: HNil, _1, V2],
    ev3: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: HNil, _2, V3],
    ev4: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: HNil, _3, V4],
    ev5: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: HNil, _4, V5],
    ev6: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: HNil, _5, V6]
  ): Matrix6D[V1, V2, V3, V4, V5, V6] = Matrix6D(data)

  implicit def toMatrix7D[
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
    ev1: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: HNil, _0, V1],
    ev2: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: HNil, _1, V2],
    ev3: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: HNil, _2, V3],
    ev4: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: HNil, _3, V4],
    ev5: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: HNil, _4, V5],
    ev6: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: HNil, _5, V6],
    ev7: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: HNil, _6, V7]
  ): Matrix7D[V1, V2, V3, V4, V5, V6, V7] = Matrix7D(data)

  implicit def toMatrix8D[
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
    ev1: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: HNil, _0, V1],
    ev2: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: HNil, _1, V2],
    ev3: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: HNil, _2, V3],
    ev4: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: HNil, _3, V4],
    ev5: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: HNil, _4, V5],
    ev6: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: HNil, _5, V6],
    ev7: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: HNil, _6, V7],
    ev8: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: HNil, _7, V8]
  ): Matrix8D[V1, V2, V3, V4, V5, V6, V7, V8] = Matrix8D(data)

  implicit def toMatrix9D[
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
    ev1: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: V9 :: HNil, _0, V1],
    ev2: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: V9 :: HNil, _1, V2],
    ev3: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: V9 :: HNil, _2, V3],
    ev4: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: V9 :: HNil, _3, V4],
    ev5: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: V9 :: HNil, _4, V5],
    ev6: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: V9 :: HNil, _5, V6],
    ev7: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: V9 :: HNil, _6, V7],
    ev8: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: V9 :: HNil, _7, V8],
    ev9: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: V9 :: HNil, _8, V9]
  ): Matrix9D[V1, V2, V3, V4, V5, V6, V7, V8, V9] = Matrix9D(data)

  implicit def toMultiDimensionMatrix[
    P <: HList
  ](
    data: Context.U[Cell[P]]
  )(implicit
    ev: Position.IsMultiDimensionalConstraints[P]
  ): MultiDimensionMatrix[P] = MultiDimensionMatrix(data)

  implicit def listToMatrix[P <: HList](data: List[Cell[P]])(implicit ctx: Context): Matrix[P] = Matrix(data)

  implicit def listToMatrix1D[
    V1 <: Value[_]
  ](
    data: List[Cell[V1 :: HNil]]
  )(implicit
    ctx: Context,
    ev1: Position.IndexConstraints.Aux[V1 :: HNil, _0, V1]
  ): Matrix1D[V1] = Matrix1D(data)

  implicit def listToMatrix2D[
    V1 <: Value[_],
    V2 <: Value[_]
  ](
    data: List[Cell[V1 :: V2 :: HNil]]
  )(implicit
    ctx: Context,
    ev1: Position.IndexConstraints.Aux[V1 :: V2 :: HNil, _0, V1],
    ev2: Position.IndexConstraints.Aux[V1 :: V2 :: HNil, _1, V2]
  ): Matrix2D[V1, V2] = Matrix2D(data)

  implicit def listToMatrix3D[
    V1 <: Value[_],
    V2 <: Value[_],
    V3 <: Value[_]
  ](
    data: List[Cell[V1 :: V2 :: V3 :: HNil]]
  )(implicit
    ctx: Context,
    ev1: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: HNil, _0, V1],
    ev2: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: HNil, _1, V2],
    ev3: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: HNil, _2, V3]
  ): Matrix3D[V1, V2, V3] = Matrix3D(data)

  implicit def listToMatrix4D[
    V1 <: Value[_],
    V2 <: Value[_],
    V3 <: Value[_],
    V4 <: Value[_]
  ](
    data: List[Cell[V1 :: V2 :: V3 :: V4 :: HNil]]
  )(implicit
    ctx: Context,
    ev1: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: HNil, _0, V1],
    ev2: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: HNil, _1, V2],
    ev3: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: HNil, _2, V3],
    ev4: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: HNil, _3, V4]
  ): Matrix4D[V1, V2, V3, V4] = Matrix4D(data)

  implicit def listToMatrix5D[
    V1 <: Value[_],
    V2 <: Value[_],
    V3 <: Value[_],
    V4 <: Value[_],
    V5 <: Value[_]
  ](
    data: List[Cell[V1 :: V2 :: V3 :: V4 :: V5 :: HNil]]
  )(implicit
    ctx: Context,
    ev1: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: V5 :: HNil, _0, V1],
    ev2: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: V5 :: HNil, _1, V2],
    ev3: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: V5 :: HNil, _2, V3],
    ev4: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: V5 :: HNil, _3, V4],
    ev5: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: V5 :: HNil, _4, V5]
  ): Matrix5D[V1, V2, V3, V4, V5] = Matrix5D(data)

  implicit def listToMatrix6D[
    V1 <: Value[_],
    V2 <: Value[_],
    V3 <: Value[_],
    V4 <: Value[_],
    V5 <: Value[_],
    V6 <: Value[_]
  ](
    data: List[Cell[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: HNil]]
  )(implicit
    ctx: Context,
    ev1: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: HNil, _0, V1],
    ev2: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: HNil, _1, V2],
    ev3: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: HNil, _2, V3],
    ev4: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: HNil, _3, V4],
    ev5: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: HNil, _4, V5],
    ev6: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: HNil, _5, V6]
  ): Matrix6D[V1, V2, V3, V4, V5, V6] = Matrix6D(data)

  implicit def listToMatrix7D[
    V1 <: Value[_],
    V2 <: Value[_],
    V3 <: Value[_],
    V4 <: Value[_],
    V5 <: Value[_],
    V6 <: Value[_],
    V7 <: Value[_]
  ](
    data: List[Cell[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: HNil]]
  )(implicit
    ctx: Context,
    ev1: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: HNil, _0, V1],
    ev2: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: HNil, _1, V2],
    ev3: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: HNil, _2, V3],
    ev4: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: HNil, _3, V4],
    ev5: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: HNil, _4, V5],
    ev6: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: HNil, _5, V6],
    ev7: Position.IndexConstraints.Aux[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: HNil, _6, V7]
  ): Matrix7D[V1, V2, V3, V4, V5, V6, V7] = Matrix7D(data)

  implicit def listToMatrix8D[
    V1 <: Value[_],
    V2 <: Value[_],
    V3 <: Value[_],
    V4 <: Value[_],
    V5 <: Value[_],
    V6 <: Value[_],
    V7 <: Value[_],
    V8 <: Value[_]
  ](
    data: List[Cell[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: HNil]]
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
  ): Matrix8D[V1, V2, V3, V4, V5, V6, V7, V8] = Matrix8D(data)

  implicit def listToMatrix9D[
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
    data: List[Cell[V1 :: V2 :: V3 :: V4 :: V5 :: V6 :: V7 :: V8 :: V9 :: HNil]]
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
  ): Matrix9D[V1, V2, V3, V4, V5, V6, V7, V8, V9] = Matrix9D(data)

  implicit def listToMultiDimensionMatrix[
    P <: HList
  ](
    data: List[Cell[P]]
  )(implicit
    ctx: Context,
    ev: Position.IsMultiDimensionalConstraints[P]
  ): MultiDimensionMatrix[P] = MultiDimensionMatrix(data)

  implicit def tuple1ToMatrix[
    T1 <% Value[T1]
  ](
    list: List[(T1, Content)]
  )(implicit
    ctx: Context
  ): Matrix[Coordinates1[T1]] = Matrix(list.map { case (t1, c) => Cell(Position(t1), c) })

  implicit def tuple1ToMatrix1D[
    T1 <% Value[T1]
  ](
    list: List[(T1, Content)]
  )(implicit
    ctx: Context,
    ev1: Position.IndexConstraints.Aux[Coordinates1[T1], _0, Value[T1]]
  ): Matrix1D[Value[T1]] = Matrix1D(list.map { case (t1, c) => Cell(Position(t1), c) })

  implicit def tuple2ToMatrix[
    T1 <% Value[T1],
    T2 <% Value[T2]
  ](
    list: List[(T1, T2, Content)]
  )(implicit
    ctx: Context
  ): Matrix[Coordinates2[T1, T2]] = Matrix(list.map { case (t1, t2, c) => Cell(Position(t1, t2), c) })

  implicit def tuple2ToMatrix2D[
    T1 <% Value[T1],
    T2 <% Value[T2]
  ](
    list: List[(T1, T2, Content)]
  )(implicit
    ctx: Context,
    ev1: Position.IndexConstraints.Aux[Coordinates2[T1, T2], _0, Value[T1]],
    ev2: Position.IndexConstraints.Aux[Coordinates2[T1, T2], _1, Value[T2]]
  ): Matrix2D[Value[T1], Value[T2]] = Matrix2D(list.map { case (t1, t2, c) => Cell(Position(t1, t2), c) })

  implicit def tuple2ToMultiDimensionMatrix[
    T1 <% Value[T1],
    T2 <% Value[T2]
  ](
    list: List[(T1, T2, Content)]
  )(implicit
    ctx: Context
  ): MultiDimensionMatrix[Coordinates2[T1, T2]] = MultiDimensionMatrix(
    list.map { case (t1, t2, c) => Cell(Position(t1, t2), c) }
  )

  implicit def tuple3ToMatrix[
    T1 <% Value[T1],
    T2 <% Value[T2],
    T3 <% Value[T3]
  ](
    list: List[(T1, T2, T3, Content)]
  )(implicit
    ctx: Context
  ): Matrix[Coordinates3[T1, T2, T3]] = Matrix(
    list.map { case (t1, t2, t3, c) => Cell(Position(t1, t2, t3), c) }
  )

  implicit def tuple3ToMatrix3D[
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
  ): Matrix3D[Value[T1], Value[T2], Value[T3]] = Matrix3D(
    list.map { case (t1, t2, t3, c) => Cell(Position(t1, t2, t3), c) }
  )

  implicit def tuple3ToMultiDimensionMatrix[
    T1 <% Value[T1],
    T2 <% Value[T2],
    T3 <% Value[T3]
  ](
    list: List[(T1, T2, T3, Content)]
  )(implicit
    ctx: Context
  ): MultiDimensionMatrix[Coordinates3[T1, T2, T3]] = MultiDimensionMatrix(
    list.map { case (t1, t2, t3, c) => Cell(Position(t1, t2, t3), c) }
  )

  implicit def tuple4ToMatrix[
    T1 <% Value[T1],
    T2 <% Value[T2],
    T3 <% Value[T3],
    T4 <% Value[T4]
  ](
    list: List[(T1, T2, T3, T4, Content)]
  )(implicit
    ctx: Context
  ): Matrix[Coordinates4[T1, T2, T3, T4]] = Matrix(
    list.map { case (t1, t2, t3, t4, c) => Cell(Position(t1, t2, t3, t4), c) }
  )

  implicit def tuple4ToMatrix4D[
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
  ): Matrix4D[Value[T1], Value[T2], Value[T3], Value[T4]] = Matrix4D(
    list.map { case (t1, t2, t3, t4, c) => Cell(Position(t1, t2, t3, t4), c) }
  )

  implicit def tuple4ToMultiDimensionMatrix[
    T1 <% Value[T1],
    T2 <% Value[T2],
    T3 <% Value[T3],
    T4 <% Value[T4]
  ](
    list: List[(T1, T2, T3, T4, Content)]
  )(implicit
    ctx: Context
  ): MultiDimensionMatrix[Coordinates4[T1, T2, T3, T4]] = MultiDimensionMatrix(
    list.map { case (t1, t2, t3, t4, c) => Cell(Position(t1, t2, t3, t4), c) }
  )

  implicit def tuple5ToMatrix[
    T1 <% Value[T1],
    T2 <% Value[T2],
    T3 <% Value[T3],
    T4 <% Value[T4],
    T5 <% Value[T5]
  ](
    list: List[(T1, T2, T3, T4, T5, Content)]
  )(implicit
    ctx: Context
  ): Matrix[Coordinates5[T1, T2, T3, T4, T5]] = Matrix(
    list.map { case (t1, t2, t3, t4, t5, c) => Cell(Position(t1, t2, t3, t4, t5), c) }
  )

  implicit def tuple5ToMatrix5D[
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
  ): Matrix5D[Value[T1], Value[T2], Value[T3], Value[T4], Value[T5]] = Matrix5D(
    list.map { case (t1, t2, t3, t4, t5, c) => Cell(Position(t1, t2, t3, t4, t5), c) }
  )

  implicit def tuple5ToMultiDimensionMatrix[
    T1 <% Value[T1],
    T2 <% Value[T2],
    T3 <% Value[T3],
    T4 <% Value[T4],
    T5 <% Value[T5]
  ](
    list: List[(T1, T2, T3, T4, T5, Content)]
  )(implicit
    ctx: Context
  ): MultiDimensionMatrix[Coordinates5[T1, T2, T3, T4, T5]] = MultiDimensionMatrix(
    list.map { case (t1, t2, t3, t4, t5, c) => Cell(Position(t1, t2, t3, t4, t5), c) }
  )

  implicit def tuple6ToMatrix[
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
  ): Matrix[Coordinates6[T1, T2, T3, T4, T5, T6]] = Matrix(
    list.map { case (t1, t2, t3, t4, t5, t6, c) => Cell(Position(t1, t2, t3, t4, t5, t6), c) }
  )

  implicit def tuple6ToMatrix6D[
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
  ): Matrix6D[Value[T1], Value[T2], Value[T3], Value[T4], Value[T5], Value[T6]] = Matrix6D(
    list.map { case (t1, t2, t3, t4, t5, t6, c) => Cell(Position(t1, t2, t3, t4, t5, t6), c) }
  )

  implicit def tuple6ToMultiDimensionMatrix[
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
  ): MultiDimensionMatrix[Coordinates6[T1, T2, T3, T4, T5, T6]] = MultiDimensionMatrix(
    list.map { case (t1, t2, t3, t4, t5, t6, c) => Cell(Position(t1, t2, t3, t4, t5, t6), c) }
  )

  implicit def tuple7ToMatrix[
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
  ): Matrix[Coordinates7[T1, T2, T3, T4, T5, T6, T7]] = Matrix(
    list.map { case (t1, t2, t3, t4, t5, t6, t7, c) => Cell(Position(t1, t2, t3, t4, t5, t6, t7), c) }
  )

  implicit def tuple7ToMatrix7D[
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
  ): Matrix7D[Value[T1], Value[T2], Value[T3], Value[T4], Value[T5], Value[T6], Value[T7]] = Matrix7D(
    list.map { case (t1, t2, t3, t4, t5, t6, t7, c) => Cell(Position(t1, t2, t3, t4, t5, t6, t7), c) }
  )

  implicit def tuple7ToMultiDimensionMatrix[
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
  ): MultiDimensionMatrix[Coordinates7[T1, T2, T3, T4, T5, T6, T7]] = MultiDimensionMatrix(
    list.map { case (t1, t2, t3, t4, t5, t6, t7, c) => Cell(Position(t1, t2, t3, t4, t5, t6, t7), c) }
  )

  implicit def tuple8ToMatrix[
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
  ): Matrix[Coordinates8[T1, T2, T3, T4, T5, T6, T7, T8]] = Matrix(
    list.map { case (t1, t2, t3, t4, t5, t6, t7, t8, c) => Cell(Position(t1, t2, t3, t4, t5, t6, t7, t8), c) }
  )

  implicit def tuple8ToMatrix8D[
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
  ): Matrix8D[Value[T1], Value[T2], Value[T3], Value[T4], Value[T5], Value[T6], Value[T7], Value[T8]] = Matrix8D(
    list.map { case (t1, t2, t3, t4, t5, t6, t7, t8, c) => Cell(Position(t1, t2, t3, t4, t5, t6, t7, t8), c) }
  )

  implicit def tuple8ToMultiDimensionMatrix[
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
  ): MultiDimensionMatrix[Coordinates8[T1, T2, T3, T4, T5, T6, T7, T8]] = MultiDimensionMatrix(
    list.map { case (t1, t2, t3, t4, t5, t6, t7, t8, c) => Cell(Position(t1, t2, t3, t4, t5, t6, t7, t8), c) }
  )

  implicit def tuple9ToMatrix[
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
  ): Matrix[Coordinates9[T1, T2, T3, T4, T5, T6, T7, T8, T9]] = Matrix(
    list.map { case (t1, t2, t3, t4, t5, t6, t7, t8, t9, c) => Cell(Position(t1, t2, t3, t4, t5, t6, t7, t8, t9), c) }
  )

  implicit def tuple9ToMatrix9D[
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
  ): Matrix9D[
    Value[T1],
    Value[T2],
    Value[T3],
    Value[T4],
    Value[T5],
    Value[T6],
    Value[T7],
    Value[T8],
    Value[T9]
  ] = Matrix9D(
    list.map { case (t1, t2, t3, t4, t5, t6, t7, t8, t9, c) => Cell(Position(t1, t2, t3, t4, t5, t6, t7, t8, t9), c) }
  )

  implicit def tuple9ToMultiDimensionMatrix[
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
  ): MultiDimensionMatrix[Coordinates9[T1, T2, T3, T4, T5, T6, T7, T8, T9]] = MultiDimensionMatrix(
    list.map { case (t1, t2, t3, t4, t5, t6, t7, t8, t9, c) => Cell(Position(t1, t2, t3, t4, t5, t6, t7, t8, t9), c) }
  )
}

/** Implements all partition implicits. */
case object PartitionImplicits extends FwPartitionImplicits[Context] {
  implicit def toPartitions[
    P <: HList,
    I : Ordering
  ](
    data: Context.U[(I, Cell[P])]
  ): Partitions[P, I] = Partitions(data)
}

/** Implements all position implicits. */
case object PositionImplicits extends FwPositionImplicits[Context] {
  implicit def tToU[
    T <% Value[T]
  ](
    t: T
  )(implicit
    ctx: Context
  ): Context.U[Position[Coordinates1[T]]] = List(Position(t))

  implicit def listTToU[T <% Value[T]](l: List[T])(implicit ctx: Context): Context.U[Position[Coordinates1[T]]] = l
    .map { case t => Position(t) }

  implicit def valueToU[V <: Value[_]](v: V)(implicit ctx: Context): Context.U[Position[V :: HNil]] = List(Position(v))

  implicit def listValueToU[V <: Value[_]](l: List[V])(implicit ctx: Context): Context.U[Position[V :: HNil]] = l
    .map { case v => Position(v) }

  implicit def positionToU[P <: HList](p: Position[P])(implicit ctx: Context): Context.U[Position[P]] = List(p)

  implicit def listPositionToU[P <: HList](l: List[Position[P]])(implicit ctx: Context): Context.U[Position[P]] = l

  implicit def toPositions[P <: HList](data: Context.U[Position[P]]): Positions[P] = Positions(data)
}

/** Implements all native operations. */
case class NativeOperations[X](data: Context.U[X]) extends FwNativeOperations[X, Context.U] {
  def ++(other: Context.U[X]): Context.U[X] = data ++ other

  def filter(f: (X) => Boolean): Context.U[X] = data.filter(f)

  def flatMap[Y : ClassTag](f: (X) => TraversableOnce[Y]): Context.U[Y] = data.flatMap(f)

  def map[Y : ClassTag](f: (X) => Y): Context.U[Y] = data.map(f)
}

/** Implements all value operations. */
case class ValueOperations[X](value: Context.E[X]) extends FwValueOperations[X, Context.E] {
  def cross[Y](that: Context.E[Y])(implicit ev: Y =:!= Nothing): Context.E[(X, Y)] = (value, that)

  def map[Y](f: (X) => Y): Context.E[Y] = f(value)
}

