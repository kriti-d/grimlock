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

package commbank.grimlock.scalding.environment.implicits

import commbank.grimlock.framework.Cell
import commbank.grimlock.framework.environment.implicits.{
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
import commbank.grimlock.framework.position.Position
import commbank.grimlock.framework.utility.=:!=

import commbank.grimlock.scalding.{
  Matrix1D,
  Matrix2D,
  Matrix3D,
  Matrix4D,
  Matrix5D,
  Matrix6D,
  Matrix7D,
  Matrix8D,
  Matrix9D,
  SaveStringsAsText
}
import commbank.grimlock.scalding.content.{ Contents, IndexedContents }
import commbank.grimlock.scalding.environment.Context
import commbank.grimlock.scalding.partition.Partitions
import commbank.grimlock.scalding.position.Positions

import com.twitter.scalding.typed.IterablePipe

import scala.reflect.ClassTag

import shapeless.Nat
import shapeless.nat.{ _1, _2, _3, _4, _5, _6, _7, _8, _9 }
import shapeless.ops.nat.Diff

/** Implements all implicits for `context`. */
case class Implicits(context: Context) extends FwImplicits[Context.U, Context.E, Context] {
  val cell = CellImplicits(context)
  val content = ContentImplicits(context)
  val environment = EnvironmentImplicits(context)
  val matrix = MatrixImplicits(context)
  val partition = PartitionImplicits(context)
  val position = PositionImplicits(context)
}

/** Implements all cell implicits for `context`. */
case class CellImplicits(
  context: Context
) extends FwCellImplicits[Context.U, Context.E, Context] {
  implicit def cellToU[P <: Nat](t: Cell[P]): Context.U[Cell[P]] = IterablePipe(List(t))

  implicit def listCellToU[P <: Nat](t: List[Cell[P]]): Context.U[Cell[P]] = IterablePipe(t)
}

/** Implements all content implicits for `context`. */
case class ContentImplicits(
  context: Context
) extends FwContentImplicits[Context.U, Context.E, Context] {
  implicit def toContents(data: Context.U[Content]): Contents = Contents(context, data)

  implicit def toIndexed[
    P <: Nat
  ](
    data: Context.U[(Position[P], Content)]
  ): IndexedContents[P] = IndexedContents(context, data)
}

/** Implements all environment implicits for `context`. */
case class EnvironmentImplicits(
  context: Context
) extends FwEnvironmentImplicits[Context.U, Context.E, Context]  {
  implicit def saveStringsAsText(data: Context.U[String]): SaveStringsAsText = SaveStringsAsText(context, data)

  implicit def nativeFunctions[X](data: Context.U[X]): NativeOperations[X] = NativeOperations(data)

  implicit def valueFunctions[X](value: Context.E[X]): ValueOperations[X] = ValueOperations(value)

  implicit def eToU[X : ClassTag](value: Context.E[X]): Context.U[X] = value.toTypedPipe

  /** Implicit FlowDef for write operations. */
  implicit val implicitFlow = context.flow

  /** Implicit Mode for write and Execution.waitFor operations. */
  implicit val implicitMode = context.mode

  /** Implicit Config for Execution.waitFor operations. */
  implicit val implicitConfig = context.config
}

/** Implements all matrix implicits for `context`. */
case class MatrixImplicits(
  context: Context
) extends FwMatrixImplicits[Context.U, Context.E, Context] {
  implicit def toMatrix1D(data: Context.U[Cell[_1]]): Matrix1D = Matrix1D(context, data)

  implicit def toMatrix2D(data: Context.U[Cell[_2]]): Matrix2D = Matrix2D(context, data)

  implicit def toMatrix3D(data: Context.U[Cell[_3]]): Matrix3D = Matrix3D(context, data)

  implicit def toMatrix4D(data: Context.U[Cell[_4]]): Matrix4D = Matrix4D(context, data)

  implicit def toMatrix5D(data: Context.U[Cell[_5]]): Matrix5D = Matrix5D(context, data)

  implicit def toMatrix6D(data: Context.U[Cell[_6]]): Matrix6D = Matrix6D(context, data)

  implicit def toMatrix7D(data: Context.U[Cell[_7]]): Matrix7D = Matrix7D(context, data)

  implicit def toMatrix8D(data: Context.U[Cell[_8]]): Matrix8D = Matrix8D(context, data)

  implicit def toMatrix9D(data: Context.U[Cell[_9]]): Matrix9D = Matrix9D(context, data)

  implicit def listToMatrix1D(data: List[Cell[_1]]): Matrix1D = Matrix1D(context, IterablePipe(data))

  implicit def listToMatrix2D(data: List[Cell[_2]]): Matrix2D = Matrix2D(context, IterablePipe(data))

  implicit def listToMatrix3D(data: List[Cell[_3]]): Matrix3D = Matrix3D(context, IterablePipe(data))

  implicit def listToMatrix4D(data: List[Cell[_4]]): Matrix4D = Matrix4D(context, IterablePipe(data))

  implicit def listToMatrix5D(data: List[Cell[_5]]): Matrix5D = Matrix5D(context, IterablePipe(data))

  implicit def listToMatrix6D(data: List[Cell[_6]]): Matrix6D = Matrix6D(context, IterablePipe(data))

  implicit def listToMatrix7D(data: List[Cell[_7]]): Matrix7D = Matrix7D(context, IterablePipe(data))

  implicit def listToMatrix8D(data: List[Cell[_8]]): Matrix8D = Matrix8D(context, IterablePipe(data))

  implicit def listToMatrix9D(data: List[Cell[_9]]): Matrix9D = Matrix9D(context, IterablePipe(data))

  implicit def tupleToMatrix1D[
    V <% Value
  ](
    list: List[(V, Content)]
  ): Matrix1D = Matrix1D(context, IterablePipe(list.map { case (v, c) => Cell(Position(v), c) }))

  implicit def tupleToMatrix2D[
    V <% Value,
    W <% Value
  ](
    list: List[(V, W, Content)]
  ): Matrix2D = Matrix2D(context, IterablePipe(list.map { case (v, w, c) => Cell(Position(v, w), c) }))

  implicit def tupleToMatrix3D[
    V <% Value,
    W <% Value,
    X <% Value
  ](
    list: List[(V, W, X, Content)]
  ): Matrix3D = Matrix3D(context, IterablePipe(list.map { case (v, w, x, c) => Cell(Position(v, w, x), c) }))

  implicit def tupleToMatrix4D[
    V <% Value,
    W <% Value,
    X <% Value,
    Y <% Value
  ](
    list: List[(V, W, X, Y, Content)]
  ): Matrix4D = Matrix4D(context, IterablePipe(list.map { case (v, w, x, y, c) => Cell(Position(v, w, x, y), c) }))

  implicit def tupleToMatrix5D[
    V <% Value,
    W <% Value,
    X <% Value,
    Y <% Value,
    Z <% Value
  ](
    list: List[(V, W, X, Y, Z, Content)]
  ): Matrix5D = Matrix5D(
    context,
    IterablePipe(list.map { case (v, w, x, y, z, c) => Cell(Position(v, w, x, y, z), c) })
  )
  implicit def tupleToMatrix6D[
    T <% Value,
    V <% Value,
    W <% Value,
    X <% Value,
    Y <% Value,
    Z <% Value
  ](
    list: List[(T, V, W, X, Y, Z, Content)]
  ): Matrix6D = Matrix6D(
    context,
    IterablePipe(list.map { case (t, v, w, x, y, z, c) => Cell(Position(t, v, w, x, y, z), c) })
  )

  implicit def tupleToMatrix7D[
    S <% Value,
    T <% Value,
    V <% Value,
    W <% Value,
    X <% Value,
    Y <% Value,
    Z <% Value
  ](
    list: List[(S, T, V, W, X, Y, Z, Content)]
  ): Matrix7D = Matrix7D(
    context,
    IterablePipe(list.map { case (s, t, v, w, x, y, z, c) => Cell(Position(s, t, v, w, x, y, z), c) })
  )

  implicit def tupleToMatrix8D[
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
  ): Matrix8D = Matrix8D(
    context,
    IterablePipe(list.map { case (r, s, t, v, w, x, y, z, c) => Cell(Position(r, s, t, v, w, x, y, z), c) })
  )

  implicit def tupleToMatrix9D[
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
  ): Matrix9D = Matrix9D(
    context,
    IterablePipe(list.map { case (q, r, s, t, v, w, x, y, z, c) => Cell(Position(q, r, s, t, v, w, x, y, z), c) })
  )
}

/** Implements all partition implicits for `context`. */
case class PartitionImplicits(
  context: Context
) extends FwPartitionImplicits[Context.U, Context.E, Context] {
  implicit def toPartitions[
    P <: Nat,
    I : Ordering
  ](
    data: Context.U[(I, Cell[P])]
  ): Partitions[P, I] = Partitions(context, data)
}

/** Implements all position implicits for `context`. */
case class PositionImplicits(
  context: Context
) extends FwPositionImplicits[Context.U, Context.E, Context] {
  implicit def valueToU[
    T <% Value
  ](
    t: T
  ): Context.U[Position[_1]] = IterablePipe(List(Position(t)))

  implicit def listValueToU[
    T <% Value
  ](
    t: List[T]
  ): Context.U[Position[_1]] = IterablePipe(t.map { case v => Position(v) })

  implicit def positionToU[
    T <: Nat
  ](
    t: Position[T]
  ): Context.U[Position[T]] = IterablePipe(List(t))

  implicit def listPositionToU[
    T <: Nat
  ](
    t: List[Position[T]]
  ): Context.U[Position[T]] = IterablePipe(t)

  implicit def toPositions[
    L <: Nat,
    P <: Nat
  ](
    data: Context.U[Position[P]]
  )(implicit
    ev: Diff.Aux[P, _1, L]
  ): Positions[L, P] = Positions(context, data)
}

/** Implements all native operations for `context`. */
case class NativeOperations[
  X
](
  data: Context.U[X]
) extends FwNativeOperations[X, Context.U, Context.E, Context] {
  def ++(other: Context.U[X]): Context.U[X] = data ++ other

  def filter(f: (X) => Boolean): Context.U[X] = data.filter(f)

  def flatMap[Y : ClassTag](f: (X) => TraversableOnce[Y]): Context.U[Y] = data.flatMap(f)

  def map[Y : ClassTag](f: (X) => Y): Context.U[Y] = data.map(f)
}

/** Implements all value operations for `context`. */
case class ValueOperations[
  X
](
  value: Context.E[X]
) extends FwValueOperations[X, Context.U, Context.E, Context] {
  def cross[Y](that: Context.E[Y])(implicit ev: Y =:!= Nothing): Context.E[(X, Y)] = value
    .leftCross(that)
    .map {
      case (x, Some(y)) => (x, y)
      case (x, None) => throw new Exception("Empty ValuePipe not supported")
    }

  def map[Y](f: (X) => Y): Context.E[Y] = value.map(f)
}

