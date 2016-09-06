// Copyright 2016 Commonwealth Bank of Australia
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

package commbank.grimlock.scalding.environment

import commbank.grimlock.framework.{ Cell, MatrixWithParseErrors, Type }
import commbank.grimlock.framework.environment.{
  Context => FwContext,
  DistributedData => FwDistributedData,
  Environment => FwEnvironment,
  UserData => FwUserData
}
import commbank.grimlock.framework.content.Content
import commbank.grimlock.framework.encoding.Value
import commbank.grimlock.framework.position.Position

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
  Types
}
import commbank.grimlock.scalding.content.{ Contents, IndexedContents }
import commbank.grimlock.scalding.partition.Partitions
import commbank.grimlock.scalding.position.Positions

import cascading.flow.FlowDef

import com.twitter.scalding.{ Config, Mode }
import com.twitter.scalding.typed.{ IterablePipe, TypedPipe, ValuePipe }

import shapeless.Nat
import shapeless.nat.{ _1, _2, _3, _4, _5, _6, _7, _8, _9 }
import shapeless.ops.nat.Diff

/**
 * Scalding operating context state.
 *
 * @param flow   The job `FlowDef`.
 * @param mode   The job `Mode`.
 * @param config The job `Config`.
 */
case class Context(flow: FlowDef, mode: Mode, config: Config) extends FwContext {
  /** Implicit FlowDef for write operations. */
  implicit val implicitFlow = flow

  /** Implicit Mode for write and Execution.waitFor operations. */
  implicit val implicitMode = mode

  /** Implicit Config for Execution.waitFor operations. */
  implicit val implicitConfig = config
}

/** Companion object to `Context` with additional constructors and implicit. */
object Context {
  def apply()(implicit config: Config, flow: FlowDef, mode: Mode): Context = Context(flow, mode, config)

  /** Converts a `TypedPipe[Content]` to a `Contents`. */
  implicit def pipeToContents(data: TypedPipe[Content]): Contents = Contents(data)

  /** Converts a `TypedPipe[(Position[P], Content)]` to a `IndexedContents`. */
  implicit def pipeToIndexed[
    P <: Nat
  ](
    data: TypedPipe[(Position[P], Content)]
  ): IndexedContents[P] = IndexedContents(data)

  /** Converts a `Cell[P]` into a `TypedPipe[Cell[P]]`. */
  implicit def cellToPipe[P <: Nat](t: Cell[P]): TypedPipe[Cell[P]] = IterablePipe(List(t))

  /** Converts a `List[Cell[P]]` into a `TypedPipe[Cell[P]]`. */
  implicit def listCellToPipe[P <: Nat](t: List[Cell[P]]): TypedPipe[Cell[P]] = IterablePipe(t)

  /** Conversion from `TypedPipe[Cell[_1]]` to a Scalding `Matrix1D`. */
  implicit def pipeToMatrix1(data: TypedPipe[Cell[_1]]): Matrix1D = Matrix1D(data)

  /** Conversion from `TypedPipe[Cell[_2]]` to a Scalding `Matrix2D`. */
  implicit def pipeToMatrix2(data: TypedPipe[Cell[_2]]): Matrix2D = Matrix2D(data)

  /** Conversion from `TypedPipe[Cell[_3]]` to a Scalding `Matrix3D`. */
  implicit def pipeToMatrix3(data: TypedPipe[Cell[_3]]): Matrix3D = Matrix3D(data)

  /** Conversion from `TypedPipe[Cell[_4]]` to a Scalding `Matrix4D`. */
  implicit def pipeToMatrix4(data: TypedPipe[Cell[_4]]): Matrix4D = Matrix4D(data)

  /** Conversion from `TypedPipe[Cell[_5]]` to a Scalding `Matrix5D`. */
  implicit def pipeToMatrix5(data: TypedPipe[Cell[_5]]): Matrix5D = Matrix5D(data)

  /** Conversion from `TypedPipe[Cell[_6]]` to a Scalding `Matrix6D`. */
  implicit def pipeToMatrix6(data: TypedPipe[Cell[_6]]): Matrix6D = Matrix6D(data)

  /** Conversion from `TypedPipe[Cell[_7]]` to a Scalding `Matrix7D`. */
  implicit def pipeToMatrix7(data: TypedPipe[Cell[_7]]): Matrix7D = Matrix7D(data)

  /** Conversion from `TypedPipe[Cell[_8]]` to a Scalding `Matrix8D`. */
  implicit def pipeToMatrix8(data: TypedPipe[Cell[_8]]): Matrix8D = Matrix8D(data)

  /** Conversion from `TypedPipe[Cell[_9]]` to a Scalding `Matrix9D`. */
  implicit def pipeToMatrix9(data: TypedPipe[Cell[_9]]): Matrix9D = Matrix9D(data)

  /** Conversion from `List[Cell[_1]]` to a Scalding `Matrix1D`. */
  implicit def listToScaldingMatrix1(data: List[Cell[_1]]): Matrix1D = Matrix1D(IterablePipe(data))

  /** Conversion from `List[Cell[_2]]` to a Scalding `Matrix2D`. */
  implicit def listToScaldingMatrix2(data: List[Cell[_2]]): Matrix2D = Matrix2D(IterablePipe(data))

  /** Conversion from `List[Cell[_3]]` to a Scalding `Matrix3D`. */
  implicit def listToScaldingMatrix3(data: List[Cell[_3]]): Matrix3D = Matrix3D(IterablePipe(data))

  /** Conversion from `List[Cell[_4]]` to a Scalding `Matrix4D`. */
  implicit def listToScaldingMatrix4(data: List[Cell[_4]]): Matrix4D = Matrix4D(IterablePipe(data))

  /** Conversion from `List[Cell[_5]]` to a Scalding `Matrix5D`. */
  implicit def listToScaldingMatrix5(data: List[Cell[_5]]): Matrix5D = Matrix5D(IterablePipe(data))

  /** Conversion from `List[Cell[_6]]` to a Scalding `Matrix6D`. */
  implicit def listToScaldingMatrix6(data: List[Cell[_6]]): Matrix6D = Matrix6D(IterablePipe(data))

  /** Conversion from `List[Cell[_7]]` to a Scalding `Matrix7D`. */
  implicit def listToScaldingMatrix7(data: List[Cell[_7]]): Matrix7D = Matrix7D(IterablePipe(data))

  /** Conversion from `List[Cell[_8]]` to a Scalding `Matrix8D`. */
  implicit def listToScaldingMatrix8(data: List[Cell[_8]]): Matrix8D = Matrix8D(IterablePipe(data))

  /** Conversion from `List[Cell[_9]]` to a Scalding `Matrix9D`. */
  implicit def listToScaldingMatrix9(data: List[Cell[_9]]): Matrix9D = Matrix9D(IterablePipe(data))

  /** Conversion from `List[(Value, Content)]` to a Scalding `Matrix1D`. */
  implicit def tupleToScaldingMatrix1[V <% Value](list: List[(V, Content)]): Matrix1D = Matrix1D(
    IterablePipe(list.map { case (v, c) => Cell(Position(v), c) })
  )

  /** Conversion from `List[(Value, Value, Content)]` to a Scalding `Matrix2D`. */
  implicit def tupleToScaldingMatrix2[V <% Value, W <% Value](list: List[(V, W, Content)]): Matrix2D = Matrix2D(
    IterablePipe(list.map { case (v, w, c) => Cell(Position(v, w), c) })
  )

  /** Conversion from `List[(Value, Value, Value, Content)]` to a Scalding `Matrix3D`. */
  implicit def tupleToScaldingMatrix3[
    V <% Value,
    W <% Value,
    X <% Value
  ](
    list: List[(V, W, X, Content)]
  ): Matrix3D = Matrix3D(IterablePipe(list.map { case (v, w, x, c) => Cell(Position(v, w, x), c) }))

  /** Conversion from `List[(Value, Value, Value, Value, Content)]` to a Scalding `Matrix4D`. */
  implicit def tupleToScaldingMatrix4[
    V <% Value,
    W <% Value,
    X <% Value,
    Y <% Value
  ](
    list: List[(V, W, X, Y, Content)]
  ): Matrix4D = Matrix4D(IterablePipe(list.map { case (v, w, x, y, c) => Cell(Position(v, w, x, y), c) }))

  /** Conversion from `List[(Value, Value, Value, Value, Value, Content)]` to a Scalding `Matrix5D`. */
  implicit def tupleToScaldingMatrix5[
    V <% Value,
    W <% Value,
    X <% Value,
    Y <% Value,
    Z <% Value
  ](
    list: List[(V, W, X, Y, Z, Content)]
  ): Matrix5D = Matrix5D(IterablePipe(list.map { case (v, w, x, y, z, c) => Cell(Position(v, w, x, y, z), c) }))

  /** Conversion from `List[(Value, Value, Value, Value, Value, Value, Content)]` to a Scalding `Matrix6D`. */
  implicit def tupleToScaldingMatrix6[
    T <% Value,
    V <% Value,
    W <% Value,
    X <% Value,
    Y <% Value,
    Z <% Value
  ](
    list: List[(T, V, W, X, Y, Z, Content)]
  ): Matrix6D = Matrix6D(IterablePipe(list.map { case (t, v, w, x, y, z, c) => Cell(Position(t, v, w, x, y, z), c) }))

  /** Conversion from `List[(Value, Value, Value, Value, Value, Value, Value, Content)]` to a Scalding `Matrix7D`. */
  implicit def tupleToScaldingMatrix7[
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
    IterablePipe(list.map { case (s, t, v, w, x, y, z, c) => Cell(Position(s, t, v, w, x, y, z), c) })
  )

  /**
   * Conversion from `List[(Value, Value, Value, Value, Value, Value, Value, Value, Content)]` to a
   * Scalding `Matrix8D`.
   */
  implicit def tupleToScaldingMatrix8[
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
    IterablePipe(list.map { case (r, s, t, v, w, x, y, z, c) => Cell(Position(r, s, t, v, w, x, y, z), c) })
  )

  /**
   * Conversion from `List[(Value, Value, Value, Value, Value, Value, Value, Value, Value, Content)]` to a
   * Scalding `Matrix9D`.
   */
  implicit def tupleToScaldingMatrix9[
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
    IterablePipe(list.map { case (q, r, s, t, v, w, x, y, z, c) => Cell(Position(q, r, s, t, v, w, x, y, z), c) })
  )

  /** Converts a `(T, Cell.Predicate[P])` to a `List[(TypedPipe[Position[S]], Cell.Predicate[P])]`. */
  implicit def predicateToScaldingList[
    P <: Nat,
    S <: Nat,
    T <% TypedPipe[Position[S]]
  ](
    t: (T, Cell.Predicate[P])
  ): List[(TypedPipe[Position[S]], Cell.Predicate[P])] = {
    val pipe: TypedPipe[Position[S]] = t._1

    List((pipe, t._2))
  }

  /** Converts a `List[(T, Cell.Predicate[P])]` to a `List[(TypedPipe[Position[S]], Cell.Predicate[P])]`. */
  implicit def listPredicateToScaldingList[
    P <: Nat,
    S <: Nat,
    T <% TypedPipe[Position[S]]
  ](
    t: List[(T, Cell.Predicate[P])]
  ): List[(TypedPipe[Position[S]], Cell.Predicate[P])] = t
    .map { case (i, p) =>
      val pipe: TypedPipe[Position[S]] = i

      (pipe, p)
    }

  /** Conversion from matrix with errors tuple to `MatrixWithParseErrors`. */
  implicit def tupleToScaldingParseErrors[
    P <: Nat
  ](
    t: (TypedPipe[Cell[P]], TypedPipe[String])
  ): MatrixWithParseErrors[P, TypedPipe] = MatrixWithParseErrors(t._1, t._2)

  /** Conversion from `TypedPipe[(I, Cell[P])]` to a Scalding `Partitions`. */
  implicit def pipeToPartitions[
    P <: Nat,
    I : Ordering
  ](
    data: TypedPipe[(I, Cell[P])]
  ): Partitions[P, I] = Partitions(data)

  /** Converts a `TypedPipe[Position[P]]` to a `Positions`. */
  implicit def pipeToPositions[
    L <: Nat,
    P <: Nat
  ](
    data: TypedPipe[Position[P]]
  )(implicit
    ev: Diff.Aux[P, _1, L]
  ): Positions[L, P] = Positions(data)

  /** Converts a `Value` to a `TypedPipe[Position[_1]]`. */
  implicit def valueToPipe[T <% Value](t: T): TypedPipe[Position[_1]] = IterablePipe(List(Position(t)))

  /** Converts a `List[Value]` to a `TypedPipe[Position[_1]]`. */
  implicit def listValueToPipe[T <% Value](t: List[T]): TypedPipe[Position[_1]] = IterablePipe(t.map(Position(_)))

  /** Converts a `Position[T]` to a `TypedPipe[Position[T]]`. */
  implicit def positionToPipe[T <: Nat](t: Position[T]): TypedPipe[Position[T]] = IterablePipe(List(t))

  /** Converts a `List[Position[T]]` to a `TypedPipe[Position[T]]`. */
  implicit def listPositionToPipe[T <: Nat](t: List[Position[T]]): TypedPipe[Position[T]] = IterablePipe(t)

  /** Conversion from `TypedPipe[(Position[P], Type)]` to a `Types`. */
  implicit def pipeToTypes[P <: Nat](data: TypedPipe[(Position[P], Type)]): Types[P] = Types(data)
}

trait DistributedData extends FwDistributedData {
  type U[X] = TypedPipe[X]
}

trait UserData extends FwUserData {
  type E[X] = ValuePipe[X]
}

trait Environment extends FwEnvironment {
  type C = Context
}

