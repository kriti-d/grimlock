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

package commbank.grimlock.framework.environment.implicits

import commbank.grimlock.framework.{
  Cell,
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
  MatrixWithParseErrors,
  MultiDimensionMatrix,
  SaveStringsAsText
}
import commbank.grimlock.framework.content.{ Content, Contents, IndexedContents }
import commbank.grimlock.framework.encoding.Value
import commbank.grimlock.framework.partition.Partitions
import commbank.grimlock.framework.position.{ Position, Positions }

import scala.reflect.ClassTag

import shapeless.{ =:!=, Nat }
import shapeless.nat.{ _1, _2, _3, _4, _5, _6, _7, _8, _9 }
import shapeless.ops.nat.GT

/** Defines standard functional operations for dealing with distributed lists. */
trait NativeOperations[X, U[_]] {
  /** Return the union of this `U` and `other`. */
  def ++(other: U[X]): U[X]

  /** Keep only items that satisfy the predicate `f`. */
  def filter(f: (X) => Boolean): U[X]

  /** Apply function `f`, then flatten the results. */
  def flatMap[Y : ClassTag](f: (X) => TraversableOnce[Y]): U[Y]

  /** Map each element using the function `f`. */
  def map[Y : ClassTag](f: (X) => Y): U[Y]
}

/** Defines operations for dealing with user defined values. */
trait ValueOperations[X, E[_]] {
  /** Cross this value with `that`. */
  def cross[Y](that: E[Y])(implicit ev: Y =:!= Nothing): E[(X, Y)]

  /** Apply the function `f` to the value. */
  def map[Y](f: (X) => Y): E[Y]
}

/** Defines convenience implicits for dealing with distributed cells. */
trait CellImplicits[U[_]] {
  /** Converts a `Cell[P]` into a `U[Cell[P]]`. */
  implicit def cellToU[P <: Nat](c: Cell[P]): U[Cell[P]]

  /** Converts a `List[Cell[P]]` into a `U[Cell[P]]`. */
  implicit def listCellToU[P <: Nat](l: List[Cell[P]]): U[Cell[P]]
}

/** Defines convenience implicits for dealing with distributed contents. */
trait ContentImplicits[U[_]] {
  /** Converts a `U[Content]` to a `Contents`. */
  implicit def toContents(data: U[Content]): Contents[U]

  /** Converts a `U[(Position[P], Content)]` to a `IndexedContents`. */
  implicit def toIndexed[P <: Nat](data: U[(Position[P], Content)]): IndexedContents[P, U]
}

/** Defines convenience implicits for dealing with distributed strings. */
trait EnvironmentImplicits[U[_], E[_]] {
  /** Converts a `U[String]` to a `SaveStringsAsText`. */
  implicit def saveStringsAsText(data: U[String]): SaveStringsAsText[U]

  /** Make available native functions of `U`. */
  implicit def nativeFunctions[X](data: U[X]): NativeOperations[X, U]

  /** Make available functions of `E`. */
  implicit def valueFunctions[X](value: E[X]): ValueOperations[X, E]

  /** Convert an `E` to a `U`. */
  implicit def eToU[X : ClassTag](value: E[X]): U[X]
}

/** Defines convenience implicits for dealing with matrices. */
trait MatrixImplicits[U[_], E[_]] {
  /** Converts a `U[Cell[P]]` to a `Matrix`. */
  implicit def toMatrix[P <: Nat](data: U[Cell[P]]): Matrix[P, U, E]

  /** Conversion from `U[Cell[_1]]` to a `Matrix1D`. */
  implicit def toMatrix1D(data: U[Cell[_1]]): Matrix1D[U, E]

  /** Conversion from `U[Cell[_2]]` to a `Matrix2D`. */
  implicit def toMatrix2D(data: U[Cell[_2]]): Matrix2D[U, E]

  /** Conversion from `U[Cell[_3]]` to a `Matrix3D`. */
  implicit def toMatrix3D(data: U[Cell[_3]]): Matrix3D[U, E]

  /** Conversion from `U[Cell[_4]]` to a `Matrix4D`. */
  implicit def toMatrix4D(data: U[Cell[_4]]): Matrix4D[U, E]

  /** Conversion from `U[Cell[_5]]` to a `Matrix5D`. */
  implicit def toMatrix5D(data: U[Cell[_5]]): Matrix5D[U, E]

  /** Conversion from `U[Cell[_6]]` to a `Matrix6D`. */
  implicit def toMatrix6D(data: U[Cell[_6]]): Matrix6D[U, E]

  /** Conversion from `U[Cell[_7]]` to a `Matrix7D`. */
  implicit def toMatrix7D(data: U[Cell[_7]]): Matrix7D[U, E]

  /** Conversion from `U[Cell[_8]]` to a `Matrix8D`. */
  implicit def toMatrix8D(data: U[Cell[_8]]): Matrix8D[U, E]

  /** Conversion from `U[Cell[_9]]` to a `Matrix9D`. */
  implicit def toMatrix9D(data: U[Cell[_9]]): Matrix9D[U, E]

  /** Converts a `U[Cell[P]]` to a `MultiDimensionMatrix`. */
  implicit def toMultiDimensionMatrix[P <: Nat](data: U[Cell[P]])(implicit ev: GT[P, _1]): MultiDimensionMatrix[P, U, E]

  /** Converts a `List[Cell[P]]` to a `Matrix`. */
  implicit def listToMatrix[P <: Nat](data: List[Cell[P]]): Matrix[P, U, E]

  /** Conversion from `List[Cell[_1]]` to a `Matrix1D`. */
  implicit def listToMatrix1D(data: List[Cell[_1]]): Matrix1D[U, E]

  /** Conversion from `List[Cell[_2]]` to a `Matrix2D`. */
  implicit def listToMatrix2D(data: List[Cell[_2]]): Matrix2D[U, E]

  /** Conversion from `List[Cell[_3]]` to a `Matrix3D`. */
  implicit def listToMatrix3D(data: List[Cell[_3]]): Matrix3D[U, E]

  /** Conversion from `List[Cell[_4]]` to a `Matrix4D`. */
  implicit def listToMatrix4D(data: List[Cell[_4]]): Matrix4D[U, E]

  /** Conversion from `List[Cell[_5]]` to a `Matrix5D`. */
  implicit def listToMatrix5D(data: List[Cell[_5]]): Matrix5D[U, E]

  /** Conversion from `List[Cell[_6]]` to a `Matrix6D`. */
  implicit def listToMatrix6D(data: List[Cell[_6]]): Matrix6D[U, E]

  /** Conversion from `List[Cell[_7]]` to a `Matrix7D`. */
  implicit def listToMatrix7D(data: List[Cell[_7]]): Matrix7D[U, E]

  /** Conversion from `List[Cell[_8]]` to a `Matrix8D`. */
  implicit def listToMatrix8D(data: List[Cell[_8]]): Matrix8D[U, E]

  /** Conversion from `List[Cell[_9]]` to a `Matrix9D`. */
  implicit def listToMatrix9D(data: List[Cell[_9]]): Matrix9D[U, E]

  /** Converts a `List[Cell[P]]` to a `MultiDimensionMatrix`. */
  implicit def listToMultiDimensionMatrix[
    P <: Nat
  ](
    data: List[Cell[P]]
  )(implicit
    ev: GT[P, _1]
  ): MultiDimensionMatrix[P, U, E]

  /** Conversion from `List[(Value, Content)]` to a `Matrix`. */
  implicit def tuple1ToMatrix[V <% Value](list: List[(V, Content)]): Matrix[_1, U, E]

  /** Conversion from `List[(Value, Content)]` to a `Matrix1D`. */
  implicit def tuple1ToMatrix1D[V <% Value](list: List[(V, Content)]): Matrix1D[U, E]

  /** Conversion from `List[(Value, Value, Content)]` to a `Matrix`. */
  implicit def tuple2ToMatrix[V1 <% Value, V2 <% Value](list: List[(V1, V2, Content)]): Matrix[_2, U, E]

  /** Conversion from `List[(Value, Value, Content)]` to a `Matrix2D`. */
  implicit def tuple2ToMatrix2D[V1 <% Value, V2 <% Value](list: List[(V1, V2, Content)]): Matrix2D[U, E]

  /** Conversion from `List[(Value, Value, Content)]` to a `Matrix`. */
  implicit def tuple2ToMultiDimensionMatrix[
    V1 <% Value,
    V2 <% Value
  ](
    list: List[(V1, V2, Content)]
  ): MultiDimensionMatrix[_2, U, E]

  /** Conversion from `List[(Value, Value, Value, Content)]` to a `Matrix`. */
  implicit def tuple3ToMatrix[
    V1 <% Value,
    V2 <% Value,
    V3 <% Value
  ](
    list: List[(V1, V2, V3, Content)]
  ): Matrix[_3, U, E]

  /** Conversion from `List[(Value, Value, Value, Content)]` to a `Matrix3D`. */
  implicit def tuple3ToMatrix3D[
    V1 <% Value,
    V2 <% Value,
    V3 <% Value
  ](
    list: List[(V1, V2, V3, Content)]
  ): Matrix3D[U, E]

  /** Conversion from `List[(Value, Value, Value, Content)]` to a `MultiDimensionMatrix`. */
  implicit def tuple3ToMultiDimensionMatrix[
    V1 <% Value,
    V2 <% Value,
    V3 <% Value
  ](
    list: List[(V1, V2, V3, Content)]
  ): MultiDimensionMatrix[_3, U, E]

  /** Conversion from `List[(Value, Value, Value, Value, Content)]` to a `Matrix`. */
  implicit def tuple4ToMatrix[
    V1 <% Value,
    V2 <% Value,
    V3 <% Value,
    V4 <% Value
  ](
    list: List[(V1, V2, V3, V4, Content)]
  ): Matrix[_4, U, E]

  /** Conversion from `List[(Value, Value, Value, Value, Content)]` to a `Matrix4D`. */
  implicit def tuple4ToMatrix4D[
    V1 <% Value,
    V2 <% Value,
    V3 <% Value,
    V4 <% Value
  ](
    list: List[(V1, V2, V3, V4, Content)]
  ): Matrix4D[U, E]

  /** Conversion from `List[(Value, Value, Value, Value, Content)]` to a `MultiDimensionMatrix`. */
  implicit def tuple4ToMultiDimensionMatrix[
    V1 <% Value,
    V2 <% Value,
    V3 <% Value,
    V4 <% Value
  ](
    list: List[(V1, V2, V3, V4, Content)]
  ): MultiDimensionMatrix[_4, U, E]

  /** Conversion from `List[(Value, Value, Value, Value, Value, Content)]` to a `Matrix`. */
  implicit def tuple5ToMatrix[
    V1 <% Value,
    V2 <% Value,
    V3 <% Value,
    V4 <% Value,
    V5 <% Value
  ](
    list: List[(V1, V2, V3, V4, V5, Content)]
  ): Matrix[_5, U, E]

  /** Conversion from `List[(Value, Value, Value, Value, Value, Content)]` to a `Matrix5D`. */
  implicit def tuple5ToMatrix5D[
    V1 <% Value,
    V2 <% Value,
    V3 <% Value,
    V4 <% Value,
    V5 <% Value
  ](
    list: List[(V1, V2, V3, V4, V5, Content)]
  ): Matrix5D[U, E]

  /** Conversion from `List[(Value, Value, Value, Value, Value, Content)]` to a `MultiDimensionMatrix`. */
  implicit def tuple5ToMultiDimensionMatrix[
    V1 <% Value,
    V2 <% Value,
    V3 <% Value,
    V4 <% Value,
    V5 <% Value
  ](
    list: List[(V1, V2, V3, V4, V5, Content)]
  ): MultiDimensionMatrix[_5, U, E]

  /** Conversion from `List[(Value, Value, Value, Value, Value, Value, Content)]` to a `Matrix`. */
  implicit def tuple6ToMatrix[
    V1 <% Value,
    V2 <% Value,
    V3 <% Value,
    V4 <% Value,
    V5 <% Value,
    V6 <% Value
  ](
    list: List[(V1, V2, V3, V4, V5, V6, Content)]
  ): Matrix[_6, U, E]

  /** Conversion from `List[(Value, Value, Value, Value, Value, Value, Content)]` to a `Matrix6D`. */
  implicit def tuple6ToMatrix6D[
    V1 <% Value,
    V2 <% Value,
    V3 <% Value,
    V4 <% Value,
    V5 <% Value,
    V6 <% Value
  ](
    list: List[(V1, V2, V3, V4, V5, V6, Content)]
  ): Matrix6D[U, E]

  /** Conversion from `List[(Value, Value, Value, Value, Value, Value, Content)]` to a `MultiDimensionMatrix`. */
  implicit def tuple6ToMultiDimensionMatrix[
    V1 <% Value,
    V2 <% Value,
    V3 <% Value,
    V4 <% Value,
    V5 <% Value,
    V6 <% Value
  ](
    list: List[(V1, V2, V3, V4, V5, V6, Content)]
  ): MultiDimensionMatrix[_6, U, E]

  /** Conversion from `List[(Value, Value, Value, Value, Value, Value, Value, Content)]` to a `Matrix`. */
  implicit def tuple7ToMatrix[
    V1 <% Value,
    V2 <% Value,
    V3 <% Value,
    V4 <% Value,
    V5 <% Value,
    V6 <% Value,
    V7 <% Value
  ](
    list: List[(V1, V2, V3, V4, V5, V6, V7, Content)]
  ): Matrix[_7, U, E]

  /** Conversion from `List[(Value, Value, Value, Value, Value, Value, Value, Content)]` to a `Matrix7D`. */
  implicit def tuple7ToMatrix7D[
    V1 <% Value,
    V2 <% Value,
    V3 <% Value,
    V4 <% Value,
    V5 <% Value,
    V6 <% Value,
    V7 <% Value
  ](
    list: List[(V1, V2, V3, V4, V5, V6, V7, Content)]
  ): Matrix7D[U, E]

  /**
   * Conversion from `List[(Value, Value, Value, Value, Value, Value, Value, Content)]` to a `MultiDimensionMatrix`.
   */
  implicit def tuple7ToMultiDimensionMatrix[
    V1 <% Value,
    V2 <% Value,
    V3 <% Value,
    V4 <% Value,
    V5 <% Value,
    V6 <% Value,
    V7 <% Value
  ](
    list: List[(V1, V2, V3, V4, V5, V6, V7, Content)]
  ): MultiDimensionMatrix[_7, U, E]

  /** Conversion from `List[(Value, Value, Value, Value, Value, Value, Value, Value, Content)]` to a `Matrix`. */
  implicit def tuple8ToMatrix[
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
  ): Matrix[_8, U, E]

  /** Conversion from `List[(Value, Value, Value, Value, Value, Value, Value, Value, Content)]` to a `Matrix8D`. */
  implicit def tuple8ToMatrix8D[
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
  ): Matrix8D[U, E]

  /**
   * Conversion from `List[(Value, Value, Value, Value, Value, Value, Value, Value, Content)]` to a
   * `MultiDimensionMatrix`.
   */
  implicit def tuple8ToMultiDimensionMatrix[
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
  ): MultiDimensionMatrix[_8, U, E]

  /**
   * Conversion from `List[(Value, Value, Value, Value, Value, Value, Value, Value, Value, Content)]` to a `Matrix`.
   */
  implicit def tuple9ToMatrix[
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
  ): Matrix[_9, U, E]

  /**
   * Conversion from `List[(Value, Value, Value, Value, Value, Value, Value, Value, Value, Content)]` to a `Matrix9D`.
   */
  implicit def tuple9ToMatrix9D[
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
  ): Matrix9D[U, E]

  /**
   * Conversion from `List[(Value, Value, Value, Value, Value, Value, Value, Value, Value, Content)]` to a
   * `MultiDimensionMatrix`.
   */
  implicit def tuple9ToMultiDimensionMatrix[
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
  ): MultiDimensionMatrix[_9, U, E]

  /** Conversion from matrix with errors tuple to `MatrixWithParseErrors`. */
  implicit def tupleToParseErrors[
    P <: Nat
  ](
    t: (U[Cell[P]], U[String])
  ): MatrixWithParseErrors[P, U] = MatrixWithParseErrors(t._1, t._2)
}

/** Defines convenience implicits for dealing with distributed partitions. */
trait PartitionImplicits[U[_]] {
  /** Conversion from `U[(I, Cell[P])]` to a `Partitions`. */
  implicit def toPartitions[P <: Nat, I : Ordering](data: U[(I, Cell[P])]): Partitions[P, I, U]
}

/** Defines convenience implicits for dealing with distributed positions. */
trait PositionImplicits[U[_]] {
  /** Converts a `Value` to a `U[Position[_1]]`. */
  implicit def valueToU[V <% Value](v: V): U[Position[_1]]

  /** Converts a `List[Value]` to a `U[Position[_1]]`. */
  implicit def listValueToU[V <% Value](l: List[V]): U[Position[_1]]

  /** Converts a `Position[P]` to a `U[Position[P]]`. */
  implicit def positionToU[P <: Nat](p: Position[P]): U[Position[P]]

  /** Converts a `List[Position[P]]` to a `U[Position[P]]`. */
  implicit def listPositionToU[P <: Nat](l: List[Position[P]]): U[Position[P]]

  /** Converts a `U[Position[P]]` to a `Positions`. */
  implicit def toPositions[P <: Nat](data: U[Position[P]]): Positions[P, U]

  /** Converts a `(T, Cell.Predicate[P])` to a `List[(U[Position[S]], Cell.Predicate[P])]`. */
  implicit def predicateToU[
    P <: Nat,
    S <: Nat,
    T <% U[Position[S]]
  ](
    t: (T, Cell.Predicate[P])
  ): List[(U[Position[S]], Cell.Predicate[P])] = {
    val u: U[Position[S]] = t._1

    List((u, t._2))
  }

  /** Converts a `List[(T, Cell.Predicate[P])]` to a `List[(U[Position[S]], Cell.Predicate[P])]`. */
  implicit def listPredicateToU[
    P <: Nat,
    S <: Nat,
    T <% U[Position[S]]
  ](
    l: List[(T, Cell.Predicate[P])]
  ): List[(U[Position[S]], Cell.Predicate[P])] = l
    .map { case (i, p) =>
      val u: U[Position[S]] = i

      (u, p)
    }
}

/** Capture all implicits together. */
trait Implicits[U[_], E[_]] {
  /** Cell related implicits. */
  val cell: CellImplicits[U]

  /** Content related implicits. */
  val content: ContentImplicits[U]

  /** Environment related implicits. */
  val environment: EnvironmentImplicits[U, E]

  /** Matrix related implicits. */
  val matrix: MatrixImplicits[U, E]

  /** Partition related implicits. */
  val partition: PartitionImplicits[U]

  /** Position related implicits. */
  val position: PositionImplicits[U]
}

