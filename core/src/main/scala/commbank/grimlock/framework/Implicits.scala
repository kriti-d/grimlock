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
import commbank.grimlock.framework.environment.Context
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
trait CellImplicits[C <: Context[C]] {
  /** Converts a `Cell[P]` into a `C#U[Cell[P]]`. */
  implicit def cellToU[P <: Nat](c: Cell[P])(implicit ctx: C): C#U[Cell[P]]

  /** Converts a `List[Cell[P]]` into a `C#U[Cell[P]]`. */
  implicit def listCellToU[P <: Nat](l: List[Cell[P]])(implicit ctx: C): C#U[Cell[P]]
}

/** Defines convenience implicits for dealing with distributed contents. */
trait ContentImplicits[C <: Context[C]] {
  /** Converts a `C#U[Content]` to a `Contents`. */
  implicit def toContents(data: C#U[Content]): Contents[C]

  /** Converts a `C#U[(Position[P], Content)]` to a `IndexedContents`. */
  implicit def toIndexed[P <: Nat](data: C#U[(Position[P], Content)]): IndexedContents[P, C]
}

/** Defines convenience implicits for dealing with distributed strings. */
trait EnvironmentImplicits[C <: Context[C]] {
  /** Converts a `C#U[String]` to a `SaveStringsAsText`. */
  implicit def saveStringsAsText(data: C#U[String]): SaveStringsAsText[C]

  /** Make available native functions of `C#U`. */
  implicit def nativeFunctions[X](data: C#U[X]): NativeOperations[X, C#U]

  /** Make available functions of `C#E`. */
  implicit def valueFunctions[X](value: C#E[X]): ValueOperations[X, C#E]

  /** Convert an `C#E` to a `C#U`. */
  implicit def eToU[X : ClassTag](value: C#E[X])(implicit ctx: C): C#U[X]
}

/** Defines convenience implicits for dealing with matrices. */
trait MatrixImplicits[C <: Context[C]] {
  /** Converts a `C#U[Cell[P]]` to a `Matrix`. */
  implicit def toMatrix[P <: Nat](data: C#U[Cell[P]]): Matrix[P, C]

  /** Conversion from `C#U[Cell[_1]]` to a `Matrix1D`. */
  implicit def toMatrix1D(data: C#U[Cell[_1]]): Matrix1D[C]

  /** Conversion from `C#U[Cell[_2]]` to a `Matrix2D`. */
  implicit def toMatrix2D(data: C#U[Cell[_2]]): Matrix2D[C]

  /** Conversion from `C#U[Cell[_3]]` to a `Matrix3D`. */
  implicit def toMatrix3D(data: C#U[Cell[_3]]): Matrix3D[C]

  /** Conversion from `C#U[Cell[_4]]` to a `Matrix4D`. */
  implicit def toMatrix4D(data: C#U[Cell[_4]]): Matrix4D[C]

  /** Conversion from `C#U[Cell[_5]]` to a `Matrix5D`. */
  implicit def toMatrix5D(data: C#U[Cell[_5]]): Matrix5D[C]

  /** Conversion from `C#U[Cell[_6]]` to a `Matrix6D`. */
  implicit def toMatrix6D(data: C#U[Cell[_6]]): Matrix6D[C]

  /** Conversion from `C#U[Cell[_7]]` to a `Matrix7D`. */
  implicit def toMatrix7D(data: C#U[Cell[_7]]): Matrix7D[C]

  /** Conversion from `C#U[Cell[_8]]` to a `Matrix8D`. */
  implicit def toMatrix8D(data: C#U[Cell[_8]]): Matrix8D[C]

  /** Conversion from `C#U[Cell[_9]]` to a `Matrix9D`. */
  implicit def toMatrix9D(data: C#U[Cell[_9]]): Matrix9D[C]

  /** Converts a `C#U[Cell[P]]` to a `MultiDimensionMatrix`. */
  implicit def toMultiDimensionMatrix[P <: Nat](data: C#U[Cell[P]])(implicit ev: GT[P, _1]): MultiDimensionMatrix[P, C]

  /** Converts a `List[Cell[P]]` to a `Matrix`. */
  implicit def listToMatrix[P <: Nat](data: List[Cell[P]])(implicit ctx: C): Matrix[P, C]

  /** Conversion from `List[Cell[_1]]` to a `Matrix1D`. */
  implicit def listToMatrix1D(data: List[Cell[_1]])(implicit ctx: C): Matrix1D[C]

  /** Conversion from `List[Cell[_2]]` to a `Matrix2D`. */
  implicit def listToMatrix2D(data: List[Cell[_2]])(implicit ctx: C): Matrix2D[C]

  /** Conversion from `List[Cell[_3]]` to a `Matrix3D`. */
  implicit def listToMatrix3D(data: List[Cell[_3]])(implicit ctx: C): Matrix3D[C]

  /** Conversion from `List[Cell[_4]]` to a `Matrix4D`. */
  implicit def listToMatrix4D(data: List[Cell[_4]])(implicit ctx: C): Matrix4D[C]

  /** Conversion from `List[Cell[_5]]` to a `Matrix5D`. */
  implicit def listToMatrix5D(data: List[Cell[_5]])(implicit ctx: C): Matrix5D[C]

  /** Conversion from `List[Cell[_6]]` to a `Matrix6D`. */
  implicit def listToMatrix6D(data: List[Cell[_6]])(implicit ctx: C): Matrix6D[C]

  /** Conversion from `List[Cell[_7]]` to a `Matrix7D`. */
  implicit def listToMatrix7D(data: List[Cell[_7]])(implicit ctx: C): Matrix7D[C]

  /** Conversion from `List[Cell[_8]]` to a `Matrix8D`. */
  implicit def listToMatrix8D(data: List[Cell[_8]])(implicit ctx: C): Matrix8D[C]

  /** Conversion from `List[Cell[_9]]` to a `Matrix9D`. */
  implicit def listToMatrix9D(data: List[Cell[_9]])(implicit ctx: C): Matrix9D[C]

  /** Converts a `List[Cell[P]]` to a `MultiDimensionMatrix`. */
  implicit def listToMultiDimensionMatrix[
    P <: Nat
  ](
    data: List[Cell[P]]
  )(implicit
    ctx: C,
    ev: GT[P, _1]
  ): MultiDimensionMatrix[P, C]

  /** Conversion from `List[(Value, Content)]` to a `Matrix`. */
  implicit def tuple1ToMatrix[V <% Value](list: List[(V, Content)])(implicit ctx: C): Matrix[_1, C]

  /** Conversion from `List[(Value, Content)]` to a `Matrix1D`. */
  implicit def tuple1ToMatrix1D[V <% Value](list: List[(V, Content)])(implicit ctx: C): Matrix1D[C]

  /** Conversion from `List[(Value, Value, Content)]` to a `Matrix`. */
  implicit def tuple2ToMatrix[
    V1 <% Value,
    V2 <% Value
  ](
    list: List[(V1, V2, Content)]
  )(implicit
    ctx: C
  ): Matrix[_2, C]

  /** Conversion from `List[(Value, Value, Content)]` to a `Matrix2D`. */
  implicit def tuple2ToMatrix2D[
    V1 <% Value,
    V2 <% Value
  ](
    list: List[(V1, V2, Content)]
  )(implicit
    ctx: C
  ): Matrix2D[C]

  /** Conversion from `List[(Value, Value, Content)]` to a `Matrix`. */
  implicit def tuple2ToMultiDimensionMatrix[
    V1 <% Value,
    V2 <% Value
  ](
    list: List[(V1, V2, Content)]
  )(implicit
    ctx: C
  ): MultiDimensionMatrix[_2, C]

  /** Conversion from `List[(Value, Value, Value, Content)]` to a `Matrix`. */
  implicit def tuple3ToMatrix[
    V1 <% Value,
    V2 <% Value,
    V3 <% Value
  ](
    list: List[(V1, V2, V3, Content)]
  )(implicit
    ctx: C
  ): Matrix[_3, C]

  /** Conversion from `List[(Value, Value, Value, Content)]` to a `Matrix3D`. */
  implicit def tuple3ToMatrix3D[
    V1 <% Value,
    V2 <% Value,
    V3 <% Value
  ](
    list: List[(V1, V2, V3, Content)]
  )(implicit
    ctx: C
  ): Matrix3D[C]

  /** Conversion from `List[(Value, Value, Value, Content)]` to a `MultiDimensionMatrix`. */
  implicit def tuple3ToMultiDimensionMatrix[
    V1 <% Value,
    V2 <% Value,
    V3 <% Value
  ](
    list: List[(V1, V2, V3, Content)]
  )(implicit
    ctx: C
  ): MultiDimensionMatrix[_3, C]

  /** Conversion from `List[(Value, Value, Value, Value, Content)]` to a `Matrix`. */
  implicit def tuple4ToMatrix[
    V1 <% Value,
    V2 <% Value,
    V3 <% Value,
    V4 <% Value
  ](
    list: List[(V1, V2, V3, V4, Content)]
  )(implicit
    ctx: C
  ): Matrix[_4, C]

  /** Conversion from `List[(Value, Value, Value, Value, Content)]` to a `Matrix4D`. */
  implicit def tuple4ToMatrix4D[
    V1 <% Value,
    V2 <% Value,
    V3 <% Value,
    V4 <% Value
  ](
    list: List[(V1, V2, V3, V4, Content)]
  )(implicit
    ctx: C
  ): Matrix4D[C]

  /** Conversion from `List[(Value, Value, Value, Value, Content)]` to a `MultiDimensionMatrix`. */
  implicit def tuple4ToMultiDimensionMatrix[
    V1 <% Value,
    V2 <% Value,
    V3 <% Value,
    V4 <% Value
  ](
    list: List[(V1, V2, V3, V4, Content)]
  )(implicit
    ctx: C
  ): MultiDimensionMatrix[_4, C]

  /** Conversion from `List[(Value, Value, Value, Value, Value, Content)]` to a `Matrix`. */
  implicit def tuple5ToMatrix[
    V1 <% Value,
    V2 <% Value,
    V3 <% Value,
    V4 <% Value,
    V5 <% Value
  ](
    list: List[(V1, V2, V3, V4, V5, Content)]
  )(implicit
    ctx: C
  ): Matrix[_5, C]

  /** Conversion from `List[(Value, Value, Value, Value, Value, Content)]` to a `Matrix5D`. */
  implicit def tuple5ToMatrix5D[
    V1 <% Value,
    V2 <% Value,
    V3 <% Value,
    V4 <% Value,
    V5 <% Value
  ](
    list: List[(V1, V2, V3, V4, V5, Content)]
  )(implicit
    ctx: C
  ): Matrix5D[C]

  /** Conversion from `List[(Value, Value, Value, Value, Value, Content)]` to a `MultiDimensionMatrix`. */
  implicit def tuple5ToMultiDimensionMatrix[
    V1 <% Value,
    V2 <% Value,
    V3 <% Value,
    V4 <% Value,
    V5 <% Value
  ](
    list: List[(V1, V2, V3, V4, V5, Content)]
  )(implicit
    ctx: C
  ): MultiDimensionMatrix[_5, C]

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
  )(implicit
    ctx: C
  ): Matrix[_6, C]

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
  )(implicit
    ctx: C
  ): Matrix6D[C]

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
  )(implicit
    ctx: C
  ): MultiDimensionMatrix[_6, C]

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
  )(implicit
    ctx: C
  ): Matrix[_7, C]

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
  )(implicit
    ctx: C
  ): Matrix7D[C]

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
  )(implicit
    ctx: C
  ): MultiDimensionMatrix[_7, C]

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
  )(implicit
    ctx: C
  ): Matrix[_8, C]

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
  )(implicit
    ctx: C
  ): Matrix8D[C]

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
  )(implicit
    ctx: C
  ): MultiDimensionMatrix[_8, C]

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
  )(implicit
    ctx: C
  ): Matrix[_9, C]

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
  )(implicit
    ctx: C
  ): Matrix9D[C]

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
  )(implicit
    ctx: C
  ): MultiDimensionMatrix[_9, C]

  /** Conversion from matrix with errors tuple to `MatrixWithParseErrors`. */
  implicit def tupleToParseErrors[
    P <: Nat
  ](
    t: (C#U[Cell[P]], C#U[String])
  ): MatrixWithParseErrors[P, C#U] = MatrixWithParseErrors(t._1, t._2)
}

/** Defines convenience implicits for dealing with distributed partitions. */
trait PartitionImplicits[C <: Context[C]] {
  /** Conversion from `C#U[(I, Cell[P])]` to a `Partitions`. */
  implicit def toPartitions[P <: Nat, I : Ordering](data: C#U[(I, Cell[P])]): Partitions[P, I, C]
}

/** Defines convenience implicits for dealing with distributed positions. */
trait PositionImplicits[C <: Context[C]] {
  /** Converts a `Value` to a `C#U[Position[_1]]`. */
  implicit def valueToU[V <% Value](v: V)(implicit ctx: C): C#U[Position[_1]]

  /** Converts a `List[Value]` to a `C#U[Position[_1]]`. */
  implicit def listValueToU[V <% Value](l: List[V])(implicit ctx: C): C#U[Position[_1]]

  /** Converts a `Position[P]` to a `C#U[Position[P]]`. */
  implicit def positionToU[P <: Nat](p: Position[P])(implicit ctx: C): C#U[Position[P]]

  /** Converts a `List[Position[P]]` to a `C#U[Position[P]]`. */
  implicit def listPositionToU[P <: Nat](l: List[Position[P]])(implicit ctx: C): C#U[Position[P]]

  /** Converts a `C#U[Position[P]]` to a `Positions`. */
  implicit def toPositions[P <: Nat](data: C#U[Position[P]]): Positions[P, C]

  /** Converts a `(T, Cell.Predicate[P])` to a `List[(C#U[Position[S]], Cell.Predicate[P])]`. */
  implicit def predicateToU[
    P <: Nat,
    S <: Nat,
    T <% C#U[Position[S]]
  ](
    t: (T, Cell.Predicate[P])
  )(implicit
    ctx: C
  ): List[(C#U[Position[S]], Cell.Predicate[P])] = {
    val u: C#U[Position[S]] = t._1

    List((u, t._2))
  }

  /** Converts a `List[(T, Cell.Predicate[P])]` to a `List[(C#U[Position[S]], Cell.Predicate[P])]`. */
  implicit def listPredicateToU[
    P <: Nat,
    S <: Nat,
    T <% C#U[Position[S]]
  ](
    l: List[(T, Cell.Predicate[P])]
  )(implicit
    ctx: C
  ): List[(C#U[Position[S]], Cell.Predicate[P])] = l
    .map { case (i, p) =>
      val u: C#U[Position[S]] = i

      (u, p)
    }
}

/** Capture all implicits together. */
trait Implicits[C <: Context[C]] {
  /** Cell related implicits. */
  val cell: CellImplicits[C]

  /** Content related implicits. */
  val content: ContentImplicits[C]

  /** Environment related implicits. */
  val environment: EnvironmentImplicits[C]

  /** Matrix related implicits. */
  val matrix: MatrixImplicits[C]

  /** Partition related implicits. */
  val partition: PartitionImplicits[C]

  /** Position related implicits. */
  val position: PositionImplicits[C]
}

