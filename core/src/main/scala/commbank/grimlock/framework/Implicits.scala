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
  SaveStringsAsText
}
import commbank.grimlock.framework.content.{ Content, Contents, IndexedContents }
import commbank.grimlock.framework.encoding.Value
import commbank.grimlock.framework.environment.Context
import commbank.grimlock.framework.partition.Partitions
import commbank.grimlock.framework.position.{ Position, Positions }
import commbank.grimlock.framework.utility.=:!=

import scala.reflect.ClassTag

import shapeless.Nat
import shapeless.nat.{ _1, _2, _3, _4, _5, _6, _7, _8, _9 }
import shapeless.ops.nat.Diff

/** Defines standard functional operations for dealing with distributed lists. */
trait NativeOperations[X, U[_], E[_], C <: Context[U, E]] {
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
trait ValueOperations[X, U[_], E[_], C <: Context[U, E]] {
  /** Cross this value with `that`. */
  def cross[Y](that: E[Y])(implicit ev: Y =:!= Nothing): E[(X, Y)]

  /** Apply the function `f` to the value. */
  def map[Y](f: (X) => Y): E[Y]
}

/** Defines convenience implicits for dealing with distributed cells. */
trait CellImplicits[U[_], E[_], C <: Context[U, E]] {
  /** Converts a `Cell[P]` into a `U[Cell[P]]`. */
  implicit def cellToU[P <: Nat](t: Cell[P]): U[Cell[P]]

  /** Converts a `List[Cell[P]]` into a `U[Cell[P]]`. */
  implicit def listCellToU[P <: Nat](t: List[Cell[P]]): U[Cell[P]]
}

/** Defines convenience implicits for dealing with distributed contents. */
trait ContentImplicits[U[_], E[_], C <: Context[U, E]] {
  /** Converts a `U[Content]` to a `Contents`. */
  implicit def toContents(data: U[Content]): Contents[U, E, C]

  /** Converts a `U[(Position[P], Content)]` to a `IndexedContents`. */
  implicit def toIndexed[P <: Nat](data: U[(Position[P], Content)]): IndexedContents[P, U, E, C]
}

/** Defines convenience implicits for dealing with distributed strings. */
trait EnvironmentImplicits[U[_], E[_], C <: Context[U, E]] {
  /** Converts a `U[String]` to a `SaveStringsAsText`. */
  implicit def saveStringsAsText(data: U[String]): SaveStringsAsText[U, E, C]

  /** Make available native functions of `U`. */
  implicit def nativeFunctions[X](data: U[X]): NativeOperations[X, U, E, C]

  /** Make available functions of `E`. */
  implicit def valueFunctions[X](value: E[X]): ValueOperations[X, U, E, C]

  /** Convert an `E` to a `U`. */
  implicit def eToU[X : ClassTag](value: E[X]): U[X]
}

/** Defines convenience implicits for dealing with matrices. */
trait MatrixImplicits[U[_], E[_], C <: Context[U, E]] {
  /** Conversion from `U[Cell[_1]]` to a `Matrix1D`. */
  implicit def toMatrix1D(data: U[Cell[_1]]): Matrix1D[U, E, C]

  /** Conversion from `U[Cell[_2]]` to a `Matrix2D`. */
  implicit def toMatrix2D(data: U[Cell[_2]]): Matrix2D[U, E, C]

  /** Conversion from `U[Cell[_3]]` to a `Matrix3D`. */
  implicit def toMatrix3D(data: U[Cell[_3]]): Matrix3D[U, E, C]

  /** Conversion from `U[Cell[_4]]` to a `Matrix4D`. */
  implicit def toMatrix4D(data: U[Cell[_4]]): Matrix4D[U, E, C]

  /** Conversion from `U[Cell[_5]]` to a `Matrix5D`. */
  implicit def toMatrix5D(data: U[Cell[_5]]): Matrix5D[U, E, C]

  /** Conversion from `U[Cell[_6]]` to a `Matrix6D`. */
  implicit def toMatrix6D(data: U[Cell[_6]]): Matrix6D[U, E, C]

  /** Conversion from `U[Cell[_7]]` to a `Matrix7D`. */
  implicit def toMatrix7D(data: U[Cell[_7]]): Matrix7D[U, E, C]

  /** Conversion from `U[Cell[_8]]` to a `Matrix8D`. */
  implicit def toMatrix8D(data: U[Cell[_8]]): Matrix8D[U, E, C]

  /** Conversion from `U[Cell[_9]]` to a `Matrix9D`. */
  implicit def toMatrix9D(data: U[Cell[_9]]): Matrix9D[U, E, C]

  /** Conversion from `List[Cell[_1]]` to a `Matrix1D`. */
  implicit def listToMatrix1D(data: List[Cell[_1]]): Matrix1D[U, E, C]

  /** Conversion from `List[Cell[_2]]` to a `Matrix2D`. */
  implicit def listToMatrix2D(data: List[Cell[_2]]): Matrix2D[U, E, C]

  /** Conversion from `List[Cell[_3]]` to a `Matrix3D`. */
  implicit def listToMatrix3D(data: List[Cell[_3]]): Matrix3D[U, E, C]

  /** Conversion from `List[Cell[_4]]` to a `Matrix4D`. */
  implicit def listToMatrix4D(data: List[Cell[_4]]): Matrix4D[U, E, C]

  /** Conversion from `List[Cell[_5]]` to a `Matrix5D`. */
  implicit def listToMatrix5D(data: List[Cell[_5]]): Matrix5D[U, E, C]

  /** Conversion from `List[Cell[_6]]` to a `Matrix6D`. */
  implicit def listToMatrix6D(data: List[Cell[_6]]): Matrix6D[U, E, C]

  /** Conversion from `List[Cell[_7]]` to a `Matrix7D`. */
  implicit def listToMatrix7D(data: List[Cell[_7]]): Matrix7D[U, E, C]

  /** Conversion from `List[Cell[_8]]` to a `Matrix8D`. */
  implicit def listToMatrix8D(data: List[Cell[_8]]): Matrix8D[U, E, C]

  /** Conversion from `List[Cell[_9]]` to a `Matrix9D`. */
  implicit def listToMatrix9D(data: List[Cell[_9]]): Matrix9D[U, E, C]

  /** Conversion from `List[(Value, Content)]` to a `Matrix1D`. */
  implicit def tupleToMatrix1D[V <% Value](list: List[(V, Content)]): Matrix1D[U, E, C]

  /** Conversion from `List[(Value, Value, Content)]` to a `Matrix2D`. */
  implicit def tupleToMatrix2D[V <% Value, W <% Value](list: List[(V, W, Content)]): Matrix2D[U, E, C]

  /** Conversion from `List[(Value, Value, Value, Content)]` to a `Matrix3D`. */
  implicit def tupleToMatrix3D[V <% Value, W <% Value, X <% Value](list: List[(V, W, X, Content)]): Matrix3D[U, E, C]

  /** Conversion from `List[(Value, Value, Value, Value, Content)]` to a `Matrix4D`. */
  implicit def tupleToMatrix4D[
    V <% Value,
    W <% Value,
    X <% Value,
    Y <% Value
  ](
    list: List[(V, W, X, Y, Content)]
  ): Matrix4D[U, E, C]

  /** Conversion from `List[(Value, Value, Value, Value, Value, Content)]` to a `Matrix5D`. */
  implicit def tupleToMatrix5D[
    V <% Value,
    W <% Value,
    X <% Value,
    Y <% Value,
    Z <% Value
  ](
    list: List[(V, W, X, Y, Z, Content)]
  ): Matrix5D[U, E, C]

  /** Conversion from `List[(Value, Value, Value, Value, Value, Value, Content)]` to a `Matrix6D`. */
  implicit def tupleToMatrix6D[
    T <% Value,
    V <% Value,
    W <% Value,
    X <% Value,
    Y <% Value,
    Z <% Value
  ](
    list: List[(T, V, W, X, Y, Z, Content)]
  ): Matrix6D[U, E, C]

  /** Conversion from `List[(Value, Value, Value, Value, Value, Value, Value, Content)]` to a `Matrix7D`. */
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
  ): Matrix7D[U, E, C]

  /** Conversion from `List[(Value, Value, Value, Value, Value, Value, Value, Value, Content)]` to a `Matrix8D`. */
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
  ): Matrix8D[U, E, C]

  /**
   * Conversion from `List[(Value, Value, Value, Value, Value, Value, Value, Value, Value, Content)]` to a `Matrix9D`.
   */
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
  ): Matrix9D[U, E, C]

  /** Conversion from matrix with errors tuple to `MatrixWithParseErrors`. */
  implicit def tupleToParseErrors[
    P <: Nat
  ](
    t: (U[Cell[P]], U[String])
  ): MatrixWithParseErrors[P, U] = MatrixWithParseErrors(t._1, t._2)
}

/** Defines convenience implicits for dealing with distributed partitions. */
trait PartitionImplicits[U[_], E[_], C <: Context[U, E]] {
  /** Conversion from `U[(I, Cell[P])]` to a `Partitions`. */
  implicit def toPartitions[P <: Nat, I : Ordering](data: U[(I, Cell[P])]): Partitions[P, I, U, E, C]
}

/** Defines convenience implicits for dealing with distributed positions. */
trait PositionImplicits[U[_], E[_], C <: Context[U, E]] {
  /** Converts a `Value` to a `U[Position[_1]]`. */
  implicit def valueToU[T <% Value](t: T): U[Position[_1]]

  /** Converts a `List[Value]` to a `U[Position[_1]]`. */
  implicit def listValueToU[T <% Value](t: List[T]): U[Position[_1]]

  /** Converts a `Position[T]` to a `U[Position[T]]`. */
  implicit def positionToU[T <: Nat](t: Position[T]): U[Position[T]]

  /** Converts a `List[Position[T]]` to a `U[Position[T]]`. */
  implicit def listPositionToU[T <: Nat](t: List[Position[T]]): U[Position[T]]

  /** Converts a `U[Position[P]]` to a `Positions`. */
  implicit def toPositions[
    L <: Nat,
    P <: Nat
  ](
    data: U[Position[P]]
  )(implicit
    ev: Diff.Aux[P, _1, L]
  ): Positions[L, P, U, E, C]

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
    t: List[(T, Cell.Predicate[P])]
  ): List[(U[Position[S]], Cell.Predicate[P])] = t
    .map { case (i, p) =>
      val u: U[Position[S]] = i

      (u, p)
    }
}

/** Capture all implicits together. */
trait Implicits[U[_], E[_], C <: Context[U, E]] {
  /** Cell related implicits. */
  val cell: CellImplicits[U, E, C]

  /** Content related implicits. */
  val content: ContentImplicits[U, E, C]

  /** Environment related implicits. */
  val environment: EnvironmentImplicits[U, E, C]

  /** Matrix related implicits. */
  val matrix: MatrixImplicits[U, E, C]

  /** Partition related implicits. */
  val partition: PartitionImplicits[U, E, C]

  /** Position related implicits. */
  val position: PositionImplicits[U, E, C]
}

