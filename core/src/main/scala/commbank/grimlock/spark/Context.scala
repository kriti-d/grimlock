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

package commbank.grimlock.spark.environment

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

import commbank.grimlock.spark.{
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
import commbank.grimlock.spark.content.{ Contents, IndexedContents }
import commbank.grimlock.spark.partition.Partitions
import commbank.grimlock.spark.position.Positions

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

import shapeless.Nat
import shapeless.nat.{ _1, _2, _3, _4, _5, _6, _7, _8, _9 }
import shapeless.ops.nat.Diff

/**
 * Spark operating context state.
 *
 * @param context The Spark context.
 */
case class Context(context: SparkContext) extends FwContext

/** Companion object to `Context` with implicit. */
object Context {
  /** Converts a `RDD[Content]` to a `Contents`. */
  implicit def rddToContents(data: RDD[Content]): Contents = Contents(data)

  /** Converts a `RDD[(Position[P], Content)]` to a `IndexedContents`. */
  implicit def rddToIndexed[P <: Nat](data: RDD[(Position[P], Content)]): IndexedContents[P] = IndexedContents[P](data)

  /** Converts a `Cell[P]` into a `RDD[Cell[P]]`. */
  implicit def cellToRDD[P <: Nat](t: Cell[P])(implicit ctx: Context, ev: ClassTag[Position[P]]): RDD[Cell[P]] = ctx
    .context
    .parallelize(List(t))

  /** Converts a `List[Cell[P]]` into a `RDD[Cell[P]]`. */
  implicit def listCellToRDD[P <: Nat](t: List[Cell[P]])(implicit ctx: Context, ev: ClassTag[P]): RDD[Cell[P]] = ctx
    .context
    .parallelize(t)

  /** Conversion from `RDD[Cell[_1]]` to a Spark `Matrix1D`. */
  implicit def rddToMatrix1(data: RDD[Cell[_1]]): Matrix1D = Matrix1D(data)

  /** Conversion from `RDD[Cell[_2]]` to a Spark `Matrix2D`. */
  implicit def rddToMatrix2(data: RDD[Cell[_2]]): Matrix2D = Matrix2D(data)

  /** Conversion from `RDD[Cell[_3]]` to a Spark `Matrix3D`. */
  implicit def rddToMatrix3(data: RDD[Cell[_3]]): Matrix3D = Matrix3D(data)

  /** Conversion from `RDD[Cell[_4]]` to a Spark `Matrix4D`. */
  implicit def rddToMatrix4(data: RDD[Cell[_4]]): Matrix4D = Matrix4D(data)

  /** Conversion from `RDD[Cell[_5]]` to a Spark `Matrix5D`. */
  implicit def rddToMatrix5(data: RDD[Cell[_5]]): Matrix5D = Matrix5D(data)

  /** Conversion from `RDD[Cell[_6]]` to a Spark `Matrix6D`. */
  implicit def rddToMatrix6(data: RDD[Cell[_6]]): Matrix6D = Matrix6D(data)

  /** Conversion from `RDD[Cell[_7]]` to a Spark `Matrix7D`. */
  implicit def rddToMatrix7(data: RDD[Cell[_7]]): Matrix7D = Matrix7D(data)

  /** Conversion from `RDD[Cell[_8]]` to a Spark `Matrix8D`. */
  implicit def rddToMatrix8(data: RDD[Cell[_8]]): Matrix8D = Matrix8D(data)

  /** Conversion from `RDD[Cell[_9]]` to a Spark `Matrix9D`. */
  implicit def rddToMatrix9(data: RDD[Cell[_9]]): Matrix9D = Matrix9D(data)

  /** Conversion from `List[Cell[_1]]` to a Spark `Matrix1D`. */
  implicit def listToSparkMatrix1(data: List[Cell[_1]])(implicit ctx: Context): Matrix1D = Matrix1D(
    ctx.context.parallelize(data)
  )

  /** Conversion from `List[Cell[_2]]` to a Spark `Matrix2D`. */
  implicit def listToSparkMatrix2(data: List[Cell[_2]])(implicit ctx: Context): Matrix2D = Matrix2D(
    ctx.context.parallelize(data)
  )

  /** Conversion from `List[Cell[_3]]` to a Spark `Matrix3D`. */
  implicit def listToSparkMatrix3(data: List[Cell[_3]])(implicit ctx: Context): Matrix3D = Matrix3D(
    ctx.context.parallelize(data)
  )

  /** Conversion from `List[Cell[_4]]` to a Spark `Matrix4D`. */
  implicit def listToSparkMatrix4(data: List[Cell[_4]])(implicit ctx: Context): Matrix4D = Matrix4D(
    ctx.context.parallelize(data)
  )

  /** Conversion from `List[Cell[_5]]` to a Spark `Matrix5D`. */
  implicit def listToSparkMatrix5(data: List[Cell[_5]])(implicit ctx: Context): Matrix5D = Matrix5D(
    ctx.context.parallelize(data)
  )

  /** Conversion from `List[Cell[_6]]` to a Spark `Matrix6D`. */
  implicit def listToSparkMatrix6(data: List[Cell[_6]])(implicit ctx: Context): Matrix6D = Matrix6D(
    ctx.context.parallelize(data)
  )

  /** Conversion from `List[Cell[_7]]` to a Spark `Matrix7D`. */
  implicit def listToSparkMatrix7(data: List[Cell[_7]])(implicit ctx: Context): Matrix7D = Matrix7D(
    ctx.context.parallelize(data)
  )

  /** Conversion from `List[Cell[_8]]` to a Spark `Matrix8D`. */
  implicit def listToSparkMatrix8(data: List[Cell[_8]])(implicit ctx: Context): Matrix8D = Matrix8D(
    ctx.context.parallelize(data)
  )

  /** Conversion from `List[Cell[_9]]` to a Spark `Matrix9D`. */
  implicit def listToSparkMatrix9(data: List[Cell[_9]])(implicit ctx: Context): Matrix9D = Matrix9D(
    ctx.context.parallelize(data)
  )

  /** Conversion from `List[(Value, Content)]` to a Spark `Matrix1D`. */
  implicit def tupleToSparkMatrix1[V <% Value](list: List[(V, Content)])(implicit ctx: Context): Matrix1D = Matrix1D(
    ctx.context.parallelize(list.map { case (v, c) => Cell(Position(v), c) })
  )

  /** Conversion from `List[(Value, Value, Content)]` to a Spark `Matrix2D`. */
  implicit def tupleToSparkMatrix2[
    V <% Value,
    W <% Value
  ](
    list: List[(V, W, Content)]
  )(implicit
    ctx: Context
  ): Matrix2D = Matrix2D(ctx.context.parallelize(list.map { case (v, w, c) => Cell(Position(v, w), c) }))

  /** Conversion from `List[(Value, Value, Value, Content)]` to a Spark `Matrix3D`. */
  implicit def tupleToSparkMatrix3[
    V <% Value,
    W <% Value,
    X <% Value
  ](
    list: List[(V, W, X, Content)]
  )(implicit
    ctx: Context
  ): Matrix3D = Matrix3D(ctx.context.parallelize(list.map { case (v, w, x, c) => Cell(Position(v, w, x), c) }))

  /** Conversion from `List[(Value, Value, Value, Value, Content)]` to a Spark `Matrix4D`. */
  implicit def tupleToSparkMatrix4[
    V <% Value,
    W <% Value,
    X <% Value,
    Y <% Value
  ](
    list: List[(V, W, X, Y, Content)]
  )(implicit
    ctx: Context
  ): Matrix4D = Matrix4D(ctx.context.parallelize(list.map { case (v, w, x, y, c) => Cell(Position(v, w, x, y), c) }))

  /** Conversion from `List[(Value, Value, Value, Value, Value, Content)]` to a Spark `Matrix5D`. */
  implicit def tupleToSparkMatrix5[
    V <% Value,
    W <% Value,
    X <% Value,
    Y <% Value,
    Z <% Value
  ](
    list: List[(V, W, X, Y, Z, Content)]
  )(implicit
    ctx: Context
  ): Matrix5D = Matrix5D(
    ctx.context.parallelize(list.map { case (v, w, x, y, z, c) => Cell(Position(v, w, x, y, z), c) })
  )

  /** Conversion from `List[(Value, Value, Value, Value, Value, Value, Content)]` to a Spark `Matrix6D`. */
  implicit def tupleToSparkMatrix6[
    T <% Value,
    V <% Value,
    W <% Value,
    X <% Value,
    Y <% Value,
    Z <% Value
  ](
    list: List[(T, V, W, X, Y, Z, Content)]
  )(implicit
    ctx: Context
  ): Matrix6D = Matrix6D(
    ctx.context.parallelize(list.map { case (t, v, w, x, y, z, c) => Cell(Position(t, v, w, x, y, z), c) })
  )

  /** Conversion from `List[(Value, Value, Value, Value, Value, Value, Value, Content)]` to a Spark `Matrix7D`. */
  implicit def tupleToSparkMatrix7[
    S <% Value,
    T <% Value,
    V <% Value,
    W <% Value,
    X <% Value,
    Y <% Value,
    Z <% Value
  ](
    list: List[(S, T, V, W, X, Y, Z, Content)]
  )(implicit
    ctx: Context
  ): Matrix7D = Matrix7D(
    ctx.context.parallelize(list.map { case (s, t, v, w, x, y, z, c) => Cell(Position(s, t, v, w, x, y, z), c) })
  )

  /**
   * Conversion from `List[(Value, Value, Value, Value, Value, Value, Value, Value, Content)]` to a Spark `Matrix8D`.
   */
  implicit def tupleToSparkMatrix8[
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
  )(implicit
    ctx: Context
  ): Matrix8D = Matrix8D(
    ctx.context.parallelize(list.map { case (r, s, t, v, w, x, y, z, c) => Cell(Position(r, s, t, v, w, x, y, z), c) })
  )

  /**
   * Conversion from `List[(Value, Value, Value, Value, Value, Value, Value, Value, Value, Content)]` to a
   * Spark `Matrix9D`.
   */
  implicit def tupleToSparkMatrix9[
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
  )(implicit
    ctx: Context
  ): Matrix9D = Matrix9D(
    ctx.context.parallelize(
      list.map { case (q, r, s, t, v, w, x, y, z, c) => Cell(Position(q, r, s, t, v, w, x, y, z), c) }
    )
  )

  /** Converts a `(T, Cell.Predicate[P])` to a `List[(RDD[Position[S]], Cell.Predicate[P])]`. */
  implicit def predicateToSparkList[
    P <: Nat,
    S <: Nat,
    T <% RDD[Position[S]]
  ](
    t: (T, Cell.Predicate[P])
  ): List[(RDD[Position[S]], Cell.Predicate[P])] = {
    val rdd: RDD[Position[S]] = t._1

    List((rdd, t._2))
  }

  /**
   * Converts a `List[(T, Cell.Predicate[P])]` to a `List[(RDD[Position[S]], Cell.Predicate[P])]`.
   */
  implicit def listPredicateToScaldingList[
    P <: Nat,
    S <: Nat,
    T <% RDD[Position[S]]
  ](
    t: List[(T, Cell.Predicate[P])]
  ): List[(RDD[Position[S]], Cell.Predicate[P])] = t
    .map { case (i, p) =>
      val rdd: RDD[Position[S]] = i

      (rdd, p)
    }

  /** Conversion from matrix with errors tuple to `MatrixWithParseErrors`. */
  implicit def tupleToSparkParseErrors[
    P <: Nat
  ](
    t: (RDD[Cell[P]], RDD[String])
  ): MatrixWithParseErrors[P, RDD] = MatrixWithParseErrors(t._1, t._2)

  /** Conversion from `RDD[(I, Cell[P])]` to a Spark `Partitions`. */
  implicit def rddToPartitions[P <: Nat, I : Ordering](data: RDD[(I, Cell[P])]): Partitions[P, I] = Partitions(data)

  /** Converts a `RDD[Position[P]]` to a Spark `Positions`. */
  implicit def rddToPositions[
    L <: Nat,
    P <: Nat
  ](
    data: RDD[Position[P]]
  )(implicit
    ev: Diff.Aux[P, _1, L]
  ): Positions[L, P] = Positions(data)

  /** Converts a `Value` to a `RDD[Position[_1]]`. */
  implicit def valueToRDD[T <% Value](t: T)(implicit ctx: Context, ev: ClassTag[Position[_1]]): RDD[Position[_1]] = ctx
    .context
    .parallelize(List(Position(t)))

  /** Converts a `List[Value]` to a `RDD[Position[_1]]`. */
  implicit def listValueToRDD[
    T <% Value
  ](
    t: List[T]
  )(implicit
    ctx: Context,
    ev: ClassTag[Position[_1]]
  ): RDD[Position[_1]] = ctx.context.parallelize(t.map(Position(_)))

  /** Converts a `Position[T]` to a `RDD[Position[T]]`. */
  implicit def positionToRDD[
    T <: Nat
  ](
    t: Position[T]
  )(implicit
    ctx: Context,
    ev: ClassTag[Position[T]]
  ): RDD[Position[T]] = ctx.context.parallelize(List(t))

  /** Converts a `List[Position[T]]` to a `RDD[Position[T]]`. */
  implicit def listPositionToRDD[
    T <: Nat
  ](
    t: List[Position[T]]
  )(implicit
    ctx: Context,
    ev: ClassTag[Position[T]]
  ): RDD[Position[T]] = ctx.context.parallelize(t)

  /** Conversion from `RDD[(Position[P], Type)]` to a `Types`. */
  implicit def rddToTypes[P <: Nat](data: RDD[(Position[P], Type)]): Types[P] = Types(data)
}

trait DistributedData extends FwDistributedData {
  type U[X] = RDD[X]
}

trait UserData extends FwUserData {
  type E[X] = X
}

trait Environment extends FwEnvironment {
  type C = Context
}

