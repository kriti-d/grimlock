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

package commbank.grimlock.framework.partition

import commbank.grimlock.framework.{ Cell, Persist }
import commbank.grimlock.framework.environment.Context
import commbank.grimlock.framework.environment.tuner.Tuner
import commbank.grimlock.framework.utility.JSON

import play.api.libs.json.{ Json, Writes }

import scala.reflect.ClassTag

import shapeless.HList

/** Trait for partitioners. */
trait Partitioner[P <: HList, I] extends PartitionerWithValue[P, I] {
  type V = Any

  def assignWithValue(cell: Cell[P], ext: V): TraversableOnce[I] = assign(cell)

  /**
   * Assign the cell to a partition.
   *
   * @param cell The cell to assign to a partition.
   *
   * @return Zero or more partitition identifiers.
   */
  def assign(cell: Cell[P]): TraversableOnce[I]
}

/** Companion object for the `Partitioner` trait. */
object Partitioner {
  /** Converts a `(Cell[P]) => I` to a `Partitioner[P, S]`. */
  implicit def funcToPartitioner[P <: HList, I](func: (Cell[P]) => I) = new Partitioner[P, I] {
    def assign(cell: Cell[P]): TraversableOnce[I] = List(func(cell))
  }

  /** Converts a `(Cell[P]) => List[S]` to a `Partitioner[P, I]`. */
  implicit def funcListToPartitioner[P <: HList, I](func: (Cell[P]) => List[I]) = new Partitioner[P, I] {
    def assign(cell: Cell[P]): TraversableOnce[I] = func(cell)
  }

  /** Converts a `Seq[Partitioner[P, I]]` to a single `Partitioner[P, I]`. */
  implicit def seqToPartitioner[P <: HList, I](partitioners: Seq[Partitioner[P, I]]) = new Partitioner[P, I] {
    def assign(cell: Cell[P]): TraversableOnce[I] = partitioners.flatMap(_.assign(cell))
  }
}

/** Trait for partitioners that use a user supplied value. */
trait PartitionerWithValue[P <: HList, I] {
  /** Type of the external value. */
  type V

  /**
   * Assign the cell to a partition using a user supplied value.
   *
   * @param cell The cell to assign to a partition.
   * @param ext  The user supplied value.
   *
   * @return Zero or more partitition identifiers.
   */
  def assignWithValue(cell: Cell[P], ext: V): TraversableOnce[I]
}

/** Companion object for the `PartitionerWithValue` trait. */
object PartitionerWithValue {
  /** Converts a `(Cell[P], W) => I` to a `PartitionerWithValue[P, I] { type V >: W }`. */
  implicit def funcToPartitionerWithValue[
    P <: HList,
    W,
    I
  ](
    func: (Cell[P], W) => I
  ): PartitionerWithValue[P, I] { type V >: W } = new PartitionerWithValue[P, I] {
    type V = W

    def assignWithValue(cell: Cell[P], ext: W): TraversableOnce[I] = List(func(cell, ext))
  }

  /** Converts a `(Cell[P], W) => List[I]` to a `PartitionerWithValue[P, I] { type V >: W }`. */
  implicit def funcListToPartitionerWithValue[
    P <: HList,
    W,
    I
  ](
    func: (Cell[P], W) => List[I]
  ): PartitionerWithValue[P, I] { type V >: W } = new PartitionerWithValue[P, I] {
    type V = W

    def assignWithValue(cell: Cell[P], ext: W): TraversableOnce[I] = func(cell, ext)
  }

  /**
   * Converts a `Seq[PartitionerWithValue[P, I] { type V >: W }]` to a single
   * `PartitionerWithValue[P, I] { type V >: W }`.
   */
  implicit def seqToPartitionerWithValue[
    P <: HList,
    W,
    I
  ](
    partitioners: Seq[PartitionerWithValue[P, I] { type V >: W }]
  ): PartitionerWithValue[P, I] { type V >: W } = new PartitionerWithValue[P, I] {
    type V = W

    def assignWithValue(cell: Cell[P], ext: V): TraversableOnce[I] = partitioners
      .flatMap(_.assignWithValue(cell, ext).toList)
  }
}

/** Trait that represents the partitions of matrices */
trait Partitions[P <: HList, I, C <: Context[C]] extends Persist[(I, Cell[P]), C] {
  /**
   * Add a partition.
   *
   * @param id        The partition identifier.
   * @param partition The partition to add.
   *
   * @return A `C#U[(I, Cell[P])]` containing existing and new paritions.
   */
  def add(id: I, partition: C#U[Cell[P]]): C#U[(I, Cell[P])]

  /**
   * Apply function `fn` to all partitions.
   *
   * @param context The operating context.
   * @param fn      The function to apply to each partition.
   * @param exclude List of partition ids to exclude from applying `fn` to.
   * @param tuner   The tuner for the job.
   *
   * @return A `C#U[(I, Cell[Q])]` containing the paritions with `fn` applied to them.
   *
   * @note This will pull all partition ids into memory, so only use this if there is sufficient memory
   *       available to keep all (distinct) partition ids in memory.
   */
  def forAll[
    Q <: HList,
    T <: Tuner
  ](
    context: C,
    fn: (I, C#U[Cell[P]]) => C#U[Cell[Q]],
    exclude: List[I] = List(),
    tuner: T
  )(implicit
    ev1: ClassTag[I],
    ev2: Partitions.ForAllTuner[C#U, T]
  ): C#U[(I, Cell[Q])]

  /**
   * Apply function `fn` to each partition in `ids`.
   *
   * @param ids   List of partition ids to apply `fn` to.
   * @param fn    The function to apply to each partition.
   *
   * @return A `C#U[(I, Cell[Q])]` containing the paritions with `fn` applied to them.
   */
  def forEach[Q <: HList](ids: List[I], fn: (I, C#U[Cell[P]]) => C#U[Cell[Q]]): C#U[(I, Cell[Q])]

  /**
   * Return the data for the partition `id`.
   *
   * @param id The partition for which to get the data.
   *
   * @return A `C#U[Cell[P]]`; that is a matrix.
   */
  def get(id: I): C#U[Cell[P]]

  /**
   * Return the partition identifiers.
   *
   * @param tuner The tuner for the job.
   */
  def ids[T <: Tuner](tuner: T)(implicit ev1: ClassTag[I], ev2: Partitions.IdsTuner[C#U, T]): C#U[I]

  /**
   * Merge partitions into a single matrix.
   *
   * @param ids List of partition keys to merge.
   *
   * @return A `C#U[Cell[P]]` containing the merge partitions.
   */
  def merge(ids: List[I]): C#U[Cell[P]]

  /**
   * Remove a partition.
   *
   * @param id The identifier for the partition to remove.
   *
   * @return A `C#U[(I, Cell[P])]` with the selected parition removed.
   */
  def remove(id: I): C#U[(I, Cell[P])]

  /**
   * Persist to disk.
   *
   * @param context The operating context.
   * @param file    Name of the output file.
   * @param writer  Writer that converts `(I, Cell[P])` to string.
   * @param tuner   The tuner for the job.
   *
   * @return A `C#U[(I, Cell[P])]` which is this object's data.
   */
  def saveAsText[
    T <: Tuner
  ](
    context: C,
    file: String,
    writer: Persist.TextWriter[(I, Cell[P])],
    tuner: T
  )(implicit
    ev: Persist.SaveAsTextTuner[C#U, T]
  ): C#U[(I, Cell[P])]
}

/** Companion object to `Partitions` with types, implicits, etc. */
object Partitions {
  /** Trait for tuners permitted on a call to `forAll`. */
  trait ForAllTuner[U[_], T <: Tuner] extends java.io.Serializable

  /** Trait for tuners permitted on a call to `ids`. */
  trait IdsTuner[U[_], T <: Tuner] extends java.io.Serializable

  /**
   * Return function that returns a string representation of a partition.
   *
   * @param verbose   Indicator if codec and schema are required or not.
   * @param separator The separator to use between various fields.
   */
  def toShortString[
    I,
    P <: HList
  ](
    verbose: Boolean,
    separator: String
  ): ((I, Cell[P])) => TraversableOnce[String] = (t: (I, Cell[P])) =>
    List(t._1.toString + separator + t._2.toShortString(verbose, separator))

  /**
   * Return function that returns a JSON representations of a partition.
   *
   * @param verbose Indicator if the JSON should be self describing or not.
   * @param pretty  Indicator if the resulting JSON string to be indented.
   *
   * @note The key and cell are separately encoded and then combined using the separator.
   */
  def toJSON[
    I,
    P <: HList
  ](
    verbose: Boolean,
    pretty: Boolean = false
  )(implicit
    ev: Writes[I]
  ): ((I, Cell[P])) => TraversableOnce[String] = {
    implicit val cell = Cell.writes[P](verbose)

    val writes = new Writes[(I, Cell[P])] {
      def writes(t: (I, Cell[P])) = Json.obj("id" -> t._1, "cell" -> t._2)
    }

    (t: (I, Cell[P])) => List(JSON.to(t, writes, pretty))
  }
}

