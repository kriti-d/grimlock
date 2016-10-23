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

package commbank.grimlock.framework

import commbank.grimlock.framework.content._
import commbank.grimlock.framework.encoding._
import commbank.grimlock.framework.position._

import com.twitter.scrooge.ThriftStruct

import java.util.regex.Pattern

import org.apache.hadoop.io.Writable

import scala.util.Try

import shapeless.{ ::, DepFn0, Generic, HList, HNil, Nat, Poly1, Poly2 }
import shapeless.nat.{ _1, _2, _3, _4, _5, _6 }
import shapeless.ops.hlist.{ ConstMapper, LeftFolder, Mapper, Prepend, ZipApply }
import shapeless.ops.nat.{ LTEq, ToInt }
import shapeless.ops.traversable.FromTraversable

private trait PosFromArgs extends DepFn0 with Serializable

private object PosFromArgs {
  type Aux[Out0] = PosFromArgs { type Out = Out0 }

  def apply(implicit p: PosFromArgs): Aux[p.Out] = p

  implicit def position1D[A <: Value]: Aux[(A :: HNil) => Position[_1]] = new PosFromArgs {
    type Out = (A :: HNil) => Position[_1]

    def apply(): Out = { case a :: HNil => Position(a) }
  }

  implicit def position2D[A <: Value, B <: Value]: Aux[(A :: B :: HNil) => Position[_2]] = new PosFromArgs {
    type Out = (A :: B :: HNil) => Position[_2]

    def apply(): Out = { case a :: b :: HNil => Position(a, b) }
  }

  implicit def position3D[
    A <: Value,
    B <: Value,
    C <: Value
  ]: Aux[(A :: B :: C :: HNil) => Position[_3]] = new PosFromArgs {
    type Out = (A :: B :: C :: HNil) => Position[_3]

    def apply(): Out = { case a :: b :: c :: HNil => Position(a, b, c) }
  }

  implicit def position4D[
    A <: Value,
    B <: Value,
    C <: Value,
    D <: Value
  ]: Aux[(A :: B :: C :: D :: HNil) => Position[_4]] = new PosFromArgs {
    type Out = (A :: B :: C :: D :: HNil) => Position[_4]

    def apply(): Out = { case a :: b :: c :: d :: HNil => Position(a, b, c, d) }
  }

  implicit def position5D[
    A <: Value,
    B <: Value,
    C <: Value,
    D <: Value,
    E <: Value
  ]: Aux[(A :: B :: C :: D :: E :: HNil) => Position[_5]] = new PosFromArgs {
    type Out = (A :: B :: C :: D :: E :: HNil) => Position[_5]

    def apply(): Out = { case a :: b :: c :: d :: e :: HNil => Position(a, b, c, d, e) }
  }

  implicit def position6D[
    A <: Value,
    B <: Value,
    C <: Value,
    D <: Value,
    E <: Value,
    F <: Value
  ]: Aux[(A :: B :: C :: D :: E :: F :: HNil) => Position[_6]] = new PosFromArgs {
    type Out = (A :: B :: C :: D :: E :: F :: HNil) => Position[_6]

    def apply(): Out = { case a :: b :: c :: d :: e :: f :: HNil => Position(a, b, c, d, e, f) }
  }
}

private trait CellFactory[P <: Product] {
  type Out

  def apply(cds: P, pos: Array[String], value: String, decoder: Content.Parser): Out
}

private object CellFactory {
  type Aux[P <: Product, Out0] = CellFactory[P] { type Out = Out0 }

  object toFns extends Poly1 {
    implicit def default[C <: Codec]: Case.Aux[C, String => Option[Value]] = at[C](
      (c: C) => (s: String) => c.decode(s)
    )
  }

  object Sequence extends Poly2 {
    implicit def caseSome[
      L <: HList,
      T
    ](implicit
      prep: Prepend[L, T :: HNil]
    ) = at[Option[L], Option[T]] { case (a, v) =>
      for {
        acc <- a
        value <- v
      } yield (acc :+ value)
    }
  }

  implicit def makeCell[
    R <: Nat,
    P <: Product,
    L <: HList,
    CO <: HList,
    MC <: HList,
    AO <: HList,
    FO <: HList
  ](implicit
    gen: Generic.Aux[P, L],
    cm: ConstMapper.Aux[String, L, CO],
    tr: FromTraversable[CO],
    ma: Mapper.Aux[toFns.type, L, MC],
    za: ZipApply.Aux[MC, CO, AO],
    f: LeftFolder.Aux[AO, Option[HNil.type], Sequence.type, Option[FO]],
    p: PosFromArgs.Aux[FO => Position[R]]
  ): Aux[P, Option[Cell[R]]] = new CellFactory[P] {
    type Out = Option[Cell[R]]

    def apply(cds: P, pos: Array[String], value: String, decoder: Content.Parser): Out = for {
      rawPositionsHList <- tr(pos)
      positionOptions = gen.to(cds) map toFns zipApply rawPositionsHList
      optionPositions <- (positionOptions.foldLeft(Option(HNil))(Sequence))
      position = p()(optionPositions)
      content <- decoder(value)
    } yield Cell(position, content)
  }
}

/**
 * Cell in a matrix.
 *
 * @param position The position of the cell in the matri.
 * @param content  The contents of the cell.
 */
case class Cell[P <: Nat](position: Position[P], content: Content) {
  /**
   * Relocate this cell.
   *
   * @param relocator Function that returns the new position for this cell.
   */
  def relocate[X <: Nat](relocator: (Cell[P]) => Position[X]): Cell[X] = Cell(relocator(this), content)

  /**
   * Mutate the content of this cell.
   *
   * @param mutator Function that returns the new content for this cell.
   */
  def mutate(mutator: (Cell[P]) => Content): Cell[P] = Cell(position, mutator(this))

  /**
   * Return string representation of a cell.
   *
   * @param separator   The separator to use between various fields.
   * @param codec       Indicator if codec is required or not.
   * @param schema      Indicator if schema is required or not.
   */
  def toShortString(separator: String, codec: Boolean = true, schema: Boolean = true): String =
    position.toShortString(separator) + separator + content.toShortString(separator, codec, schema)
}

/** Companion object to the Cell class. */
object Cell {
  /** Predicate used in, for example, the `which` methods of a matrix for finding content. */
  type Predicate[P <: Nat] = Cell[P] => Boolean

  /** Type for parsing a string into either a `Cell[P]` or an error message. */
  type TextParser[P <: Nat] = (String) => TraversableOnce[Either[String, Cell[P]]]

  /** Type for parsing a key value tuple into either a `Cell[P]` or an error message. */
  type SequenceParser[K <: Writable, V <: Writable, P <: Nat] = (K, V) => TraversableOnce[Either[String, Cell[P]]]

  /** Type for parsing Parquet data. */
  type ParquetParser[T <: ThriftStruct, P <: Nat] = (T) => TraversableOnce[Either[String, Cell[P]]]

  /**
   * Parse a line into a `Cell[_1]`.
   *
   * @param separator The column separator.
   * @param first     The codec for decoding the first dimension.
   */
  def parse1D(
    separator: String = "|",
    first: Codec = StringCodec
  ): (String) => TraversableOnce[Either[String, Cell[_1]]] = line =>
    parseXD[_1, Tuple1[Codec]](separator, Tuple1(first), line)

  /**
   * Parse a line data into a `Cell[_1]` with a dictionary.
   *
   * @param dict      The dictionary describing the features in the data.
   * @param separator The column separator.
   * @param first     The codec for decoding the first dimension.
   */
  def parse1DWithDictionary(
    dict: Map[String, Content.Parser],
    separator: String = "|",
    first: Codec = StringCodec
  ): (String) => TraversableOnce[Either[String, Cell[_1]]] = line =>
    parseXDWithDictionary[_1, Tuple1[Codec], _1](dict, separator, Tuple1(first), line)

  /**
   * Parse a line into a `Cell[_1]` with a schema.
   *
   * @param schema    The schema for decoding the data.
   * @param separator The column separator.
   * @param first     The codec for decoding the first dimension.
   */
  def parse1DWithSchema(
    schema: Content.Parser,
    separator: String = "|",
    first: Codec = StringCodec
  ): (String) => TraversableOnce[Either[String, Cell[_1]]] = line =>
    parseXDWithSchema[_1, Tuple1[Codec]](schema, separator, Tuple1(first), line)

  /**
   * Parse a line into a `Cell[_2]`.
   *
   * @param separator The column separator.
   * @param first     The codec for decoding the first dimension.
   * @param second    The codec for decoding the second dimension.
   */
  def parse2D(
    separator: String = "|",
    first: Codec = StringCodec,
    second: Codec = StringCodec
  ): (String) => TraversableOnce[Either[String, Cell[_2]]] = line =>
    parseXD[_2, (Codec, Codec)](separator, (first, second), line)

  /**
   * Parse a line into a `Cell[_2]` with a dictionary.
   *
   * @param dict      The dictionary describing the features in the data.
   * @param dim       The dimension on which to apply the dictionary.
   * @param separator The column separator.
   * @param first     The codec for decoding the first dimension.
   * @param second    The codec for decoding the second dimension.
   */
  def parse2DWithDictionary(
    dict: Map[String, Content.Parser],
    dim: Nat,
    separator: String = "|",
    first: Codec = StringCodec,
    second: Codec = StringCodec
  )(implicit
    ev1: LTEq[dim.N, _2],
    ev2: ToInt[dim.N]
  ): (String) => TraversableOnce[Either[String, Cell[_2]]] = line =>
    parseXDWithDictionary[_2, (Codec, Codec), dim.N](dict, separator, (first, second), line)

  /**
   * Parse a line into a `Cell[_2]` with a schema.
   *
   * @param schema    The schema for decoding the data.
   * @param separator The column separator.
   * @param first     The codec for decoding the first dimension.
   * @param second    The codec for decoding the second dimension.
   */
  def parse2DWithSchema(
    schema: Content.Parser,
    separator: String = "|",
    first: Codec = StringCodec,
    second: Codec = StringCodec
  ): (String) => TraversableOnce[Either[String, Cell[_2]]] = line =>
    parseXDWithSchema[_2, (Codec, Codec)](schema, separator, (first, second), line)

  /**
   * Parse a line into a `Cell[_3]`.
   *
   * @param separator The column separator.
   * @param first     The codec for decoding the first dimension.
   * @param second    The codec for decoding the second dimension.
   * @param third     The codec for decoding the third dimension.
   */
  def parse3D(
    separator: String = "|",
    first: Codec = StringCodec,
    second: Codec = StringCodec,
    third: Codec = StringCodec
  ): (String) => TraversableOnce[Either[String, Cell[_3]]] = line =>
    parseXD[_3, (Codec, Codec, Codec)](separator, (first, second, third), line)

  /**
   * Parse a line into a `Cell[_3]` with a dictionary.
   *
   * @param dict      The dictionary describing the features in the data.
   * @param dim       The dimension on which to apply the dictionary.
   * @param separator The column separator.
   * @param first     The codec for decoding the first dimension.
   * @param second    The codec for decoding the second dimension.
   * @param third     The codec for decoding the third dimension.
   */
  def parse3DWithDictionary(
    dict: Map[String, Content.Parser],
    dim: Nat,
    separator: String = "|",
    first: Codec = StringCodec,
    second: Codec = StringCodec,
    third: Codec = StringCodec
  )(implicit
    ev1: LTEq[dim.N, _3],
    ev2: ToInt[dim.N]
  ): (String) => TraversableOnce[Either[String, Cell[_3]]] = line =>
    parseXDWithDictionary[_3, (Codec, Codec, Codec), dim.N](dict, separator, (first, second, third), line)

  /**
   * Parse a line into a `Cell[_3]` with a schema.
   *
   * @param schema    The schema for decoding the data.
   * @param separator The column separator.
   * @param first     The codec for decoding the first dimension.
   * @param second    The codec for decoding the second dimension.
   * @param third     The codec for decoding the third dimension.
   */
  def parse3DWithSchema(
    schema: Content.Parser,
    separator: String = "|",
    first: Codec = StringCodec,
    second: Codec = StringCodec,
    third: Codec = StringCodec
  ): (String) => TraversableOnce[Either[String, Cell[_3]]] = line =>
    parseXDWithSchema[_3, (Codec, Codec, Codec)](schema, separator, (first, second, third), line)

  /**
   * Parse a line into a `Cell[_4]`.
   *
   * @param separator The column separator.
   * @param first     The codec for decoding the first dimension.
   * @param second    The codec for decoding the second dimension.
   * @param third     The codec for decoding the third dimension.
   * @param fourth    The codec for decoding the fourth dimension.
   */
  def parse4D(
    separator: String = "|",
    first: Codec = StringCodec,
    second: Codec = StringCodec,
    third: Codec = StringCodec,
    fourth: Codec = StringCodec
  ): (String) => TraversableOnce[Either[String, Cell[_4]]] = line =>
    parseXD[_4, (Codec, Codec, Codec, Codec)](separator, (first, second, third, fourth), line)

  /**
   * Parse a line into a `Cell[_4]` with a dictionary.
   *
   * @param dict      The dictionary describing the features in the data.
   * @param dim       The dimension on which to apply the dictionary.
   * @param separator The column separator.
   * @param first     The codec for decoding the first dimension.
   * @param second    The codec for decoding the second dimension.
   * @param third     The codec for decoding the third dimension.
   * @param fourth    The codec for decoding the fourth dimension.
   */
  def parse4DWithDictionary(
    dict: Map[String, Content.Parser],
    dim: Nat,
    separator: String = "|",
    first: Codec = StringCodec,
    second: Codec = StringCodec,
    third: Codec = StringCodec,
    fourth: Codec = StringCodec
  )(implicit
    ev1: LTEq[dim.N, _4],
    ev2: ToInt[dim.N]
  ): (String) => TraversableOnce[Either[String, Cell[_4]]] = line =>
    parseXDWithDictionary[
      _4,
      (Codec, Codec, Codec, Codec),
      dim.N
    ](
      dict,
      separator,
      (first, second, third, fourth),
      line
    )

  /**
   * Parse a line into a `Cell[_4]` with a schema.
   *
   * @param schema    The schema for decoding the data.
   * @param separator The column separator.
   * @param first     The codec for decoding the first dimension.
   * @param second    The codec for decoding the second dimension.
   * @param third     The codec for decoding the third dimension.
   * @param fourth    The codec for decoding the fourth dimension.
   */
  def parse4DWithSchema(
    schema: Content.Parser,
    separator: String = "|",
    first: Codec = StringCodec,
    second: Codec = StringCodec,
    third: Codec = StringCodec,
    fourth: Codec = StringCodec
  ): (String) => TraversableOnce[Either[String, Cell[_4]]] = line =>
    parseXDWithSchema[_4, (Codec, Codec, Codec, Codec)](schema, separator, (first, second, third, fourth), line)

  /**
   * Parse a line into a `Cell[_5]`.
   *
   * @param separator The column separator.
   * @param first     The codec for decoding the first dimension.
   * @param second    The codec for decoding the second dimension.
   * @param third     The codec for decoding the third dimension.
   * @param fourth    The codec for decoding the fourth dimension.
   * @param fifth     The codec for decoding the fifth dimension.
   */
  def parse5D(
    separator: String = "|",
    first: Codec = StringCodec,
    second: Codec = StringCodec,
    third: Codec = StringCodec,
    fourth: Codec = StringCodec,
    fifth: Codec = StringCodec
  ): (String) => TraversableOnce[Either[String, Cell[_5]]] = line =>
    parseXD[_5, (Codec, Codec, Codec, Codec, Codec)](separator, (first, second, third, fourth, fifth), line)

  /**
   * Parse a line into a `Cell[_5]` with a dictionary.
   *
   * @param dict      The dictionary describing the features in the data.
   * @param dim       The dimension on which to apply the dictionary.
   * @param separator The column separator.
   * @param first     The codec for decoding the first dimension.
   * @param second    The codec for decoding the second dimension.
   * @param third     The codec for decoding the third dimension.
   * @param fourth    The codec for decoding the fourth dimension.
   * @param fifth     The codec for decoding the fifth dimension.
   */
  def parse5DWithDictionary(
    dict: Map[String, Content.Parser],
    dim: Nat,
    separator: String = "|",
    first: Codec = StringCodec,
    second: Codec = StringCodec,
    third: Codec = StringCodec,
    fourth: Codec = StringCodec,
    fifth: Codec = StringCodec
  )(implicit
    ev1: LTEq[dim.N, _5],
    ev2: ToInt[dim.N]
  ): (String) => TraversableOnce[Either[String, Cell[_5]]] = line =>
    parseXDWithDictionary[
      _5,
      (Codec, Codec, Codec, Codec, Codec),
      dim.N
    ](
      dict,
      separator,
      (first, second, third, fourth, fifth),
      line
    )

  /**
   * Parse a line into a `Cell[_5]` with a schema.
   *
   * @param schema    The schema for decoding the data.
   * @param separator The column separator.
   * @param first     The codec for decoding the first dimension.
   * @param second    The codec for decoding the second dimension.
   * @param third     The codec for decoding the third dimension.
   * @param fourth    The codec for decoding the fourth dimension.
   * @param fifth     The codec for decoding the fifth dimension.
   */
  def parse5DWithSchema(
    schema: Content.Parser,
    separator: String = "|",
    first: Codec = StringCodec,
    second: Codec = StringCodec,
    third: Codec = StringCodec,
    fourth: Codec = StringCodec,
    fifth: Codec = StringCodec
  ): (String) => TraversableOnce[Either[String, Cell[_5]]] = line =>
    parseXDWithSchema[
      _5,
      (Codec, Codec, Codec, Codec, Codec)
    ](
      schema,
      separator,
      (first, second, third, fourth, fifth),
      line
    )

  /**
   * Parse a line into a `Cell[_6]`.
   *
   * @param separator The column separator.
   * @param first     The codec for decoding the first dimension.
   * @param second    The codec for decoding the second dimension.
   * @param third     The codec for decoding the third dimension.
   * @param fourth    The codec for decoding the fourth dimension.
   * @param fifth     The codec for decoding the fifth dimension.
   * @param sixth     The codec for decoding the sixth dimension.
   */
  def parse6D(
    separator: String = "|",
    first: Codec = StringCodec,
    second: Codec = StringCodec,
    third: Codec = StringCodec,
    fourth: Codec = StringCodec,
    fifth: Codec = StringCodec,
    sixth: Codec = StringCodec
  ): (String) => TraversableOnce[Either[String, Cell[_6]]] = line =>
    parseXD[
      _6,
      (Codec, Codec, Codec, Codec, Codec, Codec)
    ](
      separator,
      (first, second, third, fourth, fifth, sixth),
      line
    )

  /**
   * Parse a line into a `Cell[_6]` with a dictionary.
   *
   * @param dict      The dictionary describing the features in the data.
   * @param dim       The dimension on which to apply the dictionary.
   * @param separator The column separator.
   * @param first     The codec for decoding the first dimension.
   * @param second    The codec for decoding the second dimension.
   * @param third     The codec for decoding the third dimension.
   * @param fourth    The codec for decoding the fourth dimension.
   * @param fifth     The codec for decoding the fifth dimension.
   * @param sixth     The codec for decoding the sixth dimension.
   */
  def parse6DWithDictionary(
    dict: Map[String, Content.Parser],
    dim: Nat,
    separator: String = "|",
    first: Codec = StringCodec,
    second: Codec = StringCodec,
    third: Codec = StringCodec,
    fourth: Codec = StringCodec,
    fifth: Codec = StringCodec,
    sixth: Codec = StringCodec
  )(implicit
    ev1: LTEq[dim.N, _6],
    ev2: ToInt[dim.N]
  ): (String) => TraversableOnce[Either[String, Cell[_6]]] = line =>
    parseXDWithDictionary[
      _6,
      (Codec, Codec, Codec, Codec, Codec, Codec),
      dim.N
    ](
      dict,
      separator,
      (first, second, third, fourth, fifth, sixth),
      line
    )

  /**
   * Parse a line into a `Cell[_6]` with a schema.
   *
   * @param schema    The schema for decoding the data.
   * @param separator The column separator.
   * @param first     The codec for decoding the first dimension.
   * @param second    The codec for decoding the second dimension.
   * @param third     The codec for decoding the third dimension.
   * @param fourth    The codec for decoding the fourth dimension.
   * @param fifth     The codec for decoding the fifth dimension.
   * @param sixth     The codec for decoding the sixth dimension.
   */
  def parse6DWithSchema(
    schema: Content.Parser,
    separator: String = "|",
    first: Codec = StringCodec,
    second: Codec = StringCodec,
    third: Codec = StringCodec,
    fourth: Codec = StringCodec,
    fifth: Codec = StringCodec,
    sixth: Codec = StringCodec
  ): (String) => TraversableOnce[Either[String, Cell[_6]]] = line =>
    parseXDWithSchema[
      _6,
      (Codec, Codec, Codec, Codec, Codec, Codec)
    ](
      schema,
      separator,
      (first, second, third, fourth, fifth, sixth),
      line
    )

  /**
   * Parse a line into a `List[Cell[_2]]` with column definitions.
   *
   * @param columns   `List[(String, Content.Parser)]` describing each column in the table.
   * @param pkeyIndex Index (into `columns`) describing which column is the primary key.
   * @param separator The column separator.
   */
  def parseTable(
    columns: List[(String, Content.Parser)],
    pkeyIndex: Int = 0,
    separator: String = "\u0001"
  ): (String) => TraversableOnce[Either[String, Cell[_2]]] = line => {
    val parts = line.trim.split(Pattern.quote(separator), columns.length)

    if (parts.length == columns.length) {
      val pkey = parts(pkeyIndex)

      columns.zipWithIndex.flatMap { case ((name, decoder), idx) =>
        if (idx != pkeyIndex)
          decoder(parts(idx))
            .map(con => Right(Cell(Position(pkey, name), con)))
            .orElse(Option(Left("Unable to decode: '" + line + "'")))
        else
          None
      }
    } else
      List(Left("Unable to split: '" + line + "'"))
  }

  /**
   * Return function that returns a string representation of a cell.
   *
   * @param descriptive Indicator if descriptive string is required or not.
   * @param separator   The separator to use between various fields (only used if descriptive is `false`).
   * @param codec       Indicator if codec is required or not (only used if descriptive is `false`).
   * @param schema      Indicator if schema is required or not (only used if descriptive is `false`).
   */
  def toString[
    P <: Nat
  ](
    descriptive: Boolean = false,
    separator: String = "|",
    codec: Boolean = true,
    schema: Boolean = true
  ): (Cell[P]) => TraversableOnce[String] = (t: Cell[P]) =>
    List(if (descriptive) t.toString else t.toShortString(separator, codec, schema))

  private def parseXD[
    Q <: Nat : ToInt,
    P <: Product
  ](
    separator: String,
    codecs: P,
    line: String
  )(implicit
    mc: CellFactory.Aux[P, Option[Cell[Q]]]
  ): TraversableOnce[Either[String, Cell[Q]]] = {
    val split = Nat.toInt[Q]

    line.trim.split(Pattern.quote(separator), split + 3).splitAt(split) match {
      case (pos, Array(c, s, v)) => mc(
          codecs,
          pos,
          v,
          (v: String) => Content.fromShortString(c + separator + s + separator + v, separator)
        )
        .map(Right(_))
        .orElse(Option(Left("Unable to decode: '" + line + "'")))
      case _ => List(Left("Unable to split: '" + line + "'"))
    }
  }

  private def parseXDWithSchema[
    Q <: Nat : ToInt,
    P <: Product
  ](
    schema: Content.Parser,
    separator: String,
    codecs: P,
    line: String
  )(implicit
    mc: CellFactory.Aux[P, Option[Cell[Q]]]
  ): TraversableOnce[Either[String, Cell[Q]]] = {
    val split = Nat.toInt[Q]

    line.trim.split(Pattern.quote(separator), split + 1).splitAt(split) match {
      case (pos, Array(v)) => mc(
          codecs,
          pos,
          v,
          schema
        )
        .map(Right(_))
        .orElse(Option(Left("Unable to decode: '" + line + "'")))
      case _ => List(Left("Unable to split: '" + line + "'"))
    }
  }

  private def parseXDWithDictionary[
    Q <: Nat : ToInt,
    P <: Product,
    D <: Nat : ToInt
  ](
    dict: Map[String, Content.Parser],
    separator: String,
    codecs: P,
    line: String
  )(implicit
    mc: CellFactory.Aux[P, Option[Cell[Q]]]
  ): TraversableOnce[Either[String, Cell[Q]]] = {
    val split = Nat.toInt[Q]
    val idx = Nat.toInt[D]

    def getD(positions: Array[String]): Option[Content.Parser] = for {
      pos <- Try(positions(if (idx == 0) positions.length - 1 else idx - 1)).toOption
      decoder <- dict.get(pos)
    } yield decoder

    line.trim.split(Pattern.quote(separator), split + 1).splitAt(split) match {
      case (pos, Array(v)) => getD(pos) match {
        case Some(decoder) => mc(
            codecs,
            pos,
            v,
            decoder
          )
          .map(Right(_))
          .orElse(Option(Left("Unable to decode: '" + line + "'")))
        case _ => List(Left("Missing schema for: '" + line + "'"))
      }
      case _ => List(Left("Unable to split: '" + line + "'"))
    }
  }
}

