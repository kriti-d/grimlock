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

import shapeless.{ Nat, Sized }
import shapeless.nat.{ _1, _2, _3, _4, _5, _6 }
import shapeless.ops.nat.{ LTEq, ToInt }
import shapeless.syntax.sized._

private trait CellContentDecoder {
  val parts: Int

  def decode(pos: Array[String]): Option[(Array[String]) => Option[Content]]
}

private case class DecodeFromParts(separator: String) extends CellContentDecoder {
  val parts = 3

  def decode(pos: Array[String]) = Option(
    (con: Array[String]) => con match {
      case Array(c, s, v) => Content.fromComponents(c, s, v)
      case _ => None
    }
  )
}

private case class DecodeWithSchema(schema: Content.Parser) extends CellContentDecoder {
  val parts = 1

  def decode(pos: Array[String]) = Option(
    (con: Array[String]) => con match {
      case Array(v) => schema(v)
      case _ => None
    }
  )
}

private case class DecodeWithDictionary[D <: Nat : ToInt](
  dict: Map[String, Content.Parser]
) extends CellContentDecoder {
  val parts = 1
  val idx = Nat.toInt[D]

  def decode(pos: Array[String]) = {
    val decoder = for {
      key <- Try(pos(if (idx == 0) pos.length - 1 else idx - 1)).toOption
      dec <- dict.get(key)
    } yield dec

    decoder.map(dec =>
      (con: Array[String]) => con match {
        case Array(v) => dec(v)
        case _ => None
      }
    )
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
    parseXD(line, separator, Sized.wrap(List(first)), DecodeFromParts(separator))

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
    parseXD(line, separator, Sized.wrap(List(first)), DecodeWithDictionary[_1](dict))

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
    parseXD(line, separator, Sized.wrap(List(first)), DecodeWithSchema(schema))

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
    parseXD(line, separator, Sized.wrap(List(first, second)), DecodeFromParts(separator))

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
    parseXD(line, separator, Sized.wrap(List(first, second)), DecodeWithDictionary[dim.N](dict))

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
    parseXD(line, separator, Sized.wrap(List(first, second)), DecodeWithSchema(schema))

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
    parseXD(line, separator, Sized.wrap(List(first, second, third)), DecodeFromParts(separator))

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
    parseXD(line, separator, Sized.wrap(List(first, second, third)), DecodeWithDictionary[dim.N](dict))

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
    parseXD(line, separator, Sized.wrap(List(first, second, third)), DecodeWithSchema(schema))

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
    parseXD(line, separator, Sized.wrap(List(first, second, third, fourth)), DecodeFromParts(separator))

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
    parseXD(line, separator, Sized.wrap(List(first, second, third, fourth)), DecodeWithDictionary[dim.N](dict))

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
    parseXD(line, separator, Sized.wrap(List(first, second, third, fourth)), DecodeWithSchema(schema))

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
    parseXD(line, separator, Sized.wrap(List(first, second, third, fourth, fifth)), DecodeFromParts(separator))

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
    parseXD(line, separator, Sized.wrap(List(first, second, third, fourth, fifth)), DecodeWithDictionary[dim.N](dict))

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
    parseXD(line, separator, Sized.wrap(List(first, second, third, fourth, fifth)), DecodeWithSchema(schema))

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
    parseXD(line, separator, Sized.wrap(List(first, second, third, fourth, fifth, sixth)), DecodeFromParts(separator))

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
    parseXD(
      line,
      separator,
      Sized.wrap(List(first, second, third, fourth, fifth, sixth)),
      DecodeWithDictionary[dim.N](dict)
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
    parseXD(line, separator, Sized.wrap(List(first, second, third, fourth, fifth, sixth)), DecodeWithSchema(schema))

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
    Q <: Nat : ToInt
  ](
    line: String,
    separator: String,
    codecs: Sized[List[Codec], Q],
    decoder: CellContentDecoder
  ): TraversableOnce[Either[String, Cell[Q]]] = {
    val split = Nat.toInt[Q]

    val (pos, con) = line.trim.split(Pattern.quote(separator), split + decoder.parts).splitAt(split)

    if (pos.size != split || con.size != decoder.parts)
      List(Left("Unable to split: '" + line + "'"))
    else
      decoder.decode(pos) match {
        case Some(dec) =>
          val cell = for {
            p <- codecs.zip(pos).flatMap { case (c, p) => c.decode(p) }.sized[Q]
            c <- dec(con)
          } yield Right(Cell(Position(p), c))

          cell.orElse(Option(Left("Unable to decode: '" + line + "'")))
        case _ =>  List(Left("Missing schema for: '" + line + "'"))
      }
  }
}

