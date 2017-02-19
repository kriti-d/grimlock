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

import commbank.grimlock.framework.content.Content
import commbank.grimlock.framework.encoding.{ Codec, StringCodec, Value }
import commbank.grimlock.framework.position.Position

import com.twitter.scrooge.ThriftStruct

import java.util.regex.Pattern

import org.apache.hadoop.io.Writable

import play.api.libs.json.{ JsError, JsObject, Json, JsResult, JsSuccess, JsValue, Reads, Writes }

import scala.util.Try

import shapeless.{ Nat, Sized }
import shapeless.nat.{ _1, _2, _3, _4, _5, _6, _7, _8, _9 }
import shapeless.ops.nat.{ LTEq, ToInt }
import shapeless.syntax.sized._

private trait CellContentParser {
  val textParts: Int

  def getTextParser(pos: Array[String]): Option[(Array[String]) => Option[Content]]
  def getJSONParser(pos: List[Value]): Option[Content.Parser]
}

private case class ParserFromParts(separator: String = "") extends CellContentParser {
  val textParts = 3

  def getTextParser(pos: Array[String]) = Option(
    (con: Array[String]) => con match {
      case Array(c, s, v) => Content.fromComponents(c, s, v)
      case _ => None
    }
  )
  def getJSONParser(pos: List[Value]) = None
}

private case class ParserFromSchema(schema: Content.Parser) extends CellContentParser {
  val textParts = 1

  def getTextParser(pos: Array[String]) = Option(
    (con: Array[String]) => con match {
      case Array(v) => schema(v)
      case _ => None
    }
  )
  def getJSONParser(pos: List[Value]) = Option(schema)
}

private case class ParserFromDictionary[D <: Nat : ToInt](
  dict: Map[String, Content.Parser]
) extends CellContentParser {
  val textParts = 1

  def getTextParser(pos: Array[String]) = {
    val parser = for {
      key <- Try(pos(if (idx == 0) pos.length - 1 else idx - 1)).toOption
      prs <- dict.get(key)
    } yield prs

    parser.map(dec =>
      (con: Array[String]) => con match {
        case Array(v) => dec(v)
        case _ => None
      }
    )
  }
  def getJSONParser(pos: List[Value]) = for {
    key <- Try(pos(if (idx == 0) pos.length - 1 else idx - 1)).toOption
    parser <- dict.get(key.toShortString)
  } yield parser

  private val idx = Nat.toInt[D]
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
   * @param descriptive Indicator if codec and schema are required or not.
   */
  def toShortString(separator: String, descriptive: Boolean = true): String =
    position.toShortString(separator) + separator + content.toShortString(separator, descriptive)

  /**
   * Converts the cell to a JSON string.
   *
   * @param pretty      Indicator if the resulting JSON string to be indented.
   * @param descriptive Indicator if the JSON should be self describing (true) or not.
   */
  def toJSON(pretty: Boolean = false, descriptive: Boolean = true): String = {
    implicit val wrt = Cell.writes[P](descriptive)
    val json = Json.toJson(this)

    if (pretty) Json.prettyPrint(json) else Json.stringify(json)
  }
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
    parseXD(line, separator, Sized.wrap(List(first)), ParserFromParts(separator))

  /**
   * Parse a line into a `Cell[_1]` with a dictionary.
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
    parseXD(line, separator, Sized.wrap(List(first)), ParserFromDictionary[_1](dict))

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
    parseXD(line, separator, Sized.wrap(List(first)), ParserFromSchema(schema))

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
    parseXD(line, separator, Sized.wrap(List(first, second)), ParserFromParts(separator))

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
    parseXD(line, separator, Sized.wrap(List(first, second)), ParserFromDictionary[dim.N](dict))

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
    parseXD(line, separator, Sized.wrap(List(first, second)), ParserFromSchema(schema))

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
    parseXD(line, separator, Sized.wrap(List(first, second, third)), ParserFromParts(separator))

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
    parseXD(line, separator, Sized.wrap(List(first, second, third)), ParserFromDictionary[dim.N](dict))

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
    parseXD(line, separator, Sized.wrap(List(first, second, third)), ParserFromSchema(schema))

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
    parseXD(line, separator, Sized.wrap(List(first, second, third, fourth)), ParserFromParts(separator))

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
    parseXD(line, separator, Sized.wrap(List(first, second, third, fourth)), ParserFromDictionary[dim.N](dict))

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
    parseXD(line, separator, Sized.wrap(List(first, second, third, fourth)), ParserFromSchema(schema))

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
    parseXD(line, separator, Sized.wrap(List(first, second, third, fourth, fifth)), ParserFromParts(separator))

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
    parseXD(line, separator, Sized.wrap(List(first, second, third, fourth, fifth)), ParserFromDictionary[dim.N](dict))

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
    parseXD(line, separator, Sized.wrap(List(first, second, third, fourth, fifth)), ParserFromSchema(schema))

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
    parseXD(line, separator, Sized.wrap(List(first, second, third, fourth, fifth, sixth)), ParserFromParts(separator))

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
      ParserFromDictionary[dim.N](dict)
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
    parseXD(line, separator, Sized.wrap(List(first, second, third, fourth, fifth, sixth)), ParserFromSchema(schema))

  /**
   * Parse a line into a `Cell[_7]`.
   *
   * @param separator The column separator.
   * @param first     The codec for decoding the first dimension.
   * @param second    The codec for decoding the second dimension.
   * @param third     The codec for decoding the third dimension.
   * @param fourth    The codec for decoding the fourth dimension.
   * @param fifth     The codec for decoding the fifth dimension.
   * @param sixth     The codec for decoding the sixth dimension.
   * @param seventh   The codec for decoding the seventh dimension.
   */
  def parse7D(
    separator: String = "|",
    first: Codec = StringCodec,
    second: Codec = StringCodec,
    third: Codec = StringCodec,
    fourth: Codec = StringCodec,
    fifth: Codec = StringCodec,
    sixth: Codec = StringCodec,
    seventh: Codec = StringCodec
  ): (String) => TraversableOnce[Either[String, Cell[_7]]] = line =>
    parseXD(
      line,
      separator,
      Sized.wrap(List(first, second, third, fourth, fifth, sixth, seventh)),
      ParserFromParts(separator)
    )

  /**
   * Parse a line into a `Cell[_7]` with a dictionary.
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
   * @param seventh   The codec for decoding the seventh dimension.
   */
  def parse7DWithDictionary(
    dict: Map[String, Content.Parser],
    dim: Nat,
    separator: String = "|",
    first: Codec = StringCodec,
    second: Codec = StringCodec,
    third: Codec = StringCodec,
    fourth: Codec = StringCodec,
    fifth: Codec = StringCodec,
    sixth: Codec = StringCodec,
    seventh: Codec = StringCodec
  )(implicit
    ev1: LTEq[dim.N, _7],
    ev2: ToInt[dim.N]
  ): (String) => TraversableOnce[Either[String, Cell[_7]]] = line =>
    parseXD(
      line,
      separator,
      Sized.wrap(List(first, second, third, fourth, fifth, sixth, seventh)),
      ParserFromDictionary[dim.N](dict)
    )

  /**
   * Parse a line into a `Cell[_7]` with a schema.
   *
   * @param schema    The schema for decoding the data.
   * @param separator The column separator.
   * @param first     The codec for decoding the first dimension.
   * @param second    The codec for decoding the second dimension.
   * @param third     The codec for decoding the third dimension.
   * @param fourth    The codec for decoding the fourth dimension.
   * @param fifth     The codec for decoding the fifth dimension.
   * @param sixth     The codec for decoding the sixth dimension.
   * @param seventh   The codec for decoding the seventh dimension.
   */
  def parse7DWithSchema(
    schema: Content.Parser,
    separator: String = "|",
    first: Codec = StringCodec,
    second: Codec = StringCodec,
    third: Codec = StringCodec,
    fourth: Codec = StringCodec,
    fifth: Codec = StringCodec,
    sixth: Codec = StringCodec,
    seventh: Codec = StringCodec
  ): (String) => TraversableOnce[Either[String, Cell[_7]]] = line =>
    parseXD(
      line,
      separator,
      Sized.wrap(List(first, second, third, fourth, fifth, sixth, seventh)),
      ParserFromSchema(schema)
    )

  /**
   * Parse a line into a `Cell[_8]`.
   *
   * @param separator The column separator.
   * @param first     The codec for decoding the first dimension.
   * @param second    The codec for decoding the second dimension.
   * @param third     The codec for decoding the third dimension.
   * @param fourth    The codec for decoding the fourth dimension.
   * @param fifth     The codec for decoding the fifth dimension.
   * @param sixth     The codec for decoding the sixth dimension.
   * @param seventh   The codec for decoding the seventh dimension.
   * @param eighth    The codec for decoding the eighth dimension.
   */
  def parse8D(
    separator: String = "|",
    first: Codec = StringCodec,
    second: Codec = StringCodec,
    third: Codec = StringCodec,
    fourth: Codec = StringCodec,
    fifth: Codec = StringCodec,
    sixth: Codec = StringCodec,
    seventh: Codec = StringCodec,
    eighth: Codec = StringCodec
  ): (String) => TraversableOnce[Either[String, Cell[_8]]] = line =>
    parseXD(
      line,
      separator,
      Sized.wrap(List(first, second, third, fourth, fifth, sixth, seventh, eighth)),
      ParserFromParts(separator)
    )

  /**
   * Parse a line into a `Cell[_8]` with a dictionary.
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
   * @param seventh   The codec for decoding the seventh dimension.
   * @param eighth    The codec for decoding the eighth dimension.
   */
  def parse8DWithDictionary(
    dict: Map[String, Content.Parser],
    dim: Nat,
    separator: String = "|",
    first: Codec = StringCodec,
    second: Codec = StringCodec,
    third: Codec = StringCodec,
    fourth: Codec = StringCodec,
    fifth: Codec = StringCodec,
    sixth: Codec = StringCodec,
    seventh: Codec = StringCodec,
    eighth: Codec = StringCodec
  )(implicit
    ev1: LTEq[dim.N, _8],
    ev2: ToInt[dim.N]
  ): (String) => TraversableOnce[Either[String, Cell[_8]]] = line =>
    parseXD(
      line,
      separator,
      Sized.wrap(List(first, second, third, fourth, fifth, sixth, seventh, eighth)),
      ParserFromDictionary[dim.N](dict)
    )

  /**
   * Parse a line into a `Cell[_8]` with a schema.
   *
   * @param schema    The schema for decoding the data.
   * @param separator The column separator.
   * @param first     The codec for decoding the first dimension.
   * @param second    The codec for decoding the second dimension.
   * @param third     The codec for decoding the third dimension.
   * @param fourth    The codec for decoding the fourth dimension.
   * @param fifth     The codec for decoding the fifth dimension.
   * @param sixth     The codec for decoding the sixth dimension.
   * @param seventh   The codec for decoding the seventh dimension.
   * @param eighth    The codec for decoding the eighth dimension.
   */
  def parse8DWithSchema(
    schema: Content.Parser,
    separator: String = "|",
    first: Codec = StringCodec,
    second: Codec = StringCodec,
    third: Codec = StringCodec,
    fourth: Codec = StringCodec,
    fifth: Codec = StringCodec,
    sixth: Codec = StringCodec,
    seventh: Codec = StringCodec,
    eighth: Codec = StringCodec
  ): (String) => TraversableOnce[Either[String, Cell[_8]]] = line =>
    parseXD(
      line,
      separator,
      Sized.wrap(List(first, second, third, fourth, fifth, sixth, seventh, eighth)),
      ParserFromSchema(schema)
    )

  /**
   * Parse a line into a `Cell[_9]`.
   *
   * @param separator The column separator.
   * @param first     The codec for decoding the first dimension.
   * @param second    The codec for decoding the second dimension.
   * @param third     The codec for decoding the third dimension.
   * @param fourth    The codec for decoding the fourth dimension.
   * @param fifth     The codec for decoding the fifth dimension.
   * @param sixth     The codec for decoding the sixth dimension.
   * @param seventh   The codec for decoding the seventh dimension.
   * @param eighth    The codec for decoding the eighth dimension.
   * @param ninth     The codec for decoding the ninth dimension.
   */
  def parse9D(
    separator: String = "|",
    first: Codec = StringCodec,
    second: Codec = StringCodec,
    third: Codec = StringCodec,
    fourth: Codec = StringCodec,
    fifth: Codec = StringCodec,
    sixth: Codec = StringCodec,
    seventh: Codec = StringCodec,
    eighth: Codec = StringCodec,
    ninth: Codec = StringCodec
  ): (String) => TraversableOnce[Either[String, Cell[_9]]] = line =>
    parseXD(
      line,
      separator,
      Sized.wrap(List(first, second, third, fourth, fifth, sixth, seventh, eighth, ninth)),
      ParserFromParts(separator)
    )

  /**
   * Parse a line into a `Cell[_9]` with a dictionary.
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
   * @param seventh   The codec for decoding the seventh dimension.
   * @param eighth    The codec for decoding the eighth dimension.
   * @param ninth     The codec for decoding the ninth dimension.
   */
  def parse9DWithDictionary(
    dict: Map[String, Content.Parser],
    dim: Nat,
    separator: String = "|",
    first: Codec = StringCodec,
    second: Codec = StringCodec,
    third: Codec = StringCodec,
    fourth: Codec = StringCodec,
    fifth: Codec = StringCodec,
    sixth: Codec = StringCodec,
    seventh: Codec = StringCodec,
    eighth: Codec = StringCodec,
    ninth: Codec = StringCodec
  )(implicit
    ev1: LTEq[dim.N, _9],
    ev2: ToInt[dim.N]
  ): (String) => TraversableOnce[Either[String, Cell[_9]]] = line =>
    parseXD(
      line,
      separator,
      Sized.wrap(List(first, second, third, fourth, fifth, sixth, seventh, eighth, ninth)),
      ParserFromDictionary[dim.N](dict)
    )

  /**
   * Parse a line into a `Cell[_9]` with a schema.
   *
   * @param schema    The schema for decoding the data.
   * @param separator The column separator.
   * @param first     The codec for decoding the first dimension.
   * @param second    The codec for decoding the second dimension.
   * @param third     The codec for decoding the third dimension.
   * @param fourth    The codec for decoding the fourth dimension.
   * @param fifth     The codec for decoding the fifth dimension.
   * @param sixth     The codec for decoding the sixth dimension.
   * @param seventh   The codec for decoding the seventh dimension.
   * @param eighth    The codec for decoding the eighth dimension.
   * @param ninth     The codec for decoding the ninth dimension.
   */
  def parse9DWithSchema(
    schema: Content.Parser,
    separator: String = "|",
    first: Codec = StringCodec,
    second: Codec = StringCodec,
    third: Codec = StringCodec,
    fourth: Codec = StringCodec,
    fifth: Codec = StringCodec,
    sixth: Codec = StringCodec,
    seventh: Codec = StringCodec,
    eighth: Codec = StringCodec,
    ninth: Codec = StringCodec
  ): (String) => TraversableOnce[Either[String, Cell[_9]]] = line =>
    parseXD(
      line,
      separator,
      Sized.wrap(List(first, second, third, fourth, fifth, sixth, seventh, eighth, ninth)),
      ParserFromSchema(schema)
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
   * Parse JSON into a `Cell[_1]`.
   *
   * @param first The codec for decoding the first dimension.
   */
  def parse1DJSON(
    first: Codec = StringCodec
  ): (String) => TraversableOnce[Either[String, Cell[_1]]] = json =>
    parseXDJSON(json, Sized.wrap(List(first)), ParserFromParts())

  /**
   * Parse JSON into a `Cell[_1]` with a dictionary.
   *
   * @param dict  The dictionary describing the features in the data.
   * @param first The codec for decoding the first dimension.
   */
  def parse1DJSONWithDictionary(
    dict: Map[String, Content.Parser],
    first: Codec = StringCodec
  ): (String) => TraversableOnce[Either[String, Cell[_1]]] = json =>
    parseXDJSON(json, Sized.wrap(List(first)), ParserFromDictionary[_1](dict))

  /**
   * Parse JSON into a `Cell[_1]` with a schema.
   *
   * @param schema The schema for decoding the data.
   * @param first  The codec for decoding the first dimension.
   */
  def parse1DJSONWithSchema(
    schema: Content.Parser,
    first: Codec = StringCodec
  ): (String) => TraversableOnce[Either[String, Cell[_1]]] = json =>
    parseXDJSON(json, Sized.wrap(List(first)), ParserFromSchema(schema))

  /**
   * Parse JSON into a `Cell[_2]`.
   *
   * @param first  The codec for decoding the first dimension.
   * @param second The codec for decoding the second dimension.
   */
  def parse2DJSON(
    first: Codec = StringCodec,
    second: Codec = StringCodec
  ): (String) => TraversableOnce[Either[String, Cell[_2]]] = json =>
    parseXDJSON(json, Sized.wrap(List(first, second)), ParserFromParts())

  /**
   * Parse JSON into a `Cell[_2]` with a dictionary.
   *
   * @param dict   The dictionary describing the features in the data.
   * @param dim    The dimension on which to apply the dictionary.
   * @param first  The codec for decoding the first dimension.
   * @param second The codec for decoding the second dimension.
   */
  def parse2DJSONWithDictionary(
    dict: Map[String, Content.Parser],
    dim: Nat,
    first: Codec = StringCodec,
    second: Codec = StringCodec
  )(implicit
    ev1: LTEq[dim.N, _2],
    ev2: ToInt[dim.N]
  ): (String) => TraversableOnce[Either[String, Cell[_2]]] = json =>
    parseXDJSON(json, Sized.wrap(List(first, second)), ParserFromDictionary[dim.N](dict))

  /**
   * Parse JSON into a `Cell[_2]` with a schema.
   *
   * @param schema The schema for decoding the data.
   * @param first  The codec for decoding the first dimension.
   * @param second The codec for decoding the second dimension.
   */
  def parse2DJSONWithSchema(
    schema: Content.Parser,
    first: Codec = StringCodec,
    second: Codec = StringCodec
  ): (String) => TraversableOnce[Either[String, Cell[_2]]] = json =>
    parseXDJSON(json, Sized.wrap(List(first, second)), ParserFromSchema(schema))

  /**
   * Parse JSON into a `Cell[_3]`.
   *
   * @param first  The codec for decoding the first dimension.
   * @param second The codec for decoding the second dimension.
   * @param third  The codec for decoding the third dimension.
   */
  def parse3DJSON(
    first: Codec = StringCodec,
    second: Codec = StringCodec,
    third: Codec = StringCodec
  ): (String) => TraversableOnce[Either[String, Cell[_3]]] = json =>
    parseXDJSON(json, Sized.wrap(List(first, second, third)), ParserFromParts())

  /**
   * Parse JSON into a `Cell[_3]` with a dictionary.
   *
   * @param dict   The dictionary describing the features in the data.
   * @param dim    The dimension on which to apply the dictionary.
   * @param first  The codec for decoding the first dimension.
   * @param second The codec for decoding the second dimension.
   * @param third  The codec for decoding the third dimension.
   */
  def parse3DJSONWithDictionary(
    dict: Map[String, Content.Parser],
    dim: Nat,
    first: Codec = StringCodec,
    second: Codec = StringCodec,
    third: Codec = StringCodec
  )(implicit
    ev1: LTEq[dim.N, _3],
    ev2: ToInt[dim.N]
  ): (String) => TraversableOnce[Either[String, Cell[_3]]] = json =>
    parseXDJSON(json, Sized.wrap(List(first, second, third)), ParserFromDictionary[dim.N](dict))

  /**
   * Parse JSON into a `Cell[_3]` with a schema.
   *
   * @param schema The schema for decoding the data.
   * @param first  The codec for decoding the first dimension.
   * @param second The codec for decoding the second dimension.
   * @param third  The codec for decoding the third dimension.
   */
  def parse3DJSONWithSchema(
    schema: Content.Parser,
    first: Codec = StringCodec,
    second: Codec = StringCodec,
    third: Codec = StringCodec
  ): (String) => TraversableOnce[Either[String, Cell[_3]]] = json =>
    parseXDJSON(json, Sized.wrap(List(first, second, third)), ParserFromSchema(schema))

  /**
   * Parse JSON into a `Cell[_4]`.
   *
   * @param first  The codec for decoding the first dimension.
   * @param second The codec for decoding the second dimension.
   * @param third  The codec for decoding the third dimension.
   * @param fourth The codec for decoding the fourth dimension.
   */
  def parse4DJSON(
    first: Codec = StringCodec,
    second: Codec = StringCodec,
    third: Codec = StringCodec,
    fourth: Codec = StringCodec
  ): (String) => TraversableOnce[Either[String, Cell[_4]]] = json =>
    parseXDJSON(json, Sized.wrap(List(first, second, third, fourth)), ParserFromParts())

  /**
   * Parse JSON into a `Cell[_4]` with a dictionary.
   *
   * @param dict   The dictionary describing the features in the data.
   * @param dim    The dimension on which to apply the dictionary.
   * @param first  The codec for decoding the first dimension.
   * @param second The codec for decoding the second dimension.
   * @param third  The codec for decoding the third dimension.
   * @param fourth The codec for decoding the fourth dimension.
   */
  def parse4DJSONWithDictionary(
    dict: Map[String, Content.Parser],
    dim: Nat,
    first: Codec = StringCodec,
    second: Codec = StringCodec,
    third: Codec = StringCodec,
    fourth: Codec = StringCodec
  )(implicit
    ev1: LTEq[dim.N, _4],
    ev2: ToInt[dim.N]
  ): (String) => TraversableOnce[Either[String, Cell[_4]]] = json =>
    parseXDJSON(json, Sized.wrap(List(first, second, third, fourth)), ParserFromDictionary[dim.N](dict))

  /**
   * Parse JSON into a `Cell[_4]` with a schema.
   *
   * @param schema The schema for decoding the data.
   * @param first  The codec for decoding the first dimension.
   * @param second The codec for decoding the second dimension.
   * @param third  The codec for decoding the third dimension.
   * @param fourth The codec for decoding the fourth dimension.
   */
  def parse4DJSONWithSchema(
    schema: Content.Parser,
    first: Codec = StringCodec,
    second: Codec = StringCodec,
    third: Codec = StringCodec,
    fourth: Codec = StringCodec
  ): (String) => TraversableOnce[Either[String, Cell[_4]]] = json =>
    parseXDJSON(json, Sized.wrap(List(first, second, third, fourth)), ParserFromSchema(schema))

  /**
   * Parse JSON into a `Cell[_5]`.
   *
   * @param first  The codec for decoding the first dimension.
   * @param second The codec for decoding the second dimension.
   * @param third  The codec for decoding the third dimension.
   * @param fourth The codec for decoding the fourth dimension.
   * @param fifth  The codec for decoding the fifth dimension.
   */
  def parse5DJSON(
    first: Codec = StringCodec,
    second: Codec = StringCodec,
    third: Codec = StringCodec,
    fourth: Codec = StringCodec,
    fifth: Codec = StringCodec
  ): (String) => TraversableOnce[Either[String, Cell[_5]]] = json =>
    parseXDJSON(json, Sized.wrap(List(first, second, third, fourth, fifth)), ParserFromParts())

  /**
   * Parse JSON into a `Cell[_5]` with a dictionary.
   *
   * @param dict   The dictionary describing the features in the data.
   * @param dim    The dimension on which to apply the dictionary.
   * @param first  The codec for decoding the first dimension.
   * @param second The codec for decoding the second dimension.
   * @param third  The codec for decoding the third dimension.
   * @param fourth The codec for decoding the fourth dimension.
   * @param fifth  The codec for decoding the fifth dimension.
   */
  def parse5DJSONWithDictionary(
    dict: Map[String, Content.Parser],
    dim: Nat,
    first: Codec = StringCodec,
    second: Codec = StringCodec,
    third: Codec = StringCodec,
    fourth: Codec = StringCodec,
    fifth: Codec = StringCodec
  )(implicit
    ev1: LTEq[dim.N, _5],
    ev2: ToInt[dim.N]
  ): (String) => TraversableOnce[Either[String, Cell[_5]]] = json =>
    parseXDJSON(json, Sized.wrap(List(first, second, third, fourth, fifth)), ParserFromDictionary[dim.N](dict))

  /**
   * Parse JSON into a `Cell[_5]` with a schema.
   *
   * @param schema The schema for decoding the data.
   * @param first  The codec for decoding the first dimension.
   * @param second The codec for decoding the second dimension.
   * @param third  The codec for decoding the third dimension.
   * @param fourth The codec for decoding the fourth dimension.
   * @param fifth  The codec for decoding the fifth dimension.
   */
  def parse5DJSONWithSchema(
    schema: Content.Parser,
    first: Codec = StringCodec,
    second: Codec = StringCodec,
    third: Codec = StringCodec,
    fourth: Codec = StringCodec,
    fifth: Codec = StringCodec
  ): (String) => TraversableOnce[Either[String, Cell[_5]]] = json =>
    parseXDJSON(json, Sized.wrap(List(first, second, third, fourth, fifth)), ParserFromSchema(schema))

  /**
   * Parse JSON into a `Cell[_6]`.
   *
   * @param first  The codec for decoding the first dimension.
   * @param second The codec for decoding the second dimension.
   * @param third  The codec for decoding the third dimension.
   * @param fourth The codec for decoding the fourth dimension.
   * @param fifth  The codec for decoding the fifth dimension.
   * @param sixth  The codec for decoding the sixth dimension.
   */
  def parse6DJSON(
    first: Codec = StringCodec,
    second: Codec = StringCodec,
    third: Codec = StringCodec,
    fourth: Codec = StringCodec,
    fifth: Codec = StringCodec,
    sixth: Codec = StringCodec
  ): (String) => TraversableOnce[Either[String, Cell[_6]]] = json =>
    parseXDJSON(json, Sized.wrap(List(first, second, third, fourth, fifth, sixth)), ParserFromParts())

  /**
   * Parse JSON into a `Cell[_6]` with a dictionary.
   *
   * @param dict   The dictionary describing the features in the data.
   * @param dim    The dimension on which to apply the dictionary.
   * @param first  The codec for decoding the first dimension.
   * @param second The codec for decoding the second dimension.
   * @param third  The codec for decoding the third dimension.
   * @param fourth The codec for decoding the fourth dimension.
   * @param fifth  The codec for decoding the fifth dimension.
   * @param sixth  The codec for decoding the sixth dimension.
   */
  def parse6DJSONWithDictionary(
    dict: Map[String, Content.Parser],
    dim: Nat,
    first: Codec = StringCodec,
    second: Codec = StringCodec,
    third: Codec = StringCodec,
    fourth: Codec = StringCodec,
    fifth: Codec = StringCodec,
    sixth: Codec = StringCodec
  )(implicit
    ev1: LTEq[dim.N, _6],
    ev2: ToInt[dim.N]
  ): (String) => TraversableOnce[Either[String, Cell[_6]]] = json =>
    parseXDJSON(json, Sized.wrap(List(first, second, third, fourth, fifth, sixth)), ParserFromDictionary[dim.N](dict))

  /**
   * Parse JSON into a `Cell[_6]` with a schema.
   *
   * @param schema The schema for decoding the data.
   * @param first  The codec for decoding the first dimension.
   * @param second The codec for decoding the second dimension.
   * @param third  The codec for decoding the third dimension.
   * @param fourth The codec for decoding the fourth dimension.
   * @param fifth  The codec for decoding the fifth dimension.
   * @param sixth  The codec for decoding the sixth dimension.
   */
  def parse6DJSONWithSchema(
    schema: Content.Parser,
    first: Codec = StringCodec,
    second: Codec = StringCodec,
    third: Codec = StringCodec,
    fourth: Codec = StringCodec,
    fifth: Codec = StringCodec,
    sixth: Codec = StringCodec
  ): (String) => TraversableOnce[Either[String, Cell[_6]]] = json =>
    parseXDJSON(json, Sized.wrap(List(first, second, third, fourth, fifth, sixth)), ParserFromSchema(schema))

  /**
   * Parse JSON into a `Cell[_7]`.
   *
   * @param first   The codec for decoding the first dimension.
   * @param second  The codec for decoding the second dimension.
   * @param third   The codec for decoding the third dimension.
   * @param fourth  The codec for decoding the fourth dimension.
   * @param fifth   The codec for decoding the fifth dimension.
   * @param sixth   The codec for decoding the sixth dimension.
   * @param seventh The codec for decoding the seventh dimension.
   */
  def parse7DJSON(
    first: Codec = StringCodec,
    second: Codec = StringCodec,
    third: Codec = StringCodec,
    fourth: Codec = StringCodec,
    fifth: Codec = StringCodec,
    sixth: Codec = StringCodec,
    seventh: Codec = StringCodec
  ): (String) => TraversableOnce[Either[String, Cell[_7]]] = json =>
    parseXDJSON(json, Sized.wrap(List(first, second, third, fourth, fifth, sixth, seventh)), ParserFromParts())

  /**
   * Parse JSON into a `Cell[_7]` with a dictionary.
   *
   * @param dict    The dictionary describing the features in the data.
   * @param dim     The dimension on which to apply the dictionary.
   * @param first   The codec for decoding the first dimension.
   * @param second  The codec for decoding the second dimension.
   * @param third   The codec for decoding the third dimension.
   * @param fourth  The codec for decoding the fourth dimension.
   * @param fifth   The codec for decoding the fifth dimension.
   * @param sixth   The codec for decoding the sixth dimension.
   * @param seventh The codec for decoding the seventh dimension.
   */
  def parse7DJSONWithDictionary(
    dict: Map[String, Content.Parser],
    dim: Nat,
    first: Codec = StringCodec,
    second: Codec = StringCodec,
    third: Codec = StringCodec,
    fourth: Codec = StringCodec,
    fifth: Codec = StringCodec,
    sixth: Codec = StringCodec,
    seventh: Codec = StringCodec
  )(implicit
    ev1: LTEq[dim.N, _7],
    ev2: ToInt[dim.N]
  ): (String) => TraversableOnce[Either[String, Cell[_7]]] = json =>
    parseXDJSON(
      json,
      Sized.wrap(List(first, second, third, fourth, fifth, sixth, seventh)),
      ParserFromDictionary[dim.N](dict)
    )

  /**
   * Parse JSON into a `Cell[_7]` with a schema.
   *
   * @param schema  The schema for decoding the data.
   * @param first   The codec for decoding the first dimension.
   * @param second  The codec for decoding the second dimension.
   * @param third   The codec for decoding the third dimension.
   * @param fourth  The codec for decoding the fourth dimension.
   * @param fifth   The codec for decoding the fifth dimension.
   * @param sixth   The codec for decoding the sixth dimension.
   * @param seventh The codec for decoding the seventh dimension.
   */
  def parse7DJSONWithSchema(
    schema: Content.Parser,
    first: Codec = StringCodec,
    second: Codec = StringCodec,
    third: Codec = StringCodec,
    fourth: Codec = StringCodec,
    fifth: Codec = StringCodec,
    sixth: Codec = StringCodec,
    seventh: Codec = StringCodec
  ): (String) => TraversableOnce[Either[String, Cell[_7]]] = json =>
    parseXDJSON(json, Sized.wrap(List(first, second, third, fourth, fifth, sixth, seventh)), ParserFromSchema(schema))

  /**
   * Parse JSON into a `Cell[_8]`.
   *
   * @param first   The codec for decoding the first dimension.
   * @param second  The codec for decoding the second dimension.
   * @param third   The codec for decoding the third dimension.
   * @param fourth  The codec for decoding the fourth dimension.
   * @param fifth   The codec for decoding the fifth dimension.
   * @param sixth   The codec for decoding the sixth dimension.
   * @param seventh The codec for decoding the seventh dimension.
   * @param eighth  The codec for decoding the eighth dimension.
   */
  def parse8DJSON(
    first: Codec = StringCodec,
    second: Codec = StringCodec,
    third: Codec = StringCodec,
    fourth: Codec = StringCodec,
    fifth: Codec = StringCodec,
    sixth: Codec = StringCodec,
    seventh: Codec = StringCodec,
    eighth: Codec = StringCodec
  ): (String) => TraversableOnce[Either[String, Cell[_8]]] = json =>
    parseXDJSON(json, Sized.wrap(List(first, second, third, fourth, fifth, sixth, seventh, eighth)), ParserFromParts())

  /**
   * Parse JSON into a `Cell[_8]` with a dictionary.
   *
   * @param dict    The dictionary describing the features in the data.
   * @param dim     The dimension on which to apply the dictionary.
   * @param first   The codec for decoding the first dimension.
   * @param second  The codec for decoding the second dimension.
   * @param third   The codec for decoding the third dimension.
   * @param fourth  The codec for decoding the fourth dimension.
   * @param fifth   The codec for decoding the fifth dimension.
   * @param sixth   The codec for decoding the sixth dimension.
   * @param seventh The codec for decoding the seventh dimension.
   * @param eighth  The codec for decoding the eighth dimension.
   */
  def parse8DJSONWithDictionary(
    dict: Map[String, Content.Parser],
    dim: Nat,
    first: Codec = StringCodec,
    second: Codec = StringCodec,
    third: Codec = StringCodec,
    fourth: Codec = StringCodec,
    fifth: Codec = StringCodec,
    sixth: Codec = StringCodec,
    seventh: Codec = StringCodec,
    eighth: Codec = StringCodec
  )(implicit
    ev1: LTEq[dim.N, _8],
    ev2: ToInt[dim.N]
  ): (String) => TraversableOnce[Either[String, Cell[_8]]] = json =>
    parseXDJSON(
      json,
      Sized.wrap(List(first, second, third, fourth, fifth, sixth, seventh, eighth)),
      ParserFromDictionary[dim.N](dict)
    )

  /**
   * Parse JSON into a `Cell[_8]` with a schema.
   *
   * @param schema  The schema for decoding the data.
   * @param first   The codec for decoding the first dimension.
   * @param second  The codec for decoding the second dimension.
   * @param third   The codec for decoding the third dimension.
   * @param fourth  The codec for decoding the fourth dimension.
   * @param fifth   The codec for decoding the fifth dimension.
   * @param sixth   The codec for decoding the sixth dimension.
   * @param seventh The codec for decoding the seventh dimension.
   * @param eighth  The codec for decoding the eighth dimension.
   */
  def parse8DJSONWithSchema(
    schema: Content.Parser,
    first: Codec = StringCodec,
    second: Codec = StringCodec,
    third: Codec = StringCodec,
    fourth: Codec = StringCodec,
    fifth: Codec = StringCodec,
    sixth: Codec = StringCodec,
    seventh: Codec = StringCodec,
    eighth: Codec = StringCodec
  ): (String) => TraversableOnce[Either[String, Cell[_8]]] = json =>
    parseXDJSON(
      json,
      Sized.wrap(List(first, second, third, fourth, fifth, sixth, seventh, eighth)),
      ParserFromSchema(schema)
    )

  /**
   * Parse JSON into a `Cell[_9]`.
   *
   * @param first   The codec for decoding the first dimension.
   * @param second  The codec for decoding the second dimension.
   * @param third   The codec for decoding the third dimension.
   * @param fourth  The codec for decoding the fourth dimension.
   * @param fifth   The codec for decoding the fifth dimension.
   * @param sixth   The codec for decoding the sixth dimension.
   * @param seventh The codec for decoding the seventh dimension.
   * @param eighth  The codec for decoding the eighth dimension.
   * @param ninth   The codec for decoding the ninth dimension.
   */
  def parse9DJSON(
    first: Codec = StringCodec,
    second: Codec = StringCodec,
    third: Codec = StringCodec,
    fourth: Codec = StringCodec,
    fifth: Codec = StringCodec,
    sixth: Codec = StringCodec,
    seventh: Codec = StringCodec,
    eighth: Codec = StringCodec,
    ninth: Codec = StringCodec
  ): (String) => TraversableOnce[Either[String, Cell[_9]]] = json =>
    parseXDJSON(
      json,
      Sized.wrap(List(first, second, third, fourth, fifth, sixth, seventh, eighth, ninth)),
      ParserFromParts()
    )

  /**
   * Parse JSON into a `Cell[_9]` with a dictionary.
   *
   * @param dict    The dictionary describing the features in the data.
   * @param dim     The dimension on which to apply the dictionary.
   * @param first   The codec for decoding the first dimension.
   * @param second  The codec for decoding the second dimension.
   * @param third   The codec for decoding the third dimension.
   * @param fourth  The codec for decoding the fourth dimension.
   * @param fifth   The codec for decoding the fifth dimension.
   * @param sixth   The codec for decoding the sixth dimension.
   * @param seventh The codec for decoding the seventh dimension.
   * @param eighth  The codec for decoding the eighth dimension.
   * @param ninth   The codec for decoding the ninth dimension.
   */
  def parse9DJSONWithDictionary(
    dict: Map[String, Content.Parser],
    dim: Nat,
    first: Codec = StringCodec,
    second: Codec = StringCodec,
    third: Codec = StringCodec,
    fourth: Codec = StringCodec,
    fifth: Codec = StringCodec,
    sixth: Codec = StringCodec,
    seventh: Codec = StringCodec,
    eighth: Codec = StringCodec,
    ninth: Codec = StringCodec
  )(implicit
    ev1: LTEq[dim.N, _9],
    ev2: ToInt[dim.N]
  ): (String) => TraversableOnce[Either[String, Cell[_9]]] = json =>
    parseXDJSON(
      json,
      Sized.wrap(List(first, second, third, fourth, fifth, sixth, seventh, eighth, ninth)),
      ParserFromDictionary[dim.N](dict)
    )

  /**
   * Parse JSON into a `Cell[_9]` with a schema.
   *
   * @param schema  The schema for decoding the data.
   * @param first   The codec for decoding the first dimension.
   * @param second  The codec for decoding the second dimension.
   * @param third   The codec for decoding the third dimension.
   * @param fourth  The codec for decoding the fourth dimension.
   * @param fifth   The codec for decoding the fifth dimension.
   * @param sixth   The codec for decoding the sixth dimension.
   * @param seventh The codec for decoding the seventh dimension.
   * @param eighth  The codec for decoding the eighth dimension.
   * @param ninth   The codec for decoding the ninth dimension.
   */
  def parse9DJSONWithSchema(
    schema: Content.Parser,
    first: Codec = StringCodec,
    second: Codec = StringCodec,
    third: Codec = StringCodec,
    fourth: Codec = StringCodec,
    fifth: Codec = StringCodec,
    sixth: Codec = StringCodec,
    seventh: Codec = StringCodec,
    eighth: Codec = StringCodec,
    ninth: Codec = StringCodec
  ): (String) => TraversableOnce[Either[String, Cell[_9]]] = json =>
    parseXDJSON(
      json,
      Sized.wrap(List(first, second, third, fourth, fifth, sixth, seventh, eighth, ninth)),
      ParserFromSchema(schema)
    )

  /**
   * Return function that returns a string representation of a cell.
   *
   * @param verbose     Indicator if verbose string is required or not.
   * @param separator   The separator to use between various fields (only used if verbose is `false`).
   * @param descriptive Indicator if codec and schema are required or not (only used if verbose is `false`).
   */
  def toString[
    P <: Nat
  ](
    verbose: Boolean = false,
    separator: String = "|",
    descriptive: Boolean = true
  ): (Cell[P]) => TraversableOnce[String] = (t: Cell[P]) =>
    List(if (verbose) t.toString else t.toShortString(separator, descriptive))

  /**
   * Return function that returns a JSON representation of a cell.
   *
   * @param pretty      Indicator if the resulting JSON string to be indented.
   * @param descriptive Indicator if the JSON should be self describing (true) or not.
   */
  def toJSON[P <: Nat ](pretty: Boolean = false, descriptive: Boolean = true): (Cell[P]) => TraversableOnce[String] =
    (t: Cell[P]) => List(t.toJSON(pretty, descriptive))

  private def parseXD[
    Q <: Nat : ToInt
  ](
    line: String,
    separator: String,
    codecs: Sized[List[Codec], Q],
    parser: CellContentParser
  ): TraversableOnce[Either[String, Cell[Q]]] = {
    val split = Nat.toInt[Q]

    val (pos, con) = line.trim.split(Pattern.quote(separator), split + parser.textParts).splitAt(split)

    if (pos.size != split || con.size != parser.textParts)
      List(Left("Unable to split: '" + line + "'"))
    else
      parser.getTextParser(pos) match {
        case Some(prs) =>
          val cell = for {
            p <- codecs.zip(pos).flatMap { case (c, p) => c.decode(p) }.sized[Q]
            c <- prs(con)
          } yield Right(Cell(Position(p), c))

          cell.orElse(Option(Left("Unable to decode: '" + line + "'")))
        case _ =>  List(Left("Missing schema for: '" + line + "'"))
      }
  }

  private def parseXDJSON[
    Q <: Nat : ToInt
  ](
    json: String,
    codecs: Sized[List[Codec], Q],
    parser: CellContentParser
  ): TraversableOnce[Either[String, Cell[Q]]] = List(
    Json.fromJson[Cell[Q]](Json.parse(json))(reads(codecs, parser)) match {
      case JsSuccess(cell, _) => Right(cell)
      case _ => Left(s"Unable to decode: '" + json + "'")
    }
  )

  /**
   * Return a `Reads` for parsing a JSON cell.
   *
   * @param codecs List of codecs for parsing the position.
   * @param parser Optional parser; in case the JSON content is not self describing.
   */
  def reads[P <: Nat : ToInt](
    codecs: Sized[List[Codec], P],
    parser: CellContentParser
  ): Reads[Cell[P]] = new Reads[Cell[P]] {
    implicit val prd = Position.reads(codecs)

    def reads(json: JsValue): JsResult[Cell[P]] = {
      val fields = json.as[JsObject].value

      if (fields.size == 2)
        (
          for {
            pos <- fields.get("position").map(_.as[Position[P]])
            con <- fields.get("content").map(_.as[Content](Content.reads(parser.getJSONParser(pos.coordinates))))
          } yield JsSuccess(Cell(pos, con))
        ).getOrElse(JsError("Unable to parse cell"))
      else
        JsError("Incorrect number of fields")
    }
  }

  /**
   * Return a `Writes` for writing JSON cell.
   *
   * @param descriptive Indicator if the JSON should be self describing (true) or not.
   */
  def writes[P <: Nat](descriptive: Boolean): Writes[Cell[P]] = new Writes[Cell[P]] {
    implicit val wrt = Content.writes(descriptive)

    def writes(o: Cell[P]): JsValue = Json.obj("position" -> o.position, "content" -> o.content)
  }
}

