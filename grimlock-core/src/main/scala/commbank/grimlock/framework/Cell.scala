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
import commbank.grimlock.framework.encoding.{ Codec, Value }
import commbank.grimlock.framework.metadata.Schema
import commbank.grimlock.framework.position.{ Coordinates2, Position }
import commbank.grimlock.framework.utility.JSON

import java.util.regex.Pattern

import play.api.libs.json.{ JsError, JsObject, Json, JsResult, JsSuccess, JsValue, Reads, Writes }

import shapeless.{ HList, Nat }
import shapeless.ops.nat.ToInt

/**
 * Cell in a matrix.
 *
 * @param position The position of the cell in the matri.
 * @param content  The contents of the cell.
 */
case class Cell[P <: HList](position: Position[P], content: Content) {
  /**
   * Mutate the content of this cell.
   *
   * @param mutator Function that returns the new content for this cell.
   */
  def mutate(mutator: (Cell[P]) => Content): Cell[P] = Cell(position, mutator(this))

  /**
   * Relocate this cell.
   *
   * @param relocator Function that returns the new position for this cell.
   */
  def relocate[Q <: HList](relocator: (Cell[P]) => Position[Q]): Cell[Q] = Cell(relocator(this), content)

  /**
   * Converts the cell to a JSON string.
   *
   * @param verbose Indicator if the JSON should be self describing or not.
   * @param pretty  Indicator if the resulting JSON string to be indented.
   */
  def toJSON(verbose: Boolean, pretty: Boolean = false): String = JSON.to(this, Cell.writes[P](verbose), pretty)

  /**
   * Return string representation of a cell.
   *
   * @param verbose   Indicator if codec and schema are required or not.
   * @param separator The separator to use between various fields.
   */
  def toShortString(verbose: Boolean, separator: String): String = position.toShortString(separator) +
    separator +
    (if (verbose) content.toShortString(separator) else content.toShortString)
}

/** Companion object to the Cell class. */
object Cell {
  /** Predicate used in, for example, the `which` methods of a matrix for finding content. */
  type Predicate[P <: HList] = Cell[P] => Boolean

  /**
   * Parse a cell from string components.
   *
   * @param coordinates The coordinates of the position.
   * @param codec       The codec string to decode content with.
   * @param schema      The schema string to validate content with.
   * @param value       The content string value to parse.
   * @param codecs      The codecs for decoding the position.
   *
   * @return A `Some[Cell[Q]]` if successful, `None` otherwise.
   */
  def fromComponents[
    L <: HList,
    Q <: HList
  ](
    coordinates: List[String],
    codec: String,
    schema: String,
    value: String,
    codecs: L
  )(implicit
    ev: Position.TextParseConstraints.Aux[L, Q]
  ): Option[Cell[Q]] = for {
    pos <- Position.fromComponents(coordinates, codecs)
    con <- Content.fromComponents(codec, schema, value)
  } yield Cell(pos, con)

  /**
   * Parse self-describing JSON into a `Cell[Q]`.
   *
   * @param str    The JSON string to decode.
   * @param codecs The codecs for decoding the position.
   *
   * @return A `Some[Cell[Q]]` if successful, `None` otherwise.
   */
  def fromJSON[
    L <: HList,
    Q <: HList
  ](
    str: String,
    codecs: L
  )(implicit
    ev: Position.TextParseConstraints.Aux[L, Q]
  ): Option[Cell[Q]] = JSON.from(str, reads(codecs)).right.toOption

  /**
   * Parse JSON into a `Cell[Q]`.
   *
   * @param str     The JSON string to decode.
   * @param codecs  The codecs for decoding the position.
   * @param decoder Single decoder to decode all data.
   *
   * @return A `Some[Cell[Q]]` if successful, `None` otherwise.
   */
  def fromJSON[
    L <: HList,
    Q <: HList
  ](
    str: String,
    codecs: L,
    decoder: Content.Decoder
  )(implicit
    ev: Position.TextParseConstraints.Aux[L, Q]
  ): Option[Cell[Q]] = JSON.from(str, reads(codecs, decoder)).right.toOption

  /**
   * Parse JSON into a `Cell[Q]`.
   *
   * @param str    The JSON string to decode.
   * @param codecs The codecs for decoding the position.
   * @param codec  Single codec to use to decode all data.
   * @param schema Single schema to validate all data.
   *
   * @return A `Some[Cell[Q]]` if successful, `None` otherwise.
   */
  def fromJSON[
    L <: HList,
    T,
    Q <: HList
  ](
    str: String,
    codecs: L,
    codec: Codec[T],
    schema: Schema[T]
  )(implicit
    ev: Position.TextParseConstraints.Aux[L, Q]
  ): Option[Cell[Q]] = JSON.from(str, reads(codecs, codec, schema)).right.toOption

  /**
   * Parse JSON into a `Cell[Q]`.
   *
   * @param str        The JSON string to decode.
   * @param codecs     The codecs for decoding the position.
   * @param dictionary Map of coordinate to content decoder.
   * @param dimension  The index of the position's coordinate to use to lookup the decoder.
   *
   * @return A `Some[Cell[Q]]` if successful, `None` otherwise.
   */
  def fromJSON[
    L <: HList,
    T,
    D <: Nat,
    Q <: HList
  ](
    str: String,
    codecs: L,
    dictionary: Map[T, Content.Decoder],
    dimension: D
  )(implicit
    ev1: Position.TextParseConstraints.Aux[L, Q],
    ev2: Position.IndexConstraints.Aux[Q, D, Value[T]]
  ): Option[Cell[Q]] = JSON.from(str, reads(codecs, dictionary, dimension)).right.toOption

  /**
   * Parse a self-describing short string into a `Cell[Q]`.
   *
   * @param str       The JSON string to decode.
   * @param codecs    The codecs for decoding the position.
   * @param separator The column separator.
   *
   * @return A `Some[Cell[Q]]` if successful, `None` otherwise.
   */
  def fromShortString[
    L <: HList,
    Q <: HList
  ](
    str: String,
    codecs: L,
    separator: String
  )(implicit
    ev: Position.TextParseConstraints.Aux[L, Q]
  ): Option[Cell[Q]] = parse(str, codecs, separator, SelfDescribing(separator)).right.toOption

  /**
   * Parse a short string into a `Cell[Q]`.
   *
   * @param str       The JSON string to decode.
   * @param codecs    The codecs for decoding the position.
   * @param decoder   Single decoder to decode all data.
   * @param separator The column separator.
   *
   * @return A `Some[Cell[Q]]` if successful, `None` otherwise.
   */
  def fromShortString[
    L <: HList,
    Q <: HList
  ](
    str: String,
    codecs: L,
    decoder: Content.Decoder,
    separator: String
  )(implicit
    ev: Position.TextParseConstraints.Aux[L, Q]
  ): Option[Cell[Q]] = parse(str, codecs, separator, FromDecoder(decoder)).right.toOption

  /**
   * Parse a short string into a `Cell[Q]`.
   *
   * @param str       The JSON string to decode.
   * @param codecs    The codecs for decoding the position.
   * @param codec     Single codec to use to decode all data.
   * @param schema    Single schema to validate all data.
   * @param separator The column separator.
   *
   * @return A `Some[Cell[Q]]` if successful, `None` otherwise.
   */
  def fromShortString[
    L <: HList,
    T,
    Q <: HList
  ](
    str: String,
    codecs: L,
    codec: Codec[T],
    schema: Schema[T],
    separator: String
  )(implicit
    ev: Position.TextParseConstraints.Aux[L, Q]
  ): Option[Cell[Q]] = parse(str, codecs, separator, FromCodecSchema(codec, schema)).right.toOption

  /**
   * Parse a short string into a `Cell[Q]`.
   *
   * @param str        The JSON string to decode.
   * @param codecs     The codecs for decoding the position.
   * @param dictionary Map of coordinate to content decoder.
   * @param dimension  The index of the position's coordinate to use to lookup the decoder.
   * @param separator  The column separator.
   *
   * @return A `Some[Cell[Q]]` if successful, `None` otherwise.
   */
  def fromShortString[
    L <: HList,
    T,
    D <: Nat,
    Q <: HList
  ](
    str: String,
    codecs: L,
    dictionary: Map[T, Content.Decoder],
    dimension: D,
    separator: String
  )(implicit
    ev1: Position.TextParseConstraints.Aux[L, Q],
    ev2: Position.IndexConstraints.Aux[Q, D, Value[T]]
  ): Option[Cell[Q]] = parse(str, codecs, separator, FromDictionary(dictionary, dimension)).right.toOption

  /**
   * Parse self-describing JSON into a `Cell[Q]`.
   *
   * @param codecs The codecs for decoding the position.
   *
   * @return The parser function.
   */
  def jsonParser[
    L <: HList,
    Q <: HList
  ](
    codecs: L
  )(implicit
    ev: Position.TextParseConstraints.Aux[L, Q]
  ): Persist.TextParser[Cell[Q]] = (str) => List(JSON.from(str, reads(codecs)))

  /**
   * Parse JSON into a `Cell[Q]`.
   *
   * @param codecs  The codecs for decoding the position.
   * @param decoder Single decoder to decode all data.
   *
   * @return The parser function.
   */
  def jsonParser[
    L <: HList,
    Q <: HList
  ](
    codecs: L,
    decoder: Content.Decoder
  )(implicit
    ev: Position.TextParseConstraints.Aux[L, Q]
  ): Persist.TextParser[Cell[Q]] = (str) => List(JSON.from(str, reads(codecs, decoder)))

  /**
   * Parse JSON into a `Cell[Q]`.
   *
   * @param codecs The codecs for decoding the position.
   * @param codec  Single codec to use to decode all data.
   * @param schema Single schema to validate all data.
   *
   * @return The parser function.
   */
  def jsonParser[
    L <: HList,
    T,
    Q <: HList
  ](
    codecs: L,
    codec: Codec[T],
    schema: Schema[T]
  )(implicit
    ev: Position.TextParseConstraints.Aux[L, Q]
  ): Persist.TextParser[Cell[Q]] = (str) => List(JSON.from(str, reads(codecs, codec, schema)))

  /**
   * Parse JSON into a `Cell[Q]`.
   *
   * @param codecs     The codecs for decoding the position.
   * @param dictionary Map of coordinate to content decoder.
   * @param dimension  The index of the position's coordinate to use to lookup the decoder.
   *
   * @return The parser function.
   */
  def jsonParser[
    L <: HList,
    T,
    D <: Nat,
    Q <: HList
  ](
    codecs: L,
    dictionary: Map[T, Content.Decoder],
    dimension: D
  )(implicit
    ev1: Position.TextParseConstraints.Aux[L, Q],
    ev2: Position.IndexConstraints.Aux[Q, D, Value[T]]
  ): Persist.TextParser[Cell[Q]] = (str) => List(JSON.from(str, reads(codecs, dictionary, dimension)))

  /**
   * Return a `Reads` for parsing a self-describing JSON cell.
   *
   * @param codecs  List of codecs for parsing the position.
   *
   * @return The JSON `Reads`.
   */
  def reads[
    L <: HList,
    Q <: HList
  ](
    codecs: L
  )(implicit
    ev: Position.TextParseConstraints.Aux[L, Q]
  ): Reads[Cell[Q]] = reads(Position.reads(codecs), (_: Position[Q]) => Option(Content.reads))

  /**
   * Return a `Reads` for parsing a JSON cell.
   *
   * @param codecs  List of codecs for parsing the position.
   * @param decoder Single decoder to decode all data.
   *
   * @return The JSON `Reads`.
   */
  def reads[
    L <: HList,
    Q <: HList
  ](
    codecs: L,
    decoder: Content.Decoder
  )(implicit
    ev: Position.TextParseConstraints.Aux[L, Q]
  ): Reads[Cell[Q]] = reads(Position.reads(codecs), (_: Position[Q]) => Option(Content.reads(decoder)))

  /**
   * Return a `Reads` for parsing a JSON cell.
   *
   * @param codecs List of codecs for parsing the position.
   * @param codec  Single codec to use to decode all data.
   * @param schema Single schema to validate all data.
   *
   * @return The JSON `Reads`.
   */
  def reads[
    L <: HList,
    T,
    Q <: HList
  ](
    codecs: L,
    codec: Codec[T],
    schema: Schema[T]
  )(implicit
    ev: Position.TextParseConstraints.Aux[L, Q]
  ): Reads[Cell[Q]] = reads(Position.reads(codecs), (_: Position[Q]) => Option(Content.reads(codec, schema)))

  /**
   * Return a `Reads` for parsing a JSON cell.
   *
   * @param codecs     List of codecs for parsing the position.
   * @param dictionary Map of coordinate to content decoder.
   * @param dimension  The index of the position's coordinate to use to lookup the decoder.
   *
   * @return The JSON `Reads`.
   */
  def reads[
    L <: HList,
    T,
    D <: Nat,
    Q <: HList
  ](
    codecs: L,
    dictionary: Map[T, Content.Decoder],
    dimension: D
  )(implicit
    ev1: Position.TextParseConstraints.Aux[L, Q],
    ev2: Position.IndexConstraints.Aux[Q, D, Value[T]]
  ): Reads[Cell[Q]] = reads(
    Position.reads(codecs),
    (pos: Position[Q]) => dictionary.get(pos(dimension).value).map(Content.reads(_))
  )

  /**
   * Parse a self-describing short string into a `Cell[Q]`.
   *
   * @param codecs    The codecs for decoding the position.
   * @param separator The column separator.
   *
   * @return The parser function.
   */
  def shortStringParser[
    L <: HList,
    Q <: HList
  ](
    codecs: L,
    separator: String
  )(implicit
    ev: Position.TextParseConstraints.Aux[L, Q]
  ): Persist.TextParser[Cell[Q]] = (str) => List(parse(str, codecs, separator, SelfDescribing(separator)))

  /**
   * Parse a short string into a `Cell[Q]`.
   *
   * @param codecs    The codecs for decoding the position.
   * @param decoder   Single decoder to decode all data.
   * @param separator The column separator.
   *
   * @return The parser function.
   */
  def shortStringParser[
    L <: HList,
    Q <: HList
  ](
    codecs: L,
    decoder: Content.Decoder,
    separator: String
  )(implicit
    ev: Position.TextParseConstraints.Aux[L, Q]
  ): Persist.TextParser[Cell[Q]] = (str) => List(parse(str, codecs, separator, FromDecoder(decoder)))

  /**
   * Parse a short string into a `Cell[Q]`.
   *
   * @param codecs    The codecs for decoding the position.
   * @param codec     Single codec to use to decode all data.
   * @param schema    Single schema to validate all data.
   * @param separator The column separator.
   *
   * @return The parser function.
   */
  def shortStringParser[
    L <: HList,
    T,
    Q <: HList
  ](
    codecs: L,
    codec: Codec[T],
    schema: Schema[T],
    separator: String
  )(implicit
    ev: Position.TextParseConstraints.Aux[L, Q]
  ): Persist.TextParser[Cell[Q]] = (str) => List(parse(str, codecs, separator, FromCodecSchema(codec, schema)))

  /**
   * Parse a short string into a `Cell[Q]`.
   *
   * @param codecs     The codecs for decoding the position.
   * @param dictionary Map of coordinate to content decoder.
   * @param dimension  The index of the position's coordinate to use to lookup the decoder.
   * @param separator  The column separator.
   *
   * @return The parser function.
   */
  def shortStringParser[
    L <: HList,
    T,
    D <: Nat,
    Q <: HList
  ](
    codecs: L,
    dictionary: Map[T, Content.Decoder],
    dimension: D,
    separator: String
  )(implicit
    ev1: Position.TextParseConstraints.Aux[L, Q],
    ev2: Position.IndexConstraints.Aux[Q, D, Value[T]]
  ): Persist.TextParser[Cell[Q]] = (str) => List(parse(str, codecs, separator, FromDictionary(dictionary, dimension)))

  /**
   * Returns a function that parses a line of tabular data into a `List[Cell[Coordinates2[K, V]]]`.
   *
   * @param pkey      The primary key decoder.
   * @param columns   A set of column decoders for decoding the data in the table.
   * @param separator The column separator.
   *
   * @return The parser function.
   */
  def tableParser[
    K,
    V
  ](
    pkey: KeyDecoder[K],
    columns: Set[ColumnDecoder[V]],
    separator: String
  ): Persist.TextParser[Cell[Coordinates2[K, V]]] = (str) => {
    val parts = str.trim.split(Pattern.quote(separator))

    if (columns.exists(_.equals(pkey)))
      throw new Exception("Primary key can't be in columns") // TODO: can this be enforced at compile time?
    else if (!pkey.validate(parts.length) || columns.exists(!_.validate(parts.length)))
      List(Left(s"Out of bounds index: '${str}'"))
    else
      pkey.decode(parts(pkey.index)) match {
        case Right(key) => columns.map { case dec =>
          dec.decode(key, parts(dec.index)) match {
            case Right(cell) => Right(cell)
            case Left(err) => Left(s"${err} - '${str}'")
          }
        }
        case Left(err) => List(Left(s"${err} - '${str}'"))
      }
  }

  /**
   * Return function that returns a JSON representation of a cell.
   *
   * @param verbose Indicator if the JSON should be self describing or not.
   * @param pretty  Indicator if the resulting JSON string to be indented.
   *
   * @return The writer function.
   */
  def toJSON[
    P <: HList
  ](
    verbose: Boolean,
    pretty: Boolean = false
  ): Persist.TextWriter[Cell[P]] = (cell) => List(cell.toJSON(verbose, pretty))

  /**
   * Return function that returns a short string representation of a cell.
   *
   * @param verbose   Indicator if codec and schema are required or not.
   * @param separator The separator to use between various fields.
   *
   * @return The writer function.
   */
  def toShortString[
    P <: HList
  ](
    verbose: Boolean,
    separator: String
  ): Persist.TextWriter[Cell[P]] = (cell) => List(cell.toShortString(verbose, separator))

  /**
   * Return a `Writes` for writing JSON cell.
   *
   * @param verbose Indicator if the JSON should be self describing or not.
   *
   * @return The JSON `Writes`.
   */
  def writes[P <: HList](verbose: Boolean): Writes[Cell[P]] = new Writes[Cell[P]] {
    implicit val con = Content.writes(verbose)
    implicit val pos = Position.writes[P]

    def writes(cell: Cell[P]): JsValue = Json.obj("position" -> cell.position, "content" -> cell.content)
  }

  private def parse[
    L <: HList,
    Q <: HList
  ](
    str: String,
    codecs: L,
    separator: String,
    cfg: ParseConfig[Q]
  )(implicit
    ev: Position.TextParseConstraints.Aux[L, Q]
  ): Either[String, Cell[Q]] = {
    val (pstr, cstr) = str.splitAt(cfg.idx(str, separator))

    if (pstr.isEmpty || cstr.isEmpty)
      Left("Unable to split: '" + str + "'")
    else {
      val cell = for {
        pos <- Position.fromShortString(pstr, codecs, separator)
        decoder <- cfg.dec(pos)
        con <- decoder(cstr.drop(1))
      } yield Right(Cell(pos, con))

      cell.getOrElse(Left("Unable to decode: '" + str + "'"))
    }
  }

  private def reads[
    Q <: HList
  ](
    prd: Reads[Position[Q]],
    crd: (Position[Q]) => Option[Reads[Content]]
  ): Reads[Cell[Q]] = new Reads[Cell[Q]] {
    def reads(json: JsValue): JsResult[Cell[Q]] = {
      val fields = json.as[JsObject].value

      if (fields.size == 2)
        (
          for {
            pos <- fields.get("position").map(_.as[Position[Q]](prd))
            rds <- crd(pos)
            con <- fields.get("content").map(_.as[Content](rds))
          } yield JsSuccess(Cell(pos, con))
        ).getOrElse(JsError("Unable to parse cell"))
      else
        JsError("Incorrect number of fields")
    }
  }
}

/** Base trait for parsing tabular data. */
trait TableDecoder {
  /** Column index. */
  val index: Int

  /**
   * Check if this can be compared to `other`.
   *
   * @param other The other object to check if it can be compared.
   *
   * @see https://www.artima.com/pins1ed/object-equality.html
   */
  def canEqual(other: Any) = other.isInstanceOf[TableDecoder]

  override def equals(other: Any) = other match {
    case that: TableDecoder => that.canEqual(this) && this.index == that.index
    case _ => false
  }

  override def hashCode = index

  /**
   * Check if this decoder can be applied to a number of columns.
   *
   * @param columns The number of columns in the data.
   *
   * @return Indicator if this decoder can be applied.
   */
  def validate(columns: Int): Boolean = columns.compare(index) > 0
}

/** Decode column data. */
trait ColumnDecoder[V] extends TableDecoder {
  /** The column index. */
  val index: Int

  /** The name/identifier for the column coordinate in the matrix. */
  val coordinate: Value[V]

  /** The decoder to decode the data of the column. */
  val decoder: Content.Decoder

  /**
   * Decode a value in the column.
   *
   * @param pkey The primary key for row.
   * @param str  The column's value for the row.
   *
   * @return Either an error string or a `Cell` with the data.
   */
  def decode[K](pkey: Value[K], str: String): Either[String, Cell[Coordinates2[K, V]]] = decoder(str)
    .map(con => Right(Cell(Position(pkey, coordinate), con)))
    .getOrElse(Left(s"Unable to decode content: '${str}'"))
}

/** Companion object with constructor. */
object ColumnDecoder {
  /**
   * Decode column data.
   *
   * @param index      The column index.
   * @param coordinate The name/identifier for the column coordinate in the matrix.
   * @param decoder    The decoder to decode the data of the column.
   */
  def apply[
    V <% Value[V]
  ](
    index: Nat,
    coordinate: V,
    decoder: Content.Decoder
  )(implicit
    ev: ToInt[index.N]
  ): ColumnDecoder[V] = ColumnDecoderImpl(Nat.toInt[index.N], coordinate, decoder)

  private case class ColumnDecoderImpl[
    V
  ](
    index: Int,
    coordinate: Value[V],
    decoder: Content.Decoder
  ) extends ColumnDecoder[V]
}

/** Decode primary key data. */
trait KeyDecoder[K] extends TableDecoder {
  /** The primary key column index. */
  val index: Int

  /** The decoder to decode the primary key data. */
  val decoder: (String) => Option[Value[K]]

  /**
   * Decode the primary key.
   *
   * @param str The primary key data.
   *
   * @return Either an error string or a `Value` with the primary key.
   */
  def decode(str: String): Either[String, Value[K]] = decoder(str)
    .map(key => Right(key))
    .getOrElse(Left(s"Unable to decode pkey: '${str}'"))
}

/** Companion object with constructor. */
object KeyDecoder {
  /**
   * Decode primary key data.
   *
   * @param index The primary key column index.
   * @param codec The codec to decode the primary key data.
   */
  def apply[
    K
  ](
    index: Nat,
    codec: Codec[K]
  )(implicit
    ev: ToInt[index.N]
  ): KeyDecoder[K] = KeyDecoderImpl(Nat.toInt[index.N], (str) => Value.fromShortString(str, codec))

  private case class KeyDecoderImpl[K](index: Int, decoder: (String) => Option[Value[K]]) extends KeyDecoder[K]
}

private trait ParseConfig[Q <: HList] {
  def idx(str: String, sep: String): Int
  def dec(pos: Position[Q]): Option[Content.Decoder]
}

private case class SelfDescribing[Q <: HList](separator: String) extends ParseConfig[Q] {
  def idx(str: String, sep: String): Int = str.lastIndexOf(sep, str.lastIndexOf(sep, str.lastIndexOf(sep) - 1) - 1)
  def dec(pos: Position[Q]): Option[Content.Decoder] = Option((str) => Content.fromShortString(str, separator))
}

private case class FromDecoder[Q <: HList](decoder: Content.Decoder) extends ParseConfig[Q] {
  def idx(str: String, sep: String) = str.lastIndexOf(sep)
  def dec(pos: Position[Q]): Option[Content.Decoder] = Option((str) => Content.fromShortString(str, decoder))
}

private case class FromCodecSchema[Q <: HList, T](codec: Codec[T], schema: Schema[T]) extends ParseConfig[Q] {
  def idx(str: String, sep: String) = str.lastIndexOf(sep)
  def dec(pos: Position[Q]): Option[Content.Decoder] = Option((str) => Content.fromShortString(str, codec, schema))
}

private case class FromDictionary[
  Q <: HList,
  T,
  D <: Nat
](
  dictionary: Map[T, Content.Decoder],
  dimension: D
)(implicit
  ev: Position.IndexConstraints.Aux[Q, D, Value[T]]
) extends ParseConfig[Q] {
  def idx(str: String, sep: String) = str.lastIndexOf(sep)
  def dec(pos: Position[Q]): Option[Content.Decoder] = dictionary
    .get(pos(dimension).value)
    .map(decoder => (str) => Content.fromShortString(str, decoder))
}

