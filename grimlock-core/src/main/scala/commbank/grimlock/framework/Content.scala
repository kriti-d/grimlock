// Copyright 2014,2015,2016,2017,2018 Commonwealth Bank of Australia
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

package commbank.grimlock.framework.content

import commbank.grimlock.framework.Persist
import commbank.grimlock.framework.encoding.{
  BinaryCodec,
  BooleanCodec,
  BoundedStringCodec,
  Codec,
  DateCodec,
  DecimalCodec,
  DoubleCodec,
  IntCodec,
  LongCodec,
  StringCodec,
  TimestampCodec,
  TypeCodec,
  Value
}
import commbank.grimlock.framework.environment.Context
import commbank.grimlock.framework.environment.tuner.Tuner
import commbank.grimlock.framework.error.{
  IncorrectNumberOfFields,
  InvalidValue,
  UnableToDecodeCodec,
  UnableToDecodeContent,
  UnableToDecodeSchema
}
import commbank.grimlock.framework.metadata.{
  ContinuousSchema,
  DateSchema,
  DiscreteSchema,
  NominalSchema,
  OrdinalSchema,
  Schema,
  Type
}
import commbank.grimlock.framework.position.Position
import commbank.grimlock.framework.utility.JSON

import java.util.regex.Pattern

import play.api.libs.json.{ JsError, JsObject, JsResult, Json, JsString, JsSuccess, JsValue, Reads, Writes }

import scala.util.{ Failure, Success, Try }

import shapeless.{ HList, Poly1 }

/** Contents of a cell in a matrix. */
trait Content {
  /** Type of the value. */
  val classification: Type

  /** The value of the variable. */
  val value: Value[_]

  override def toString: String = "Content(" + classification.toString + "," + value.toString + ")"

  /**
   * Converts the content to a JSON string.
   *
   * @param verbose Indicator if the JSON should be self describing or not.
   * @param pretty  Indicator if the resulting JSON string to be indented.
   */
  def toJSON(verbose: Boolean, pretty: Boolean = false): String = JSON.to(this, Content.writes(verbose), pretty)

  /** Return this content as an option. */
  def toOption: Option[Content] = Option(this)

  /**
   * Converts the content to a consise (terse) string.
   *
   * @return Short string representation.
   */
  def toShortString: String = value.toShortString

  /**
   * Converts the content to a consise (terse) self-describing string.
   *
   * @param separator The separator to use between the fields.
   *
   * @return Short string representation.
   */
  def toShortString(separator: String): String = value.codec.toShortString +
    separator +
    classification.toShortString +
    separator +
    value.toShortString
}

/** Companion object to `Content` trait. */
object Content {
  /** Type for decoding a string to `Content`. */
  type Decoder = (String) => Option[Content]

  /**
   * Constructor for a `Content`.
   *
   * @param schema The schema from which to get the classification.
   * @param value  The value of the content.
   */
  def apply[T](schema: Schema[T], value: Value[T]): Content = ContentImpl(schema.classification, value)

  /**
   * Return a decoder from component strings.
   *
   * @param codec  The codec to use in the decoder.
   * @param schema The schema to use in the decoder.
   *
   * @return A content decoder.
   */
  def decoder[T](codec: Codec[T], schema: Schema[T]): Decoder = (str: String) =>
    parse(str, codec, schema).toOption

  /**
   * Return a decoder from component strings.
   *
   * @param codec  The string of the codec to use in the decoder.
   * @param schema The string of the schema to use in the decoder.
   *
   * @return A `Some[Decoder]` in case of success, `None` in case of failure.
   */
  def decoderFromComponents(codec: String, schema: String): Option[Decoder] = {
    object codecToDecoder extends Poly1 {
      implicit val atBinaryCodec = at[BinaryCodec.type](c => toDecoder(c))
      implicit val atBooleanCodec = at[BooleanCodec.type](c => toDecoder(c))
      implicit val atBoundedStringCodec = at[BoundedStringCodec](c => toDecoder(c))
      implicit val atDateCodec = at[DateCodec](c => toDecoder(c))
      implicit val atDecimalCodec = at[DecimalCodec](c => toDecoder(c))
      implicit val atDoubleCodec = at[DoubleCodec.type](c => toDecoder(c))
      implicit val atIntCodec = at[IntCodec.type](c => toDecoder(c))
      implicit val atLongCodec = at[LongCodec.type](c => toDecoder(c))
      implicit val atStringCodec = at[StringCodec.type](c => toDecoder(c))
      implicit val atTimestampCodec = at[TimestampCodec.type](c => toDecoder(c))
      implicit val atTypeCodec = at[TypeCodec.type](c => toDecoder(c))
    }

    def toDecoder[T](c: Codec[T]) = {
      object schemaToDecoder extends Poly1 {
        implicit val atContinuousSchema = at[ContinuousSchema[T]](s => toDecoder(s))
        implicit val atDateSchema = at[DateSchema[T]](s => toDecoder(s))
        implicit val atDiscreteSchema = at[DiscreteSchema[T]](s => toDecoder(s))
        implicit val atNominalSchema = at[NominalSchema[T]](s => toDecoder(s))
        implicit val atOrdinalSchema = at[OrdinalSchema[T]](s => toDecoder(s))
      }

      def toDecoder(s: Schema[T]) = Option((str: String) => parse(str, c, s).toOption)

      Schema.fromShortString[Schema.DefaultSchemas[T], T](schema, c).flatMap(_.fold(schemaToDecoder))
    }

    Codec.fromShortString[Codec.DefaultCodecs](codec).flatMap(_.fold(codecToDecoder))
  }

  /**
   * Parse a content from string components.
   *
   * @param codec  The codec string to decode content with.
   * @param schema The schema string to validate content with.
   * @param value  The content string value to parse.
   *
   * @return A `Some[Content]` if successful, `None` otherwise.
   */
  def fromComponents(codec: String, schema: String, value: String): Option[Content] = parse(codec, schema, value)
    .toOption

  /**
   * Parse a content from a self-describing JSON string.
   *
   * @param str The string to parse.
   *
   * @return A `Some[Content]` if successful, `None` otherwise.
   */
  def fromJSON(str: String): Option[Content] = JSON.from(str, reads).toOption

  /**
   * Parse a content from a JSON string that does not have codec or schema information in it.
   *
   * @param str     The string to parse.
   * @param decoder The decoder to parse with.
   *
   * @return A `Some[Content]` if successful, `None` otherwise.
   */
  def fromJSON(str: String, decoder: Decoder): Option[Content] = JSON.from(str, reads(decoder)).toOption

  /**
   * Parse a content from a JSON string that does not have codec or schema information in it.
   *
   * @param str    The string to parse.
   * @param codec  The codec string to decode content with.
   * @param schema The schema string to validate content with.
   *
   * @return A `Some[Content]` if successful, `None` otherwise.
   */
  def fromJSON[
    T
  ](
    str: String,
    codec: Codec[T],
    schema: Schema[T]
  ): Option[Content] = JSON.from(str, reads(codec, schema)).toOption

  /**
   * Parse a content from a short string that does not have codec or schema information in it.
   *
   * @param str     The string to parse.
   * @param decoder The decoder to parse with.
   *
   * @return A `Some[Content]` if successful, `None` otherwise.
   */
  def fromShortString(str: String, decoder: Decoder): Option[Content] = parse(str, decoder).toOption

  /**
   * Parse a content from a self-describing short string.
   *
   * @param str       The string to parse.
   * @param separator The string separator.
   *
   * @return A `Some[Content]` if successful, `None` otherwise.
   */
  def fromShortString(str: String, separator: String): Option[Content] = parse(str, separator).toOption

  /**
   * Parse a content from a short string that does not have codec or schema information in it.
   *
   * @param str    The string to parse.
   * @param codec  The codec string to decode content with.
   * @param schema The schema string to validate content with.
   *
   * @return A `Some[Content]` if successful, `None` otherwise.
   */
  def fromShortString[T](str: String, codec: Codec[T], schema: Schema[T]): Option[Content] = parse(str, codec, schema)
    .toOption

  /**
   * Return content parser for self describing JSON strings.
   *
   * @return A content parser.
   */
  def jsonParser: Persist.TextParser[Content] = (str) => List(JSON.from(str, reads))

  /**
   * Return content parser from a decoder. This parses JSON strings that do not have codec or schema information.
   *
   * @param decoder The decoder to decode the string with.
   *
   * @return A content parser.
   */
  def jsonParser(decoder: Decoder): Persist.TextParser[Content] = (str) => List(JSON.from(str, reads(decoder)))

  /**
   * Return content parser from codec and schema. This parses JSON strings that do not have codec or schema information.
   *
   * @param codec  The codec to decode content with.
   * @param schema The schema to validate content with.
   *
   * @return A content parser.
   */
  def jsonParser[T](codec: Codec[T], schema: Schema[T]): Persist.TextParser[Content] = (str) =>
    List(JSON.from(str, reads(codec, schema)))

  /** Return a `Reads` for parsing self-describing JSON content. */
  def reads: Reads[Content] = reads(None)

  /**
   * Return a `Reads` for parsing self-describing JSON content. This parses JSON strings that do
   * not have codec or schema information.
   *
   * @param decoder The decoder to decode the string with.
   */
  def reads(decoder: Decoder): Reads[Content] = reads(Option(decoder))

  /**
   * Return a `Reads` for parsing self-describing JSON content. This parses JSON strings that do
   * not have codec or schema information.
   *
   * @param codec  The codec to decode content with.
   * @param schema The schema to validate content with.
   */
  def reads[
    T
  ](
    codec: Codec[T],
    schema: Schema[T]
  ): Reads[Content] = reads(Option((str: String) => parse(str, codec, schema).toOption))

  /**
   * Return content parser from a decoder. This parses strings that do not have codec or schema information.
   *
   * @param decoder The decoder to decode the string with.
   *
   * @return A content parser.
   */
  def shortStringParser(decoder: Decoder): Persist.TextParser[Content] = (str) => List(parse(str, decoder))

  /**
   * Return content parser for self describing strings.
   *
   * @param separator The string separator.
   *
   * @return A content parser.
   */
  def shortStringParser(separator: String): Persist.TextParser[Content] = (str) => List(parse(str, separator))

  /**
   * Return content parser from codec and schema. This parses strings that do not have codec or schema information.
   *
   * @param codec  The codec to decode content with.
   * @param schema The schema to validate content with.
   *
   * @return A content parser.
   */
  def shortStringParser[T](codec: Codec[T], schema: Schema[T]): Persist.TextParser[Content] = (str) =>
    List(parse(str, codec, schema))

  /**
   * Return function that returns a JSON representation of a content.
   *
   * @param verbose Indicator if the JSON should be self-describing or not.
   * @param pretty  Indicator if the resulting JSON string to be indented.
   */
  def toJSON(verbose: Boolean, pretty: Boolean = false): Persist.TextWriter[Content] = (con) =>
    List(con.toJSON(verbose, pretty))

  /** Return function that returns a short string representation of a content. */
  def toShortString: Persist.TextWriter[Content] = (con) => List(con.toShortString)

  /**
   * Return function that returns a short string representation of a content. The string includes
   * codec and schema information.
   *
   * @param separator The separator to use between various fields (only used if verbose is `false`).
   */
  def toShortString(separator: String): Persist.TextWriter[Content] = (con) => List(con.toShortString(separator))

  /** Standard `unapply` method for pattern matching. */
  def unapply(con: Content): Option[(Type, Value[_])] = Option((con.classification, con.value))

  /**
   * Return a `Writes` for writing JSON content.
   *
   * @param verbose Indicator if the JSON should be self-describing or not.
   */
  def writes(verbose: Boolean): Writes[Content] = new Writes[Content] {
    def writes(con: Content): JsValue = JsObject(
      (
        if (verbose)
          Seq(
            "codec" -> JsString(con.value.codec.toShortString),
            "schema" -> JsString(con.classification.toShortString)
          )
        else
          Seq.empty
      ) ++ Seq("value" -> JsString(con.value.toShortString))
    )
  }

  private def parse(str: String, decoder: Decoder): Try[Content] = decoder(str)
    .map(con => Success(con))
    .getOrElse(Failure(UnableToDecodeContent(str)))

  private def parse(str: String, separator: String): Try[Content] = str.split(Pattern.quote(separator), -1) match {
    case Array(c, s, v) => parse(c, s, v)
    case _ => Failure(IncorrectNumberOfFields(str))
  }

  private def parse(codec: String, schema: String, value: String): Try[Content] = {
    object codecToContent extends Poly1 {
      implicit val atBinaryCodec = at[BinaryCodec.type](c => toContent(c))
      implicit val atBooleanCodec = at[BooleanCodec.type](c => toContent(c))
      implicit val atBoundedStringCodec = at[BoundedStringCodec](c => toContent(c))
      implicit val atDateCodec = at[DateCodec](c => toContent(c))
      implicit val atDecimalCodec = at[DecimalCodec](c => toContent(c))
      implicit val atDoubleCodec = at[DoubleCodec.type](c => toContent(c))
      implicit val atIntCodec = at[IntCodec.type](c => toContent(c))
      implicit val atLongCodec = at[LongCodec.type](c => toContent(c))
      implicit val atStringCodec = at[StringCodec.type](c => toContent(c))
      implicit val atTimestampCodec = at[TimestampCodec.type](c => toContent(c))
      implicit val atTypeCodec = at[TypeCodec.type](c => toContent(c))
    }

    def toContent[T](c: Codec[T]) = {
      object schemaToContent extends Poly1 {
        implicit val atContinuousSchema = at[ContinuousSchema[T]](s => toContent(s))
        implicit val atDateSchema = at[DateSchema[T]](s => toContent(s))
        implicit val atDiscreteSchema = at[DiscreteSchema[T]](s => toContent(s))
        implicit val atNominalSchema = at[NominalSchema[T]](s => toContent(s))
        implicit val atOrdinalSchema = at[OrdinalSchema[T]](s => toContent(s))
      }

      def toContent(s: Schema[T]) = parse(value, c, s)

      Schema.fromShortString[Schema.DefaultSchemas[T], T](schema, c) match {
        case Some(schemas) => schemas.fold(schemaToContent)
        case None => Failure(UnableToDecodeSchema(schema))
      }
    }

    Codec.fromShortString[Codec.DefaultCodecs](codec) match {
      case Some(codecs) => codecs.fold(codecToContent)
      case None => Failure(UnableToDecodeCodec(codec))
    }
  }

  private def parse[
    T
  ](
    str: String,
    codec: Codec[T],
    schema: Schema[T]
  ): Try[Content] = Value.fromShortString(str, codec)
    .map {
      case value if (schema.validate(value)) => Success(Content(schema, value))
      case _ => Failure(InvalidValue(str))
    }
    .getOrElse(Failure(UnableToDecodeContent(str)))

  private def reads(decoder: Option[Decoder]): Reads[Content] = new Reads[Content] {
    def reads(json: JsValue): JsResult[Content] = {
      val fields = json.as[JsObject].value

      if ((fields.size == 3 && decoder.isEmpty) || (fields.size == 1 && decoder.isDefined))
        (
          for {
            dec <- decoder.orElse(
              for {
                codec <- fields.get("codec").map(_.as[String])
                schema <- fields.get("schema").map(_.as[String])
              } yield (str: String) => fromComponents(codec, schema, str)
            )
            value <- fields.get("value").map(_.as[String])
            content <- dec(value)
          } yield JsSuccess(content)
        ).getOrElse(JsError("Unable to parse content"))
      else
        JsError("Incorrect number of fields")
    }
  }
}

private case class ContentImpl(classification: Type, value: Value[_]) extends Content

/** Trait that represents the contents of a matrix. */
trait Contents[C <: Context[C]] extends Persist[Content, C] {
  /**
   * Persist to disk.
   *
   * @param context The operating context.
   * @param file    Name of the output file.
   * @param writer  Writer that converts `Content` to string.
   * @param tuner   The tuner for the job.
   *
   * @return A `C#U[Content]` which is this object's data.
   */
  def saveAsText[
    T <: Tuner
  ](
    context: C,
    file: String,
    writer: Persist.TextWriter[Content],
    tuner: T
  )(implicit
    ev: Persist.SaveAsTextTuner[C#U, T]
  ): C#U[Content]
}

/** Trait that represents the output of uniqueByPosition. */
trait IndexedContents[P <: HList, C <: Context[C]] extends Persist[(Position[P], Content), C] {
  /**
   * Persist to disk.
   *
   * @param context The operating context.
   * @param file    Name of the output file.
   * @param writer  Writer that converts `IndexedContent` to string.
   * @param tuner   The tuner for the job.
   *
   * @return A `C#U[(Position[P], Content)]` which is this object's data.
   */
  def saveAsText[
    T <: Tuner
  ](
    context: C,
    file: String,
    writer: Persist.TextWriter[(Position[P], Content)],
    tuner: T
  )(implicit
    ev: Persist.SaveAsTextTuner[C#U, T]
  ): C#U[(Position[P], Content)]
}

/** Object for `IndexedContents` functions. */
object IndexedContents {
  /**
   * Return string representation of an indexed content.
   *
   * @param verbose   Indicator if the string should be self-describing or not.
   * @param separator The separator to use between various fields.
   */
  def toShortString[
    P <: HList
  ](
    verbose: Boolean,
    separator: String
  ): ((Position[P], Content)) => TraversableOnce[String] = (t: (Position[P], Content)) => List(
    t._1.toShortString(separator) + separator + (if (verbose) t._2.toShortString(separator) else t._2.toShortString)
  )

  /**
   * Return function that returns a JSON representations of an indexed content.
   *
   * @param verbose Indicator if the JSON should be self-describing or not.
   * @param pretty  Indicator if the resulting JSON string to be indented.
   *
   * @note The index (Position) and content are separately encoded and then combined using the separator.
   */
  def toJSON[
    P <: HList
  ](
    verbose: Boolean,
    pretty: Boolean = false
  ): ((Position[P], Content)) => TraversableOnce[String] = {
    implicit val con = Content.writes(verbose)
    implicit val pos = Position.writes[P]

    val writes = new Writes[(Position[P], Content)] {
      def writes(t: (Position[P], Content)) = Json.obj("index" -> t._1, "content" -> t._2)
    }

    (t: (Position[P], Content)) => List(JSON.to(t, writes, pretty))
  }
}

