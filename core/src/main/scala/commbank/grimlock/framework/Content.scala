// Copyright 2014,2015,2016,2017 Commonwealth Bank of Australia
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
import commbank.grimlock.framework.encoding.{ Codec, Value }
import commbank.grimlock.framework.environment.tuner.Tuner
import commbank.grimlock.framework.metadata.Schema
import commbank.grimlock.framework.position.Position

import java.util.regex.Pattern

import play.api.libs.json.{ JsError, JsObject, Json, JsResult, JsString, JsSuccess, JsValue, Reads, Writes }

import shapeless.Nat

/** Contents of a cell in a matrix. */
trait Content { self =>
  /** Type of the data. */
  type D

  /** Schema (description) of the value. */
  val schema: Schema { type D = self.D }

  /** The value of the variable. */
  val value: Value { type D = self.D }

  /**
   * Converts the content to a consise (terse) string.
   *
   * @param separator   The separator to use between the fields.
   * @param descriptive Indicator if codec and schema are required or not.
   *
   * @return Short string representation.
   */
  def toShortString(separator: String, descriptive: Boolean = true): String = (
    if (descriptive)
      value.codec.toShortString + separator + schema.toShortString(value.codec) + separator
    else
      ""
  ) + value.toShortString

  override def toString(): String = "Content(" + schema.toString + "," + value.toString + ")"

  /**
   * Converts the content to a JSON string.
   *
   * @param pretty      Indicator if the resulting JSON string to be indented.
   * @param descriptive Indicator if the JSON should be self describing (true) or not.
   */
  def toJSON(pretty: Boolean = false, descriptive: Boolean = true): String = {
    implicit val wrt = Content.writes(descriptive)

    val json = Json.toJson(this)

    if (pretty) Json.prettyPrint(json) else Json.stringify(json)
  }
}

/** Companion object to `Content` trait. */
object Content {
  /** Type for parsing a string to `Content`. */
  type Parser = (String) => Option[Content]

  /**
   * Construct a content from a schema and value.
   *
   * @param schema Schema of the variable value.
   * @param value  Value of the variable.
   */
  def apply[T](schema: Schema { type D = T }, value: Value { type D = T }): Content = ContentImpl(schema, value)

  /** Standard `unapply` method for pattern matching. */
  def unapply(con: Content): Option[(Schema, Value)] = Option((con.schema, con.value))

  /**
   * Return content parser from codec and schema.
   *
   * @param codec  The codec to decode content with.
   * @param schema The schema to validate content with.
   *
   * @return A content parser.
   */
  def parser[T](codec: Codec { type D = T }, schema: Schema { type D = T }): Parser = (str: String) => codec
    .decode(str)
    .flatMap { case v => if (schema.validate(v)) Option(Content(schema, v)) else None }

  /**
   * Return content parser from codec and schema strings.
   *
   * @param codec  The codec string to decode content with.
   * @param schema The schema string to validate content with.
   *
   * @return A content parser.
   */
  def parserFromComponents(codec: String, schema: String): Option[Parser] = Codec.fromShortString(codec)
    .flatMap(c => Schema.fromShortString(schema, c).map(s => parser[c.D](c, s)))

  /**
   * Parse a content from string components
   *
   * @param codec  The codec string to decode content with.
   * @param schema The schema string to validate content with.
   * @param value  The content string value to parse.
   *
   * @return A `Some[Content]` if successful, `None` otherwise.
   */
  def fromComponents(
    codec: String,
    schema: String,
    value: String
  ): Option[Content] = parserFromComponents(codec, schema).flatMap(parser => parser(value))

  /**
   * Parse a content from string.
   *
   * @param str       The string to parse.
   * @param separator The separator between codec, schema and value.
   *
   * @return A `Some[Content]` if successful, `None` otherwise.
   */
  def fromShortString(
    str: String,
    separator: String = "|"
  ): Option[Content] = str.split(Pattern.quote(separator)) match {
    case Array(c, s, v) => fromComponents(c, s, v)
    case _ => None
  }

  /**
   * Return string representation of a content.
   *
   * @param verbose     Indicator if verbose string is required or not.
   * @param separator   The separator to use between various fields (only used if verbose is `false`).
   * @param descriptive Indicator if codec and schema are required or not (only used if verbose is `false`).
   */
  def toString(
    verbose: Boolean = false,
    separator: String = "|",
    descriptive: Boolean = true
  ): (Content) => TraversableOnce[String] = (t: Content) =>
    List(if (verbose) t.toString else t.toShortString(separator, descriptive))

  /**
   * Return function that returns a JSON representation of a content.
   *
   * @param pretty      Indicator if the resulting JSON string to be indented.
   * @param descriptive Indicator if the JSON should be self describing (true) or not.
   */
  def toJSON(
    pretty: Boolean = false,
    descriptive: Boolean = true
  ): (Content) => TraversableOnce[String] = (t: Content) => List(t.toJSON(pretty, descriptive))

  /**
   * Return a `Reads` for parsing a JSON content.
   *
   * @param parser Optional parser; in case the JSON is not self describing.
   */
  def reads(parser: Option[Parser]): Reads[Content] = new Reads[Content] {
    def reads(json: JsValue): JsResult[Content] = {
      val fields = json.as[JsObject].value

      if ((fields.size == 3 && parser.isEmpty) || (fields.size == 1 && parser.isDefined))
        (
          for {
            decoder <- parser.orElse(
              for {
                codec <- fields.get("codec").map(_.as[String])
                schema <- fields.get("schema").map(_.as[String])
                pfc <- Content.parserFromComponents(codec, schema)
              } yield pfc
            )
            value <- fields.get("value").map(_.as[String])
            content <- decoder(value)
          } yield JsSuccess(content)
        ).getOrElse(JsError("Unable to parse content"))
      else
        JsError("Incorrect number of fields")
    }
  }

  /**
   * Return a `Writes` for writing JSON content.
   *
   * @param descriptive Indicator if the JSON should be self describing (true) or not.
   */
  def writes(descriptive: Boolean): Writes[Content] = new Writes[Content] {
    def writes(o: Content): JsValue = JsObject(
      (
        if (descriptive)
          Seq(
            "codec" -> JsString(o.value.codec.toShortString),
            "schema" -> JsString(o.schema.toShortString(o.value.codec))
          )
        else
          Seq()
      ) ++ Seq("value" -> JsString(o.value.toShortString))
    )
  }
}

private case class ContentImpl[T](schema: Schema { type D = T }, value: Value { type D = T }) extends Content {
  type D = T
}

/** Trait that represents the contents of a matrix. */
trait Contents[U[_]] extends Persist[Content, U] {
  /**
   * Persist to disk.
   *
   * @param file   Name of the output file.
   * @param writer Writer that converts `Content` to string.
   * @param tuner  The tuner for the job.
   *
   * @return A `U[Content]` which is this object's data.
   */
  def saveAsText[
    T <: Tuner
  ](
    file: String,
    writer: Persist.TextWriter[Content] = Content.toString(),
    tuner: T
  )(implicit
    ev: Persist.SaveAsTextTuner[U, T]
  ): U[Content]
}

/** Trait that represents the output of uniqueByPosition. */
trait IndexedContents[P <: Nat, U[_]] extends Persist[(Position[P], Content), U] {
  /**
   * Persist to disk.
   *
   * @param file   Name of the output file.
   * @param writer Writer that converts `IndexedContent` to string.
   * @param tuner  The tuner for the job.
   *
   * @return A `U[(Position[P], Content)]` which is this object's data.
   */
  def saveAsText[
    T <: Tuner
  ](
    file: String,
    writer: Persist.TextWriter[(Position[P], Content)] = IndexedContents.toString(),
    tuner: T
  )(implicit
    ev: Persist.SaveAsTextTuner[U, T]
  ): U[(Position[P], Content)]
}

/** Object for `IndexedContents` functions. */
object IndexedContents {
  /**
   * Return string representation of an indexed content.
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
  ): ((Position[P], Content)) => TraversableOnce[String] = (t: (Position[P], Content)) =>
    List(
      if (verbose)
        t.toString
      else
        t._1.toShortString(separator) + separator + t._2.toShortString(separator, descriptive)
    )

  /**
   * Return function that returns a JSON representations of an indexed content.
   *
   * @param pretty      Indicator if the resulting JSON string to be indented.
   * @param separator   The separator to use between various both JSON strings.
   * @param descriptive Indicator if the JSON should be self describing (true) or not.
   *
   * @note The index (Position) and content are separately encoded and then combined using the separator.
   */
  def toJSON[
    P <: Nat
  ](
    pretty: Boolean = false,
    separator: String = ",",
    descriptive: Boolean = false
  ): ((Position[P], Content)) => TraversableOnce[String] = (t: (Position[P], Content)) =>
    List(t._1.toJSON(pretty) + separator + t._2.toJSON(pretty, descriptive))
}

