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

package commbank.grimlock.framework.content

import commbank.grimlock.framework._
import commbank.grimlock.framework.encoding._
import commbank.grimlock.framework.content.metadata._
import commbank.grimlock.framework.position._

import java.util.regex.Pattern

import shapeless.Nat

/** Contents of a cell in a matrix. */
trait Content {
  /** Type of the data. */
  type T

  /** Schema (description) of the value. */
  val schema: Schema { type S = T }

  /** The value of the variable. */
  val value: Value { type V = T }

  override def toString(): String = "Content(" + schema.toString + "," + value.toString + ")"

  /**
   * Converts the content to a consise (terse) string.
   *
   * @param separator The separator to use between the fields.
   * @param codec     Indicator if codec is required or not.
   * @param schema    Indicator if schema is required or not.
   *
   * @return Short string representation.
   */
  def toShortString(separator: String, codec: Boolean = true, schema: Boolean = true): String = (
    if (codec) value.codec.toShortString + separator else ""
  ) + (
    if (schema) this.schema.toShortString(value.codec) + separator else ""
  ) +
    value.toShortString
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
  def apply[T](schema: Schema { type S = T }, value: Value { type V = T }): Content = ContentImpl(schema, value)

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
  def parser[T](codec: Codec { type C = T }, schema: Schema { type S = T }): Parser = (str: String) =>
    codec
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
    .flatMap(c => Schema.fromShortString(schema, c).map(s => parser[c.C](c, s)))

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
    case Array(c, s, v) => parserFromComponents(c, s).flatMap(f => f(v))
    case _ => None
  }

  /**
   * Return string representation of a content.
   *
   * @param descriptive Indicator if descriptive string is required or not.
   * @param separator   The separator to use between various fields (only used if descriptive is `false`).
   * @param codec       Indicator if codec is required or not (only used if descriptive is `false`).
   * @param schema      Indicator if schema is required or not (only used if descriptive is `false`).
   */
  def toString(
    descriptive: Boolean = false,
    separator: String = "|",
    codec: Boolean = true,
    schema: Boolean = true
  ): (Content) => TraversableOnce[String] = (t: Content) =>
    List(if (descriptive) t.toString else t.toShortString(separator, codec, schema))
}

private case class ContentImpl[X](schema: Schema { type S = X }, value: Value { type V = X }) extends Content {
  type T = X
}

/** Base trait that represents the contents of a matrix. */
trait Contents extends Persist[Content] {
  /**
   * Persist to disk.
   *
   * @param ctx    The context used to persist the contents.
   * @param file   Name of the output file.
   * @param writer Writer that converts `Content` to string.
   *
   * @return A `U[Content]` which is this object's data.
   */
  def saveAsText(ctx: C, file: String, writer: TextWriter = Content.toString()): U[Content]
}

/** Base trait that represents the output of uniqueByPosition. */
trait IndexedContents[P <: Nat] extends Persist[(Position[P], Content)] {
  /**
   * Persist to disk.
   *
   * @param ctx    The context used to persist the indexed contents.
   * @param file   Name of the output file.
   * @param writer Writer that converts `IndexedContent` to string.
   *
   * @return A `U[(Position[P], Content)]` which is this object's data.
   */
  def saveAsText(ctx: C, file: String, writer: TextWriter = IndexedContent.toString()): U[(Position[P], Content)]
}

/** Object for `IndexedContent` functions. */
object IndexedContent {
  /**
   * Return string representation of an indexed content.
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
  ): ((Position[P], Content)) => TraversableOnce[String] = (t: (Position[P], Content)) =>
    List(
      if (descriptive)
        t.toString
      else
        t._1.toShortString(separator) + separator + t._2.toShortString(separator, codec, schema)
    )
}

