// Copyright 2018 Commonwealth Bank of Australia
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

package commbank.grimlock.framework.error

/**
 * Dictionary entry decoder error.
 *
 * @param codec  The codec string.
 * @param schema The schema string.
 */
case class InvalidDecoder(codec: String, schema: String) extends Throwable(s"codec [${codec}] - schema [${schema}]")

/**
 * JSON parse error.
 *
 * @param error The error string.
 */
case class InvalidJSON(error: String) extends Throwable(error)

/**
 * The content value does not conform the the schema constraints (range/domain).
 *
 * @param value The invalid content value.
 */
case class InvalidValue(value: String) extends Throwable(value)

/**
 * The string to parse has an incorrect number of fields.
 *
 * @param str The string with an incorrect number of fields.
 */
case class IncorrectNumberOfFields(str: String) extends Throwable(str)

/**
 * A string can't be parsed as a cell.
 *
 * @param value The string value that can't be decoded.
 * @param cause Optional parse error for parts of the cell (i.e. content or position)
 */
case class UnableToDecodeCell(value: String, cause: Throwable = null) extends Throwable(value, cause)

/**
 * A string can't be parsed as a codec.
 *
 * @param value The string value that can't be decoded.
 */
case class UnableToDecodeCodec(value: String) extends Throwable(value)

/**
 * A string can't be parsed as a content.
 *
 * @param value The string value that can't be decoded.
 */
case class UnableToDecodeContent(value: String) extends Throwable(value)

/**
 * A string can't be parsed as a position.
 *
 * @param value The string value that can't be decoded.
 */
case class UnableToDecodePosition(value: String) extends Throwable(value)

/**
 * A string can't be parsed as a schema.
 *
 * @param value The string value that can't be decoded.
 */
case class UnableToDecodeSchema(value: String) extends Throwable(value)

