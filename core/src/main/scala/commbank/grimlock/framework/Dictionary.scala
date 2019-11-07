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

package commbank.grimlock.framework.metadata

import commbank.grimlock.framework.error.{ IncorrectNumberOfFields, InvalidDecoder }
import commbank.grimlock.framework.content.Content

import java.util.regex.Pattern

import scala.io.Source

object Dictionary {
  /**
   * Load a dictionary from file.
   *
   * @param source    Source to read dictionary data from.
   * @param separator Separator for splitting dictionary into data fields.
   * @param key       Index (into data fields) for schema key.
   * @param codec     Index (into data fields) for the codec identifier.
   * @param schema    Index (into data fields) for the schema identifier
   *
   * @return A tuple consisting of the dictionary object and an iterator containing parse errors.
   */
  def load(
    source: Source,
    separator: String,
    key: Int = 0,
    codec: Int = 1,
    schema: Int = 2
  ): (Map[String, Content.Decoder], Iterator[Throwable]) = {
    val result = source
      .getLines()
      .map { case line =>
        val parts = line.split(Pattern.quote(separator), -1)

        if (List(key, codec, schema).exists(_ >= parts.length))
          Left(IncorrectNumberOfFields(line))
        else
          Content.decoderFromComponents(parts(codec), parts(schema)) match {
            case Some(p) => Right((parts(key), p))
            case None => Left(InvalidDecoder(parts(codec), parts(schema)))
          }
      }

    (result.collect { case Right(entry) => entry }.toMap, result.collect { case Left(error) => error })
  }
}

