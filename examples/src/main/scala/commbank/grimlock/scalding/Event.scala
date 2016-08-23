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

package commbank.grimlock.scalding.examples

import commbank.grimlock.framework._
import commbank.grimlock.framework.content._
import commbank.grimlock.framework.content.metadata._
import commbank.grimlock.framework.encoding._
import commbank.grimlock.framework.nlp._
import commbank.grimlock.framework.position._
import commbank.grimlock.framework.transform._

import commbank.grimlock.library.aggregate._
import commbank.grimlock.library.transform._

import commbank.grimlock.scalding.environment._
import commbank.grimlock.scalding.environment.Context._

import com.twitter.scalding.{ Args, Job, TextLine }
import com.twitter.scalding.TDsl.sourceToTypedPipe
import com.twitter.scalding.typed.TypedPipe

import shapeless.nat.{ _1, _2, _3 }

// Define a simple event (structured) data type. It has an id, a type, a start time and duration. It applies to one or
// more instances and has a detailed information field.
case class ExampleEvent(
  eventId: String,
  eventType: String,
  startTime: java.util.Date,
  duration: Long,
  instances: List[String],
  details: String
) extends Structured

object ExampleEvent {
  // Function to read a file with event data.
  def load(file: String): TypedPipe[Cell[_1]] = TextLine(file)
    .flatMap { case line =>
      ExampleEventCodec.decode(line).map(ev => Cell(Position(ev.value.eventId), Content(ExampleEventSchema, ev)))
    }
}

// Define a schema that specifies what legal values are for the example event. For this example, all events are valid.
case object ExampleEventSchema extends StructuredSchema {
  type S = ExampleEvent

  val kind = Type.Structured

  def validate(value: Value { type V = S }): Boolean = true
}

// Define a codec for dealing with the example event. Note that comparison, for this example, is simply comparison
// on the event id.
case object ExampleEventCodec extends StructuredCodec {
  type C = ExampleEvent

  def decode(str: String): Option[StructuredValue[C, ExampleEventCodec.type] { type V >: C }] = {
    val dfmt = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val parts = str.split("#")

    Option(
      StructuredValue(
        ExampleEvent(
          parts(0),
          parts(1),
          dfmt.parse(parts(2)),
          parts(3).toLong,
          parts(4).split(",").toList,
          parts(5)
        ),
        this
      )
    )
  }

  def encode(value: C): String = {
    val dfmt = new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    value.eventId + "#" +
    value.eventType + "#" +
    dfmt.format(value.startTime) + "#" +
    value.duration.toString +
    value.instances.mkString(",") + "#" +
    value.details
  }

  def compare(x: Value, y: Value): Option[Int] = (x.asStructured, y.asStructured) match {
    case (Some(ExampleEvent(l, _, _, _, _, _)), Some(ExampleEvent(r, _, _, _, _, _))) => Option(l.compare(r))
    case _ => None
  }

  def toShortString(): String = "exampleevent"
}

// Transformer for denormalising events; that is, create a separate cell in the matrix for each (event, instance) pair.
// Assumes that the initial position is 1D with event id (as is the output from `load` above).
case class Denormalise() extends Transformer[_1, _2] {
  def present(cell: Cell[_1]): TraversableOnce[Cell[_2]] = cell.content match {
    case Content(_, StructuredValue(ExampleEvent(_, _, _, _, instances, _), _)) =>
      for { iid <- instances } yield Cell(cell.position.append(iid), cell.content)
    case _ => List()
  }
}

// For each event, get the details out. Split the details string, apply filtering, and (optionally) add ngrams. Then
// simply return the count for each term (word or ngram) in the document (i.e. event).
case class WordCounts(
  minLength: Long = Long.MinValue,
  ngrams: Int = 1,
  separator: String = "_",
  stopwords: List[String] = Stopwords.English
) extends Transformer[_2, _3] {
  def present(cell: Cell[_2]): TraversableOnce[Cell[_3]] = cell.content match {
    case Content(_, StructuredValue(ExampleEvent(_, _, _, _, _, details), _)) =>
      // Get words from details. Optionally filter by length and/or stopwords.
      val words = details
        .toLowerCase
        .split("""[ ,!.?;:"'#)($+></\\=~_&-@\[\]%`{}]+""")
        .toList
        .filterNot { case word =>
          word.isEmpty || word.exists(Character.isDigit) || word.length < minLength || stopwords.contains(word)
        }
      // Get terms from words. Optionally add ngrams.
      val terms = if (ngrams > 1) words ++ words.sliding(ngrams).map(_.mkString(separator)).toList else words

      // Return the term and it's count in the document.
      terms
        .groupBy(identity)
        .map { case (k, v) => Cell(cell.position.append(k), Content(DiscreteSchema[Long](), v.size)) }
        .toList
    case _ => List()
  }
}

// Simple tf-idf example (input data is same as tf-idf example here: http://en.wikipedia.org/wiki/Tf%E2%80%93idf).
class InstanceCentricTfIdf(args: Args) extends Job(args) {

  // Define implicit context.
  implicit val ctx = Context()

  // Path to data files, output folder
  val path = args.getOrElse("path", "../../data")
  val output = "scalding"

  // Read event data, then de-normalises the events and return a 2D matrix (event id x instance id).
  val data = ExampleEvent.load(s"${path}/exampleEvents.txt")
    .transform(Denormalise())

  // For each event, append the word counts to the 3D matrix. The result is a 3D matrix (event id x instance id x word
  // count). Then aggregate out the event id. The result is a 2D matrix (instance x word count) where the counts are
  // the sums over all events.
  val tf = data
    .transform(WordCounts(stopwords = List()))
    .summarise(Along(_1))(Sum())

  // Get the number of instances (i.e. documents)
  val n = tf
    .size(_1)
    .compact(Over(_1))()

  // Define extractor to get data out of map.
  val extractN = ExtractWithKey[_1, Content](Position.indexString[_1]).andThenPresent(_.value.asDouble)

  // Using the number of documents, compute Idf:
  //  1/ Compute document frequency;
  //  2/ Apply Idf transformation (using document count);
  //  3/ Compact into a Map for use in Tf-Idf below.
  val idf = tf
    .summarise(Along(_1))(Count())
    .transformWithValue(Idf(extractN, (df, n) => math.log10(n / df)), n)
    .compact(Over(_1))()

  // Define extractor to get data out of idf map.
  val extractIdf = ExtractWithDimension[_2, Content](_2).andThenPresent(_.value.asDouble)

  // Apply TfIdf to the term frequency matrix with the Idf values, then save the results to file.
  //
  // Uncomment one of the 3 lines below to try different tf-idf versions.
  val tfIdf = tf
    //.transform(BooleanTf())
    //.transform(LogarithmicTf())
    //.transformWithValue(
    //  AugmentedTf(ExtractWithDimension[_2, Content](_1).andThenPresent(_.value.asDouble)),
    //  tf.summarise(Along(_2))(Max()).compact(Over(_1))()
    //)
    .transformWithValue(TfIdf(extractIdf), idf)
    .saveAsText(ctx, s"./demo.${output}/tfidf_entity.out")
}

