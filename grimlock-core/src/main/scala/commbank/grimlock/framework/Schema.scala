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

package commbank.grimlock.framework.metadata

import commbank.grimlock.framework.encoding.{ Codec, Value }

import java.util.Date

import shapeless.{ :+:, CNil, Coproduct }
import shapeless.ops.coproduct.Inject

/** Trait for variable schemas. */
trait Schema[T] {
  /** The type of variable. */
  val classification: Type

  override def toString: String = getClass.getSimpleName + "(" + paramString(false, _.toString) + ")"

  /**
   * Return a consise (terse) string representation of a schema.
   *
   * @param codec The codec used to encode this schema's data.
   */
  def toShortString(codec: Codec[T]): String = classification.toShortString + round(paramString(true, codec.encode(_)))

  /**
   * Validates if a value confirms to this schema.
   *
   * @param value The value to validate.
   *
   * @return True is the value confirms to this schema, false otherwise.
   */
  def validate(value: Value[T]): Boolean

  protected def paramString(short: Boolean, f: (T) => String): String = ""

  private def round(str: String): String = if (str.isEmpty) str else "(" + str + ")"
}

/** Companion object to the `Schema` trait. */
object Schema {
  /** Type for a default Codec co-product when parsing Schemas from string. */
  type DefaultSchemas[T] = ContinuousSchema[T] :+:
    DateSchema[T] :+:
    DiscreteSchema[T] :+:
    NominalSchema[T] :+:
    OrdinalSchema[T] :+:
    CNil

  /** Type that captures all constraints for parsing default Schemas from string. */
  trait TextParseConstraints[C <: Coproduct, T] extends java.io.Serializable {
    implicit val asContinuous: Inject[C, ContinuousSchema[T]]
    implicit val asDate: Inject[C, DateSchema[T]]
    implicit val asDiscrete: Inject[C, DiscreteSchema[T]]
    implicit val asNominal: Inject[C, NominalSchema[T]]
    implicit val asOrdinal: Inject[C, OrdinalSchema[T]]
  }

  /** Implicit meeting text parsing constraints for the default Schemas. */
  implicit def schemaTextParseConstraints[
    C <: Coproduct,
    T
  ](implicit
    ev1: Inject[C, ContinuousSchema[T]],
    ev2: Inject[C, DateSchema[T]],
    ev3: Inject[C, DiscreteSchema[T]],
    ev4: Inject[C, NominalSchema[T]],
    ev5: Inject[C, OrdinalSchema[T]]
  ): TextParseConstraints[C, T] = new TextParseConstraints[C, T] {
    implicit val asContinuous = ev1
    implicit val asDate = ev2
    implicit val asDiscrete = ev3
    implicit val asNominal = ev4
    implicit val asOrdinal = ev5
  }

  /**
   * Parse a schema from a string.
   *
   * @param str   String from which to parse the schema.
   * @param codec Codec with which to decode the data.
   *
   * @return A `Some[C]` in case of success, `None` otherwise.
   */
  def fromShortString[
    C <: Coproduct,
    T
  ](
    str: String,
    codec: Codec[T]
  )(implicit
    ev: TextParseConstraints[C, T]
  ): Option[C] = {
    import ev._

    str match {
      case ContinuousSchema.Pattern(_) => ContinuousSchema.fromShortString(str, codec).map(Coproduct(_))
      case DateSchema.Pattern(_) => DateSchema.fromShortString(str, codec).map(Coproduct(_))
      case DiscreteSchema.PatternStep(_, _) => DiscreteSchema.fromShortString(str, codec).map(Coproduct(_))
      case DiscreteSchema.Pattern(_) => DiscreteSchema.fromShortString(str, codec).map(Coproduct(_))
      case NominalSchema.Pattern(_) => NominalSchema.fromShortString(str, codec).map(Coproduct(_))
      case OrdinalSchema.Pattern(_) => OrdinalSchema.fromShortString(str, codec).map(Coproduct(_))
      case _ => None
    }
  }
}

/** Trait for schemas for numerical variables. */
trait NumericalSchema[T] extends Schema[T] {
  protected def validateRange(
    value: Value[T],
    lower: T,
    upper: T
  )(implicit
    ev: Numeric[T]
  ): Boolean = value.cmp(lower) >= 0 && value.cmp(upper) <= 0
}

/**
 * Schema for continuous variables.
 *
 * @param range The optional range of the variable.
 */
case class ContinuousSchema[T : Numeric](range: Option[(T, T)]) extends NumericalSchema[T] {
  val classification = ContinuousType

  def validate(value: Value[T]): Boolean = range
    .map { case (lower, upper) => validateRange(value, lower, upper) }
    .getOrElse(true)

  override protected def paramString(
    short: Boolean,
    f: (T) => String
  ): String = SchemaParameters.writeRange(short, range, f)
}

/** Companion object to `ContinuousSchema` case class. */
object ContinuousSchema {
  /** Pattern for matching short string continuous schema. */
  val Pattern = (ContinuousType.name + """(?:\((?:(-?\d+\.?\d*:-?\d+\.?\d*))?\))?""").r

  /** Construct a continuous schema with unbounded range. */
  def apply[T : Numeric](): ContinuousSchema[T] = ContinuousSchema(None)

  /**
   * Construct a continuous schema with bounded range.
   *
   * @param lower The lower bound (minimum value).
   * @param upper The upper bound (maximum value).
   */
  def apply[T : Numeric](lower: T, upper: T): ContinuousSchema[T] = ContinuousSchema(Option((lower, upper)))

  /**
   * Parse a continuous schema from components.
   *
   * @param min   The minimum value string to parse.
   * @param max   The maximum value string to parse.
   * @param codec The codec to parse with.
   *
   * @return A `Some[ContinuousSchema]` if successful, `None` otherwise.
   */
  def fromComponents[T](min: String, max: String, codec: Codec[T]): Option[ContinuousSchema[T]] = for {
    low <- SchemaParameters.parse(codec, min)
    upp <- SchemaParameters.parse(codec, max)
    num <- codec.numeric
  } yield ContinuousSchema(low, upp)(num)

  /**
   * Parse a continuous schema from string.
   *
   * @param str   The string to parse.
   * @param codec The codec to parse with.
   *
   * @return A `Some[ContinuousSchema]` if successful, `None` otherwise.
   */
  def fromShortString[T](str: String, codec: Codec[T]): Option[ContinuousSchema[T]] = (codec.numeric, str) match {
    case (Some(num), Pattern(null)) => Option(ContinuousSchema()(num))
    case (Some(num), Pattern(range)) => SchemaParameters.splitRange(range)
      .flatMap { case (min, max) => fromComponents(min, max, codec) }
    case _ => None
  }
}

/**
 * Schema for discrete variables.
 *
 * @param range The optional range of the variable; optionally with a step size.
 */
case class DiscreteSchema[T : Integral](range: Option[Either[(T, T), ((T, T), T)]]) extends NumericalSchema[T] {
  val classification = DiscreteType

  def validate(value: Value[T]): Boolean = {
    val ev = implicitly[Integral[T]]

    import ev._

    range
      .map {
        case Left((lower, upper)) => validateRange(value, lower, upper)
        case Right(((lower, upper), step)) => validateRange(value, lower, upper) && (value.value % step) == 0
      }
      .getOrElse(true)
  }

  override protected def paramString(
    short: Boolean,
    f: (T) => String
  ): String = range match {
    case None => ""
    case Some(Left(range)) => SchemaParameters.writeRange(short, Option(range), f)
    case Some(Right((range, step))) => SchemaParameters.writeRange(short, Option(range), f) + "," + f(step)
  }
}

/** Companion object to `DiscreteSchema` case class. */
object DiscreteSchema {
  /** Pattern for matching short string discrete schema without step. */
  val Pattern = (DiscreteType.name + """(?:\((?:(-?\d+:-?\d+))?\))?""").r

  /** Pattern for matching short string discrete schema with step. */
  val PatternStep = (DiscreteType.name + """(?:\((?:(-?\d+:-?\d+),(\d+))?\))?""").r

  /** Construct a discrete schema with unbounded range and step size 1. */
  def apply[T : Integral](): DiscreteSchema[T] = DiscreteSchema(None)

  /**
   * Construct a discrete schema with bounded range and step size 1.
   *
   * @param lower The lower bound (minimum value).
   * @param upper The upper bound (maximum value).
   */
  def apply[T : Integral](lower: T, upper: T): DiscreteSchema[T] = DiscreteSchema(Option(Left((lower, upper))))

  /**
   * Construct a discrete schema with bounded range and step size.
   *
   * @param lower The lower bound (minimum value).
   * @param upper The upper bound (maximum value).
   * @param step  The step size.
   */
  def apply[
    T : Integral
  ](
    lower: T,
    upper: T,
    step: T
  ): DiscreteSchema[T] = DiscreteSchema(Option(Right(((lower, upper), step))))

  /**
   * Parse a discrete schema from components.
   *
   * @param min   The minimum value string to parse.
   * @param max   The maximum value string to parse.
   * @param codec The codec to parse with.
   *
   * @return A `Some[DiscreteSchema]` if successful, `None` otherwise.
   */
  def fromComponents[T](min: String, max: String, codec: Codec[T]): Option[DiscreteSchema[T]] = for {
    low <- SchemaParameters.parse(codec, min)
    upp <- SchemaParameters.parse(codec, max)
    int <- codec.integral
  } yield DiscreteSchema(low, upp)(int)

  /**
   * Parse a discrete schema from components.
   *
   * @param min   The minimum value string to parse.
   * @param max   The maximum value string to parse.
   * @param step  The step size string to parse.
   * @param codec The codec to parse with.
   *
   * @return A `Some[DiscreteSchema]` if successful, `None` otherwise.
   */
  def fromComponents[T](min: String, max: String, step: String, codec: Codec[T]): Option[DiscreteSchema[T]] = for {
    low <- SchemaParameters.parse(codec, min)
    upp <- SchemaParameters.parse(codec, max)
    stp <- SchemaParameters.parse(codec, step)
    int <- codec.integral
  } yield DiscreteSchema(low, upp, stp)(int)

  /**
   * Parse a discrete schema from string.
   *
   * @param str   The string to parse.
   * @param codec The codec to parse with.
   *
   * @return A `Some[DiscreteSchema]` if successful, `None` otherwise.
   */
  def fromShortString[T](str: String, codec: Codec[T]): Option[DiscreteSchema[T]] = (codec.integral, str) match {
    case (Some(int), Pattern(null)) => Option(DiscreteSchema()(int))
    case (Some(int), PatternStep(range, step)) => SchemaParameters.splitRange(range)
      .flatMap { case (min, max) => fromComponents(min, max, step, codec) }
    case (Some(int), Pattern(range)) => SchemaParameters.splitRange(range)
      .flatMap { case (min, max) => fromComponents(min, max, codec) }
    case _ => None
  }
}

/** Trait for schemas for categorical variables. */
trait CategoricalSchema[T] extends Schema[T] {
  /** Values the variable can take. */
  val domain: Set[T]

  def validate(value: Value[T]): Boolean = domain.isEmpty || domain.contains(value.value)
}

/**
 * Schema for nominal variables.
 *
 * @param domain The values of the variable.
 */
case class NominalSchema[T](domain: Set[T] = Set.empty[T]) extends CategoricalSchema[T] {
  val classification = NominalType

  override protected def paramString(
    short: Boolean,
    f: (T) => String
  ): String = SchemaParameters.writeSet(short, domain, f)
}

/** Companion object to `NominalSchema` case class. */
object NominalSchema {
  /** Pattern for matching short string nominal schema. */
  val Pattern = (NominalType.name + """(?:\((?:(.*?))?\))?""").r

  /**
   * Parse a nominal schema from string components.
   *
   * @param dom   The domain value strings to parse.
   * @param codec The codec to parse with.
   *
   * @return A `Some[NominalSchema]` if successful, `None` otherwise.
   */
  def fromComponents[T](dom: Set[String], codec: Codec[T]): Option[NominalSchema[T]] = {
    val values = dom.flatMap(SchemaParameters.parse(codec, _))

    if (values.isEmpty || values.size != dom.size) None else Option(NominalSchema(values))
  }

  /**
   * Parse a nominal schema from string.
   *
   * @param str   The string to parse.
   * @param codec The codec to parse with.
   *
   * @return A `Some[NominalSchema]` if successful, `None` otherwise.
   */
  def fromShortString[T](str: String, codec: Codec[T]): Option[NominalSchema[T]] = str match {
    case Pattern(null) => Option(NominalSchema())
    case Pattern("") => Option(NominalSchema())
    case Pattern(domain) => fromComponents(SchemaParameters.splitSet(domain), codec)
    case _ => None
  }
}

/**
 * Schema for ordinal variables.
 *
 * @param domain The optional values of the variable.
 */
case class OrdinalSchema[T : Ordering](domain: Set[T] = Set.empty[T]) extends CategoricalSchema[T] {
  val classification = OrdinalType

  override protected def paramString(
    short: Boolean,
    f: (T) => String
  ): String = SchemaParameters.writeOrderedSet(short, domain, f)
}

/** Companion object to `OrdinalSchema`. */
object OrdinalSchema {
  /** Pattern for matching short string ordinal schema. */
  val Pattern = (OrdinalType.name + """(?:\((?:(.*?))?\))?""").r

  /**
   * Parse a ordinal schema from string components.
   *
   * @param dom   The domain value strings to parse.
   * @param codec The codec to parse with.
   *
   * @return A `Some[OrdinalSchema]` if successful, `None` otherwise.
   */
  def fromComponents[T](dom: Set[String], codec: Codec[T]): Option[OrdinalSchema[T]] = {
    val values = dom.flatMap(SchemaParameters.parse(codec, _))

    values.isEmpty || values.size != dom.size match {
      case false => Option(OrdinalSchema(values)(codec.ordering))
      case _ => None
    }
  }

  /**
   * Parse a ordinal schema from string.
   *
   * @param str   The string to parse.
   * @param codec The codec to parse with.
   *
   * @return A `Some[OrdinalSchema]` if successful, `None` otherwise.
   */
  def fromShortString[T](str: String, codec: Codec[T]): Option[OrdinalSchema[T]] = str match {
    case Pattern(null) => Option(OrdinalSchema()(codec.ordering))
    case Pattern("") => Option(OrdinalSchema()(codec.ordering))
    case Pattern(domain) => fromComponents(SchemaParameters.splitSet(domain), codec)
    case _ => None
  }
}

/**
 * Schema for date variables.
 *
 * @param dates The optional values of the variable.
 */
case class DateSchema[T <% Date](dates: Option[Either[(T, T), Set[T]]]) extends Schema[T] {
  val classification = DateType

  def validate(value: Value[T]): Boolean = dates match {
    case None => true
    case Some(Left((lower, upper))) => (value.cmp(lower) >= 0) && (value.cmp(upper) <= 0)
    case Some(Right(domain)) => domain.contains(value.value)
  }

  override protected def paramString(
    short: Boolean,
    f: (T) => String
  ): String = dates match {
    case None => ""
    case Some(Left(range)) => SchemaParameters.writeRange(short, Option(range), f)
    case Some(Right(domain)) => SchemaParameters.writeSet(short, domain, f)
  }
}

/** Companion object to `DateSchema`. */
object DateSchema {
  /** Pattern for matching short string date schema. */
  val Pattern = (DateType.name + """(?:\((?:(.*?))?\))?""").r

  /** Construct an unbounded date schema. */
  def apply[T <% Date](): DateSchema[T] = DateSchema(None)

  /**
   * Construct a date schema with bounded range.
   *
   * @param lower The lower bound (minimum value).
   * @param upper The upper bound (maximum value).
   */
  def apply[T <% Date](lower: T, upper: T): DateSchema[T] = DateSchema(Option(Left((lower, upper))))

  /**
   * Construct a date schema with a set of valid dates.
   *
   * @param domain The set of legal values.
   */
  def apply[T <% Date](domain: Set[T]): DateSchema[T] = DateSchema(Option(Right(domain)))

  /**
   * Parse a date schema from components.
   *
   * @param min   The minimum value string to parse.
   * @param max   The maximum value string to parse.
   * @param codec The codec to parse with.
   *
   * @return A `Some[DateSchema]` if successful, `None` otherwise.
   */
  def fromComponents[T](min: String, max: String, codec: Codec[T]): Option[DateSchema[T]] = for {
    low <- SchemaParameters.parse(codec, min)
    upp <- SchemaParameters.parse(codec, max)
    ev <- codec.date
  } yield DateSchema(low, upp)(ev)

  /**
   * Parse a date schema from string components.
   *
   * @param dom   The domain value strings to parse.
   * @param codec The codec to parse with.
   *
   * @return A `Some[DateSchema]` if successful, `None` otherwise.
   */
  def fromComponents[T](dom: Set[String], codec: Codec[T]): Option[DateSchema[T]] = {
    val values = dom.flatMap(SchemaParameters.parse(codec, _))

    (codec.date, values.isEmpty || values.size != dom.size) match {
      case (Some(ev), false) => Option(DateSchema(values)(ev))
      case _ => None
    }
  }

  /**
   * Parse a date schema from string.
   *
   * @param str   The string to parse.
   * @param codec The codec to parse with.
   *
   * @return A `Some[DateSchema]` if successful, `None` otherwise.
   */
  def fromShortString[T](str: String, codec: Codec[T]): Option[DateSchema[T]] = (codec.date, str) match {
    case (Some(ev), Pattern(null)) => Option(DateSchema()(ev))
    case (Some(ev), Pattern("")) => Option(DateSchema()(ev))
    case (Some(ev), RangePattern(range)) => SchemaParameters.splitRange(range)
      .flatMap { case (lower, upper) => fromComponents(lower, upper, codec) }
    case (Some(ev), Pattern(domain)) => fromComponents(SchemaParameters.splitSet(domain), codec)
    case _ => None
  }

  private val RangePattern = (DateType.name + """(?:\((?:(.*?:.*))?\))?""").r
}

/** Functions for dealing with schema parameters. */
private object SchemaParameters {
  def parse[T](codec: Codec[T], value: String): Option[T] = codec.decode(value)

  def splitRange(range: String): Option[(String, String)] = range.split(":") match {
    case Array(lower, upper) => Option((lower, upper))
    case _ => None
  }

  def splitSet(set: String): Set[String] = set.split("(?<!\\\\),", -1).toSet

  def writeList[T](short: Boolean, list: List[T], f: (T) => String, name: String): String =
    if (list.isEmpty)
      ""
    else {
      val args = list.map(d => f(d).replaceAll(",", "\\\\,")).mkString(",")

      if (short) args else name + "(" + args + ")"
    }

  def writeOrderedSet[
    T : Ordering
  ](
    short: Boolean,
    set: Set[T],
    f: (T) => String
  ): String = writeList(short, set.toList.sorted, f, "Set")

  def writeRange[T](short: Boolean, range: Option[(T, T)], f: (T) => String): String = range
    .map { case (lower, upper) => f(lower) + (if (short) ":" else ",") + f(upper) }
    .getOrElse("")

  def writeSet[T](short: Boolean, set: Set[T], f: (T) => String): String = writeList(short, set.toList, f, "Set")
}

