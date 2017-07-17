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

/** Trait for variable types. */
trait Type extends java.io.Serializable {
  /** Returns the most general super type of `this`. */
  def getRootType: Type = parent.map(_.getRootType).getOrElse(this)

  /** Check if this is a sub-type of `that`. */
  def isOfType(that: Type): Boolean = (this == that) || parent.map(_.isOfType(that)).getOrElse(false)

  /** Return the first type in common between this and `that`, or `Mixed` in case of no common ancestor. */
  def getCommonType(that: Type): Type = {
    if (this == that) this
    else if (this.isRootType) { if (that.isOfType(this)) this else MixedType }
    else if (that.isRootType) { if (this.isOfType(that)) that else MixedType }
    else sharedParent(that).getOrElse(MixedType)
  }

  override def toString: String = toShortString.capitalize + "Type"

  /** Return a consise (terse) string representation of a type. */
  def toShortString(): String

  protected val parent: Option[Type]

  private def isRootType: Boolean = parent.isEmpty
  private def sharedParent(that: Type): Option[Type] = parent
    .flatMap(p => if (that.isOfType(p)) Option(p) else p.sharedParent(that))
}

/** Companion object to `Type` trait. */
object Type {
  /**
   * Parse a type from a string.
   *
   * @param str String from which to parse the type.
   *
   * @return A `Some[Type]` in case of success, `None` otherwise.
   */
  def fromShortString(str: String): Option[Type] = str match {
    case MixedType.name => Option(MixedType)
    case CategoricalType.name => Option(CategoricalType)
    case NominalType.name => Option(NominalType)
    case OrdinalType.name => Option(OrdinalType)
    case NumericType.name => Option(NumericType)
    case ContinuousType.name => Option(ContinuousType)
    case DiscreteType.name => Option(DiscreteType)
    case DateType.name => Option(DateType)
    case StructuredType.name => Option(StructuredType)
    case _ => None
  }
}

/** Type for when the type is mixed. */
trait MixedType extends Type {
  def toShortString(): String = MixedType.name

  protected val parent: Option[Type] = None
}

/** Companion object to `MixedType` trait. */
object MixedType extends MixedType {
  /** Short name for this type. */
  val name = "mixed"
}

/** Type for numeric types. */
trait NumericType extends Type {
  def toShortString(): String = NumericType.name

  protected val parent: Option[Type] = None
}

/** Companion object to `NumericType` trait. */
object NumericType extends NumericType {
  /** Short name for this type. */
  val name = "numeric"
}

/** Type for continuous types. */
trait ContinuousType extends NumericType {
  override def toShortString(): String = ContinuousType.name

  override protected val parent: Option[Type] = Some(NumericType)
}

/** Companion object to `ContinuousType` trait. */
object ContinuousType extends ContinuousType {
  /** Short name for this type. */
  val name = "continuous"
}

/** Type for discrete types. */
trait DiscreteType extends NumericType {
  override def toShortString(): String = DiscreteType.name

  override protected val parent: Option[Type] = Some(NumericType)
}

/** Companion object to `DiscreteType` trait. */
object DiscreteType extends DiscreteType {
  /** Short name for this type. */
  val name = "discrete"
}

/** Type for categorical types. */
trait CategoricalType extends Type {
  def toShortString(): String = CategoricalType.name

  protected val parent: Option[Type] = None
}

/** Companion object to `CategoricalType` trait. */
object CategoricalType extends CategoricalType {
  /** Short name for this type. */
  val name = "categorical"
}

/** Type for nominal types. */
trait NominalType extends CategoricalType {
  override def toShortString(): String = NominalType.name

  override protected val parent: Option[Type] = Some(CategoricalType)
}

/** Companion object to `NominalType` trait. */
object NominalType extends NominalType {
  /** Short name for this type. */
  val name = "nominal"
}

/** Type for ordinal types. */
trait OrdinalType extends CategoricalType {
  override def toShortString(): String = OrdinalType.name

  override protected val parent: Option[Type] = Some(CategoricalType)
}

/** Companion object to `OrdinalType` trait. */
object OrdinalType extends OrdinalType {
  /** Short name for this type. */
  val name = "ordinal"
}

/** Type for date types. */
trait DateType extends Type {
  def toShortString(): String = DateType.name

  protected val parent: Option[Type] = None
}

/** Companion object to `DateType` trait. */
object DateType extends DateType {
  /** Short name for this type. */
  val name = "date"
}

/** Type for structured types. */
trait StructuredType extends Type {
  def toShortString(): String = StructuredType.name

  protected val parent: Option[Type] = None
}

/** Companion object to `StructuredType` trait. */
object StructuredType extends StructuredType {
  /** Short name for this type. */
  val name = "structured"
}

