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

package commbank.grimlock.framework.utility

import shapeless.{ <:!<, Coproduct, Generic, HList }
import shapeless.ops.hlist.{ ToCoproduct, ToSum }

/**
 * This code based on an answer on stackoverflow:
 *
 *   http://stackoverflow.com/questions/6909053/enforce-type-difference
 *
 * It can be used to ensure that the types of arguments are not equal. Note that
 * it has the shapeless.=:!= name because that doesn't serialise (yet).
 */
sealed class =:!=[A, B] extends java.io.Serializable

/**
 * This code based on an answer on stackoverflow:
 *
 *   http://stackoverflow.com/questions/6909053/enforce-type-difference
 *
 * It can be used to ensure that the types of arguments are not equal. Note that
 * it has the shapeless.=:!= name because that doesn't serialise (yet).
 */
private[grimlock] trait LowerPriorityImplicits {
  /** do not call explicitly! */
  implicit def equal[A]: =:!=[A, A] = sys.error("should not be called")
}

/**
 * This code based on an answer on stackoverflow:
 *
 *   http://stackoverflow.com/questions/6909053/enforce-type-difference
 *
 * It can be used to ensure that the types of arguments are not equal. Note that
 * it has the shapeless.=:!= name because that doesn't serialise (yet).
 */
object =:!= extends LowerPriorityImplicits {
  /** do not call explicitly! */
  implicit def nequal[A, B]: =:!=[A, B] = new =:!=[A, B]
}

/** Trait for ecaping special characters in a string. */
trait Escape {
  /** The special character to escape. */
  val special: String

  /**
   * Escape a string.
   *
   * @param str The string to escape.
    * @return The escaped string.
   */
  def escape(str: String): String
}

/**
 * Escape a string by enclosing it in quotes.
 *
 * @param special The special character to quote.
 * @param quote   The quoting character to use.
 * @param all     Indicator if all strings should be quoted.
 */
case class Quote(special: String, quote: String = "\"", all: Boolean = false) extends Escape {
  def escape(str: String): String = if (all || str.contains(special)) quote + str + quote else str
}

/**
 * Escape a string by replacing the special character.
 *
 * @param special The special character to replace.
 * @param pattern The escape pattern to use. Use `%1$``s` to substitute the special character.
 */
case class Replace(special: String, pattern: String = "\\%1$s") extends Escape {
  def escape(str: String): String = str.replaceAllLiterally(special, pattern.format(special))
}

/** Trait that ensures types are different. */
trait Distinct[P <: Product] { }

/** Companion object to `Distinct` trait. */
object Distinct {
  implicit def isDistinct[
    P <: Product,
    L <: HList,
    C <: Coproduct
  ](implicit
    gen: Generic.Aux[P, L],
    toCoproduct: ToCoproduct.Aux[L, C],
    toSum: ToSum.Aux[L, C]
  ): Distinct[P] = new Distinct[P] {}
}

/** Trait for specifying a union of types. */
trait UnionTypes {
  /** Type for ensuring !A. */
  type Not[A] = A => Nothing

  /** Type for ensuring !!A. */
  type NotNot[A] = Not[Not[A]]

  /** Type for ensuring `T` contains `S`. */
  type Contains[S, T <: Disjunction] = NotNot[S] <:< Not[T#D]

  /** Type for ensuring `T` does not contain `S`. */
  type NotContains[S, T <: Disjunction] = NotNot[S] <:!< Not[T#D]

  /** Type for specifying `S` is `T`. */
  type Is[S, T] = Contains[S, OneOf[T]#Or[Nothing]]

  /** Type for specifying `S` must be in `T`. */
  type In[S, T <: Disjunction] = Contains[S, T]

  /** Type for specifying `S` must not be in `T`. */
  type NotIn[S, T <: Disjunction] = NotContains[S, T]

  /** Defines values for `T`. */
  type OneOf[T] = { type Or[S] = (Disjunction {type D = Not[T]})#Or[S] }

  /** Defines alternative vaues for `T`. */
  trait Disjunction { self =>
    type D
    type Or[S] = Disjunction {
      type D = self.D with Not[S]
    }
  }
}

/** Companion object to `UnionTypes` trait. */
object UnionTypes extends UnionTypes

