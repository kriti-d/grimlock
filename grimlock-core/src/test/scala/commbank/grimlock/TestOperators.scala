// Copyright 2015,2016,2017 Commonwealth Bank of Australia
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

package commbank.grimlock.test

import commbank.grimlock.framework._
import commbank.grimlock.framework.content._
import commbank.grimlock.framework.encoding._
import commbank.grimlock.framework.environment.implicits._
import commbank.grimlock.framework.metadata._
import commbank.grimlock.framework.pairwise._
import commbank.grimlock.framework.position._

import commbank.grimlock.library.pairwise._

import shapeless.{ ::, HList, HNil }
import shapeless.nat._2

trait TestOperators extends TestGrimlock {

   type P = Value[String] :: Value[String] :: Value[String] :: HNil
   type S = Value[String] :: Value[String] :: HNil
   type R = Value[String] :: HNil

   val left = Cell(Position("left 1", "left 2", "reml"), Content(ContinuousSchema[Long](), 2L))
   val right = Cell(Position("right 1", "right 2", "remr"), Content(ContinuousSchema[Long](), 4L))
   val reml = Position("reml")
   val remr = Position("remr")
   val separator = "."
   val pattern: String

  def getPosition(call: Int, swap: Boolean = false): Position[Value[String] :: Value[String] :: HNil] = {
    val (first, second) = call match {
      case 1 => ("left 1.left 2", "right 1.right 2")
      case 2 => ("left 1.left 2", "left 1.left 2")
      case 3 => ("right 1.right 2", "left 1.left 2")
    }

    val third = if (swap) "remr" else "reml"

    Position(pattern.format(first, second), third)
  }
  def getContent(value: Double): Content = Content(ContinuousSchema[Double](), value)
}

class TestComparer extends TestOperators {

  val pattern = "not.used"

  "All" should "keep all" in {
    All.keep(left.position, right.position) shouldBe true
    All.keep(left.position, left.position) shouldBe true
    All.keep(right.position, left.position) shouldBe true
  }

  "Lower" should "keep lower" in {
    Lower.keep(left.position, right.position) shouldBe false
    Lower.keep(left.position, left.position) shouldBe false
    Lower.keep(right.position, left.position) shouldBe true
  }

  "Lower Diagonal" should "keep lower diagonal" in {
    LowerDiagonal.keep(left.position, right.position) shouldBe false
    LowerDiagonal.keep(left.position, left.position) shouldBe true
    LowerDiagonal.keep(right.position, left.position) shouldBe true
  }

  "Diagonal" should "keep diagonal" in {
    Diagonal.keep(left.position, right.position) shouldBe false
    Diagonal.keep(left.position, left.position) shouldBe true
    Diagonal.keep(right.position, left.position) shouldBe false
  }

  "Upper Diagonal" should "keep upper diagonal" in {
    UpperDiagonal.keep(left.position, right.position) shouldBe true
    UpperDiagonal.keep(left.position, left.position) shouldBe true
    UpperDiagonal.keep(right.position, left.position) shouldBe false
  }

  "Upper" should "keep upper" in {
    Upper.keep(left.position, right.position) shouldBe true
    Upper.keep(left.position, left.position) shouldBe false
    Upper.keep(right.position, left.position) shouldBe false
  }
}

class TestPlus extends TestOperators {

   val pattern = "(%1$s plus %2$s)"

  "A Plus" should "compute" in {
    val obj = Plus(Locate.PrependPairwiseSelectedStringToRemainder[P, S, R](Along(_2), pattern, true, separator))

    obj.compute(left, right) shouldBe List(Cell(getPosition(1), getContent(2 + 4)))
    obj.compute(left, left) shouldBe List(Cell(getPosition(2), getContent(2 + 2)))
    obj.compute(right, left) shouldBe List(Cell(getPosition(3, true), getContent(4 + 2)))
  }
}

class TestMinus extends TestOperators {

   val pattern = "(%1$s minus %2$s)"

  "A Minus" should "compute" in {
    val obj = Minus(
      Locate.PrependPairwiseSelectedStringToRemainder[P, S, R](Along(_2), pattern, true, separator),
      false
    )

    obj.compute(left, right) shouldBe List(Cell(getPosition(1), getContent(2 - 4)))
    obj.compute(left, left) shouldBe List(Cell(getPosition(2), getContent(2 - 2)))
    obj.compute(right, left) shouldBe List(Cell(getPosition(3, true), getContent(4 - 2)))
  }

  it should "compute inverse" in {
    val obj = Minus(Locate.PrependPairwiseSelectedStringToRemainder[P, S, R](Along(_2), pattern, true, separator), true)

    obj.compute(left, right) shouldBe List(Cell(getPosition(1), getContent(4 - 2)))
    obj.compute(left, left) shouldBe List(Cell(getPosition(2), getContent(2 - 2)))
    obj.compute(right, left) shouldBe List(Cell(getPosition(3, true), getContent(2 - 4)))
  }
}

class TestTimes extends TestOperators {

   val pattern = "(%1$s times %2$s)"

  "A Times" should "compute" in {
    val obj = Times(Locate.PrependPairwiseSelectedStringToRemainder[P, S, R](Along(_2), pattern, true, separator))

    obj.compute(left, right) shouldBe List(Cell(getPosition(1), getContent(2 * 4)))
    obj.compute(left, left) shouldBe List(Cell(getPosition(2), getContent(2 * 2)))
    obj.compute(right, left) shouldBe List(Cell(getPosition(3, true), getContent(4 * 2)))
  }
}

class TestDivide extends TestOperators {

   val pattern = "(%1$s divide %2$s)"

  "A Divide" should "compute" in {
    val obj = Divide(
      Locate.PrependPairwiseSelectedStringToRemainder[P, S, R](Along(_2), pattern, true, separator),
      false
    )

    obj.compute(left, right) shouldBe List(Cell(getPosition(1), getContent(2.0 / 4.0)))
    obj.compute(left, left) shouldBe List(Cell(getPosition(2), getContent(2.0 / 2.0)))
    obj.compute(right, left) shouldBe List(Cell(getPosition(3, true), getContent(4.0 / 2.0)))
  }

  it should "compute inverse" in {
    val obj = Divide(
      Locate.PrependPairwiseSelectedStringToRemainder[P, S, R](Along(_2), pattern, true, separator),
      true
    )

    obj.compute(left, right) shouldBe List(Cell(getPosition(1), getContent(4.0 / 2.0)))
    obj.compute(left, left) shouldBe List(Cell(getPosition(2), getContent(2.0 / 2.0)))
    obj.compute(right, left) shouldBe List(Cell(getPosition(3, true), getContent(2.0 / 4.0)))
  }
}

class TestConcatenate extends TestOperators {

   val pattern = "concat(%1$s,%2$s)"
   val format = "%1$s+%2$s"

  "A Concatenate" should "compute" in {
    val obj = Concatenate(
      Locate.PrependPairwiseSelectedStringToRemainder[P, S, R](Along(_2), pattern, true, separator),
      format
    )

    obj.compute(left, right) shouldBe List(Cell(getPosition(1), getContent(2, 4)))
    obj.compute(left, left) shouldBe List(Cell(getPosition(2), getContent(2, 2)))
    obj.compute(right, left) shouldBe List(Cell(getPosition(3, true), getContent(4, 2)))
  }

  def getContent(left: Long, right: Long): Content = {
    Content(NominalSchema[String](), format.format(left.toString, right.toString))
  }
}

class TestCombinationOperator extends TestOperators {

  val pattern = "not.used"

  "A CombinationOperator" should "compute" in {
    val obj = List(
      Plus(Locate.PrependPairwiseSelectedStringToRemainder[P, S, R](Along(_2), "(%1$s+%2$s)", true, "|")),
      Minus(Locate.PrependPairwiseSelectedStringToRemainder[P, S, R](Along(_2), "(%1$s-%2$s)", true, "|"))
    )

    obj.compute(left, right) shouldBe List(
      Cell(getPositionWithBar(1, "(%1$s+%2$s)"), getContent(2 + 4)),
      Cell(getPositionWithBar(1, "(%1$s-%2$s)"), getContent(2 - 4))
    )
    obj.compute(left, left) shouldBe List(
      Cell(getPositionWithBar(2, "(%1$s+%2$s)"), getContent(2 + 2)),
      Cell(getPositionWithBar(2, "(%1$s-%2$s)"), getContent(2 - 2))
    )
    obj.compute(right, left) shouldBe List(
      Cell(getPositionWithBar(3, "(%1$s+%2$s)", true), getContent(4 + 2)),
      Cell(getPositionWithBar(3, "(%1$s-%2$s)", true), getContent(4 - 2))
    )
  }

  def getPositionWithBar(
    call: Int,
    pattern: String,
    swap: Boolean = false
  ): Position[Value[String] :: Value[String] :: HNil] = {
    val (first, second) = call match {
      case 1 => ("left 1|left 2", "right 1|right 2")
      case 2 => ("left 1|left 2", "left 1|left 2")
      case 3 => ("right 1|right 2", "left 1|left 2")
    }

    val third = if (swap) "remr" else "reml"

    Position(pattern.format(first, second), third)
  }
}

case class PlusWithValue[P <: HList, Q <: HList](pos: Locate.FromPairwiseCells[P, Q]) extends OperatorWithValue[P, Q] {
  type V = Map[Position[P], Content]

  val plus = Plus(pos)

  def computeWithValue(left: Cell[P], right: Cell[P], ext: V): TraversableOnce[Cell[Q]] = {
    val offset = ext(left.position).value.as[Double].get

    plus.compute(left, right).flatMap { case Cell(pos, con) =>
      con.value.as[Double].map(d => Cell(pos, Content(ContinuousSchema[Double](), d + offset)))
    }
  }
}

class TestWithPrepareOperator extends TestOperators {

  val pattern = "not.used"

  val str = Cell(Position("x"), getDoubleContent(2))
  val dbl = Cell(Position("y"), getDoubleContent(4))
  val lng = Cell(Position("z"), getDoubleContent(8))

  val ext = Map(
    Position("x") -> getDoubleContent(3),
    Position("y") -> getDoubleContent(4),
    Position("z") -> getDoubleContent(5)
  )

  def prepare[T <: HList](cell: Cell[T]): Content = cell.content.value.as[Double] match {
    case Some(d) if (d.value == 2) => cell.content
    case Some(d) if (d.value == 4) => getStringContent("not.supported")
    case Some(d) if (d.value == 8) => getLongContent(8)
  }

  def prepareWithValue[T <: HList](
    cell: Cell[T],
    ext: Map[Position[T], Content]
  ): Content = (cell.content.value.as[Double], ext(cell.position).value.as[Double]) match {
    case (Some(c), Some(d)) if (c.value == 2) => getDoubleContent(2 * d.value)
    case (Some(c), _) if (c.value == 4) => getStringContent("not.supported")
    case (Some(c), Some(d)) if (c.value == 8) => getLongContent(d.value.toLong)
  }

  def locate[T <: HList] = (left: Cell[T], right: Cell[T]) => left.position.toOption

  def getLongContent(value: Long): Content = Content(DiscreteSchema[Long](), value)
  def getDoubleContent(value: Double): Content = Content(ContinuousSchema[Double](), value)
  def getStringContent(value: String): Content = Content(NominalSchema[String](), value)

  "An Operator" should "withPrepare correctly" in {
    val obj = Plus[Value[String] :: HNil, Value[String] :: HNil](locate).withPrepare(prepare)

    obj.compute(str, lng) shouldBe List(Cell(str.position, getDoubleContent(2 + 8)))
    obj.compute(str, dbl) shouldBe List()
  }

  it should "withPrepareWithValue correctly (without value)" in {
    val obj = PlusWithValue[Value[String] :: HNil, Value[String] :: HNil](locate).withPrepare(prepare)

    obj.computeWithValue(str, lng, ext).toList shouldBe List(Cell(str.position, getDoubleContent(2 + 8 + 3)))
    obj.computeWithValue(str, dbl, ext).toList shouldBe List()
  }

  it should "withPrepareWithValue correctly" in {
    val obj = PlusWithValue[Value[String] :: HNil, Value[String] :: HNil](locate).withPrepareWithValue(prepareWithValue)

    obj.computeWithValue(str, lng, ext).toList shouldBe List(Cell(str.position, getDoubleContent(2 * 3 + 5 + 3)))
    obj.computeWithValue(str, dbl, ext).toList shouldBe List()
  }
}

class TestAndThenMutateOperator extends TestOperators {

  val pattern = "not.used"

  val str = Cell(Position("x"), getDoubleContent(2))
  val dbl = Cell(Position("y"), getDoubleContent(4))
  val lng = Cell(Position("z"), getDoubleContent(8))

  val ext = Map(
    Position("x") -> getDoubleContent(3),
    Position("y") -> getDoubleContent(4),
    Position("z") -> getDoubleContent(5)
  )

  def mutate[T <: HList](cell: Cell[T]): Option[Content] = cell.content.value.as[Double] match {
    case Some(d) if (d.value == 10) => cell.content.toOption
    case Some(d) if (d.value == 6) => getStringContent("not.supported").toOption

    case Some(d) if (d.value == 13) => cell.content.toOption
    case Some(d) if (d.value == 9) => getStringContent("not.supported").toOption
  }

  def mutateWithValue[T <: HList](
    cell: Cell[T],
    ext: Map[Position[T], Content]
  ): Option[Content] = (cell.content.value.as[Double], ext(cell.position).value.as[Double]) match {
    case (Some(c), Some(d)) if (c.value == 13) => getDoubleContent(2 * d.value).toOption
    case (Some(c), _) if (c.value == 9) => getStringContent("not.supported").toOption
  }

  def locate[T <: HList] = (left: Cell[T], right: Cell[T]) => left.position.toOption

  def getLongContent(value: Long): Content = Content(DiscreteSchema[Long](), value)
  def getDoubleContent(value: Double): Content = Content(ContinuousSchema[Double](), value)
  def getStringContent(value: String): Content = Content(NominalSchema[String](), value)

  "An Operator" should "andThenMutate correctly" in {
    val obj = Plus[Value[String] :: HNil, Value[String] :: HNil](locate).andThenMutate(mutate)

    obj.compute(str, lng).toList shouldBe List(Cell(str.position, getDoubleContent(2 + 8)))
    obj.compute(str, dbl).toList shouldBe List(Cell(str.position, getStringContent("not.supported")))
  }

  it should "andThenMutateWithValue correctly (without value)" in {
    val obj = PlusWithValue[Value[String] :: HNil, Value[String] :: HNil](locate).andThenMutate(mutate)

    obj.computeWithValue(str, lng, ext).toList shouldBe List(Cell(str.position, getDoubleContent(2 + 8 + 3)))
    obj.computeWithValue(str, dbl, ext).toList shouldBe List(Cell(str.position, getStringContent("not.supported")))
  }

  it should "andThenMutateWithValue correctly" in {
    val obj = PlusWithValue[Value[String] :: HNil, Value[String] :: HNil](locate)
      .andThenMutateWithValue(mutateWithValue)

    obj.computeWithValue(str, lng, ext).toList shouldBe List(Cell(str.position, getDoubleContent(2 * 3)))
    obj.computeWithValue(str, dbl, ext).toList shouldBe List(Cell(str.position, getStringContent("not.supported")))
  }
}

