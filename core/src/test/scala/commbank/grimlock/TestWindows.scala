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
import commbank.grimlock.framework.content.metadata._
import commbank.grimlock.framework.encoding._
import commbank.grimlock.framework.position._
import commbank.grimlock.framework.window._

import commbank.grimlock.library.window._

import shapeless.nat.{ _0, _1, _2 }

trait TestBatchMovingAverage extends TestGrimlock {
  // test prepare&initilise
  val cell = Cell(Position("foo", "bar", "baz"), Content(ContinuousSchema[Long](), 1))
  val rem = Position("bar", "baz")
  val in = 1.0
  val first = (rem, in)
  val second = (rem, in)

  // test present
  val sel = Position("sales")

  def createCell(year: String, value: Double) = List(
    Cell(Position("sales", year), Content(ContinuousSchema[Double](), value))
  )
}

class TestSimpleMovingAverage extends TestBatchMovingAverage {

  "A SimpleMovingAverage" should "prepare correctly" in {
    SimpleMovingAverage(1, Locate.AppendRemainderDimension[_1, _2](_1), false).prepare(cell) shouldBe in
  }

  it should "initialise correctly" in {
    SimpleMovingAverage(1, Locate.AppendRemainderDimension[_1, _2](_1), false)
      .initialise(rem, in) shouldBe ((List(first), List()))
    SimpleMovingAverage(1, Locate.AppendRemainderDimension[_1, _2](_1), true)
      .initialise(rem, in) shouldBe ((List(first), List((rem, 1.0))))
    SimpleMovingAverage(1, Locate.AppendRemainderDimension[_1, _2](_2), false)
      .initialise(rem, in) shouldBe ((List(second), List()))
    SimpleMovingAverage(1, Locate.AppendRemainderDimension[_1, _2](_2), true)
      .initialise(rem, in) shouldBe ((List(second), List((rem, 1.0))))
    SimpleMovingAverage(5, Locate.AppendRemainderDimension[_1, _2](_1), false)
      .initialise(rem, in) shouldBe ((List(first), List()))
    SimpleMovingAverage(5, Locate.AppendRemainderDimension[_1, _2](_1), true)
      .initialise(rem, in) shouldBe ((List(first), List((rem, 1.0))))
    SimpleMovingAverage(5, Locate.AppendRemainderDimension[_1, _2](_2), false)
      .initialise(rem, in) shouldBe ((List(second), List()))
    SimpleMovingAverage(5, Locate.AppendRemainderDimension[_1, _2](_2), true)
      .initialise(rem, in) shouldBe ((List(second), List((rem, 1.0))))
  }

  it should "update correctly" in {
    val obj = SimpleMovingAverage(5, Locate.AppendRemainderDimension[_1, _1](_1), false)

    val init = obj.initialise(Position("2003"), 4.0)
    init shouldBe ((List((Position("2003"), 4.0)), List()))

    val first = obj.update(Position("2004"), 6.0, init._1)
    first shouldBe ((List((Position("2003"), 4.0), (Position("2004"), 6.0)), List()))

    val second = obj.update(Position("2005"), 5.0, first._1)
    second shouldBe ((List((Position("2003"), 4.0), (Position("2004"), 6.0), (Position("2005"), 5.0)), List()))

    val third = obj.update(Position("2006"), 8.0, second._1)
    third shouldBe ((List(
      (Position("2003"), 4.0),
      (Position("2004"), 6.0),
      (Position("2005"), 5.0),
      (Position("2006"), 8.0)
    ), List()))

    val fourth = obj.update(Position("2007"), 9.0, third._1)
    fourth shouldBe ((List(
      (Position("2003"), 4.0),
      (Position("2004"), 6.0),
      (Position("2005"), 5.0),
      (Position("2006"), 8.0),
      (Position("2007"), 9.0)
    ), List((Position("2007"), 6.4))))

    val fifth = obj.update(Position("2008"), 5.0, fourth._1)
    fifth shouldBe ((List(
      (Position("2004"), 6.0),
      (Position("2005"), 5.0),
      (Position("2006"), 8.0),
      (Position("2007"), 9.0),
      (Position("2008"), 5.0)
    ), List((Position("2008"), 6.6))))
  }

  it should "update all correctly" in {
    val obj = SimpleMovingAverage(5, Locate.AppendRemainderDimension[_1, _1](_1), true)

    val init = obj.initialise(Position("2003"), 4.0)
    init shouldBe ((List((Position("2003"), 4.0)), List((Position("2003"), 4.0))))

    val first = obj.update(Position("2004"), 6.0, init._1)
    first shouldBe ((List((Position("2003"), 4.0), (Position("2004"), 6.0)), List((Position("2004"), 5))))

    val second = obj.update(Position("2005"), 5.0, first._1)
    second shouldBe ((List(
      (Position("2003"), 4.0),
      (Position("2004"), 6.0),
      (Position("2005"), 5.0)
    ), List((Position("2005"), 5))))

    val third = obj.update(Position("2006"), 8.0, second._1)
    third shouldBe ((List(
      (Position("2003"), 4.0),
      (Position("2004"), 6.0),
      (Position("2005"), 5.0),
      (Position("2006"), 8.0)
    ), List((Position("2006"), 5.75))))

    val fourth = obj.update(Position("2007"), 9.0, third._1)
    fourth shouldBe ((List(
      (Position("2003"), 4.0),
      (Position("2004"), 6.0),
      (Position("2005"), 5.0),
      (Position("2006"), 8.0),
      (Position("2007"), 9.0)
    ), List((Position("2007"), 6.4))))

    val fifth = obj.update(Position("2008"), 5.0, fourth._1)
    fifth shouldBe ((List(
      (Position("2004"), 6.0),
      (Position("2005"), 5.0),
      (Position("2006"), 8.0),
      (Position("2007"), 9.0),
      (Position("2008"), 5.0)
    ), List((Position("2008"), 6.6))))
  }

  it should "present" in {
    SimpleMovingAverage(1, Locate.AppendRemainderDimension[_1, _1](_1), false)
      .present(sel, (Position("2008"), 6.6)) shouldBe createCell("2008", 6.6)
  }
}

class TestCenteredMovingAverage extends TestBatchMovingAverage {

  "A CenteredMovingAverage" should "prepare correctly" in {
    CenteredMovingAverage(1, Locate.AppendRemainderDimension[_1, _2](_1))
      .prepare(cell) shouldBe in
  }

  it should "initialise correctly" in {
    CenteredMovingAverage(1, Locate.AppendRemainderDimension[_1, _2](_1))
      .initialise(rem, in) shouldBe ((List(first), List()))
    CenteredMovingAverage(1, Locate.AppendRemainderDimension[_1, _2](_2))
      .initialise(rem, in) shouldBe ((List(second), List()))
    CenteredMovingAverage(5, Locate.AppendRemainderDimension[_1, _2](_1))
      .initialise(rem, in) shouldBe ((List(first), List()))
    CenteredMovingAverage(5, Locate.AppendRemainderDimension[_1, _2](_2))
      .initialise(rem, in) shouldBe ((List(second), List()))
  }

  it should "update correctly" in {
    val obj = CenteredMovingAverage(2, Locate.AppendRemainderDimension[_1, _1](_1))

    val init = obj.initialise(Position("2003"), 4.0)
    init shouldBe ((List((Position("2003"), 4.0)), List()))

    val first = obj.update(Position("2004"), 6.0, init._1)
    first shouldBe ((List((Position("2003"), 4.0), (Position("2004"), 6.0)), List()))

    val second = obj.update(Position("2005"), 5.0, first._1)
    second shouldBe ((List((Position("2003"), 4.0), (Position("2004"), 6.0), (Position("2005"), 5.0)), List()))

    val third = obj.update(Position("2006"), 8.0, second._1)
    third shouldBe ((List(
      (Position("2003"), 4.0),
      (Position("2004"), 6.0),
      (Position("2005"), 5.0),
      (Position("2006"), 8.0)
    ), List()))

    val fourth = obj.update(Position("2007"), 9.0, third._1)
    fourth shouldBe ((List(
      (Position("2003"), 4.0),
      (Position("2004"), 6.0),
      (Position("2005"), 5.0),
      (Position("2006"), 8.0),
      (Position("2007"), 9.0)
    ), List((Position("2005"), 6.4))))

    val fifth = obj.update(Position("2008"), 5.0, fourth._1)
    fifth shouldBe ((List(
      (Position("2004"), 6.0),
      (Position("2005"), 5.0),
      (Position("2006"), 8.0),
      (Position("2007"), 9.0),
      (Position("2008"), 5.0)
    ), List((Position("2006"), 6.6))))
  }

  it should "present correctly" in {
    CenteredMovingAverage(2, Locate.AppendRemainderDimension[_1, _1](_1))
      .present(sel, (Position("2006"), 6.6)) shouldBe createCell("2006", 6.6)
  }
}

class TestWeightedMovingAverage extends TestBatchMovingAverage {

  "A WeightedMovingAverage" should "prepare correctly" in {
    WeightedMovingAverage(1, Locate.AppendRemainderDimension[_1, _2](_1), false)
      .prepare(cell) shouldBe in
  }

  it should "initialise correctly" in {
    WeightedMovingAverage(1, Locate.AppendRemainderDimension[_1, _2](_1), false)
      .initialise(rem, in) shouldBe ((List(first), List()))
    WeightedMovingAverage(1, Locate.AppendRemainderDimension[_1, _2](_1), true)
      .initialise(rem, in) shouldBe ((List(first), List((rem, in))))
    WeightedMovingAverage(1, Locate.AppendRemainderDimension[_1, _2](_2), false)
      .initialise(rem, in) shouldBe ((List(second), List()))
    WeightedMovingAverage(1, Locate.AppendRemainderDimension[_1, _2](_2), true)
      .initialise(rem, in) shouldBe ((List(second), List((rem, in))))
    WeightedMovingAverage(5, Locate.AppendRemainderDimension[_1, _2](_1), false)
      .initialise(rem, in) shouldBe ((List(first), List()))
    WeightedMovingAverage(5, Locate.AppendRemainderDimension[_1, _2](_1), true)
      .initialise(rem, in) shouldBe ((List(first), List((rem, in))))
    WeightedMovingAverage(5, Locate.AppendRemainderDimension[_1, _2](_2), false)
      .initialise(rem, in) shouldBe ((List(second), List()))
    WeightedMovingAverage(5, Locate.AppendRemainderDimension[_1, _2](_2), true)
      .initialise(rem, in) shouldBe ((List(second), List((rem, in))))
  }

  it should "update correctly" in {
    val obj = WeightedMovingAverage(5, Locate.AppendRemainderDimension[_1, _1](_1), false)

    val init = obj.initialise(Position("2003"), 4.0)
    init shouldBe ((List((Position("2003"), 4.0)), List()))

    val first = obj.update(Position("2004"), 6.0, init._1)
    first shouldBe ((List((Position("2003"), 4.0), (Position("2004"), 6.0)), List()))

    val second = obj.update(Position("2005"), 5.0, first._1)
    second shouldBe ((List((Position("2003"), 4.0), (Position("2004"), 6.0), (Position("2005"), 5.0)), List()))

    val third = obj.update(Position("2006"), 8.0, second._1)
    third shouldBe ((List(
      (Position("2003"), 4.0),
      (Position("2004"), 6.0),
      (Position("2005"), 5.0),
      (Position("2006"), 8.0)
    ), List()))

    val fourth = obj.update(Position("2007"), 9.0, third._1)
    fourth shouldBe ((List(
      (Position("2003"), 4.0),
      (Position("2004"), 6.0),
      (Position("2005"), 5.0),
      (Position("2006"), 8.0),
      (Position("2007"), 9.0)
    ), List((Position("2007"), 7.2))))

    val fifth = obj.update(Position("2008"), 5.0, fourth._1)
    fifth shouldBe ((List(
      (Position("2004"), 6.0),
      (Position("2005"), 5.0),
      (Position("2006"), 8.0),
      (Position("2007"), 9.0),
      (Position("2008"), 5.0)
    ), List((Position("2008"), 6.733333333333333))))
  }

  it should "update all correctly" in {
    val obj = WeightedMovingAverage(5, Locate.AppendRemainderDimension[_1, _1](_1), true)

    val init = obj.initialise(Position("2003"), 4.0)
    init shouldBe ((List((Position("2003"), 4.0)), List((Position("2003"), 4.0))))

    val first = obj.update(Position("2004"), 6.0, init._1)
    first shouldBe ((List(
      (Position("2003"), 4.0),
      (Position("2004"), 6.0)
    ), List((Position("2004"), 5.333333333333333))))

    val second = obj.update(Position("2005"), 5.0, first._1)
    second shouldBe ((List(
      (Position("2003"), 4.0),
      (Position("2004"), 6.0),
      (Position("2005"), 5.0)
    ), List((Position("2005"), 5.166666666666667))))

    val third = obj.update(Position("2006"), 8.0, second._1)
    third shouldBe ((List(
      (Position("2003"), 4.0),
      (Position("2004"), 6.0),
      (Position("2005"), 5.0),
      (Position("2006"), 8.0)
    ), List((Position("2006"), 6.3))))

    val fourth = obj.update(Position("2007"), 9.0, third._1)
    fourth shouldBe ((List(
      (Position("2003"), 4.0),
      (Position("2004"), 6.0),
      (Position("2005"), 5.0),
      (Position("2006"), 8.0),
      (Position("2007"), 9.0)
    ), List((Position("2007"), 7.2))))

    val fifth = obj.update(Position("2008"), 5.0, fourth._1)
    fifth shouldBe ((List(
      (Position("2004"), 6.0),
      (Position("2005"), 5.0),
      (Position("2006"), 8.0),
      (Position("2007"), 9.0),
      (Position("2008"), 5.0)
    ), List((Position("2008"), 6.733333333333333))))
  }

  it should "present correctly" in {
    WeightedMovingAverage(5, Locate.AppendRemainderDimension[_1, _1](_1), false)
      .present(sel, (Position("2008"), 6.733333333333333)) shouldBe createCell("2008", 6.733333333333333)
  }
}

trait TestOnlineMovingAverage extends TestGrimlock {
  // test prepare&initilise
  val cell = Cell(Position("foo", "bar", "baz"), Content(ContinuousSchema[Long](), 1))
  val rem = Position("bar", "baz")
  val in = 1.0
  val first = ((1.0, 1), List((rem, in)))
  val second = ((1.0, 1), List((rem, in)))
  // test present
  val sel = Position()

  def createCell(str: String, value: Double) = List(Cell(Position(str), Content(ContinuousSchema[Double](), value)))
}

class TestCumulativeMovingAverage extends TestOnlineMovingAverage {

  "A CumulativeMovingAverage" should "prepare correctly" in {
    CumulativeMovingAverage(Locate.AppendRemainderDimension[_1, _2](_1)).prepare(cell) shouldBe in
  }

  it should "initialise correctly" in {
    CumulativeMovingAverage(Locate.AppendRemainderDimension[_1, _2](_1)).initialise(rem, in) shouldBe first
    CumulativeMovingAverage(Locate.AppendRemainderDimension[_1, _2](_2)).initialise(rem, in) shouldBe second
  }

  it should "update correctly" in {
    val obj = CumulativeMovingAverage(Locate.AppendRemainderDimension[_0, _1](_1))

    val init = obj.initialise(Position("val.1"), 1.0)
    init shouldBe (((1.0, 1), List((Position("val.1"), 1.0))))

    val first = obj.update(Position("val.2"), 2.0, init._1)
    first shouldBe (((1.5, 2), List((Position("val.2"), 1.5))))

    val second = obj.update(Position("val.3"), 3.0, first._1)
    second shouldBe (((2.0, 3), List((Position("val.3"), 2))))

    val third = obj.update(Position("val.4"), 4.0, second._1)
    third shouldBe (((2.5, 4), List((Position("val.4"), 2.5))))

    val fourth = obj.update(Position("val.5"), 5.0, third._1)
    fourth shouldBe (((3.0, 5), List((Position("val.5"), 3))))
  }

  it should "present correctly" in {
    CumulativeMovingAverage(Locate.AppendRemainderDimension[_0, _1](_1))
      .present(sel, (Position("val.5"), 3.0)) shouldBe createCell("val.5", 3)
  }
}

class TestExponentialMovingAverage extends TestOnlineMovingAverage {

  "A ExponentialMovingAverage" should "prepare correctly" in {
    ExponentialMovingAverage(3, Locate.AppendRemainderDimension[_1, _2](_1)).prepare(cell) shouldBe in
  }

  it should "initialise correctly" in {
    ExponentialMovingAverage(0.33, Locate.AppendRemainderDimension[_1, _2](_1)).initialise(rem, in) shouldBe first
    ExponentialMovingAverage(3, Locate.AppendRemainderDimension[_1, _2](_1)).initialise(rem, in) shouldBe first
    ExponentialMovingAverage(0.33, Locate.AppendRemainderDimension[_1, _2](_2)).initialise(rem, in) shouldBe second
    ExponentialMovingAverage(3, Locate.AppendRemainderDimension[_1, _2](_2)).initialise(rem, in) shouldBe second
  }

  it should "update correctly" in {
    val obj = ExponentialMovingAverage(0.33, Locate.AppendRemainderDimension[_0, _1](_1))

    val init = obj.initialise(Position("day.1"), 16.0)
    init shouldBe (((16.0, 1), List((Position("day.1"), 16.0))))

    val first = obj.update(Position("day.2"), 17.0, init._1)
    first shouldBe (((16.33, 2), List((Position("day.2"), 16.33))))

    val second = obj.update(Position("day.3"), 17.0, first._1)
    second shouldBe (((16.551099999999998, 3), List((Position("day.3"), 16.551099999999998))))

    val third = obj.update(Position("day.4"), 10.0, second._1)
    third shouldBe (((14.389236999999998, 4), List((Position("day.4"), 14.389236999999998))))

    val fourth = obj.update(Position("day.5"), 17.0, third._1)
    fourth shouldBe (((15.250788789999998, 5), List((Position("day.5"), 15.250788789999998))))
  }

  it should "present correctly" in {
    ExponentialMovingAverage(0.33, Locate.AppendRemainderDimension[_0, _1](_1))
      .present(sel, (Position("day.5"), 15.250788789999998)) shouldBe createCell("day.5", 15.250788789999998)
  }
}

trait TestWindow extends TestGrimlock {

  val cell1 = Cell(Position("foo", "bar", "baz"), Content(ContinuousSchema[Long](), 1))
  val cell2 = Cell(Position("foo", "bar", "baz"), Content(NominalSchema[String](), "abc"))
  val sel = Position("foo")
  val rem = Position("bar", "baz")
  val in1 = Option(1.0)
  val in2f = None
  val in2t = Option(Double.NaN)
}

class TestCumulativeSum extends TestWindow {

  def createCell(value: Double) = List(Cell(Position("foo", "bar|baz"), Content(ContinuousSchema[Double](), value)))

  "A CumulativeSum" should "prepare correctly" in {
    CumulativeSums(Locate.AppendRemainderString[_1, _2](), true).prepare(cell1) shouldBe in1
    CumulativeSums(Locate.AppendRemainderString[_1, _2](), false).prepare(cell1) shouldBe in1
    CumulativeSums(Locate.AppendRemainderString[_1, _2](), true)
      .prepare(cell2)
      .map(_.compare(Double.NaN)) shouldBe Option(0)
    CumulativeSums(Locate.AppendRemainderString[_1, _2](), false).prepare(cell2) shouldBe in2f
  }

  it should "initialise correctly" in {
    val obj = CumulativeSums(Locate.AppendRemainderString[_1, _2](), true)

    obj.initialise(rem, Option(1.0)) shouldBe ((Option(1.0), List((rem, 1.0))))
    obj.initialise(rem, None) shouldBe ((None, List()))

    val init = obj.initialise(rem, Option(Double.NaN))
    init._1.map(_.compare(Double.NaN)) shouldBe (Option(0))
    init._2.toList.map { case (r, d) => (r, d.compare(Double.NaN)) } shouldBe (List((rem, 0)))
  }

  it should "update correctly strict" in {
    val obj = CumulativeSums(Locate.AppendRemainderString[_1, _2](), true)

    val init = obj.initialise(rem, in1)
    init shouldBe ((Option(1.0), List((rem, 1.0))))

    val first = obj.update(rem, in1, init._1)
    first shouldBe ((Option(2.0), List((rem, 2.0))))

    val second = obj.update(rem, in2t, first._1)
    second._1.map(_.compare(Double.NaN)) shouldBe (Option(0))
    second._2.toList.map { case (r, d) => (r, d.compare(Double.NaN)) } shouldBe (List((rem, 0)))

    val third = obj.update(rem, in1, second._1)
    third._1.map(_.compare(Double.NaN)) shouldBe (Option(0))
    third._2.toList.map { case (r, d) => (r, d.compare(Double.NaN)) } shouldBe (List((rem, 0)))
  }

  it should "update correctly strict on first" in {
    val obj = CumulativeSums(Locate.AppendRemainderString[_1, _2](), true)

    val init = obj.initialise(rem, in2t)
    init._1.map(_.compare(Double.NaN)) shouldBe (Option(0))
    init._2.toList.map { case (r, d) => (r, d.compare(Double.NaN)) } shouldBe (List((rem, 0)))

    val first = obj.update(rem, in1, init._1)
    first._1.map(_.compare(Double.NaN)) shouldBe (Option(0))
    first._2.toList.map { case (r, d) => (r, d.compare(Double.NaN)) } shouldBe (List((rem, 0)))

    val second = obj.update(rem, in2t, first._1)
    second._1.map(_.compare(Double.NaN)) shouldBe (Option(0))
    second._2.toList.map { case (r, d) => (r, d.compare(Double.NaN)) } shouldBe (List((rem, 0)))

    val third = obj.update(rem, in1, second._1)
    third._1.map(_.compare(Double.NaN)) shouldBe (Option(0))
    third._2.toList.map { case (r, d) => (r, d.compare(Double.NaN)) } shouldBe (List((rem, 0)))
  }

  it should "update correctly non-strict" in {
    val obj = CumulativeSums(Locate.AppendRemainderString[_1, _2](), false)

    val init = obj.initialise(rem, in1)
    init shouldBe ((Option(1.0), List((rem, 1.0))))

    val first = obj.update(rem, in1, init._1)
    first shouldBe ((Option(2.0), List((rem, 2.0))))

    val second = obj.update(rem, in2f, first._1)
    second shouldBe ((Option(2.0), List()))

    val third = obj.update(rem, in1, second._1)
    third shouldBe ((Option(3.0), List((rem, 3.0))))
  }

  it should "update correctly non-strict on first" in {
    val obj = CumulativeSums(Locate.AppendRemainderString[_1, _2](), false)

    val init = obj.initialise(rem, in2f)
    init shouldBe ((None, List()))

    val first = obj.update(rem, in1, init._1)
    first shouldBe ((Option(1.0), List((rem, 1.0))))

    val second = obj.update(rem, in2f, first._1)
    second shouldBe ((Option(1.0), List()))

    val third = obj.update(rem, in1, second._1)
    third shouldBe ((Option(2.0), List((rem, 2.0))))
  }

  it should "present correctly strict" in {
    CumulativeSums(Locate.AppendRemainderString[_1, _2](), true).present(sel, (rem, 1.0)) shouldBe createCell(1.0)
  }
}

class TestBinOp extends TestWindow {

  def createCell(value: Double) = List(
    Cell(Position("foo", "p(bar|baz, bar|baz)"), Content(ContinuousSchema[Double](), value))
  )

  "A BinOp" should "prepare correctly" in {
    BinOp(_ + _, Locate.AppendPairwiseString[_1, _2]("p(%1$s, %2$s)"), true).prepare(cell1) shouldBe in1
    BinOp(_ + _, Locate.AppendPairwiseString[_1, _2]("p(%1$s, %2$s)"), false).prepare(cell1) shouldBe in1
    BinOp(_ + _, Locate.AppendPairwiseString[_1, _2]("p(%1$s, %2$s)"), true)
      .prepare(cell2)
      .map(_.compare(Double.NaN)) shouldBe Option(0)
    BinOp(_ + _, Locate.AppendPairwiseString[_1, _2]("p(%1$s, %2$s)"), false).prepare(cell2) shouldBe in2f
  }

  it should "initialise correctly" in {
    BinOp(_ + _, Locate.AppendPairwiseString[_1, _2]("p(%1$s, %2$s)"), true)
      .initialise(rem, in1) shouldBe (((in1, rem), List()))
    BinOp(_ + _, Locate.AppendPairwiseString[_1, _2]("p(%1$s, %2$s)"), false)
      .initialise(rem, in1) shouldBe (((in1, rem), List()))
    val init = BinOp(_ + _, Locate.AppendPairwiseString[_1, _2]("p(%1$s, %2$s)"), true).initialise(rem, in2t)
    init._1._1.map(_.compare(Double.NaN)) shouldBe Option(0)
    init._1._2 shouldBe rem
    init._2 shouldBe List()
    BinOp(_ + _, Locate.AppendPairwiseString[_1, _2]("p(%1$s, %2$s)"), false)
      .initialise(rem, in2f) shouldBe (((None, rem), List()))
  }

  it should "update correctly strict" in {
    val obj = BinOp(_ + _, Locate.AppendPairwiseString[_1, _2]("p(%1$s, %2$s)"), true)

    val init = obj.initialise(rem, in1)
    init shouldBe (((Option(1.0), rem), List()))

    val first = obj.update(rem, in1, init._1)
    first shouldBe (((Option(1.0), rem), List((2.0, rem, rem))))

    val second = obj.update(rem, in2t, first._1)
    second._1._1.map(_.compare(Double.NaN)) shouldBe Option(0)
    second._1._2 shouldBe rem
    second._2.toList.map { case (d, c, p) => (d.compare(Double.NaN), c, p) } shouldBe List((0, rem, rem))

    val third = obj.update(rem, in1, second._1)
    third._1._1.map(_.compare(Double.NaN)) shouldBe Option(0)
    third._1._2 shouldBe rem
    third._2.toList.map { case (d, c, p) => (d.compare(Double.NaN), c, p) } shouldBe List((0, rem, rem))
  }

  it should "update correctly strict on first" in {
    val obj = BinOp(_ + _, Locate.AppendPairwiseString[_1, _2]("p(%1$s, %2$s)"), true)

    val init = obj.initialise(rem, in2t)
    init._1._1.map(_.compare(Double.NaN)) shouldBe Option(0)
    init._1._2 shouldBe rem
    init._2.toList shouldBe List()

    val first = obj.update(rem, in1, init._1)
    first._1._1.map(_.compare(Double.NaN)) shouldBe Option(0)
    first._1._2 shouldBe rem
    first._2.toList.map { case (d, c, p) => (d.compare(Double.NaN), c, p) } shouldBe List((0, rem, rem))

    val second = obj.update(rem, in2t, first._1)
    second._1._1.map(_.compare(Double.NaN)) shouldBe Option(0)
    second._1._2 shouldBe rem
    second._2.toList.map { case (d, c, p) => (d.compare(Double.NaN), c, p) } shouldBe List((0, rem, rem))

    val third = obj.update(rem, in1, second._1)
    third._1._1.map(_.compare(Double.NaN)) shouldBe Option(0)
    third._1._2 shouldBe rem
    third._2.toList.map { case (d, c, p) => (d.compare(Double.NaN), c, p) } shouldBe List((0, rem, rem))
  }

  it should "update correctly non-strict" in {
    val obj = BinOp(_ + _, Locate.AppendPairwiseString[_1, _2]("p(%1$s, %2$s)"), false)

    val init = obj.initialise(rem, in1)
    init shouldBe (((Option(1.0), rem), List()))

    val first = obj.update(rem, in1, init._1)
    first shouldBe (((Option(1.0), rem), List((2.0, rem, rem))))

    val second = obj.update(rem, in2f, first._1)
    second shouldBe (((Option(1.0), rem), List()))

    val third = obj.update(rem, in1, second._1)
    third shouldBe (((Option(1.0), rem), List((2.0, rem, rem))))
  }

  it should "update correctly non-strict on first" in {
    val obj = BinOp(_ + _, Locate.AppendPairwiseString[_1, _2]("p(%1$s, %2$s)"), false)

    val init = obj.initialise(rem, in2f)
    init shouldBe (((None, rem), List()))

    val first = obj.update(rem, in1, init._1)
    first shouldBe (((Option(1.0), rem), List()))

    val second = obj.update(rem, in2f, first._1)
    second shouldBe (((Option(1.0), rem), List()))

    val third = obj.update(rem, in1, second._1)
    third shouldBe (((Option(1.0), rem), List((2.0, rem, rem))))
  }

  it should "present correctly" in {
    BinOp(_ + _, Locate.AppendPairwiseString[_1, _2]("p(%1$s, %2$s)"), false)
      .present(sel, (1.0, rem, rem)) shouldBe createCell(1.0)
  }
}

class TestCombinationWindow extends TestGrimlock {

  def renamer(name: String)(cell: Cell[_2]): Option[Position[_2]] = Option(
    cell.position.update(_2, name.format(cell.position(_2).toShortString))
  )

  "A CombinationWindow" should "present correctly" in {
    val sel = Position("sales")
    val obj: Window[_2, _1, _1, _2] = List(
      SimpleMovingAverage[_2, _1, _1, _2](
        5,
        Locate.AppendRemainderDimension[_1, _1](_1),
        false
      ).andThenRelocate(renamer("%1$s.simple")),
      WeightedMovingAverage[_2, _1, _1, _2](
        5,
        Locate.AppendRemainderDimension[_1, _1](_1),
        false
      ).andThenRelocate(renamer("%1$s.weighted"))
    )

    val prep3 = obj.prepare(Cell(Position("sales", "2003"), createContent(4)))
    prep3 shouldBe List(4.0, 4.0)

    val init = obj.initialise(Position("2003"), prep3)
    init shouldBe ((List(List((Position("2003"), 4.0)), List((Position("2003"), 4.0))), List(List(List(), List()))))

    val prep4 = obj.prepare(Cell(Position("sales", "2004"), createContent(6)))
    prep4 shouldBe List(6.0, 6.0)

    val first = obj.update(Position("2004"), prep4, init._1)
    first shouldBe ((List(
      List((Position("2003"), 4.0), (Position("2004"), 6.0)),
      List((Position("2003"), 4.0), (Position("2004"), 6.0))
    ), List(List(List(), List()))))

    val prep5 = obj.prepare(Cell(Position("sales", "2005"), createContent(5)))
    prep5 shouldBe List(5.0, 5.0)

    val second = obj.update(Position("2005"), prep5, first._1)
    second shouldBe ((List(
      List((Position("2003"), 4.0), (Position("2004"), 6.0), (Position("2005"), 5.0)),
      List((Position("2003"), 4.0), (Position("2004"), 6.0), (Position("2005"), 5.0))
    ), List(List(List(), List()))))

    val prep6 = obj.prepare(Cell(Position("sales", "2006"), createContent(8)))
    prep6 shouldBe List(8.0, 8.0)

    val third = obj.update(Position("2006"), prep6, second._1)
    third shouldBe ((List(
      List((Position("2003"), 4.0), (Position("2004"), 6.0), (Position("2005"), 5.0), (Position("2006"), 8.0)),
      List((Position("2003"), 4.0), (Position("2004"), 6.0), (Position("2005"), 5.0), (Position("2006"), 8.0))
    ), List(List(List(), List()))))

    val prep7 = obj.prepare(Cell(Position("sales", "2007"), createContent(9)))
    prep7 shouldBe List(9.0, 9.0)

    val fourth = obj.update(Position("2007"), prep7, third._1)
    fourth shouldBe ((List(
      List(
        (Position("2003"), 4.0),
        (Position("2004"), 6.0),
        (Position("2005"), 5.0),
        (Position("2006"), 8.0),
        (Position("2007"), 9.0)
      ),
      List(
        (Position("2003"), 4.0),
        (Position("2004"), 6.0),
        (Position("2005"), 5.0),
        (Position("2006"), 8.0),
        (Position("2007"), 9.0)
      )
    ), List(List(List((Position("2007"), 6.4)), List((Position("2007"), 7.2))))))

    val prep8 = obj.prepare(Cell(Position("sales", "2008"), createContent(5)))
    prep8 shouldBe List(5.0, 5.0)

    val fifth = obj.update(Position("2008"), prep8, fourth._1)
    fifth shouldBe ((List(
      List(
        (Position("2004"), 6.0),
        (Position("2005"), 5.0),
        (Position("2006"), 8.0),
        (Position("2007"), 9.0),
        (Position("2008"), 5.0)
      ),
      List(
        (Position("2004"), 6.0),
        (Position("2005"), 5.0),
        (Position("2006"), 8.0),
        (Position("2007"), 9.0),
        (Position("2008"), 5.0)
      )
    ), List(List(List((Position("2008"), 6.6)), List((Position("2008"), 6.733333333333333))))))

    val cells = obj.present(sel, fifth._2.toList(0))
    cells shouldBe createCell("2008", 6.6, 6.733333333333333)
  }

  def createContent(value: Long): Content = Content(ContinuousSchema[Long](), value)
  def createCell(year: String, value1: Double, value2: Double) = List(
    Cell(Position("sales", year + ".simple"), Content(ContinuousSchema[Double](), value1)),
    Cell(Position("sales", year + ".weighted"), Content(ContinuousSchema[Double](), value2))
  )
}

case class DeltaWithValue() extends WindowWithValue[_1, _1, _0, _1] {
  type V = Map[Position[_1], Content]
  type I = Option[Double]
  type T = Option[Double]
  type O = Double

  def prepareWithValue(cell: Cell[_1], ext: V): I = cell.content.value.asDouble

  def initialise(rem: Position[_0], in: I): (T, TraversableOnce[O]) = (in, List())

  def update(rem: Position[_0], in: I, t: T): (T, TraversableOnce[O]) = (in, (in, t) match {
   case (Some(dc), Some(dt)) => List(dc - dt)
   case _ => List()
 })

  def presentWithValue(pos: Position[_1], out: O, ext: V): TraversableOnce[Cell[_1]] = List(
    Cell(pos, Content(ContinuousSchema[Double](), ext(pos).value.asDouble.get * out))
  )
}

class TestWithPrepareWindow extends TestGrimlock {

  val str = Cell(Position("x"), getStringContent("foo"))
  val dbl = Cell(Position("y"), getDoubleContent(3.14))
  val lng = Cell(Position("z"), getLongContent(42))

  val ext = Map(
    Position("x") -> getDoubleContent(1),
    Position("y") -> getDoubleContent(2),
    Position("z") -> getDoubleContent(3)
  )

  def prepare(cell: Cell[_1]): Content = cell.content.value match {
    case l: LongValue => cell.content
    case d: DoubleValue => getStringContent("not.supported")
    case s: StringValue => getLongContent(s.value.length)
  }

  def prepareWithValue(cell: Cell[_1], ext: Map[Position[_1], Content]): Content =
    (cell.content.value, ext(cell.position).value) match {
      case (l: LongValue, d: DoubleValue) => getLongContent(l.value * d.value.toLong)
      case (d: DoubleValue, _) => getStringContent("not.supported")
      case (s: StringValue, _) => getLongContent(s.value.length)
    }

  val locate = (sel: Position[_1], rem: Position[_0]) => sel.toOption

  def getLongContent(value: Long): Content = Content(DiscreteSchema[Long](), value)
  def getDoubleContent(value: Double): Content = Content(ContinuousSchema[Double](), value)
  def getStringContent(value: String): Content = Content(NominalSchema[String](), value)

  "A Window" should "withPrepare prepare correctly" in {
    val obj = CumulativeMovingAverage[_1, _1, _0, _1](locate).withPrepare(prepare)

    obj.prepare(str) shouldBe 3.0
    obj.prepare(dbl).compare(Double.NaN) shouldBe 0
    obj.prepare(lng) shouldBe 42.0
  }

  it should "withPrepareWithValue correctly (without value)" in {
    val obj = DeltaWithValue().withPrepare(prepare)

    obj.prepareWithValue(str, ext) shouldBe Option(3.0)
    obj.prepareWithValue(dbl, ext) shouldBe None
    obj.prepareWithValue(lng, ext) shouldBe Option(42.0)
  }

  it should "withPrepareWithVaue correctly" in {
    val obj = DeltaWithValue().withPrepareWithValue(prepareWithValue)

    obj.prepareWithValue(str, ext) shouldBe Option(3.0)
    obj.prepareWithValue(dbl, ext) shouldBe None
    obj.prepareWithValue(lng, ext) shouldBe Option(3 * 42.0)
  }
}

class TestAndThenMutateWindow extends TestGrimlock {

  val str = Cell(Position("x"), getStringContent("foo"))
  val dbl = Cell(Position("y"), getDoubleContent(3.14))
  val lng = Cell(Position("z"), getLongContent(42))

  val ext = Map(
    Position("x") -> getDoubleContent(3),
    Position("y") -> getDoubleContent(2),
    Position("z") -> getDoubleContent(1)
  )

  def mutate(cell: Cell[_1]): Content = cell.position match {
    case Position(StringValue("x", _)) => cell.content
    case Position(StringValue("y", _)) => getStringContent("not.supported")
    case Position(StringValue("z", _)) => getLongContent(42)
  }

  def mutateWithValue(cell: Cell[_1], ext: Map[Position[_1], Content]): Content =
    (cell.position, ext(cell.position).value) match {
      case (Position(StringValue("x", _)), DoubleValue(d, _)) => cell.content
      case (Position(StringValue("y", _)), _) => getStringContent("not.supported")
      case (Position(StringValue("z", _)), DoubleValue(d, _)) => getLongContent(42)
    }

  val locate = (sel: Position[_1], rem: Position[_0]) => sel.toOption

  def getLongContent(value: Long): Content = Content(DiscreteSchema[Long](), value)
  def getDoubleContent(value: Double): Content = Content(ContinuousSchema[Double](), value)
  def getStringContent(value: String): Content = Content(NominalSchema[String](), value)

  "A Window" should "andThenMutate prepare correctly" in {
    val obj = CumulativeMovingAverage[_1, _1, _0, _1](locate).andThenMutate(mutate)

    obj.present(str.position, (Position(), 3.14)).toList shouldBe List(Cell(str.position, getDoubleContent(3.14)))
    obj.present(dbl.position, (Position(), 3.14)).toList shouldBe List(
      Cell(dbl.position, getStringContent("not.supported"))
    )
    obj.present(lng.position, (Position(), 3.14)).toList shouldBe List(Cell(lng.position, getLongContent(42)))
  }

  it should "andThenMutateWithValue correctly (without value)" in {
    val obj = DeltaWithValue().andThenMutate(mutate)

    obj.presentWithValue(str.position, 3.14, ext).toList shouldBe List(Cell(str.position, getDoubleContent(3 * 3.14)))
    obj.presentWithValue(dbl.position, 3.14, ext).toList shouldBe List(
      Cell(dbl.position, getStringContent("not.supported"))
    )
    obj.presentWithValue(lng.position, 3.14, ext).toList shouldBe List(Cell(lng.position, getLongContent(42)))
  }

  it should "andThenMutateWithVaue correctly" in {
    val obj = DeltaWithValue().andThenMutateWithValue(mutateWithValue)

    obj.presentWithValue(str.position, 3.14, ext).toList shouldBe List(Cell(str.position, getDoubleContent(3 * 3.14)))
    obj.presentWithValue(dbl.position, 3.14, ext).toList shouldBe List(
      Cell(dbl.position, getStringContent("not.supported"))
    )
    obj.presentWithValue(lng.position, 3.14, ext).toList shouldBe List(Cell(lng.position, getLongContent(42)))
  }
}

