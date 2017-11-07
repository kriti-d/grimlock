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

import commbank.grimlock.framework.encoding._
import commbank.grimlock.framework.environment.implicits._
import commbank.grimlock.framework.metadata._

import java.util.Date

class TestDateValue extends TestGrimlock {

  val dfmt = new java.text.SimpleDateFormat("dd/MM/yyyy")
  val date2001 = dfmt.parse("01/01/2001")
  val date2002 = dfmt.parse("01/01/2002")
  val dv2001 = DateValue(date2001, DateCodec("dd/MM/yyyy"))
  val dv2002 = DateValue(date2002, DateCodec("dd/MM/yyyy"))

  "A DateValue" should "return its short string" in {
    dv2001.toShortString shouldBe "01/01/2001"
  }

  it should "return a date" in {
    dv2001.as[Date] shouldBe Option(date2001)
    dv2001.as[Date] shouldBe Option(dfmt.parse("01/01/2001"))
  }

  it should "not return a string" in {
    dv2001.as[String] shouldBe None
  }

  it should "not return a double" in {
    dv2001.as[Double] shouldBe None
  }

  it should "not return a long" in {
    dv2001.as[Long] shouldBe None
  }

  it should "not return a int" in {
    dv2001.as[Int] shouldBe None
  }

  it should "not return a boolean" in {
    dv2001.as[Boolean] shouldBe None
  }

  it should "not return a type" in {
    dv2001.as[Type] shouldBe None
  }

  it should "equal itself" in {
    dv2001.equ(dv2001) shouldBe true
    dv2001.equ(DateValue(date2001, DateCodec("dd/MM/yyyy"))) shouldBe true
  }

  it should "not equal another date" in {
    dv2001.equ(dv2002) shouldBe false
  }

  it should "not equal another value" in {
    dv2001.equ("a") shouldBe false
    dv2001.equ(2) shouldBe false
    dv2001.equ(2.0) shouldBe false
    dv2001.equ(false) shouldBe false
  }

  it should "match a matching pattern" in {
    dv2001.like("^01.*".r) shouldBe true
  }

  it should "not match a non-existing pattern" in {
    dv2001.like("^02.*".r) shouldBe false
  }

  it should "identify a smaller value calling lss" in {
    dv2001.lss(dv2002) shouldBe true
  }

  it should "not identify an equal value calling lss" in {
    dv2001.lss(dv2001) shouldBe false
  }

  it should "not identify a greater value calling lss" in {
    dv2002.lss(dv2001) shouldBe false
  }

  it should "not identify another value calling lss" in {
    dv2001.lss(2) shouldBe false
  }

  it should "identify a smaller value calling leq" in {
    dv2001.leq(dv2002) shouldBe true
  }

  it should "identify an equal value calling leq" in {
    dv2001.leq(dv2001) shouldBe true
  }

  it should "not identify a greater value calling leq" in {
    dv2002.leq(dv2001) shouldBe false
  }

  it should "not identify another value calling leq" in {
    dv2001.leq(2) shouldBe false
  }

  it should "not identify a smaller value calling gtr" in {
    dv2001.gtr(dv2002) shouldBe false
  }

  it should "not identify an equal value calling gtr" in {
    dv2001.gtr(dv2001) shouldBe false
  }

  it should "identify a greater value calling gtr" in {
    dv2002.gtr(dv2001) shouldBe true
  }

  it should "not identify another value calling gtr" in {
    dv2001.gtr(2) shouldBe false
  }

  it should "not identify a smaller value calling geq" in {
    dv2001.geq(dv2002) shouldBe false
  }

  it should "identify an equal value calling geq" in {
    dv2001.geq(dv2001) shouldBe true
  }

  it should "identify a greater value calling geq" in {
    dv2002.geq(dv2001) shouldBe true
  }

  it should "not identify another value calling geq" in {
    dv2001.geq(2) shouldBe false
  }
}

class TestStringValue extends TestGrimlock {

  val foo = "foo"
  val bar = "bar"
  val dvfoo = StringValue(foo)
  val dvbar = StringValue(bar)

  "A StringValue" should "return its short string" in {
    dvfoo.toShortString shouldBe foo
  }

  it should "not return a date" in {
    dvfoo.as[Date] shouldBe None
  }

  it should "return a string" in {
    dvfoo.as[String] shouldBe Option(foo)
    dvfoo.as[String] shouldBe Option("foo")
  }

  it should "not return a double" in {
    dvfoo.as[Double] shouldBe None
  }

  it should "not return a long" in {
    dvfoo.as[Long] shouldBe None
  }

  it should "not return a int" in {
    dvfoo.as[Int] shouldBe None
  }

  it should "not return a boolean" in {
    dvfoo.as[Boolean] shouldBe None
  }

  it should "not return a type" in {
    dvfoo.as[Type] shouldBe None
  }

  it should "equal itself" in {
    dvfoo.equ(dvfoo) shouldBe true
    dvfoo.equ(StringValue(foo)) shouldBe true
  }

  it should "not equal another string" in {
    dvfoo.equ(dvbar) shouldBe false
  }

  it should "not equal another value" in {
    dvfoo.equ(2) shouldBe false
    dvfoo.equ(2.0) shouldBe false
    dvfoo.equ(false) shouldBe false
  }

  it should "match a matching pattern" in {
    dvfoo.like("^f..".r) shouldBe true
  }

  it should "not match a non-existing pattern" in {
    dvfoo.like("^b..".r) shouldBe false
  }

  it should "identify a smaller value calling lss" in {
    dvbar.lss(dvfoo) shouldBe true
  }

  it should "not identify an equal value calling lss" in {
    dvbar.lss(dvbar) shouldBe false
  }

  it should "not identify a greater value calling lss" in {
    dvfoo.lss(dvbar) shouldBe false
  }

  it should "not identify another value calling lss" in {
    dvbar.lss(2) shouldBe false
  }

  it should "identify a smaller value calling leq" in {
    dvbar.leq(dvfoo) shouldBe true
  }

  it should "identify an equal value calling leq" in {
    dvbar.leq(dvbar) shouldBe true
  }

  it should "not identify a greater value calling leq" in {
    dvfoo.leq(dvbar) shouldBe false
  }

  it should "not identify another value calling leq" in {
    dvbar.leq(2) shouldBe false
  }

  it should "not identify a smaller value calling gtr" in {
    dvbar.gtr(dvfoo) shouldBe false
  }

  it should "not identify an equal value calling gtr" in {
    dvbar.gtr(dvbar) shouldBe false
  }

  it should "identify a greater value calling gtr" in {
    dvfoo.gtr(dvbar) shouldBe true
  }

  it should "not identify another value calling gtr" in {
    dvbar.gtr(2) shouldBe false
  }

  it should "not identify a smaller value calling geq" in {
    dvbar.geq(dvfoo) shouldBe false
  }

  it should "identify an equal value calling geq" in {
    dvbar.geq(dvbar) shouldBe true
  }

  it should "identify a greater value calling geq" in {
    dvfoo.geq(dvbar) shouldBe true
  }

  it should "not identify another value calling geq" in {
    dvbar.geq(2) shouldBe false
  }
}

class TestDoubleValue extends TestGrimlock {

  val one = 1.0
  val pi = 3.14
  val dvone = DoubleValue(one)
  val dvpi = DoubleValue(pi)

  "A DoubleValue" should "return its short string" in {
    dvone.toShortString shouldBe "1.0"
  }

  it should "not return a date" in {
    dvone.as[Date] shouldBe None
  }

  it should "not return a string" in {
    dvone.as[String] shouldBe None
  }

  it should "return a double" in {
    dvone.as[Double] shouldBe Option(one)
    dvone.as[Double] shouldBe Option(1.0)
  }

  it should "not return a long" in {
    dvone.as[Long] shouldBe None
  }

  it should "not return a int" in {
    dvone.as[Int] shouldBe None
  }

  it should "not return a boolean" in {
    dvone.as[Boolean] shouldBe None
  }

  it should "not return a type" in {
    dvone.as[Type] shouldBe None
  }

  it should "equal itself" in {
    dvone.equ(dvone) shouldBe true
    dvone.equ(DoubleValue(one)) shouldBe true
  }

  it should "not equal another double" in {
    dvone.equ(dvpi) shouldBe false
  }

  it should "not equal another value" in {
    dvone.equ("a") shouldBe false
    dvone.equ(false) shouldBe false
  }

  it should "match a matching pattern" in {
    dvone.like("..0$".r) shouldBe true
  }

  it should "not match a non-existing pattern" in {
    dvone.like("^3...".r) shouldBe false
  }

  it should "identify a smaller value calling lss" in {
    dvone.lss(dvpi) shouldBe true
  }

  it should "not identify an equal value calling lss" in {
    dvone.lss(dvone) shouldBe false
  }

  it should "not identify a greater value calling lss" in {
    dvpi.lss(dvone) shouldBe false
  }

  it should "not identify another value calling lss" in {
    dvone.lss("a") shouldBe false
  }

  it should "identify a smaller value calling leq" in {
    dvone.leq(dvpi) shouldBe true
  }

  it should "identify an equal value calling leq" in {
    dvone.leq(dvone) shouldBe true
  }

  it should "not identify a greater value calling leq" in {
    dvpi.leq(dvone) shouldBe false
  }

  it should "not identify another value calling leq" in {
    dvone.leq("a") shouldBe false
  }

  it should "not identify a smaller value calling gtr" in {
    dvone.gtr(dvpi) shouldBe false
  }

  it should "not identify an equal value calling gtr" in {
    dvone.gtr(dvpi) shouldBe false
  }

  it should "identify a greater value calling gtr" in {
    dvpi.gtr(dvone) shouldBe true
  }

  it should "not identify another value calling gtr" in {
    dvone.gtr("a") shouldBe false
  }

  it should "not identify a smaller value calling geq" in {
    dvone.geq(dvpi) shouldBe false
  }

  it should "identify an equal value calling geq" in {
    dvone.geq(dvone) shouldBe true
  }

  it should "identify a greater value calling geq" in {
    dvpi.geq(dvone) shouldBe true
  }

  it should "not identify another value calling geq" in {
    dvone.geq("a") shouldBe false
  }
}

class TestLongValue extends TestGrimlock {

  val one = 1
  val two = 2
  val dvone = LongValue(one)
  val dvtwo = LongValue(two)

  "A LongValue" should "return its short string" in {
    dvone.toShortString shouldBe "1"
  }

  it should "not return a date" in {
    dvone.as[Date] shouldBe None
  }

  it should "not return a string" in {
    dvone.as[String] shouldBe None
  }

  it should "return a double" in {
    dvone.as[Double] shouldBe Option(one)
    dvone.as[Double] shouldBe Option(1.0)
  }

  it should "return a long" in {
    dvone.as[Long] shouldBe Option(one)
    dvone.as[Long] shouldBe Option(1)
  }

  it should "not return a int" in {
    dvone.as[Int] shouldBe None
  }

  it should "not return a boolean" in {
    dvone.as[Boolean] shouldBe None
  }

  it should "not return a type" in {
    dvone.as[Type] shouldBe None
  }

  it should "equal itself" in {
    dvone.equ(dvone) shouldBe true
    dvone.equ(LongValue(one)) shouldBe true
  }

  it should "not equal another double" in {
    dvone.equ(dvtwo) shouldBe false
  }

  it should "not equal another value" in {
    dvone.equ("a") shouldBe false
    dvone.equ(false) shouldBe false
  }

  it should "match a matching pattern" in {
    dvone.like("^1$".r) shouldBe true
  }

  it should "not match a non-existing pattern" in {
    dvone.like("^3...".r) shouldBe false
  }

  it should "identify a smaller value calling lss" in {
    dvone.lss(dvtwo) shouldBe true
  }

  it should "not identify an equal value calling lss" in {
    dvone.lss(dvone) shouldBe false
  }

  it should "not identify a greater value calling lss" in {
    dvtwo.lss(dvone) shouldBe false
  }

  it should "not identify another value calling lss" in {
    dvone.lss("a") shouldBe false
  }

  it should "identify a smaller value calling leq" in {
    dvone.leq(dvtwo) shouldBe true
  }

  it should "identify an equal value calling leq" in {
    dvone.leq(dvone) shouldBe true
  }

  it should "not identify a greater value calling leq" in {
    dvtwo.leq(dvone) shouldBe false
  }

  it should "not identify another value calling leq" in {
    dvone.leq("a") shouldBe false
  }

  it should "not identify a smaller value calling gtr" in {
    dvone.gtr(dvtwo) shouldBe false
  }

  it should "not identify an equal value calling gtr" in {
    dvone.gtr(dvtwo) shouldBe false
  }

  it should "identify a greater value calling gtr" in {
    dvtwo.gtr(dvone) shouldBe true
  }

  it should "not identify another value calling gtr" in {
    dvone.gtr("a") shouldBe false
  }

  it should "not identify a smaller value calling geq" in {
    dvone.geq(dvtwo) shouldBe false
  }

  it should "identify an equal value calling geq" in {
    dvone.geq(dvone) shouldBe true
  }

  it should "identify a greater value calling geq" in {
    dvtwo.geq(dvone) shouldBe true
  }

  it should "not identify another value calling geq" in {
    dvone.geq("a") shouldBe false
  }
}

class TestIntValue extends TestGrimlock {

  val one = 1
  val two = 2
  val dvone = IntValue(one)
  val dvtwo = IntValue(two)

  "A IntValue" should "return its short string" in {
    dvone.toShortString shouldBe "1"
  }

  it should "not return a date" in {
    dvone.as[Date] shouldBe None
  }

  it should "not return a string" in {
    dvone.as[String] shouldBe None
  }

  it should "return a double" in {
    dvone.as[Double] shouldBe Option(one)
    dvone.as[Double] shouldBe Option(1.0)
  }

  it should "return a long" in {
    dvone.as[Long] shouldBe Option(one)
    dvone.as[Long] shouldBe Option(1)
  }

  it should "return a int" in {
    dvone.as[Int] shouldBe Option(one)
    dvone.as[Int] shouldBe Option(1)
  }

  it should "not return a boolean" in {
    dvone.as[Boolean] shouldBe None
  }

  it should "not return a type" in {
    dvone.as[Type] shouldBe None
  }

  it should "equal itself" in {
    dvone.equ(dvone) shouldBe true
    dvone.equ(IntValue(one)) shouldBe true
  }

  it should "not equal another double" in {
    dvone.equ(dvtwo) shouldBe false
  }

  it should "not equal another value" in {
    dvone.equ("a") shouldBe false
    dvone.equ(false) shouldBe false
  }

  it should "match a matching pattern" in {
    dvone.like("^1$".r) shouldBe true
  }

  it should "not match a non-existing pattern" in {
    dvone.like("^3...".r) shouldBe false
  }

  it should "identify a smaller value calling lss" in {
    dvone.lss(dvtwo) shouldBe true
  }

  it should "not identify an equal value calling lss" in {
    dvone.lss(dvone) shouldBe false
  }

  it should "not identify a greater value calling lss" in {
    dvtwo.lss(dvone) shouldBe false
  }

  it should "not identify another value calling lss" in {
    dvone.lss("a") shouldBe false
  }

  it should "identify a smaller value calling leq" in {
    dvone.leq(dvtwo) shouldBe true
  }

  it should "identify an equal value calling leq" in {
    dvone.leq(dvone) shouldBe true
  }

  it should "not identify a greater value calling leq" in {
    dvtwo.leq(dvone) shouldBe false
  }

  it should "not identify another value calling leq" in {
    dvone.leq("a") shouldBe false
  }

  it should "not identify a smaller value calling gtr" in {
    dvone.gtr(dvtwo) shouldBe false
  }

  it should "not identify an equal value calling gtr" in {
    dvone.gtr(dvtwo) shouldBe false
  }

  it should "identify a greater value calling gtr" in {
    dvtwo.gtr(dvone) shouldBe true
  }

  it should "not identify another value calling gtr" in {
    dvone.gtr("a") shouldBe false
  }

  it should "not identify a smaller value calling geq" in {
    dvone.geq(dvtwo) shouldBe false
  }

  it should "identify an equal value calling geq" in {
    dvone.geq(dvone) shouldBe true
  }

  it should "identify a greater value calling geq" in {
    dvtwo.geq(dvone) shouldBe true
  }

  it should "not identify another value calling geq" in {
    dvone.geq("a") shouldBe false
  }
}

class TestBooleanValue extends TestGrimlock {

  val pos = true
  val neg = false
  val dvpos = BooleanValue(pos)
  val dvneg = BooleanValue(neg)

  "A BooleanValue" should "return its short string" in {
    dvpos.toShortString shouldBe "true"
  }

  it should "not return a date" in {
    dvpos.as[Date] shouldBe None
  }

  it should "not return a string" in {
    dvpos.as[String] shouldBe None
  }

  it should "return a double" in {
    dvpos.as[Double] shouldBe Option(1.0)
    dvneg.as[Double] shouldBe Option(0.0)
  }

  it should "return a long" in {
    dvpos.as[Long] shouldBe Option(1)
    dvneg.as[Long] shouldBe Option(0)
  }

  it should "return a int" in {
    dvpos.as[Int] shouldBe Option(1)
    dvneg.as[Int] shouldBe Option(0)
  }

  it should "return a boolean" in {
    dvpos.as[Boolean] shouldBe Option(pos)
    dvpos.as[Boolean] shouldBe Option(true)
  }

  it should "not return a type" in {
    dvpos.as[Type] shouldBe None
  }

  it should "equal itself" in {
    dvpos.equ(dvpos) shouldBe true
    dvpos.equ(BooleanValue(pos)) shouldBe true
  }

  it should "not equal another boolean" in {
    dvpos.equ(dvneg) shouldBe false
  }

  it should "not equal another value" in {
    dvpos.equ("a") shouldBe false
    dvpos.equ(2) shouldBe false
    dvpos.equ(2.0) shouldBe false
  }

  it should "match a matching pattern" in {
    dvpos.like("^tr..".r) shouldBe true
  }

  it should "not match a non-existing pattern" in {
    dvpos.like("^fal..".r) shouldBe false
  }

  it should "identify a smaller value calling lss" in {
    dvneg.lss(dvpos) shouldBe true
  }

  it should "not identify an equal value calling lss" in {
    dvneg.lss(dvneg) shouldBe false
  }

  it should "not identify a greater value calling lss" in {
    dvpos.lss(dvneg) shouldBe false
  }

  it should "not identify another value calling lss" in {
    dvneg.lss(2) shouldBe false
  }

  it should "identify a smaller value calling leq" in {
    dvneg.leq(dvpos) shouldBe true
  }

  it should "identify an equal value calling leq" in {
    dvneg.leq(dvneg) shouldBe true
  }

  it should "not identify a greater value calling leq" in {
    dvpos.leq(dvneg) shouldBe false
  }

  it should "not identify another value calling leq" in {
    dvneg.leq(2) shouldBe false
  }

  it should "not identify a smaller value calling gtr" in {
    dvneg.gtr(dvpos) shouldBe false
  }

  it should "not identify an equal value calling gtr" in {
    dvneg.gtr(dvneg) shouldBe false
  }

  it should "identify a greater value calling gtr" in {
    dvpos.gtr(dvneg) shouldBe true
  }

  it should "not identify another value calling gtr" in {
    dvneg.gtr(2) shouldBe false
  }

  it should "not identify a smaller value calling geq" in {
    dvneg.geq(dvpos) shouldBe false
  }

  it should "identify an equal value calling geq" in {
    dvneg.geq(dvneg) shouldBe true
  }

  it should "identify a greater value calling geq" in {
    dvpos.geq(dvneg) shouldBe true
  }

  it should "not identify another value calling geq" in {
    dvneg.geq(2) shouldBe false
  }
}

class TestTypeValue extends TestGrimlock {

  val mix = MixedType
  val con = ContinuousType
  val dvmix = TypeValue(mix)
  val dvcon = TypeValue(con)

  "A TypeValue" should "return its short string" in {
    dvmix.toShortString shouldBe "mixed"
  }

  it should "not return a date" in {
    dvmix.as[Date] shouldBe None
  }

  it should "not return a string" in {
    dvmix.as[String] shouldBe None
  }

  it should "not return a double" in {
    dvmix.as[Double] shouldBe None
  }

  it should "not return a long" in {
    dvmix.as[Long] shouldBe None
  }

  it should "not return a int" in {
    dvmix.as[Int] shouldBe None
  }

  it should "not return a boolean" in {
    dvmix.as[Boolean] shouldBe None
  }

  it should "return a type" in {
    dvmix.as[Type] shouldBe Option(MixedType)
  }

  it should "equal itself" in {
    dvmix.equ(dvmix) shouldBe true
    dvmix.equ(TypeValue(mix)) shouldBe true
  }

  it should "not equal another boolean" in {
    dvmix.equ(dvcon) shouldBe false
  }

  it should "not equal another value" in {
    dvmix.equ("a") shouldBe false
    dvmix.equ(2) shouldBe false
    dvmix.equ(2.0) shouldBe false
  }

  it should "match a matching pattern" in {
    dvmix.like("^m.*d$".r) shouldBe true
  }

  it should "not match a non-existing pattern" in {
    dvmix.like("^c.*".r) shouldBe false
  }

  it should "identify a smaller value calling lss" in {
    dvcon.lss(dvmix) shouldBe true
  }

  it should "not identify an equal value calling lss" in {
    dvcon.lss(dvcon) shouldBe false
  }

  it should "not identify a greater value calling lss" in {
    dvmix.lss(dvcon) shouldBe false
  }

  it should "not identify another value calling lss" in {
    dvcon.lss(2) shouldBe false
  }

  it should "identify a smaller value calling leq" in {
    dvcon.leq(dvmix) shouldBe true
  }

  it should "identify an equal value calling leq" in {
    dvcon.leq(dvcon) shouldBe true
  }

  it should "not identify a greater value calling leq" in {
    dvmix.leq(dvcon) shouldBe false
  }

  it should "not identify another value calling leq" in {
    dvcon.leq(2) shouldBe false
  }

  it should "not identify a smaller value calling gtr" in {
    dvcon.gtr(dvmix) shouldBe false
  }

  it should "not identify an equal value calling gtr" in {
    dvcon.gtr(dvcon) shouldBe false
  }

  it should "identify a greater value calling gtr" in {
    dvmix.gtr(dvcon) shouldBe true
  }

  it should "not identify another value calling gtr" in {
    dvcon.gtr(2) shouldBe false
  }

  it should "not identify a smaller value calling geq" in {
    dvcon.geq(dvmix) shouldBe false
  }

  it should "identify an equal value calling geq" in {
    dvcon.geq(dvcon) shouldBe true
  }

  it should "identify a greater value calling geq" in {
    dvmix.geq(dvcon) shouldBe true
  }

  it should "not identify another value calling geq" in {
    dvcon.geq(2) shouldBe false
  }
}

