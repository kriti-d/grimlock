// Copyright 2015,2016 Commonwealth Bank of Australia
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

package commbank.grimlock

import commbank.grimlock.framework.Type._

class TestMixedType extends TestGrimlock {

  "A Mixed" should "return its short name" in {
    Mixed.toShortString shouldBe "mixed"
  }

  it should "return its name" in {
    Mixed.toString shouldBe "Mixed"
  }

  it should "return correct generalised type" in {
    Mixed.getGeneralisation shouldBe Mixed
  }

  it should "match correct specialisation" in {
    Mixed.isSpecialisationOf(Mixed) shouldBe true
  }

  it should "not match incorrect specialisation" in {
    Mixed.isSpecialisationOf(Numerical) shouldBe false
  }
}

class TestNumericalType extends TestGrimlock {

  "A Numerical" should "return its short name" in {
    Numerical.toShortString shouldBe "numerical"
  }

  it should "return its name" in {
    Numerical.toString shouldBe "Numerical"
  }

  it should "return correct generalised type" in {
    Numerical.getGeneralisation shouldBe Numerical
  }

  it should "match correct specialisation" in {
    Numerical.isSpecialisationOf(Numerical) shouldBe true
  }

  it should "not match incorrect specialisation" in {
    Numerical.isSpecialisationOf(Continuous) shouldBe false
  }
}

class TestContinuousType extends TestGrimlock {

  "A Continuous" should "return its short name" in {
    Continuous.toShortString shouldBe "continuous"
  }

  it should "return its name" in {
    Continuous.toString shouldBe "Continuous"
  }

  it should "return correct generalised type" in {
    Continuous.getGeneralisation shouldBe Numerical
  }

  it should "match correct specialisation" in {
    Continuous.isSpecialisationOf(Continuous) shouldBe true
    Continuous.isSpecialisationOf(Numerical) shouldBe true
  }

  it should "not match incorrect specialisation" in {
    Continuous.isSpecialisationOf(Mixed) shouldBe false
  }
}

class TestDiscreteType extends TestGrimlock {

  "A Discrete" should "return its short name" in {
    Discrete.toShortString shouldBe "discrete"
  }

  it should "return its name" in {
    Discrete.toString shouldBe "Discrete"
  }

  it should "return correct generalised type" in {
    Discrete.getGeneralisation shouldBe Numerical
  }

  it should "match correct specialisation" in {
    Discrete.isSpecialisationOf(Discrete) shouldBe true
    Discrete.isSpecialisationOf(Numerical) shouldBe true
  }

  it should "not match incorrect specialisation" in {
    Discrete.isSpecialisationOf(Mixed) shouldBe false
  }
}

class TestCategoricalType extends TestGrimlock {

  "A Categorical" should "return its short name" in {
    Categorical.toShortString shouldBe "categorical"
  }

  it should "return its name" in {
    Categorical.toString shouldBe "Categorical"
  }

  it should "return correct generalised type" in {
    Categorical.getGeneralisation shouldBe Categorical
  }

  it should "match correct specialisation" in {
    Categorical.isSpecialisationOf(Categorical) shouldBe true
  }

  it should "not match incorrect specialisation" in {
    Categorical.isSpecialisationOf(Continuous) shouldBe false
  }
}

class TestNominalType extends TestGrimlock {

  "A Nominal" should "return its short name" in {
    Nominal.toShortString shouldBe "nominal"
  }

  it should "return its name" in {
    Nominal.toString shouldBe "Nominal"
  }

  it should "return correct generalised type" in {
    Nominal.getGeneralisation shouldBe Categorical
  }

  it should "match correct specialisation" in {
    Nominal.isSpecialisationOf(Nominal) shouldBe true
    Nominal.isSpecialisationOf(Categorical) shouldBe true
  }

  it should "not match incorrect specialisation" in {
    Nominal.isSpecialisationOf(Mixed) shouldBe false
  }
}

class TestOrdinalType extends TestGrimlock {

  "A Ordinal" should "return its short name" in {
    Ordinal.toShortString shouldBe "ordinal"
  }

  it should "return its name" in {
    Ordinal.toString shouldBe "Ordinal"
  }

  it should "return correct generalised type" in {
    Ordinal.getGeneralisation shouldBe Categorical
  }

  it should "match correct specialisation" in {
    Ordinal.isSpecialisationOf(Ordinal) shouldBe true
    Ordinal.isSpecialisationOf(Categorical) shouldBe true
  }

  it should "not match incorrect specialisation" in {
    Ordinal.isSpecialisationOf(Mixed) shouldBe false
  }
}

class TestDateType extends TestGrimlock {

  "A Date" should "return its short name" in {
    Date.toShortString shouldBe "date"
  }

  it should "return its name" in {
    Date.toString shouldBe "Date"
  }

  it should "return correct generalised type" in {
    Date.getGeneralisation shouldBe Date
  }

  it should "match correct specialisation" in {
    Date.isSpecialisationOf(Date) shouldBe true
  }

  it should "not match incorrect specialisation" in {
    Date.isSpecialisationOf(Numerical) shouldBe false
  }
}

class TestEventType extends TestGrimlock {

  "A Structured" should "return its short name" in {
    Structured.toShortString shouldBe "structured"
  }

  it should "return its name" in {
    Structured.toString shouldBe "Structured"
  }

  it should "return correct generalised type" in {
    Structured.getGeneralisation shouldBe Structured
  }

  it should "match correct specialisation" in {
    Structured.isSpecialisationOf(Structured) shouldBe true
  }

  it should "not match incorrect specialisation" in {
    Structured.isSpecialisationOf(Numerical) shouldBe false
  }
}

