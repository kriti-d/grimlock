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

class TestMixedType extends TestGrimlock {

  "A Mixed" should "return its short name" in {
    MixedType.toShortString shouldBe "mixed"
  }

  it should "return its name" in {
    MixedType.toString shouldBe "MixedType"
  }

  it should "return correct generalised type" in {
    MixedType.getRootType shouldBe MixedType
  }

  it should "match correct specialisation" in {
    MixedType.isOfType(MixedType) shouldBe true
  }

  it should "not match incorrect specialisation" in {
    MixedType.isOfType(NumericType) shouldBe false
    MixedType.isOfType(OrdinalType) shouldBe false
  }

  it should "return correct common type" in {
    MixedType.getCommonType(MixedType) shouldBe MixedType
    MixedType.getCommonType(NominalType) shouldBe MixedType
  }
}

class TestNumericType extends TestGrimlock {

  "A Numeric" should "return its short name" in {
    NumericType.toShortString shouldBe "numeric"
  }

  it should "return its name" in {
    NumericType.toString shouldBe "NumericType"
  }

  it should "return correct generalised type" in {
    NumericType.getRootType shouldBe NumericType
  }

  it should "match correct specialisation" in {
    NumericType.isOfType(NumericType) shouldBe true
  }

  it should "not match incorrect specialisation" in {
    NumericType.isOfType(MixedType) shouldBe false
    NumericType.isOfType(OrdinalType) shouldBe false
    NumericType.isOfType(ContinuousType) shouldBe false
  }

  it should "return correct common type" in {
    NumericType.getCommonType(MixedType) shouldBe MixedType
    NumericType.getCommonType(NominalType) shouldBe MixedType
    NumericType.getCommonType(NumericType) shouldBe NumericType
    NumericType.getCommonType(DiscreteType) shouldBe NumericType
  }
}

class TestContinuousType extends TestGrimlock {

  "A Continuous" should "return its short name" in {
    ContinuousType.toShortString shouldBe "continuous"
  }

  it should "return its name" in {
    ContinuousType.toString shouldBe "ContinuousType"
  }

  it should "return correct generalised type" in {
    ContinuousType.getRootType shouldBe NumericType
  }

  it should "match correct specialisation" in {
    ContinuousType.isOfType(ContinuousType) shouldBe true
    ContinuousType.isOfType(NumericType) shouldBe true
  }

  it should "not match incorrect specialisation" in {
    ContinuousType.isOfType(MixedType) shouldBe false
    ContinuousType.isOfType(OrdinalType) shouldBe false
  }

  it should "return correct common type" in {
    ContinuousType.getCommonType(MixedType) shouldBe MixedType
    ContinuousType.getCommonType(NominalType) shouldBe MixedType
    ContinuousType.getCommonType(NumericType) shouldBe NumericType
    ContinuousType.getCommonType(ContinuousType) shouldBe ContinuousType
    ContinuousType.getCommonType(DiscreteType) shouldBe NumericType
  }
}

class TestDiscreteType extends TestGrimlock {

  "A Discrete" should "return its short name" in {
    DiscreteType.toShortString shouldBe "discrete"
  }

  it should "return its name" in {
    DiscreteType.toString shouldBe "DiscreteType"
  }

  it should "return correct generalised type" in {
    DiscreteType.getRootType shouldBe NumericType
  }

  it should "match correct specialisation" in {
    DiscreteType.isOfType(DiscreteType) shouldBe true
    DiscreteType.isOfType(NumericType) shouldBe true
  }

  it should "not match incorrect specialisation" in {
    DiscreteType.isOfType(MixedType) shouldBe false
    DiscreteType.isOfType(OrdinalType) shouldBe false
  }

  it should "return correct common type" in {
    DiscreteType.getCommonType(MixedType) shouldBe MixedType
    DiscreteType.getCommonType(NominalType) shouldBe MixedType
    DiscreteType.getCommonType(NumericType) shouldBe NumericType
    DiscreteType.getCommonType(ContinuousType) shouldBe NumericType
    DiscreteType.getCommonType(DiscreteType) shouldBe DiscreteType
  }
}

class TestCategoricalType extends TestGrimlock {

  "A Categorical" should "return its short name" in {
    CategoricalType.toShortString shouldBe "categorical"
  }

  it should "return its name" in {
    CategoricalType.toString shouldBe "CategoricalType"
  }

  it should "return correct generalised type" in {
    CategoricalType.getRootType shouldBe CategoricalType
  }

  it should "match correct specialisation" in {
    CategoricalType.isOfType(CategoricalType) shouldBe true
  }

  it should "not match incorrect specialisation" in {
    CategoricalType.isOfType(NumericType) shouldBe false
    CategoricalType.isOfType(DiscreteType) shouldBe false
    CategoricalType.isOfType(OrdinalType) shouldBe false
  }

  it should "return correct common type" in {
    CategoricalType.getCommonType(MixedType) shouldBe MixedType
    CategoricalType.getCommonType(CategoricalType) shouldBe CategoricalType
    CategoricalType.getCommonType(NominalType) shouldBe CategoricalType
    CategoricalType.getCommonType(ContinuousType) shouldBe MixedType
  }
}

class TestNominalType extends TestGrimlock {

  "A Nominal" should "return its short name" in {
    NominalType.toShortString shouldBe "nominal"
  }

  it should "return its name" in {
    NominalType.toString shouldBe "NominalType"
  }

  it should "return correct generalised type" in {
    NominalType.getRootType shouldBe CategoricalType
  }

  it should "match correct specialisation" in {
    NominalType.isOfType(NominalType) shouldBe true
    NominalType.isOfType(CategoricalType) shouldBe true
  }

  it should "not match incorrect specialisation" in {
    NominalType.isOfType(MixedType) shouldBe false
    NominalType.isOfType(DiscreteType) shouldBe false
  }

  it should "return correct common type" in {
    NominalType.getCommonType(MixedType) shouldBe MixedType
    NominalType.getCommonType(CategoricalType) shouldBe CategoricalType
    NominalType.getCommonType(NominalType) shouldBe NominalType
    NominalType.getCommonType(OrdinalType) shouldBe CategoricalType
    NominalType.getCommonType(ContinuousType) shouldBe MixedType
  }
}

class TestOrdinalType extends TestGrimlock {

  "A Ordinal" should "return its short name" in {
    OrdinalType.toShortString shouldBe "ordinal"
  }

  it should "return its name" in {
    OrdinalType.toString shouldBe "OrdinalType"
  }

  it should "return correct generalised type" in {
    OrdinalType.getRootType shouldBe CategoricalType
  }

  it should "match correct specialisation" in {
    OrdinalType.isOfType(OrdinalType) shouldBe true
    OrdinalType.isOfType(CategoricalType) shouldBe true
  }

  it should "not match incorrect specialisation" in {
    OrdinalType.isOfType(MixedType) shouldBe false
    OrdinalType.isOfType(DiscreteType) shouldBe false
  }

  it should "return correct common type" in {
    OrdinalType.getCommonType(MixedType) shouldBe MixedType
    OrdinalType.getCommonType(CategoricalType) shouldBe CategoricalType
    OrdinalType.getCommonType(NominalType) shouldBe CategoricalType
    OrdinalType.getCommonType(OrdinalType) shouldBe OrdinalType
    OrdinalType.getCommonType(ContinuousType) shouldBe MixedType
  }
}

class TestDateType extends TestGrimlock {

  "A Date" should "return its short name" in {
    DateType.toShortString shouldBe "date"
  }

  it should "return its name" in {
    DateType.toString shouldBe "DateType"
  }

  it should "return correct generalised type" in {
    DateType.getRootType shouldBe DateType
  }

  it should "match correct specialisation" in {
    DateType.isOfType(DateType) shouldBe true
  }

  it should "not match incorrect specialisation" in {
    DateType.isOfType(NumericType) shouldBe false
    DateType.isOfType(OrdinalType) shouldBe false
  }

  it should "return correct common type" in {
    DateType.getCommonType(MixedType) shouldBe MixedType
    DateType.getCommonType(DateType) shouldBe DateType
    DateType.getCommonType(NominalType) shouldBe MixedType
  }
}

class TestEventType extends TestGrimlock {

  "A Structured" should "return its short name" in {
    StructuredType.toShortString shouldBe "structured"
  }

  it should "return its name" in {
    StructuredType.toString shouldBe "StructuredType"
  }

  it should "return correct generalised type" in {
    StructuredType.getRootType shouldBe StructuredType
  }

  it should "match correct specialisation" in {
    StructuredType.isOfType(StructuredType) shouldBe true
  }

  it should "not match incorrect specialisation" in {
    StructuredType.isOfType(NumericType) shouldBe false
    StructuredType.isOfType(OrdinalType) shouldBe false
  }

  it should "return correct common type" in {
    StructuredType.getCommonType(MixedType) shouldBe MixedType
    StructuredType.getCommonType(StructuredType) shouldBe StructuredType
    StructuredType.getCommonType(NominalType) shouldBe MixedType
  }
}

