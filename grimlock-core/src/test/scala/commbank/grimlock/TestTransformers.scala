// Copyright 2015,2016,2017,2018,2019 Commonwealth Bank of Australia
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
import commbank.grimlock.framework.extract._
import commbank.grimlock.framework.metadata._
import commbank.grimlock.framework.position._

import commbank.grimlock.library.transform._

import shapeless.{ ::, HList, HNil, Nat }
import shapeless.nat.{ _0, _1, _2 }

trait TestTransformers extends TestGrimlock {
  def getLongContent(value: Long): Content = Content(DiscreteSchema[Long](), value)
  def getDoubleContent(value: Double): Content = Content(ContinuousSchema[Double](), value)
  def getStringContent(value: String): Content = Content(NominalSchema[String](), value)

  def extractor[
    P <: HList,
    D <: Nat,
    V <: Value[_]
  ](
    dim: D,
    key: String
  )(implicit
    ev: Position.IndexConstraints.Aux[P, D, V]
  ): Extract[P, Map[Position[V :: HNil], Map[Position[Value[String] :: HNil], Content]], Double] =
    ExtractWithDimensionAndKey[P, D, V, String, Content](dim, key).andThenPresent(_.value.as[Double])

  def dimExtractor[
    P <: HList,
    D <: Nat,
    V <: Value[_]
  ](
    dim: D
  )(implicit
    ev: Position.IndexConstraints.Aux[P, D, V]
  ): Extract[P, Map[Position[V :: HNil], Content], Double] =
    ExtractWithDimension[P, D, V, Content](dim).andThenPresent(_.value.as[Double])

  def keyExtractor[P <: HList](key: String): Extract[P, Map[Position[Value[String] :: HNil], Content], Double] =
    ExtractWithKey[P, String, Content](key).andThenPresent(_.value.as[Double])

  def locate(s: String = "=") = (v: Value[String], c: Value[_]) => v.toShortString + s + c.toShortString
}

class TestIndicator extends TestTransformers {
  type P = Value[String] :: Value[String] :: HNil

  val cell = Cell(Position("foo", "bar"), getDoubleContent(3.1415))

  "An Indicator" should "present" in {
    Indicator().present(cell) shouldBe List(Cell(Position("foo", "bar"), getLongContent(1)))
  }

  it should "present with name" in {
    Indicator[P]()
      .andThenRelocate(Locate.RenameDimension(_0, (v: Value[String]) => v.toShortString + ".ind"))
      .present(cell).toList shouldBe List(Cell(Position("foo.ind", "bar"), getLongContent(1)))
    Indicator[P]()
      .andThenRelocate(Locate.RenameDimension(_1, (v: Value[String]) => v.toShortString + ".ind"))
      .present(cell).toList shouldBe List(Cell(Position("foo", "bar.ind"), getLongContent(1)))
  }
}

class TestBinarise extends TestTransformers {
  type P = Value[String] :: Value[String] :: HNil
  type Q = Value[String] :: Value[String] :: HNil

  val cell = Cell(Position("foo", "bar"), getStringContent("rules"))

  "A Binarise" should "present" in {
    Binarise[P, Q](Locate.RenameDimensionWithContent(_0, locate()))
      .present(cell) shouldBe List(Cell(Position("foo=rules", "bar"), getLongContent(1)))
    Binarise[P, Q](Locate.RenameDimensionWithContent(_1, locate()))
      .present(cell) shouldBe List(Cell(Position("foo", "bar=rules"), getLongContent(1)))
  }

  it should "present with name" in {
    Binarise[P, Q](Locate.RenameDimensionWithContent(_0, locate(".")))
      .present(cell) shouldBe List(Cell(Position("foo.rules", "bar"), getLongContent(1)))
    Binarise[P, Q](Locate.RenameDimensionWithContent(_1, locate(".")))
      .present(cell) shouldBe List(Cell(Position("foo", "bar.rules"), getLongContent(1)))
  }

  it should "not present a numerical" in {
    Binarise[P, Q](Locate.RenameDimensionWithContent(_0, locate()))
      .present(Cell(Position("foo", "bar"), getDoubleContent(3.1415))) shouldBe List()
  }
}

class TestNormalise extends TestTransformers {
  type P = Value[String] :: Value[String] :: HNil
  type W = Map[Position[Value[String] :: HNil], Map[Position[Value[String] :: HNil], Content]]

  val cell = Cell(Position("foo", "bar"), getDoubleContent(3.1415))
  val ext = Map(
    Position("foo") -> Map(Position("const") -> getDoubleContent(6.283)),
    Position("bar") -> Map(Position("const") -> getDoubleContent(-1.57075))
  )

  "An Normalise" should "present" in {
    Normalise[P, W](extractor(_0, "const"))
      .presentWithValue(cell, ext) shouldBe List(Cell(Position("foo", "bar"), getDoubleContent(0.5)))
    Normalise[P, W](extractor(_1, "const"))
      .presentWithValue(cell, ext) shouldBe List(Cell(Position("foo", "bar"), getDoubleContent(-2)))
  }

  it should "present with name" in {
    Normalise[P, W](extractor(_0, "const"))
      .andThenRelocate(Locate.RenameDimension(_0, (v: Value[String]) => v.toShortString + ".norm"))
      .presentWithValue(cell, ext).toList shouldBe List(Cell(Position("foo.norm", "bar"), getDoubleContent(0.5)))
    Normalise[P, W](extractor(_1, "const"))
      .andThenRelocate(Locate.RenameDimension(_1, (v: Value[String]) => v.toShortString + ".norm"))
      .presentWithValue(cell, ext).toList shouldBe List(Cell(Position("foo", "bar.norm"), getDoubleContent(-2)))
  }

  it should "not present with missing key" in {
    Normalise[P, W](extractor(_0, "not.there")).presentWithValue(cell, ext) shouldBe List()
  }

  it should "not present with missing value" in {
    Normalise[Value[String] :: HNil, W](extractor(_0, "const"))
      .presentWithValue(Cell(Position("baz"), getDoubleContent(3.1415)), ext) shouldBe List()
  }

  it should "not present with a categorical" in {
    Normalise[Value[String] :: HNil, W](extractor(_0, "const"))
      .presentWithValue(Cell(Position("foo"), getStringContent("bar")), ext) shouldBe List()
  }
}

class TestStandardise extends TestTransformers {
  type P = Value[String] :: Value[String] :: HNil
  type W = Map[Position[Value[String] :: HNil], Map[Position[Value[String] :: HNil], Content]]

  val cell = Cell(Position("foo", "bar"), getDoubleContent(3.1415))
  val ext = Map(
    Position("foo") -> Map(Position("mean") -> getDoubleContent(0.75), Position("sd") -> getDoubleContent(1.25)),
    Position("bar") -> Map(Position("mean") -> getDoubleContent(-0.75), Position("sd") -> getDoubleContent(0.75))
  )

  "A Standardise" should "present" in {
    Standardise[P, W](extractor(_0, "mean"), extractor(_0, "sd"))
      .presentWithValue(cell, ext) shouldBe List(Cell(Position("foo", "bar"), getDoubleContent((3.1415 - 0.75) / 1.25)))
    Standardise[P, W](extractor(_1, "mean"), extractor(_1, "sd"))
      .presentWithValue(cell, ext) shouldBe List(Cell(Position("foo", "bar"), getDoubleContent((3.1415 + 0.75) / 0.75)))
  }

  it should "present with name" in {
    Standardise[P, W](extractor(_0, "mean"), extractor(_0, "sd"))
      .andThenRelocate(Locate.RenameDimension(_0, (v: Value[String]) => v.toShortString + ".std"))
      .presentWithValue(cell, ext).toList shouldBe List(
        Cell(Position("foo.std", "bar"), getDoubleContent((3.1415 - 0.75) / 1.25))
      )
    Standardise[P, W](extractor(_1, "mean"), extractor(_1, "sd"))
      .andThenRelocate(Locate.RenameDimension(_1, (v: Value[String]) => v.toShortString + ".std"))
      .presentWithValue(cell, ext).toList shouldBe List(
        Cell(Position("foo", "bar.std"), getDoubleContent((3.1415 + 0.75) / 0.75))
      )
  }

  it should "present with threshold" in {
    Standardise[P, W](extractor(_0, "mean"), extractor(_0, "sd"), 1.0)
      .presentWithValue(cell, ext) shouldBe List(Cell(Position("foo", "bar"), getDoubleContent((3.1415 - 0.75) / 1.25)))
    Standardise[P, W](extractor(_1, "mean"), extractor(_1, "sd"), 1.0)
      .presentWithValue(cell, ext) shouldBe List(Cell(Position("foo", "bar"), getDoubleContent(0)))
  }

  it should "present with N" in {
    Standardise[P, W](extractor(_0, "mean"), extractor(_0, "sd"), n=2)
      .presentWithValue(cell, ext) shouldBe List(
        Cell(Position("foo", "bar"), getDoubleContent((3.1415 - 0.75) / (2 * 1.25)))
      )
    Standardise[P, W](extractor(_1, "mean"), extractor(_1, "sd"), n=2)
      .presentWithValue(cell, ext) shouldBe List(
        Cell(Position("foo", "bar"), getDoubleContent((3.1415 + 0.75) / (2 * 0.75)))
      )
  }

  it should "present with name and threshold" in {
    Standardise[P, W](extractor(_0, "mean"), extractor(_0, "sd"), 1.0)
      .andThenRelocate(Locate.RenameDimension(_0, (v: Value[String]) => v.toShortString + ".std"))
      .presentWithValue(cell, ext).toList shouldBe List(
        Cell(Position("foo.std", "bar"), getDoubleContent((3.1415 - 0.75) / 1.25))
      )
    Standardise[P, W](extractor(_1, "mean"), extractor(_1, "sd"), 1.0)
      .andThenRelocate(Locate.RenameDimension(_1, (v: Value[String]) => v.toShortString + ".std"))
      .presentWithValue(cell, ext).toList shouldBe List(Cell(Position("foo", "bar.std"), getDoubleContent(0)))
  }

  it should "present with name and N" in {
    Standardise[P, W](extractor(_0, "mean"), extractor(_0, "sd"), n=2)
      .andThenRelocate(Locate.RenameDimension(_0, (v: Value[String]) => v.toShortString + ".std"))
      .presentWithValue(cell, ext).toList shouldBe List(
        Cell(Position("foo.std", "bar"), getDoubleContent((3.1415 - 0.75) / (2 * 1.25)))
      )
    Standardise[P, W](extractor(_1, "mean"), extractor(_1, "sd"), n=2)
      .andThenRelocate(Locate.RenameDimension(_1, (v: Value[String]) => v.toShortString + ".std"))
      .presentWithValue(cell, ext).toList shouldBe List(
        Cell(Position("foo", "bar.std"), getDoubleContent((3.1415 + 0.75) / (2 * 0.75)))
      )
  }

  it should "present with threshold and N" in {
    Standardise[P, W](extractor(_0, "mean"), extractor(_0, "sd"), 1.0, 2)
      .presentWithValue(cell, ext) shouldBe List(
        Cell(Position("foo", "bar"), getDoubleContent((3.1415 - 0.75) / (2 * 1.25)))
      )
    Standardise[P, W](extractor(_1, "mean"), extractor(_1, "sd"), 1.0, 2)
      .presentWithValue(cell, ext) shouldBe List(Cell(Position("foo", "bar"), getDoubleContent(0)))
  }

  it should "present with name, threshold and N" in {
    Standardise[P, W](extractor(_0, "mean"), extractor(_0, "sd"), 1.0, 2)
      .andThenRelocate(Locate.RenameDimension(_0, (v: Value[String]) => v.toShortString + ".std"))
      .presentWithValue(cell, ext).toList shouldBe List(
        Cell(Position("foo.std", "bar"), getDoubleContent((3.1415 - 0.75) / (2 * 1.25)))
      )
    Standardise[P, W](extractor(_1, "mean"), extractor(_1, "sd"), 1.0, 2)
      .andThenRelocate(Locate.RenameDimension(_1, (v: Value[String]) => v.toShortString + ".std"))
      .presentWithValue(cell, ext).toList shouldBe List(Cell(Position("foo", "bar.std"), getDoubleContent(0)))
  }

  it should "not present with missing key" in {
    Standardise[P, W](extractor(_0, "not.there"), extractor(_0, "sd"))
      .presentWithValue(cell, ext) shouldBe List()
    Standardise[P, W](extractor(_0, "mean"), extractor(_0, "not.there"))
      .presentWithValue(cell, ext) shouldBe List()
  }

  it should "not present with missing value" in {
    Standardise[Value[String] :: HNil, W](extractor(_0, "mean"), extractor(_0, "sd"))
      .presentWithValue(Cell(Position("baz"), getDoubleContent(3.1415)), ext) shouldBe List()
  }

  it should "not present with a categorical" in {
    Standardise[Value[String] :: HNil, W](extractor(_0, "mean"), extractor(_0, "sd"))
      .presentWithValue(Cell(Position("foo"), getStringContent("bar")), ext) shouldBe List()
  }
}

class TestClamp extends TestTransformers {
  type P = Value[String] :: Value[String] :: Value[String] :: HNil
  type W = Map[Position[Value[String] :: HNil], Map[Position[Value[String] :: HNil], Content]]

  val cell = Cell(Position("foo", "bar", "baz"), getDoubleContent(3.1415))
  val ext = Map(
    Position("foo") -> Map(Position("min") -> getDoubleContent(0), Position("max") -> getDoubleContent(6.283)),
    Position("bar") -> Map(Position("min") -> getDoubleContent(0), Position("max") -> getDoubleContent(1.57075)),
    Position("baz") -> Map(Position("min") -> getDoubleContent(4.71225), Position("max") -> getDoubleContent(6.283))
  )

  "A Clamp" should "present" in {
    Clamp[P, W](extractor(_0, "min"), extractor(_0, "max"))
      .presentWithValue(cell, ext) shouldBe List(Cell(Position("foo", "bar", "baz"), getDoubleContent(3.1415)))
    Clamp[P, W](extractor(_1, "min"), extractor(_1, "max"))
      .presentWithValue(cell, ext) shouldBe List(Cell(Position("foo", "bar", "baz"), getDoubleContent(1.57075)))
    Clamp[P, W](extractor(_2, "min"), extractor(_2, "max"))
      .presentWithValue(cell, ext) shouldBe List(Cell(Position("foo", "bar", "baz"), getDoubleContent(4.71225)))
  }

  it should "present with name" in {
    Clamp[P, W](extractor(_0, "min"), extractor(_0, "max"))
      .andThenRelocate(Locate.RenameDimension(_0, (v: Value[String]) => v.toShortString + ".std"))
      .presentWithValue(cell, ext).toList shouldBe List(
        Cell(Position("foo.std", "bar", "baz"), getDoubleContent(3.1415))
      )
    Clamp[P, W](extractor(_1, "min"), extractor(_1, "max"))
      .andThenRelocate(Locate.RenameDimension(_1, (v: Value[String]) => v.toShortString + ".std"))
      .presentWithValue(cell, ext).toList shouldBe List(
        Cell(Position("foo", "bar.std", "baz"), getDoubleContent(1.57075))
      )
    Clamp[P, W](extractor(_2, "min"), extractor(_2, "max"))
      .andThenRelocate(Locate.RenameDimension(_2, (v: Value[String]) => v.toShortString + ".std"))
      .presentWithValue(cell, ext).toList shouldBe List(
        Cell(Position("foo", "bar", "baz.std"), getDoubleContent(4.71225))
      )
  }

  it should "not present with missing key" in {
    Clamp[P, W](extractor(_0, "not.there"), extractor(_0, "max"))
      .presentWithValue(cell, ext) shouldBe List()
    Clamp[P, W](extractor(_0, "min"), extractor(_0, "not.there"))
      .presentWithValue(cell, ext) shouldBe List()
  }

  it should "not present with missing value" in {
    Clamp[Value[String] :: HNil, W](extractor(_0, "min"), extractor(_0, "max"))
      .presentWithValue(Cell(Position("abc"), getDoubleContent(3.1415)), ext) shouldBe List()
  }

  it should "not present with a categorical" in {
    Clamp[Value[String] :: HNil, W](extractor(_0, "min"), extractor(_0, "max"))
      .presentWithValue(Cell(Position("foo"), getStringContent("bar")), ext) shouldBe List()
  }
}

class TestIdf extends TestTransformers {
  type P = Value[String] :: HNil
  type W = Map[Position[Value[String] :: HNil], Content]

  val cell = Cell(Position("foo"), getLongContent(1))
  val ext = Map(Position("foo") -> getLongContent(2), Position("bar") -> getLongContent(2))

  "An Idf" should "present" in {
    Idf[P, W](dimExtractor(_0))
      .presentWithValue(cell, ext) shouldBe List(Cell(Position("foo"), getDoubleContent(0)))
  }

  it should "present with name" in {
    Idf[P, W](dimExtractor(_0))
      .andThenRelocate(Locate.RenameDimension(_0, (v: Value[String]) => v.toShortString + ".idf"))
      .presentWithValue(cell, ext).toList shouldBe List(Cell(Position("foo.idf"), getDoubleContent(0)))
  }

  it should "present with key" in {
    Idf[P, W](keyExtractor("bar"))
      .presentWithValue(cell, ext) shouldBe List(Cell(Position("foo"), getDoubleContent(0)))
  }

  it should "present with function" in {
    Idf[P, W](dimExtractor(_0), (df, n) => math.log10(n / df))
      .presentWithValue(cell, ext) shouldBe List(Cell(Position("foo"), getDoubleContent(math.log10(2))))
  }

  it should "present with name and key" in {
    Idf[P, W](keyExtractor("bar"))
      .andThenRelocate(Locate.RenameDimension(_0, (v: Value[String]) => v.toShortString + ".idf"))
      .presentWithValue(cell, ext).toList shouldBe List(Cell(Position("foo.idf"), getDoubleContent(0)))
  }

  it should "present with name and function" in {
    Idf[P, W](dimExtractor(_0), (df, n) => math.log10(n / df))
      .andThenRelocate(Locate.RenameDimension(_0, (v: Value[String]) => v.toShortString + ".idf"))
      .presentWithValue(cell, ext).toList shouldBe List(Cell(Position("foo.idf"), getDoubleContent(math.log10(2))))
  }

  it should "present with key and function" in {
    Idf[P, W](keyExtractor("bar"), (df, n) => math.log10(n / df))
      .presentWithValue(cell, ext) shouldBe List(Cell(Position("foo"), getDoubleContent(math.log10(2))))
  }

  it should "present with name, key and function" in {
    Idf[P, W](keyExtractor("bar"), (df, n) => math.log10(n / df))
      .andThenRelocate(Locate.RenameDimension(_0, (v: Value[String]) => v.toShortString + ".idf"))
      .presentWithValue(cell, ext).toList shouldBe List(Cell(Position("foo.idf"), getDoubleContent(math.log10(2))))
  }

  it should "not present with missing key" in {
    Idf[P, W](keyExtractor("not.there")).presentWithValue(cell, ext) shouldBe List()
  }

  it should "not present with missing value" in {
    Idf[P, W](dimExtractor(_0)).presentWithValue(Cell(Position("abc"), getLongContent(1)), ext) shouldBe List()
  }

  it should "not present with a categorical" in {
    Idf[P, W](dimExtractor(_0)).presentWithValue(Cell(Position("foo"), getStringContent("bar")), ext) shouldBe List()
  }
}

class TestBooleanTf extends TestTransformers {
  type P = Value[String] :: Value[String] :: HNil

  val cell = Cell(Position("foo", "bar"), getLongContent(3))

  "A BooleanTf" should "present" in {
    BooleanTf[P]().present(cell) shouldBe List(Cell(Position("foo", "bar"), getDoubleContent(1)))
  }

  it should "present with name" in {
    BooleanTf[P]()
      .andThenRelocate(Locate.RenameDimension(_0, (v: Value[String]) => v.toShortString + ".btf"))
      .present(cell).toList shouldBe List(Cell(Position("foo.btf", "bar"), getDoubleContent(1)))
    BooleanTf[P]()
      .andThenRelocate(Locate.RenameDimension(_1, (v: Value[String]) => v.toShortString + ".btf"))
      .present(cell).toList shouldBe List(Cell(Position("foo", "bar.btf"), getDoubleContent(1)))
  }

  it should "not present with a categorical" in {
    BooleanTf[P]().present(Cell(Position("foo", "bar"), getStringContent("baz"))) shouldBe List()
  }
}

class TestLogarithmicTf extends TestTransformers {
  type P = Value[String] :: Value[String] :: HNil

  val cell = Cell(Position("foo", "bar"), getLongContent(3))

  "A LogarithmicTf" should "present" in {
    LogarithmicTf[P]().present(cell) shouldBe List(Cell(Position("foo", "bar"), getDoubleContent(1 + math.log(3))))
  }

  it should "present with name" in {
    LogarithmicTf[P]()
      .andThenRelocate(Locate.RenameDimension(_0, (v: Value[String]) => v.toShortString + ".ltf"))
      .present(cell).toList shouldBe List(Cell(Position("foo.ltf", "bar"), getDoubleContent(1 + math.log(3))))
    LogarithmicTf[P]()
      .andThenRelocate(Locate.RenameDimension(_1, (v: Value[String]) => v.toShortString + ".ltf"))
      .present(cell).toList shouldBe List(Cell(Position("foo", "bar.ltf"), getDoubleContent(1 + math.log(3))))
  }

  it should "present with log" in {
    LogarithmicTf[P](math.log10 _)
      .present(cell) shouldBe List(Cell(Position("foo", "bar"), getDoubleContent(1 + math.log10(3))))
  }

  it should "present with name and log" in {
    LogarithmicTf[P](math.log10 _)
      .andThenRelocate(Locate.RenameDimension(_0, (v: Value[String]) => v.toShortString + ".ltf"))
      .present(cell).toList shouldBe List(Cell(Position("foo.ltf", "bar"), getDoubleContent(1 + math.log10(3))))
    LogarithmicTf[P](math.log10 _)
      .andThenRelocate(Locate.RenameDimension(_1, (v: Value[String]) => v.toShortString + ".ltf"))
      .present(cell).toList shouldBe List(Cell(Position("foo", "bar.ltf"), getDoubleContent(1 + math.log10(3))))
  }

  it should "not present with a categorical" in {
    LogarithmicTf[P]().present(Cell(Position("foo", "bar"), getStringContent("baz"))) shouldBe List()
  }
}

class TestAugmentedTf extends TestTransformers {
  type P = Value[String] :: Value[String] :: HNil
  type W1 = Map[Position[Value[String] :: HNil], Content]
  type W2 = Map[Position[Value[String] :: HNil], Map[Position[Value[String] :: HNil], Content]]

  val cell = Cell(Position("foo", "bar"), getLongContent(1))
  val ext = Map(Position("foo") -> getLongContent(2), Position("bar") -> getLongContent(2))
  val ext2 = Map(
    Position("foo") -> Map(Position("baz") -> getLongContent(2)),
    Position("bar") -> Map(Position("baz") -> getLongContent(2))
  )

  "An AugmentedTf" should "present" in {
    AugmentedTf[P, W1](dimExtractor(_0))
      .presentWithValue(cell, ext) shouldBe List(Cell(Position("foo", "bar"), getDoubleContent(0.5 + 0.5 * 1 / 2)))
    AugmentedTf[P, W2](extractor(_1, "baz"))
      .presentWithValue(cell, ext2) shouldBe List(Cell(Position("foo", "bar"), getDoubleContent(0.5 + 0.5 * 1 / 2)))
  }

  it should "present with name" in {
    AugmentedTf[P, W2](extractor(_0, "baz"))
      .andThenRelocate(Locate.RenameDimension(_0, (v: Value[String]) => v.toShortString + ".atf"))
      .presentWithValue(cell, ext2)
      .toList shouldBe List(Cell(Position("foo.atf", "bar"), getDoubleContent(0.5 + 0.5 * 1 / 2)))
    AugmentedTf[P, W1](dimExtractor(_1))
      .andThenRelocate(Locate.RenameDimension(_1, (v: Value[String]) => v.toShortString + ".atf"))
      .presentWithValue(cell, ext)
      .toList shouldBe List(Cell(Position("foo", "bar.atf"), getDoubleContent(0.5 + 0.5 * 1 / 2)))
  }

  it should "not present with missing value" in {
    AugmentedTf[P, W1](dimExtractor(_0))
      .presentWithValue(Cell(Position("abc", "bar"), getLongContent(1)), ext) shouldBe List()
  }

  it should "not present with a categorical" in {
    AugmentedTf[P, W1](dimExtractor(_0))
      .presentWithValue(Cell(Position("foo", "bar"), getStringContent("baz")), ext) shouldBe List()
  }
}

class TestTfIdf extends TestTransformers {
  type P = Value[String] :: Value[String] :: HNil
  type W = Map[Position[Value[String] :: HNil], Content]

  val cell = Cell(Position("foo", "bar"), getDoubleContent(1.5))
  val ext = Map(
    Position("foo") -> getDoubleContent(2),
    Position("bar") -> getDoubleContent(2),
    Position("baz") -> getDoubleContent(2)
  )

  "A TfIdf" should "present" in {
    TfIdf[P, W](dimExtractor(_0))
      .presentWithValue(cell, ext) shouldBe List(Cell(Position("foo", "bar"), getDoubleContent(3)))
    TfIdf[P, W](dimExtractor(_1))
      .presentWithValue(cell, ext) shouldBe List(Cell(Position("foo", "bar"), getDoubleContent(3)))
  }

  it should "present with name" in {
    TfIdf[P, W](dimExtractor(_0))
      .andThenRelocate(Locate.RenameDimension(_0, (v: Value[String]) => v.toShortString + ".tfidf"))
      .presentWithValue(cell, ext).toList shouldBe List(Cell(Position("foo.tfidf", "bar"), getDoubleContent(3)))
    TfIdf[P, W](dimExtractor(_1))
      .andThenRelocate(Locate.RenameDimension(_1, (v: Value[String]) => v.toShortString + ".tfidf"))
      .presentWithValue(cell, ext).toList shouldBe List(Cell(Position("foo", "bar.tfidf"), getDoubleContent(3)))
  }

  it should "present with key" in {
    TfIdf[P, W](keyExtractor("baz"))
      .presentWithValue(cell, ext) shouldBe List(Cell(Position("foo", "bar"), getDoubleContent(3)))
  }

  it should "present with name and key" in {
    TfIdf[P, W](keyExtractor("baz"))
      .andThenRelocate(Locate.RenameDimension(_0, (v: Value[String]) => v.toShortString + ".tfidf"))
      .presentWithValue(cell, ext).toList shouldBe List(Cell(Position("foo.tfidf", "bar"), getDoubleContent(3)))
    TfIdf[P, W](keyExtractor("baz"))
      .andThenRelocate(Locate.RenameDimension(_1, (v: Value[String]) => v.toShortString + ".tfidf"))
      .presentWithValue(cell, ext).toList shouldBe List(Cell(Position("foo", "bar.tfidf"), getDoubleContent(3)))
  }

  it should "not present with missing key" in {
    TfIdf[P, W](keyExtractor("not.there")).presentWithValue(cell, ext) shouldBe List()
  }

  it should "not present with missing value" in {
    TfIdf[P, W](dimExtractor(_0))
      .presentWithValue(Cell(Position("abc", "bar"), getDoubleContent(1.5)), ext) shouldBe List()
  }

  it should "not present with a categorical" in {
    TfIdf[P, W](dimExtractor(_0))
      .presentWithValue(Cell(Position("foo", "bar"), getStringContent("baz")), ext) shouldBe List()
  }
}

class TestAdd extends TestTransformers {
  type P = Value[String] :: HNil
  type W = Map[Position[Value[String] :: HNil], Content]

  val cell = Cell(Position("foo"), getDoubleContent(1))
  val ext = Map(Position("foo") -> getDoubleContent(2), Position("bar") -> getDoubleContent(2))

  "An Add" should "present" in {
    Add[P, W](dimExtractor(_0)).presentWithValue(cell, ext) shouldBe List(Cell(Position("foo"), getDoubleContent(3)))
  }

  it should "present with name" in {
    Add[P, W](dimExtractor(_0))
      .andThenRelocate(Locate.RenameDimension(_0, (v: Value[String]) => v.toShortString + ".add"))
      .presentWithValue(cell, ext).toList shouldBe List(Cell(Position("foo.add"), getDoubleContent(3)))
  }

  it should "present with key" in {
    Add[P, W](keyExtractor("bar")).presentWithValue(cell, ext) shouldBe List(Cell(Position("foo"), getDoubleContent(3)))
  }

  it should "present with name and key" in {
    Add[P, W](keyExtractor("bar"))
      .andThenRelocate(Locate.RenameDimension(_0, (v: Value[String]) => v.toShortString + ".add"))
      .presentWithValue(cell, ext).toList shouldBe List(Cell(Position("foo.add"), getDoubleContent(3)))
  }

  it should "not present with missing key" in {
    Add[P, W](keyExtractor("not.there")).presentWithValue(cell, ext) shouldBe List()
  }

  it should "not present with missing value" in {
    Add[P, W](dimExtractor(_0)).presentWithValue(Cell(Position("abc"), getDoubleContent(1)), ext) shouldBe List()
  }

  it should "not present with a categorical" in {
    Add[P, W](dimExtractor(_0)).presentWithValue(Cell(Position("foo"), getStringContent("bar")), ext) shouldBe List()
  }
}

class TestSubtract extends TestTransformers {
  type P = Value[String] :: HNil
  type W = Map[Position[Value[String] :: HNil], Content]

  val cell = Cell(Position("foo"), getDoubleContent(1))
  val ext = Map(Position("foo") -> getDoubleContent(2), Position("bar") -> getDoubleContent(2))

  "A Subtract" should "present" in {
    Subtract[P, W](dimExtractor(_0))
      .presentWithValue(cell, ext) shouldBe List(Cell(Position("foo"), getDoubleContent(-1)))
  }

  it should "present with name" in {
    Subtract[P, W](dimExtractor(_0))
      .andThenRelocate(Locate.RenameDimension(_0, (v: Value[String]) => v.toShortString + ".sub"))
      .presentWithValue(cell, ext).toList shouldBe List(Cell(Position("foo.sub"), getDoubleContent(-1)))
  }

  it should "present with key" in {
    Subtract[P, W](keyExtractor("bar"))
      .presentWithValue(cell, ext) shouldBe List(Cell(Position("foo"), getDoubleContent(-1)))
  }

  it should "present with inverse" in {
    Subtract[P, W](dimExtractor(_0), true)
      .presentWithValue(cell, ext) shouldBe List(Cell(Position("foo"), getDoubleContent(1)))
  }

  it should "present with name and key" in {
    Subtract[P, W](keyExtractor("bar"))
      .andThenRelocate(Locate.RenameDimension(_0, (v: Value[String]) => v.toShortString + ".sub"))
      .presentWithValue(cell, ext).toList shouldBe List(Cell(Position("foo.sub"), getDoubleContent(-1)))
  }

  it should "present with name and inverse" in {
    Subtract[P, W](dimExtractor(_0), true)
      .andThenRelocate(Locate.RenameDimension(_0, (v: Value[String]) => v.toShortString + ".sub"))
      .presentWithValue(cell, ext).toList shouldBe List(Cell(Position("foo.sub"), getDoubleContent(1)))
  }

  it should "present with key and inverse" in {
    Subtract[P, W](keyExtractor("bar"), true)
      .presentWithValue(cell, ext) shouldBe List(Cell(Position("foo"), getDoubleContent(1)))
  }

  it should "present with name, key and inverse" in {
    Subtract[P, W](keyExtractor("bar"), true)
      .andThenRelocate(Locate.RenameDimension(_0, (v: Value[String]) => v.toShortString + ".sub"))
      .presentWithValue(cell, ext).toList shouldBe List(Cell(Position("foo.sub"), getDoubleContent(1)))
  }

  it should "not present with missing key" in {
    Subtract[P, W](keyExtractor("not.there")).presentWithValue(cell, ext) shouldBe List()
  }

  it should "not present with missing value" in {
    Subtract[P, W](dimExtractor(_0)).presentWithValue(Cell(Position("abc"), getDoubleContent(1)), ext) shouldBe List()
  }

  it should "not present with a categorical" in {
    Subtract[P, W](dimExtractor(_0))
      .presentWithValue(Cell(Position("foo"), getStringContent("bar")), ext) shouldBe List()
  }
}

class TestMultiply extends TestTransformers {
  type P = Value[String] :: HNil
  type W = Map[Position[Value[String] :: HNil], Content]

  val cell = Cell(Position("foo"), getDoubleContent(1))
  val ext = Map(Position("foo") -> getDoubleContent(2), Position("bar") -> getDoubleContent(2))

  "A Multiply" should "present" in {
    Multiply[P, W](dimExtractor(_0))
      .presentWithValue(cell, ext) shouldBe List(Cell(Position("foo"), getDoubleContent(2)))
  }

  it should "present with name" in {
    Multiply[P, W](dimExtractor(_0))
      .andThenRelocate(Locate.RenameDimension(_0, (v: Value[String]) => v.toShortString + ".mul"))
      .presentWithValue(cell, ext).toList shouldBe List(Cell(Position("foo.mul"), getDoubleContent(2)))
  }

  it should "present with key" in {
    Multiply[P, W](keyExtractor("bar"))
      .presentWithValue(cell, ext) shouldBe List(Cell(Position("foo"), getDoubleContent(2)))
  }

  it should "present with name and key" in {
    Multiply[P, W](keyExtractor("bar"))
      .andThenRelocate(Locate.RenameDimension(_0, (v: Value[String]) => v.toShortString + ".mul"))
      .presentWithValue(cell, ext).toList shouldBe List(Cell(Position("foo.mul"), getDoubleContent(2)))
  }

  it should "not present with missing key" in {
    Multiply[P, W](keyExtractor("not.there")).presentWithValue(cell, ext) shouldBe List()
  }

  it should "not present with missing value" in {
    Multiply[P, W](dimExtractor(_0)).presentWithValue(Cell(Position("abc"), getDoubleContent(1)), ext) shouldBe List()
  }

  it should "not present with a categorical" in {
    Multiply[P, W](dimExtractor(_0))
      .presentWithValue(Cell(Position("foo"), getStringContent("bar")), ext) shouldBe List()
  }
}

class TestFraction extends TestTransformers {
  type P = Value[String] :: HNil
  type W = Map[Position[Value[String] :: HNil], Content]

  val cell = Cell(Position("foo"), getDoubleContent(1))
  val ext = Map(Position("foo") -> getDoubleContent(2), Position("bar") -> getDoubleContent(2))

  "A Fraction" should "present" in {
    Fraction[P, W](dimExtractor(_0))
      .presentWithValue(cell, ext) shouldBe List(Cell(Position("foo"), getDoubleContent(0.5)))
  }

  it should "present with name" in {
    Fraction[P, W](dimExtractor(_0))
      .andThenRelocate(Locate.RenameDimension(_0, (v: Value[String]) => v.toShortString + ".frac"))
      .presentWithValue(cell, ext).toList shouldBe List(Cell(Position("foo.frac"), getDoubleContent(0.5)))
  }

  it should "present with key" in {
    Fraction[P, W](keyExtractor("bar"))
      .presentWithValue(cell, ext) shouldBe List(Cell(Position("foo"), getDoubleContent(0.5)))
  }

  it should "present with inverse" in {
    Fraction[P, W](dimExtractor(_0), true)
      .presentWithValue(cell, ext) shouldBe List(Cell(Position("foo"), getDoubleContent(2)))
  }

  it should "present with name and key" in {
    Fraction[P, W](keyExtractor("bar"))
      .andThenRelocate(Locate.RenameDimension(_0, (v: Value[String]) => v.toShortString + ".frac"))
      .presentWithValue(cell, ext).toList shouldBe List(Cell(Position("foo.frac"), getDoubleContent(0.5)))
  }

  it should "present with name and inverse" in {
    Fraction[P, W](dimExtractor(_0), true)
      .andThenRelocate(Locate.RenameDimension(_0, (v: Value[String]) => v.toShortString + ".frac"))
      .presentWithValue(cell, ext).toList shouldBe List(Cell(Position("foo.frac"), getDoubleContent(2)))
  }

  it should "present with key and inverse" in {
    Fraction[P, W](keyExtractor("bar"), true)
      .presentWithValue(cell, ext) shouldBe List(Cell(Position("foo"), getDoubleContent(2)))
  }

  it should "present with name, key and inverse" in {
    Fraction[P, W](keyExtractor("bar"), true)
      .andThenRelocate(Locate.RenameDimension(_0, (v: Value[String]) => v.toShortString + ".frac"))
      .presentWithValue(cell, ext).toList shouldBe List(Cell(Position("foo.frac"), getDoubleContent(2)))
  }

  it should "not present with missing key" in {
    Fraction[P, W](keyExtractor("not.there")).presentWithValue(cell, ext) shouldBe List()
  }

  it should "not present with missing value" in {
    Fraction[P, W](dimExtractor(_0)).presentWithValue(Cell(Position("abc"), getDoubleContent(1)), ext) shouldBe List()
  }

  it should "not present with a categorical" in {
    Fraction[P, W](dimExtractor(_0))
      .presentWithValue(Cell(Position("foo"), getStringContent("bar")), ext) shouldBe List()
  }
}

class TestPower extends TestTransformers {
  type P = Value[String] :: HNil

  val cell = Cell(Position("foo"), getDoubleContent(3.1415))

  "A Power" should "present" in {
    Power[P](2).present(cell) shouldBe List(Cell(Position("foo"), getDoubleContent(3.1415 * 3.1415)))
  }

  it should "present with name" in {
    Power[P](2)
      .andThenRelocate(Locate.RenameDimension(_0, (v: Value[String]) => v.toShortString + ".pwr"))
      .present(cell).toList shouldBe List(Cell(Position("foo.pwr"), getDoubleContent(3.1415 * 3.1415)))
  }
}

class TestSquareRoot extends TestTransformers {
  type P = Value[String] :: HNil

  val cell = Cell(Position("foo"), getDoubleContent(3.1415))

  "A SquareRoot" should "present" in {
    SquareRoot[P]().present(cell) shouldBe List(Cell(Position("foo"), getDoubleContent(math.sqrt(3.1415))))
  }

  it should "present with name" in {
    SquareRoot[P]()
      .andThenRelocate(Locate.RenameDimension(_0, (v: Value[String]) => v.toShortString + ".sqr"))
      .present(cell).toList shouldBe List(Cell(Position("foo.sqr"), getDoubleContent(math.sqrt(3.1415))))
  }
}

class TestCut extends TestTransformers {
  type P = Value[String] :: HNil
  type W = Map[Position[Value[String] :: HNil], List[Double]]

  val cell1 = Cell(Position("foo"), getDoubleContent(3.1415))
  val cell2 = Cell(Position("foo"), getDoubleContent(4.0))
  val cell3 = Cell(Position("foo"), getDoubleContent(3.0))
  val cell4 = Cell(Position("foo"), getDoubleContent(0.0))
  val cell5 = Cell(Position("foo"), getDoubleContent(5.0))
  val ext = Map(Position("foo") -> List[Double](0,1,2,3,4,5))

  val binExtractor = ExtractWithDimension[P, _0, List[Double]]

  "A Cut" should "present" in {
    val pos = Position("foo")

    def right(idx: Int, lower: Double, upper: Double): String = s"[${lower},${upper})"

    val cut1 = Cut[P, W](binExtractor)
    val schema1 = OrdinalSchema[String](Set("(0.0,1.0]", "(1.0,2.0]", "(2.0,3.0]", "(3.0,4.0]", "(4.0,5.0]"))

    cut1.presentWithValue(cell1, ext) shouldBe List(Cell(pos, Content(schema1, "(3.0,4.0]")))
    cut1.presentWithValue(cell2, ext) shouldBe List(Cell(pos, Content(schema1, "(3.0,4.0]")))
    cut1.presentWithValue(cell3, ext) shouldBe List(Cell(pos, Content(schema1, "(2.0,3.0]")))
    cut1.presentWithValue(cell4, ext) shouldBe List()
    cut1.presentWithValue(cell5, ext) shouldBe List(Cell(pos, Content(schema1, "(4.0,5.0]")))

    val cut2 = Cut[P, W](binExtractor, right=false, name=right)
    val schema2 = OrdinalSchema[String](Set("[0.0,1.0)", "[1.0,2.0)", "[2.0,3.0)", "[3.0,4.0)", "[4.0,5.0)"))

    cut2.presentWithValue(cell1, ext) shouldBe List(Cell(pos, Content(schema2, "[3.0,4.0)")))
    cut2.presentWithValue(cell2, ext) shouldBe List(Cell(pos, Content(schema2, "[4.0,5.0)")))
    cut2.presentWithValue(cell3, ext) shouldBe List(Cell(pos, Content(schema2, "[3.0,4.0)")))
    cut2.presentWithValue(cell4, ext) shouldBe List(Cell(pos, Content(schema2, "[0.0,1.0)")))
    cut2.presentWithValue(cell5, ext) shouldBe List()

    val cut3 = Cut[P, W](binExtractor, include=true)
    val schema3 = OrdinalSchema[String](Set("(0.0,1.0]", "(1.0,2.0]", "(2.0,3.0]", "(3.0,4.0]", "(4.0,5.0]"))

    cut3.presentWithValue(cell1, ext) shouldBe List(Cell(pos, Content(schema3, "(3.0,4.0]")))
    cut3.presentWithValue(cell2, ext) shouldBe List(Cell(pos, Content(schema3, "(3.0,4.0]")))
    cut3.presentWithValue(cell3, ext) shouldBe List(Cell(pos, Content(schema3, "(2.0,3.0]")))
    cut3.presentWithValue(cell4, ext) shouldBe List(Cell(pos, Content(schema3, "(0.0,1.0]")))
    cut3.presentWithValue(cell5, ext) shouldBe List(Cell(pos, Content(schema3, "(4.0,5.0]")))

    val cut4 = Cut[P, W](binExtractor, right=false, include=true, name=right)
    val schema4 = OrdinalSchema[String](Set("[0.0,1.0)", "[1.0,2.0)", "[2.0,3.0)", "[3.0,4.0)", "[4.0,5.0)"))

    cut4.presentWithValue(cell1, ext) shouldBe List(Cell(pos, Content(schema4, "[3.0,4.0)")))
    cut4.presentWithValue(cell2, ext) shouldBe List(Cell(pos, Content(schema4, "[4.0,5.0)")))
    cut4.presentWithValue(cell3, ext) shouldBe List(Cell(pos, Content(schema4, "[3.0,4.0)")))
    cut4.presentWithValue(cell4, ext) shouldBe List(Cell(pos, Content(schema4, "[0.0,1.0)")))
    cut4.presentWithValue(cell5, ext) shouldBe List(Cell(pos, Content(schema4, "[4.0,5.0)")))
  }

  it should "present with name" in {
    Cut[P, W](binExtractor)
      .andThenRelocate(Locate.RenameDimension(_0, (v: Value[String]) => v.toShortString + ".cut"))
      .presentWithValue(cell1, ext).toList shouldBe List(
        Cell(
          Position("foo.cut"),
          Content(
            OrdinalSchema[String](Set("(0.0,1.0]", "(1.0,2.0]", "(2.0,3.0]", "(3.0,4.0]", "(4.0,5.0]")),
            "(3.0,4.0]"
          )
        )
      )
  }

  it should "not present with missing bins" in {
    Cut[P, W](binExtractor).presentWithValue(cell1, Map()) shouldBe List()
  }
}

class TestCompare extends TestTransformers {
  type P = Value[String] :: HNil

  val cell = Cell(Position("foo"), getDoubleContent(3.1415))

  def equ(v: Double) = (cell: Cell[P]) => cell.content.value.as[Double].map(_ == v).getOrElse(false)

  "A Compare" should "present" in {
    Compare[P](equ(3.1415)).present(cell) shouldBe List(
      Cell(Position("foo"), Content(NominalSchema[Boolean](), true))
    )
    Compare[P](equ(3.3)).present(cell) shouldBe List(
      Cell(Position("foo"), Content(NominalSchema[Boolean](), false))
    )
  }

  it should "present with name" in {
    Compare[P](equ(3.1415))
      .andThenRelocate(Locate.RenameDimension(_0, (v: Value[String]) => v.toShortString + ".cmp"))
      .present(cell).toList shouldBe List(Cell(Position("foo.cmp"), Content(NominalSchema[Boolean](), true)))
    Compare[P](equ(3.3))
      .andThenRelocate(Locate.RenameDimension(_0, (v: Value[String]) => v.toShortString + ".cmp"))
      .present(cell).toList shouldBe List(Cell(Position("foo.cmp"), Content(NominalSchema[Boolean](), false)))
  }
}

class TestScalaCutRules extends TestTransformers with TestScala {
  import commbank.grimlock.scala.transform.CutRules

  val stats = Map(
    Position("foo") -> Map(Position("min") -> getDoubleContent(0),
    Position("max") -> getDoubleContent(5), Position("count") -> getDoubleContent(25),
    Position("sd") -> getDoubleContent(2), Position("skewness") -> getDoubleContent(2))
  )

  "A fixed" should "cut" in {
    CutRules.fixed(stats, "min", "max", 5) shouldBe Map(Position("foo") -> List[Double](-0.005,1,2,3,4,5))

    ctx.library.rules.fixed(stats, "min", "max", 5) shouldBe Map(Position("foo") -> List[Double](-0.005,1,2,3,4,5))
  }

  it should "not cut with missing values" in {
    CutRules.fixed(stats, "not.there", "max", 5) shouldBe Map()
    CutRules.fixed(stats, "min", "not.there", 5) shouldBe Map()
  }

  "A squareRootChoice" should "cut" in {
    CutRules.squareRootChoice(stats, "count", "min", "max") shouldBe Map(
      Position("foo") -> List[Double](-0.005,1,2,3,4,5)
    )

    ctx.library.rules.squareRootChoice(stats, "count", "min", "max") shouldBe Map(
      Position("foo") -> List[Double](-0.005,1,2,3,4,5)
    )
  }

  it should "not cut with missing values" in {
    CutRules.squareRootChoice(stats, "not.there", "min", "max") shouldBe Map()
    CutRules.squareRootChoice(stats, "count", "not.there", "max") shouldBe Map()
    CutRules.squareRootChoice(stats, "count", "min", "not.there") shouldBe Map()
  }

  "A sturgesFormula" should "cut" in {
    // math.ceil(log2(25) + 1) = 6
    val vals = -0.005 +: (0.0 to 5.0 by (5.0 / 6)).tail

    CutRules.sturgesFormula(stats, "count", "min", "max") shouldBe Map(Position("foo") -> vals)

    ctx.library.rules.sturgesFormula(stats, "count", "min", "max") shouldBe Map(Position("foo") -> vals)
  }

  it should "not cut with missing values" in {
    CutRules.sturgesFormula(stats, "not.there", "min", "max") shouldBe Map()
    CutRules.sturgesFormula(stats, "count", "not.there", "max") shouldBe Map()
    CutRules.sturgesFormula(stats, "count", "min", "not.there") shouldBe Map()
  }

  "A riceRule" should "cut" in {
    // math.ceil(2 * math.pow(25, 1.0 / 3.0)) = 6
    val vals = -0.005 +: (0.0 to 5.0 by (5.0 / 6)).tail

    CutRules.riceRule(stats, "count", "min", "max") shouldBe Map(Position("foo") -> vals)

    ctx.library.rules.riceRule(stats, "count", "min", "max") shouldBe Map(Position("foo") -> vals)
  }

  it should "not cut with missing values" in {
    CutRules.riceRule(stats, "not.there", "min", "max") shouldBe Map()
    CutRules.riceRule(stats, "count", "not.there", "max") shouldBe Map()
    CutRules.riceRule(stats, "count", "min", "not.there") shouldBe Map()
  }

  "A doanesFormula" should "cut" in {
    // math.round(1 + log2(25) + log2(1 + math.abs(2) / math.sqrt((6.0 * 23.0) / (26.0 * 28.0)))) = 8
    val vals = -0.005 +: (0.0 to 5.0 by (5.0 / 8)).tail

    CutRules.doanesFormula(stats, "count", "min", "max", "skewness") shouldBe Map(Position("foo") -> vals)

    ctx.library.rules.doanesFormula(stats, "count", "min", "max", "skewness") shouldBe Map(Position("foo") -> vals)
  }

  it should "not cut with missing values" in {
    CutRules.doanesFormula(stats, "not.there", "min", "max", "skewness") shouldBe Map()
    CutRules.doanesFormula(stats, "count", "not.there", "max", "skewness") shouldBe Map()
    CutRules.doanesFormula(stats, "count", "min", "not.there", "skewness") shouldBe Map()
    CutRules.doanesFormula(stats, "count", "min", "max", "not.there") shouldBe Map()
  }

  "A scottsNormalReferenceRule" should "cut" in {
    // math.ceil((5.0 - 0) / (3.5 * 2 / math.pow(25, 1.0 / 3.0))) = 3
    val vals = -0.005 +: (0.0 to 5.0 by (5.0 / 3)).tail

    CutRules.scottsNormalReferenceRule(stats, "count", "min", "max", "sd") shouldBe Map(Position("foo") -> vals)

    ctx.library.rules.scottsNormalReferenceRule(stats, "count", "min", "max", "sd") shouldBe Map(
      Position("foo") -> vals
    )
  }

  it should "not cut with missing values" in {
    CutRules.scottsNormalReferenceRule(stats, "not.there", "min", "max", "sd") shouldBe Map()
    CutRules.scottsNormalReferenceRule(stats, "count", "not.there", "max", "sd") shouldBe Map()
    CutRules.scottsNormalReferenceRule(stats, "count", "min", "not.there", "sd") shouldBe Map()
    CutRules.scottsNormalReferenceRule(stats, "count", "min", "max", "not.there") shouldBe Map()
  }

  "A breaks" should "cut" in {
    CutRules.breaks(Map("foo" -> List[Double](0,1,2,3,4,5))) shouldBe Map(
      Position("foo") -> List[Double](0,1,2,3,4,5)
    )

    ctx.library.rules.breaks(Map("foo" -> List[Double](0,1,2,3,4,5))) shouldBe Map(
      Position("foo") -> List[Double](0,1,2,3,4,5)
    )
  }
}

class TestScaldingCutRules extends TestTransformers with TestScalding {
  import commbank.grimlock.scalding.transform.CutRules

  import com.twitter.scalding.typed.ValuePipe

  val stats = ValuePipe(
    Map(
      Position("foo") -> Map(Position("min") -> getDoubleContent(0),
      Position("max") -> getDoubleContent(5), Position("count") -> getDoubleContent(25),
      Position("sd") -> getDoubleContent(2), Position("skewness") -> getDoubleContent(2))
    )
  )

  "A fixed" should "cut" in {
    CutRules.fixed(stats, "min", "max", 5) shouldBe ValuePipe(Map(Position("foo") -> List[Double](-0.005,1,2,3,4,5)))

    ctx.library.rules.fixed(stats, "min", "max", 5) shouldBe ValuePipe(
      Map(Position("foo") -> List[Double](-0.005,1,2,3,4,5))
    )
  }

  it should "not cut with missing values" in {
    CutRules.fixed(stats, "not.there", "max", 5) shouldBe ValuePipe(Map())
    CutRules.fixed(stats, "min", "not.there", 5) shouldBe ValuePipe(Map())
  }

  "A squareRootChoice" should "cut" in {
    CutRules.squareRootChoice(stats, "count", "min", "max") shouldBe ValuePipe(
      Map(Position("foo") -> List[Double](-0.005,1,2,3,4,5))
    )

    ctx.library.rules.squareRootChoice(stats, "count", "min", "max") shouldBe ValuePipe(
      Map(Position("foo") -> List[Double](-0.005,1,2,3,4,5))
    )
  }

  it should "not cut with missing values" in {
    CutRules.squareRootChoice(stats, "not.there", "min", "max") shouldBe ValuePipe(Map())
    CutRules.squareRootChoice(stats, "count", "not.there", "max") shouldBe ValuePipe(Map())
    CutRules.squareRootChoice(stats, "count", "min", "not.there") shouldBe ValuePipe(Map())
  }

  "A sturgesFormula" should "cut" in {
    // math.ceil(log2(25) + 1) = 6
    val vals = -0.005 +: (0.0 to 5.0 by (5.0 / 6)).tail

    CutRules.sturgesFormula(stats, "count", "min", "max") shouldBe ValuePipe(Map(Position("foo") -> vals))

    ctx.library.rules.sturgesFormula(stats, "count", "min", "max") shouldBe ValuePipe(
      Map(Position("foo") -> vals)
    )
  }

  it should "not cut with missing values" in {
    CutRules.sturgesFormula(stats, "not.there", "min", "max") shouldBe ValuePipe(Map())
    CutRules.sturgesFormula(stats, "count", "not.there", "max") shouldBe ValuePipe(Map())
    CutRules.sturgesFormula(stats, "count", "min", "not.there") shouldBe ValuePipe(Map())
  }

  "A riceRule" should "cut" in {
    // math.ceil(2 * math.pow(25, 1.0 / 3.0)) = 6
    val vals = -0.005 +: (0.0 to 5.0 by (5.0 / 6)).tail

    CutRules.riceRule(stats, "count", "min", "max") shouldBe ValuePipe(Map(Position("foo") -> vals))

    ctx.library.rules.riceRule(stats, "count", "min", "max") shouldBe ValuePipe(Map(Position("foo") -> vals))
  }

  it should "not cut with missing values" in {
    CutRules.riceRule(stats, "not.there", "min", "max") shouldBe ValuePipe(Map())
    CutRules.riceRule(stats, "count", "not.there", "max") shouldBe ValuePipe(Map())
    CutRules.riceRule(stats, "count", "min", "not.there") shouldBe ValuePipe(Map())
  }

  "A doanesFormula" should "cut" in {
    // math.round(1 + log2(25) + log2(1 + math.abs(2) / math.sqrt((6.0 * 23.0) / (26.0 * 28.0)))) = 8
    val vals = -0.005 +: (0.0 to 5.0 by (5.0 / 8)).tail

    CutRules.doanesFormula(stats, "count", "min", "max", "skewness") shouldBe ValuePipe(Map(Position("foo") -> vals))

    ctx.library.rules.doanesFormula(stats, "count", "min", "max", "skewness") shouldBe ValuePipe(
      Map(Position("foo") -> vals)
    )
  }

  it should "not cut with missing values" in {
    CutRules.doanesFormula(stats, "not.there", "min", "max", "skewness") shouldBe ValuePipe(Map())
    CutRules.doanesFormula(stats, "count", "not.there", "max", "skewness") shouldBe ValuePipe(Map())
    CutRules.doanesFormula(stats, "count", "min", "not.there", "skewness") shouldBe ValuePipe(Map())
    CutRules.doanesFormula(stats, "count", "min", "max", "not.there") shouldBe ValuePipe(Map())
  }

  "A scottsNormalReferenceRule" should "cut" in {
    // math.ceil((5.0 - 0) / (3.5 * 2 / math.pow(25, 1.0 / 3.0))) = 3
    val vals = -0.005 +: (0.0 to 5.0 by (5.0 / 3)).tail

    CutRules.scottsNormalReferenceRule(stats, "count", "min", "max", "sd") shouldBe ValuePipe(
      Map(Position("foo") -> vals)
    )

    ctx.library.rules.scottsNormalReferenceRule(stats, "count", "min", "max", "sd") shouldBe ValuePipe(
      Map(Position("foo") -> vals)
    )
  }

  it should "not cut with missing values" in {
    CutRules.scottsNormalReferenceRule(stats, "not.there", "min", "max", "sd") shouldBe ValuePipe(Map())
    CutRules.scottsNormalReferenceRule(stats, "count", "not.there", "max", "sd") shouldBe ValuePipe(Map())
    CutRules.scottsNormalReferenceRule(stats, "count", "min", "not.there", "sd") shouldBe ValuePipe(Map())
    CutRules.scottsNormalReferenceRule(stats, "count", "min", "max", "not.there") shouldBe ValuePipe(Map())
  }

  "A breaks" should "cut" in {
    CutRules.breaks(Map("foo" -> List[Double](0,1,2,3,4,5))) shouldBe ValuePipe(
      Map(Position("foo") -> List[Double](0,1,2,3,4,5))
    )

    ctx.library.rules.breaks(Map("foo" -> List[Double](0,1,2,3,4,5))) shouldBe ValuePipe(
      Map(Position("foo") -> List[Double](0,1,2,3,4,5))
    )
  }
}

class TestSparkCutRules extends TestTransformers with TestSpark {
  import commbank.grimlock.spark.transform.CutRules

  val stats = Map(
    Position("foo") -> Map(Position("min") -> getDoubleContent(0),
    Position("max") -> getDoubleContent(5), Position("count") -> getDoubleContent(25),
    Position("sd") -> getDoubleContent(2), Position("skewness") -> getDoubleContent(2))
  )

  "A fixed" should "cut" in {
    CutRules.fixed(stats, "min", "max", 5) shouldBe Map(Position("foo") -> List[Double](-0.005,1,2,3,4,5))

    ctx.library.rules.fixed(stats, "min", "max", 5) shouldBe Map(Position("foo") -> List[Double](-0.005,1,2,3,4,5))
  }

  it should "not cut with missing values" in {
    CutRules.fixed(stats, "not.there", "max", 5) shouldBe Map()
    CutRules.fixed(stats, "min", "not.there", 5) shouldBe Map()
  }

  "A squareRootChoice" should "cut" in {
    CutRules.squareRootChoice(stats, "count", "min", "max") shouldBe Map(
      Position("foo") -> List[Double](-0.005,1,2,3,4,5)
    )

    ctx.library.rules.squareRootChoice(stats, "count", "min", "max") shouldBe Map(
      Position("foo") -> List[Double](-0.005,1,2,3,4,5)
    )
  }

  it should "not cut with missing values" in {
    CutRules.squareRootChoice(stats, "not.there", "min", "max") shouldBe Map()
    CutRules.squareRootChoice(stats, "count", "not.there", "max") shouldBe Map()
    CutRules.squareRootChoice(stats, "count", "min", "not.there") shouldBe Map()
  }

  "A sturgesFormula" should "cut" in {
    // math.ceil(log2(25) + 1) = 6
    val vals = -0.005 +: (0.0 to 5.0 by (5.0 / 6)).tail

    CutRules.sturgesFormula(stats, "count", "min", "max") shouldBe Map(Position("foo") -> vals)

    ctx.library.rules.sturgesFormula(stats, "count", "min", "max") shouldBe Map(Position("foo") -> vals)
  }

  it should "not cut with missing values" in {
    CutRules.sturgesFormula(stats, "not.there", "min", "max") shouldBe Map()
    CutRules.sturgesFormula(stats, "count", "not.there", "max") shouldBe Map()
    CutRules.sturgesFormula(stats, "count", "min", "not.there") shouldBe Map()
  }

  "A riceRule" should "cut" in {
    // math.ceil(2 * math.pow(25, 1.0 / 3.0)) = 6
    val vals = -0.005 +: (0.0 to 5.0 by (5.0 / 6)).tail

    CutRules.riceRule(stats, "count", "min", "max") shouldBe Map(Position("foo") -> vals)

    ctx.library.rules.riceRule(stats, "count", "min", "max") shouldBe Map(Position("foo") -> vals)
  }

  it should "not cut with missing values" in {
    CutRules.riceRule(stats, "not.there", "min", "max") shouldBe Map()
    CutRules.riceRule(stats, "count", "not.there", "max") shouldBe Map()
    CutRules.riceRule(stats, "count", "min", "not.there") shouldBe Map()
  }

  "A doanesFormula" should "cut" in {
    // math.round(1 + log2(25) + log2(1 + math.abs(2) / math.sqrt((6.0 * 23.0) / (26.0 * 28.0)))) = 8
    val vals = -0.005 +: (0.0 to 5.0 by (5.0 / 8)).tail

    CutRules.doanesFormula(stats, "count", "min", "max", "skewness") shouldBe Map(Position("foo") -> vals)

    ctx.library.rules.doanesFormula(stats, "count", "min", "max", "skewness") shouldBe Map(Position("foo") -> vals)
  }

  it should "not cut with missing values" in {
    CutRules.doanesFormula(stats, "not.there", "min", "max", "skewness") shouldBe Map()
    CutRules.doanesFormula(stats, "count", "not.there", "max", "skewness") shouldBe Map()
    CutRules.doanesFormula(stats, "count", "min", "not.there", "skewness") shouldBe Map()
    CutRules.doanesFormula(stats, "count", "min", "max", "not.there") shouldBe Map()
  }

  "A scottsNormalReferenceRule" should "cut" in {
    // math.ceil((5.0 - 0) / (3.5 * 2 / math.pow(25, 1.0 / 3.0))) = 3
    val vals = -0.005 +: (0.0 to 5.0 by (5.0 / 3)).tail

    CutRules.scottsNormalReferenceRule(stats, "count", "min", "max", "sd") shouldBe Map(Position("foo") -> vals)

    ctx.library.rules.scottsNormalReferenceRule(stats, "count", "min", "max", "sd") shouldBe Map(
      Position("foo") -> vals
    )
  }

  it should "not cut with missing values" in {
    CutRules.scottsNormalReferenceRule(stats, "not.there", "min", "max", "sd") shouldBe Map()
    CutRules.scottsNormalReferenceRule(stats, "count", "not.there", "max", "sd") shouldBe Map()
    CutRules.scottsNormalReferenceRule(stats, "count", "min", "not.there", "sd") shouldBe Map()
    CutRules.scottsNormalReferenceRule(stats, "count", "min", "max", "not.there") shouldBe Map()
  }

  "A breaks" should "cut" in {
    CutRules.breaks(Map("foo" -> List[Double](0,1,2,3,4,5))) shouldBe Map(
      Position("foo") -> List[Double](0,1,2,3,4,5)
    )

    ctx.library.rules.breaks(Map("foo" -> List[Double](0,1,2,3,4,5))) shouldBe Map(
      Position("foo") -> List[Double](0,1,2,3,4,5)
    )
  }
}

class TestAndThenTransformer extends TestTransformers {
  type P = Value[String] :: HNil
  type Q = Value[String] :: HNil

  val cell = Cell(Position("foo"), getStringContent("rules"))

  "An AndThenTransformer" should "present" in {
    Binarise[P, Q](Locate.RenameDimensionWithContent(_0, locate()))
      .andThen(Indicator()
      .andThenRelocate(Locate.RenameDimension(_0, (v: Value[String]) => v.toShortString + ".ind")))
      .present(cell).toList shouldBe List(Cell(Position("foo=rules.ind"), getLongContent(1)))
  }
}

class TestAndThenTransformerWithValue extends TestTransformers {
  type P = Value[String] :: HNil
  type W = Map[Position[Value[String] :: HNil], Map[Position[Value[String] :: HNil], Content]]

  val cell = Cell(Position("foo"), getDoubleContent(3.1415))
  val ext = Map(
    Position("foo") -> Map(
      Position("min") -> getDoubleContent(0),
      Position("max") -> getDoubleContent(2),
      Position("max.abs") -> getDoubleContent(3)
    )
  )

  "An AndThenTransformerWithValue" should "present" in {
    Clamp[P, W](extractor(_0, "min"), extractor(_0, "max"))
      .andThenWithValue(Normalise(extractor(_0, "max.abs")))
      .presentWithValue(cell, ext).toList shouldBe List(Cell(Position("foo"), getDoubleContent(2.0 / 3.0)))
  }
}

class TestWithPrepareTransformer extends TestTransformers {
  type P = Value[String] :: HNil
  type W = Map[Position[P], Content]

  val str = Cell(Position("x"), getDoubleContent(2))
  val dbl = Cell(Position("y"), getDoubleContent(4))
  val lng = Cell(Position("z"), getDoubleContent(8))

  val ext = Map(
    Position("x") -> getDoubleContent(3),
    Position("y") -> getDoubleContent(4),
    Position("z") -> getDoubleContent(5)
  )

  def prepare(cell: Cell[P]): Content = cell.content.value match {
    case DoubleValue(d) if (d.value == 2) => cell.content
    case DoubleValue(d) if (d.value == 4) => getStringContent("not.supported")
    case DoubleValue(d) if (d.value == 8) => getLongContent(8)
  }

  def prepareWithValue(cell: Cell[P], ext: W): Content =
    (cell.content.value, ext(cell.position).value) match {
      case (DoubleValue(c), DoubleValue(d)) if (c.value == 2) => getDoubleContent(2 * d.value)
      case (DoubleValue(c), _) if (c.value == 4) => getStringContent("not.supported")
      case (DoubleValue(c), DoubleValue(d)) if (c.value == 8) => getLongContent(d.value.toLong)
    }

  "A Transformer" should "withPrepare correctly" in {
    val obj = Power[P](2).withPrepare(prepare)

    obj.present(str) shouldBe List(Cell(str.position, getDoubleContent(4)))
    obj.present(dbl) shouldBe List()
    obj.present(lng) shouldBe List(Cell(lng.position, getDoubleContent(64)))
  }

  it should "withPrepareWithValue correctly (without value)" in {
    val obj = Multiply[P, W](ExtractWithPosition().andThenPresent(_.value.as[Double])).withPrepare(prepare)

    obj.presentWithValue(str, ext) shouldBe List(Cell(str.position, getDoubleContent(2 * 3)))
    obj.presentWithValue(dbl, ext) shouldBe List()
    obj.presentWithValue(lng, ext) shouldBe List(Cell(lng.position, getDoubleContent(5 * 8)))
  }

  it should "withPrepareWithVaue correctly" in {
    val obj = Multiply[P, W](ExtractWithPosition().andThenPresent(_.value.as[Double]))
      .withPrepareWithValue(prepareWithValue)

    obj.presentWithValue(str, ext) shouldBe List(Cell(str.position, getDoubleContent(2 * 3 * 3)))
    obj.presentWithValue(dbl, ext) shouldBe List()
    obj.presentWithValue(lng, ext) shouldBe List(Cell(lng.position, getDoubleContent(5 * 5)))
  }
}

class TestAndThenMutateTransformer extends TestTransformers {
  type P = Value[String] :: HNil
  type W = Map[Position[P], Content]

  val str = Cell(Position("x"), getDoubleContent(2))
  val dbl = Cell(Position("y"), getDoubleContent(4))
  val lng = Cell(Position("z"), getDoubleContent(8))

  val ext = Map(
    Position("x") -> getDoubleContent(3),
    Position("y") -> getDoubleContent(4),
    Position("z") -> getDoubleContent(5)
  )

  def mutate(cell: Cell[P]): Option[Content] = cell.position(_0) match {
    case StringValue("x") => cell.content.toOption
    case StringValue("y") => getStringContent("not.supported").toOption
    case StringValue("z") => getLongContent(8).toOption
  }

  def mutateWithValue(cell: Cell[P], ext: W): Option[Content] =
    (cell.position(_0), ext(cell.position).value) match {
      case (StringValue("x"), DoubleValue(d)) => getDoubleContent(2 * d).toOption
      case (StringValue("y"), _) => getStringContent("not.supported").toOption
      case (StringValue("z"), DoubleValue(d)) => getLongContent(d.toLong).toOption
    }

  "A Transfomer" should "andThenMutate correctly" in {
    val obj = Power[P](2).andThenMutate(mutate)

    obj.present(str).toList shouldBe List(Cell(str.position, getDoubleContent(4)))
    obj.present(dbl).toList shouldBe List(Cell(dbl.position, getStringContent("not.supported")))
    obj.present(lng).toList shouldBe List(Cell(lng.position, getLongContent(8)))
  }

  it should "andThenMutateWithValue correctly (without value)" in {
    val obj = Multiply[P, W](ExtractWithPosition().andThenPresent(_.value.as[Double])).andThenMutate(mutate)

    obj.presentWithValue(str, ext).toList shouldBe List(Cell(str.position, getDoubleContent(2 * 3)))
    obj.presentWithValue(dbl, ext).toList shouldBe List(Cell(dbl.position, getStringContent("not.supported")))
    obj.presentWithValue(lng, ext).toList shouldBe List(Cell(lng.position, getLongContent(8)))
  }

  it should "andThenMutateWithValue correctly" in {
    val obj = Multiply[P, W](ExtractWithPosition().andThenPresent(_.value.as[Double]))
      .andThenMutateWithValue(mutateWithValue)

    obj.presentWithValue(str, ext).toList shouldBe List(Cell(str.position, getDoubleContent(2 * 3)))
    obj.presentWithValue(dbl, ext).toList shouldBe List(Cell(dbl.position, getStringContent("not.supported")))
    obj.presentWithValue(lng, ext).toList shouldBe List(Cell(lng.position, getLongContent(5)))
  }
}

