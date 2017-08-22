// Copyright 2017 Commonwealth Bank of Australia
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
import commbank.grimlock.framework.aggregate._
import commbank.grimlock.framework.content._
import commbank.grimlock.framework.distance._
import commbank.grimlock.framework.encoding._
import commbank.grimlock.framework.extract._
import commbank.grimlock.framework.environment._
import commbank.grimlock.framework.environment.tuner._
import commbank.grimlock.framework.metadata._
import commbank.grimlock.framework.pairwise._
import commbank.grimlock.framework.partition._
import commbank.grimlock.framework.position._
import commbank.grimlock.framework.sample._
import commbank.grimlock.framework.transform._
import commbank.grimlock.framework.window._

import commbank.grimlock.library.aggregate._
import commbank.grimlock.library.pairwise._
import commbank.grimlock.library.partition._
import commbank.grimlock.library.squash._
import commbank.grimlock.library.transform._
import commbank.grimlock.library.window._

import scala.io.Source

import shapeless.Nat
import shapeless.nat.{ _1, _2, _3 }
import shapeless.ops.nat.{ LTEq, ToInt }

object Shared {
  def test1[
    U[_],
    E[_]
  ](
    ctx: Context[U, E],
    data: U[Cell[_3]],
    path: String,
    tool: String
  )(implicit
    ev1: Persist.SaveAsTextTuner[U, Default[NoParameters]],
    ev2: Matrix.SetTuner[U, Default[NoParameters]],
    ev3: Matrix.SliceTuner[U, InMemory[NoParameters]]
  ): Unit = {
    import ctx.implicits.cell._
    import ctx.implicits.matrix._
    import ctx.implicits.position._

    data
      .saveAsText(s"./tmp.${tool}/dat1.out", Cell.toString(verbose = true), Default())
      .toUnit

    data
      .set(
        Cell(
          Position("iid:1548763", "fid:Y", DateCodec().decode("2014-04-26").get),
          Content(ContinuousSchema[Long](), 1234)
        ),
        Default()
      )
      .slice(Over(_1), InMemory())(true, "iid:1548763")
      .saveAsText(s"./tmp.${tool}/dat2.out", Cell.toString(verbose = true), Default())
      .toUnit

    ctx
      .loadText(path + "/smallInputfile.txt", Cell.parse3D(third = DateCodec()))
      .data
      .saveAsText(s"./tmp.${tool}/dat3.out", Cell.toString(verbose = true), Default())
      .toUnit
  }

  def test2[
    U[_],
    E[_]
  ](
    ctx: Context[U, E],
    data: U[Cell[_3]],
    path: String,
    tool: String
  )(implicit
    ev1: Positions.NamesTuner[U, Default[NoParameters]],
    ev2: Persist.SaveAsTextTuner[U, Default[NoParameters]],
    ev3: Matrix.SliceTuner[U, InMemory[NoParameters]]
  ): Unit = {
    import ctx.implicits.environment._
    import ctx.implicits.matrix._
    import ctx.implicits.position._

    (data.names(Over(_1), Default()) ++ data.names(Over(_2), Default()) ++ data.names(Over(_3), Default()))
      .saveAsText(s"./tmp.${tool}/nm0.out", Position.toString(verbose = true), Default())
      .toUnit

    data
      .names(Over(_2), Default())
      .slice(false, "fid:M")
      .saveAsText(s"./tmp.${tool}/nm2.out", Position.toString(verbose = true), Default())
      .toUnit

    data
      .names(Over(_2), Default())
      .slice(true, """.*[BCD]$""".r)
      .saveAsText(s"./tmp.${tool}/nm5.out", Position.toString(verbose = true), Default())
      .toUnit
  }

  def test3[
    U[_],
    E[_]
  ](
    ctx: Context[U, E],
    data: U[Cell[_3]],
    path: String,
    tool: String
  )(implicit
    ev1: Persist.SaveAsTextTuner[U, Default[NoParameters]],
    ev2: Matrix.TypesTuner[U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.environment._
    import ctx.implicits.matrix._

    (
      data.types(Over(_1), Default())(false) ++
      data.types(Over(_2), Default())(false) ++
      data.types(Over(_3), Default())(false)
    )
      .saveAsText(s"./tmp.${tool}/typ1.out", Cell.toString(verbose = true), Default())
      .toUnit

    (
      data.types(Over(_1), Default())(true) ++
      data.types(Over(_2), Default())(true) ++
      data.types(Over(_3), Default())(true)
    )
      .saveAsText(s"./tmp.${tool}/typ2.out", Cell.toString(verbose = true), Default())
      .toUnit
  }

  def test4[
    U[_],
    E[_]
  ](
    ctx: Context[U, E],
    data: U[Cell[_3]],
    path: String,
    tool: String
  )(implicit
    ev1: Positions.NamesTuner[U, Default[NoParameters]],
    ev2: Persist.SaveAsTextTuner[U, Default[NoParameters]],
    ev3: Matrix.SliceTuner[U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.matrix._
    import ctx.implicits.position._

    data
      .slice(Over(_2), Default())(true, "fid:B")
      .saveAsText(s"./tmp.${tool}/scl0.out", Cell.toString(verbose = true), Default())
      .toUnit

    data
      .slice(Over(_2), Default())(true, List("fid:A", "fid:B"))
      .slice(Over(_1), Default())(true, "iid:0221707")
      .saveAsText(s"./tmp.${tool}/scl1.out", Cell.toString(verbose = true), Default())
      .toUnit

    val rem = List(
      "fid:B",
      "fid:D",
      "fid:F",
      "fid:H",
      "fid:J",
      "fid:L",
      "fid:N",
      "fid:P",
      "fid:R",
      "fid:T",
      "fid:V",
      "fid:X",
      "fid:Z"
    )

    data
      .slice(Over(_2), Default())(false, data.names(Over(_2), Default()).slice(false, rem))
      .saveAsText(s"./tmp.${tool}/scl2.out", Cell.toString(verbose = true), Default())
      .toUnit
  }

  def test5[
    U[_],
    E[_]
  ](
    ctx: Context[U, E],
    data: U[Cell[_3]],
    path: String,
    tool: String
  )(implicit
    ev1: Positions.NamesTuner[U, Default[NoParameters]],
    ev2: Matrix.SaveAsCSVTuner[U, Default[NoParameters]],
    ev3: Persist.SaveAsTextTuner[U, Default[NoParameters]],
    ev4: Matrix.SliceTuner[U, Default[NoParameters]],
    ev5: Matrix.SquashTuner[U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.matrix._
    import ctx.implicits.position._

    data
      .slice(Over(_2), Default())(true, List("fid:A", "fid:B"))
      .slice(Over(_1), Default())(true, "iid:0221707")
      .squash(_3, PreservingMaximumPosition(), Default())
      .saveAsText(s"./tmp.${tool}/sqs1.out", Cell.toString(verbose = true), Default())
      .toUnit

    data
      .squash(_3, PreservingMaximumPosition(), Default())
      .saveAsText(s"./tmp.${tool}/sqs2.out", Cell.toString(verbose = true), Default())
      .toUnit

    val ids = List(
      "iid:0064402",
      "iid:0066848",
      "iid:0076357",
      "iid:0216406",
      "iid:0221707",
      "iid:0262443",
      "iid:0364354",
      "iid:0375226",
      "iid:0444510",
      "iid:1004305"
    )

    data
      .slice(Over(_1), Default())(true, ids)
      .squash(_3, PreservingMaximumPosition(), Default())
      .saveAsCSV(Over(_2), Default())(s"./tmp.${tool}/sqs3.out")
      .toUnit

    data
      .slice(Over(_1), Default())(true, ids)
      .slice(Over(_2), Default())(true, List("fid:A", "fid:B", "fid:C", "fid:D", "fid:E", "fid:F", "fid:G"))
      .squash(_3, PreservingMaximumPosition(), Default())
      .saveAsCSV(Over(_2), Default())(s"./tmp.${tool}/sqs4.out")
      .toUnit
  }

  def test6[
    U[_],
    E[_]
  ](
    ctx: Context[U, E],
    data: U[Cell[_3]],
    path: String,
    tool: String
  )(implicit
    ev1: Matrix.GetTuner[U, Default[NoParameters]],
    ev2: Persist.SaveAsTextTuner[U, Default[NoParameters]],
    ev3: Matrix.SliceTuner[U, Default[NoParameters]],
    ev4: Matrix.SquashTuner[U, Default[NoParameters]],
    ev5: Matrix.SummariseTuner[U, Default[NoParameters]],
    ev6: Matrix.WhichTuner[U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.matrix._
    import ctx.implicits.position._

    data
      .which(c => c.content.schema.classification.isOfType(NumericType))
      .saveAsText(s"./tmp.${tool}/whc1.out", Position.toString(verbose = true), Default())
      .toUnit

    data
      .which(c => !c.content.value.isInstanceOf[StringValue])
      .saveAsText(s"./tmp.${tool}/whc2.out", Position.toString(verbose = true), Default())
      .toUnit

    data
      .get(
        data.which(c => (c.content.value equ 666) || (c.content.value leq 11.0) || (c.content.value equ "KQUPKFEH")),
        Default()
      )
      .saveAsText(s"./tmp.${tool}/whc3.out", Cell.toString(verbose = true), Default())
      .toUnit

    data
      .which(c => c.content.value.isInstanceOf[LongValue])
      .saveAsText(s"./tmp.${tool}/whc4.out", Position.toString(verbose = true), Default())
      .toUnit

    val aggregators: List[Aggregator[_2, _1, _2]] = List(
      Counts().andThenRelocate(_.position.append("count").toOption),
      Mean().andThenRelocate(_.position.append("mean").toOption),
      Minimum().andThenRelocate(_.position.append("min").toOption),
      Maximum().andThenRelocate(_.position.append("max").toOption),
      MaximumAbsolute().andThenRelocate(_.position.append("max.abs").toOption)
    )

    val ids = List(
      "iid:0064402",
      "iid:0066848",
      "iid:0076357",
      "iid:0216406",
      "iid:0221707",
      "iid:0262443",
      "iid:0364354",
      "iid:0375226",
      "iid:0444510",
      "iid:1004305"
    )

    data
      .slice(Over(_1), Default())(true, ids)
      .slice(Over(_2), Default())(true, List("fid:A", "fid:B", "fid:C", "fid:D", "fid:E", "fid:F", "fid:G"))
      .squash(_3, PreservingMaximumPosition(), Default())
      .summarise(Along(_1), Default())(aggregators)
      .whichByPosition(
        Over(_2),
        Default()
      )(
        List(("count", c => c.content.value leq 2), ("min", c => c.content.value equ 107))
      )
      .saveAsText(s"./tmp.${tool}/whc5.out", Position.toString(verbose = true), Default())
      .toUnit
  }

  def test7[
    U[_],
    E[_]
  ](
    ctx: Context[U, E],
    data: U[Cell[_3]],
    path: String,
    tool: String
  )(implicit
    ev1: Matrix.GetTuner[U, Default[NoParameters]],
    ev2: Persist.SaveAsTextTuner[U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.matrix._
    import ctx.implicits.position._

    data
      .get(Position("iid:1548763", "fid:Y", DateCodec().decode("2014-04-26").get), Default())
      .saveAsText(s"./tmp.${tool}/get1.out", Cell.toString(verbose = true), Default())
      .toUnit

    data
      .get(
        List(
          Position("iid:1548763", "fid:Y", DateCodec().decode("2014-04-26").get),
          Position("iid:1303823", "fid:A", DateCodec().decode("2014-05-05").get)
        ),
        Default()
      )
      .saveAsText(s"./tmp.${tool}/get2.out", Cell.toString(verbose = true), Default())
      .toUnit
  }

  def test8[
    U[_],
    E[_]
  ](
    ctx: Context[U, E],
    data: U[Cell[_3]],
    path: String,
    tool: String
  )(implicit
    ev1: Matrix.GetTuner[U, Default[NoParameters]],
    ev2: Matrix.SaveAsCSVTuner[U, Default[NoParameters]],
    ev3: Persist.SaveAsTextTuner[U, Default[NoParameters]],
    ev4: Matrix.SliceTuner[U, Default[NoParameters]],
    ev5: Matrix.SquashTuner[U, Default[NoParameters]],
    ev6: Matrix.UniqueTuner[U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.content._
    import ctx.implicits.matrix._
    import ctx.implicits.position._

    data
      .slice(Over(_2), Default())(true, "fid:B")
      .squash(_3, PreservingMaximumPosition(), Default())
      .unique(Default())
      .saveAsText(s"./tmp.${tool}/uniq.out", Content.toString(verbose = true), Default())
      .toUnit

    ctx
      .loadText(path + "/mutualInputfile.txt", Cell.parse2D())
      .data
      .uniqueByPosition(Over(_2), Default())
      .saveAsText(s"./tmp.${tool}/uni2.out", IndexedContents.toString(descriptive = false), Default())
      .toUnit

    data
      .slice(Over(_2), Default())(true, List("fid:A", "fid:B", "fid:Y", "fid:Z"))
      .slice(Over(_1), Default())(true, List("iid:0221707", "iid:0364354"))
      .squash(_3, PreservingMaximumPosition(), Default())
      .saveAsCSV(Over(_1), Default())(s"./tmp.${tool}/test.csv")
      .saveAsCSV(Over(_2), Default())(s"./tmp.${tool}/tset.csv", writeHeader = false, separator = ",")
      .toUnit

    data
      .slice(Over(_2), Default())(true, List("fid:A", "fid:B", "fid:Y", "fid:Z"))
      .slice(Over(_1), Default())(true, List("iid:0221707", "iid:0364354"))
      .squash(_3, PreservingMaximumPosition(), Default())
      .permute(_2, _1)
      .saveAsText(s"./tmp.${tool}/trs1.out", Cell.toString(verbose = true), Default())
      .toUnit

    data
      .slice(Over(_2), Default())(true, List("fid:A", "fid:B", "fid:Y", "fid:Z"))
      .slice(Over(_1), Default())(true, List("iid:0221707", "iid:0364354"))
      .squash(_3, PreservingMaximumPosition(), Default())
      .saveAsText(s"./tmp.${tool}/data.txt", tuner = Default())
      .toUnit
  }

  def test9[
    U[_],
    E[_]
  ](
    ctx: Context[U, E],
    data: U[Cell[_3]],
    path: String,
    tool: String
  )(implicit
    ev1: Persist.SaveAsTextTuner[U, Default[NoParameters]],
    ev2: Matrix.SliceTuner[U, Default[NoParameters]],
    ev3: Matrix.SquashTuner[U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.matrix._
    import ctx.implicits.partition._
    import ctx.implicits.position._

    case class StringPartitioner[D <: Nat : ToInt](dim: D)(implicit ev: LTEq[D, _2]) extends Partitioner[_2, String] {
      def assign(cell: Cell[_2]): TraversableOnce[String] = List(cell.position(dim) match {
        case StringValue("fid:A", _) => "training"
        case StringValue("fid:B", _) => "testing"
      }, "scoring")
    }

    val prt1 = data
      .slice(Over(_2), Default())(true, List("fid:A", "fid:B"))
      .slice(Over(_1), Default())(true, List("iid:0221707", "iid:0364354"))
      .squash(_3, PreservingMaximumPosition(), Default())
      .split(StringPartitioner(_2))

    prt1
      .saveAsText(s"./tmp.${tool}/prt1.out", Partitions.toString(verbose = true), Default())
      .toUnit

    case class IntTuplePartitioner[
      D <: Nat : ToInt
    ](
      dim: D
    )(implicit
      ev: LTEq[D, _2]
    ) extends Partitioner[_2, (Int, Int, Int)] {
      def assign(cell: Cell[_2]): TraversableOnce[(Int, Int, Int)] = List(cell.position(dim) match {
        case StringValue("fid:A", _) => (1, 0, 0)
        case StringValue("fid:B", _) => (0, 1, 0)
      }, (0, 0, 1))
    }

    data
      .slice(Over(_2), Default())(true, List("fid:A", "fid:B"))
      .slice(Over(_1), Default())(true, List("iid:0221707", "iid:0364354"))
      .squash(_3, PreservingMaximumPosition(), Default())
      .split(IntTuplePartitioner(_2))
      .saveAsText(s"./tmp.${tool}/prt2.out", Partitions.toString(verbose = true), Default())
      .toUnit

    prt1
      .get("training")
      .saveAsText(s"./tmp.${tool}/train.out", Cell.toString(verbose = true), Default())
      .toUnit

    prt1
      .get("testing")
      .saveAsText(s"./tmp.${tool}/test.out", Cell.toString(verbose = true), Default())
      .toUnit

    prt1
      .get("scoring")
      .saveAsText(s"./tmp.${tool}/score.out", Cell.toString(verbose = true), Default())
      .toUnit
  }

  def test10[
    U[_],
    E[_]
  ](
    ctx: Context[U, E],
    data: U[Cell[_3]],
    path: String,
    tool: String
  )(implicit
    ev1: Matrix.SaveAsCSVTuner[U, Default[NoParameters]],
    ev2: Matrix.SliceTuner[U, Default[NoParameters]],
    ev3: Matrix.SquashTuner[U, Default[NoParameters]],
    ev4: Matrix.SummariseTuner[U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.matrix._
    import ctx.implicits.position._

    data
      .summarise(Over(_2), Default())(Mean(false, true, true).andThenRelocate(_.position.append("mean").toOption))
      .saveAsCSV(Over(_1), Default())(s"./tmp.${tool}/agg1.csv")
      .toUnit

    val ids = List(
      "iid:0064402",
      "iid:0066848",
      "iid:0076357",
      "iid:0216406",
      "iid:0221707",
      "iid:0262443",
      "iid:0364354",
      "iid:0375226",
      "iid:0444510",
      "iid:1004305"
    )

    data
      .slice(Over(_1), Default())(true, ids)
      .squash(_3, PreservingMaximumPosition(), Default())
      .summarise(Along(_2), Default())(Counts().andThenRelocate(_.position.append("count").toOption))
      .saveAsCSV(Over(_1), Default())(s"./tmp.${tool}/agg2.csv")
      .toUnit

    val aggregators: List[Aggregator[_2, _1, _2]] = List(
      Counts().andThenRelocate(_.position.append("count").toOption),
      Mean().andThenRelocate(_.position.append("mean").toOption),
      StandardDeviation(biased = true).andThenRelocate(_.position.append("sd").toOption),
      Skewness().andThenRelocate(_.position.append("skewness").toOption),
      Kurtosis().andThenRelocate(_.position.append("kurtosis").toOption),
      Minimum().andThenRelocate(_.position.append("min").toOption),
      Maximum().andThenRelocate(_.position.append("max").toOption),
      MaximumAbsolute().andThenRelocate(_.position.append("max.abs").toOption)
    )

    data
      .slice(Over(_1), Default())(true, ids)
      .squash(_3, PreservingMaximumPosition(), Default())
      .summarise(Along(_1), Default())(aggregators)
      .saveAsCSV(Over(_1), Default())(s"./tmp.${tool}/agg3.csv")
      .toUnit
  }

  def test11[
    U[_],
    E[_]
  ](
    ctx: Context[U, E],
    data: U[Cell[_3]],
    path: String,
    tool: String
  )(implicit
    ev1: Matrix.SaveAsCSVTuner[U, Default[NoParameters]],
    ev2: Persist.SaveAsTextTuner[U, Default[NoParameters]],
    ev3: Matrix.SliceTuner[U, Default[NoParameters]],
    ev4: Matrix.SquashTuner[U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.matrix._
    import ctx.implicits.position._

    data
      .slice(Over(_2), Default())(true, List("fid:A", "fid:B", "fid:Y", "fid:Z"))
      .slice(Over(_1), Default())(true, List("iid:0221707", "iid:0364354"))
      .transform(Indicator().andThenRelocate(Locate.RenameDimension(_2, "%1$s.ind")))
      .saveAsText(s"./tmp.${tool}/trn2.out", Cell.toString(verbose = true), Default())
      .toUnit

    data
      .slice(Over(_2), Default())(true, List("fid:A", "fid:B", "fid:Y", "fid:Z"))
      .slice(Over(_1), Default())(true, List("iid:0221707", "iid:0364354"))
      .squash(_3, PreservingMaximumPosition(), Default())
      .transform(Binarise(Locate.RenameDimensionWithContent(_2)))
      .saveAsCSV(Over(_1), Default())(s"./tmp.${tool}/trn3.out")
      .toUnit
  }

  def test12[
    U[_],
    E[_]
  ](
    ctx: Context[U, E],
    data: U[Cell[_3]],
    path: String,
    tool: String
  )(implicit
    ev1: Matrix.FillHomogeneousTuner[U, Default[NoParameters]],
    ev2: Matrix.SaveAsCSVTuner[U, Default[NoParameters]],
    ev3: Persist.SaveAsTextTuner[U, Default[NoParameters]],
    ev4: Matrix.SliceTuner[U, Default[NoParameters]],
    ev5: Matrix.SquashTuner[U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.matrix._
    import ctx.implicits.position._

    val sliced = data
      .slice(Over(_2), Default())(true, List("fid:A", "fid:B", "fid:Y", "fid:Z"))
      .slice(Over(_1), Default())(true, List("iid:0221707", "iid:0364354"))

    sliced
      .squash(_3, PreservingMaximumPosition(), Default())
      .fillHomogeneous(Content(ContinuousSchema[Long](), 0), Default())
      .saveAsCSV(Over(_1), Default())(s"./tmp.${tool}/fll1.out")
      .toUnit

    sliced
      .fillHomogeneous(Content(ContinuousSchema[Long](), 0), Default())
      .saveAsText(s"./tmp.${tool}/fll3.out", Cell.toString(verbose = true), Default())
      .toUnit
  }

  def test13[
    U[_],
    E[_]
  ](
    ctx: Context[U, E],
    data: U[Cell[_3]],
    path: String,
    tool: String
  )(implicit
    ev1: Matrix.FillHeterogeneousTuner[U, Default[NoParameters]],
    ev2: Matrix.FillHomogeneousTuner[U, Default[NoParameters]],
    ev3: Matrix.JoinTuner[U, Default[NoParameters]],
    ev4: Matrix.SaveAsCSVTuner[U, Default[NoParameters]],
    ev5: Persist.SaveAsTextTuner[U, Default[NoParameters]],
    ev6: Matrix.SliceTuner[U, Default[NoParameters]],
    ev7: Matrix.SquashTuner[U, Default[NoParameters]],
    ev8: Matrix.SummariseTuner[U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.matrix._
    import ctx.implicits.position._

    val ids = List(
      "iid:0064402",
      "iid:0066848",
      "iid:0076357",
      "iid:0216406",
      "iid:0221707",
      "iid:0262443",
      "iid:0364354",
      "iid:0375226",
      "iid:0444510",
      "iid:1004305"
    )

    val sliced = data
      .slice(Over(_1), Default())(true, ids)
      .slice(Over(_2), Default())(true, List("fid:A", "fid:B", "fid:C", "fid:D", "fid:E", "fid:F", "fid:G"))
      .squash(_3, PreservingMaximumPosition(), Default())

    val inds = sliced
      .transform(Indicator().andThenRelocate(Locate.RenameDimension(_2, "%1$s.ind")))
      .fillHomogeneous(Content(ContinuousSchema[Long](), 0), Default())

    sliced
      .join(Over(_1), Default())(inds)
      .fillHomogeneous(Content(ContinuousSchema[Long](), 0), Default())
      .saveAsCSV(Over(_1), Default())(s"./tmp.${tool}/fll2.out")
      .toUnit

    sliced
      .fillHeterogeneous(Over(_2), Default())(data.summarise(Over(_2), Default())(Mean(false, true, true)))
      .join(Over(_1), Default())(inds)
      .saveAsCSV(Over(_1), Default())(s"./tmp.${tool}/fll4.out")
      .toUnit
  }

  def test14[
    U[_],
    E[_]
  ](
    ctx: Context[U, E],
    data: U[Cell[_3]],
    path: String,
    tool: String
  )(implicit
    ev1: Matrix.ChangeTuner[U, Default[NoParameters]],
    ev2: Persist.SaveAsTextTuner[U, Default[NoParameters]],
    ev3: Matrix.SliceTuner[U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.matrix._
    import ctx.implicits.position._

    data
      .slice(Over(_2), Default())(true, List("fid:A", "fid:B", "fid:Y", "fid:Z"))
      .slice(Over(_1), Default())(true, List("iid:0221707", "iid:0364354"))
      .change(Over(_2), Default())("fid:A", Content.parser(LongCodec, NominalSchema[Long]()))
      .data
      .saveAsText(s"./tmp.${tool}/chg1.out", Cell.toString(verbose = true), Default())
      .toUnit
  }

  def test15[
    U[_],
    E[_]
  ](
    ctx: Context[U, E],
    data: U[Cell[_3]],
    path: String,
    tool: String
  )(implicit
    ev1: Matrix.JoinTuner[U, Default[NoParameters]],
    ev2: Matrix.SaveAsCSVTuner[U, Default[NoParameters]],
    ev3: Matrix.SliceTuner[U, Default[NoParameters]],
    ev4: Matrix.SquashTuner[U, Default[NoParameters]],
    ev5: Matrix.SummariseTuner[U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.matrix._
    import ctx.implicits.position._

    data
      .slice(Over(_2), Default())(true, List("fid:A", "fid:C", "fid:E", "fid:G"))
      .slice(Over(_1), Default())(true, List("iid:0221707", "iid:0364354"))
      .summarise(Along(_3), Default())(Sums().andThenRelocate(_.position.append("sum").toOption))
      .melt(_3, _2, Value.concatenate("."))
      .saveAsCSV(Over(_1), Default())(s"./tmp.${tool}/rsh1.out")
      .toUnit

    val ids = List(
      "iid:0064402",
      "iid:0066848",
      "iid:0076357",
      "iid:0216406",
      "iid:0221707",
      "iid:0262443",
      "iid:0364354",
      "iid:0375226",
      "iid:0444510",
      "iid:1004305"
    )

    val inds = data
      .slice(Over(_1), Default())(true, ids)
      .slice(Over(_2), Default())(true, List("fid:A", "fid:B", "fid:C", "fid:D", "fid:E", "fid:F", "fid:G"))
      .squash(_3, PreservingMaximumPosition(), Default())
      .transform(Indicator().andThenRelocate(Locate.RenameDimension(_2, "%1$s.ind")))
      .saveAsCSV(Over(_1), Default())(s"./tmp.${tool}/trn1.csv")

    data
      .slice(Over(_1), Default())(true, ids)
      .slice(Over(_2), Default())(true, List("fid:A", "fid:B", "fid:C", "fid:D", "fid:E", "fid:F", "fid:G"))
      .squash(_3, PreservingMaximumPosition(), Default())
      .join(Over(_1), Default())(inds)
      .saveAsCSV(Over(_1), Default())(s"./tmp.${tool}/jn1.csv")
      .toUnit
  }

  def test16[
    U[_],
    E[_]
  ](
    ctx: Context[U, E],
    data: U[Cell[_3]],
    path: String,
    tool: String
  )(implicit
    ev: Persist.SaveAsTextTuner[U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.matrix._

    case class HashSample() extends Sampler[_3] {
      def select(cell: Cell[_3]): Boolean = (cell.position(_1).toString.hashCode % 25) == 0
    }

    data
      .subset(HashSample())
      .saveAsText(s"./tmp.${tool}/smp1.out", tuner = Default())
      .toUnit
  }

  def test17[
    U[_],
    E[_]
  ](
    ctx: Context[U, E],
    data: U[Cell[_3]],
    path: String,
    tool: String
  )(implicit
    ev1: Matrix.CompactTuner[U, Default[NoParameters]],
    ev2: Matrix.SaveAsCSVTuner[U, Default[NoParameters]],
    ev3: Matrix.SliceTuner[U, Default[NoParameters]],
    ev4: Matrix.SquashTuner[U, Default[NoParameters]],
    ev5: Matrix.SummariseTuner[U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.matrix._
    import ctx.implicits.position._

    val ids = List(
      "iid:0064402",
      "iid:0066848",
      "iid:0076357",
      "iid:0216406",
      "iid:0221707",
      "iid:0262443",
      "iid:0364354",
      "iid:0375226",
      "iid:0444510",
      "iid:1004305"
    )

    val sliced = data
      .slice(Over(_1), Default())(true, ids)
      .slice(Over(_2), Default())(true, List("fid:A", "fid:B", "fid:C", "fid:D", "fid:E", "fid:F", "fid:G"))
      .squash(_3, PreservingMaximumPosition(), Default())

    val aggregators: List[Aggregator[_2, _1, _2]] = List(
      Counts().andThenRelocate(_.position.append("count").toOption),
      Mean().andThenRelocate(_.position.append("mean").toOption),
      Minimum().andThenRelocate(_.position.append("min").toOption),
      Maximum().andThenRelocate(_.position.append("max").toOption),
      MaximumAbsolute().andThenRelocate(_.position.append("max.abs").toOption)
    )

    val stats = sliced
      .summarise(Along(_1), Default())(aggregators)
      .compact(Over(_1), Default())

    sliced
      .transformWithValue(
        stats,
        Normalise(ExtractWithDimensionAndKey[_2, Content](_2, "max.abs").andThenPresent(_.value.asDouble))
      )
      .saveAsCSV(Over(_1), Default())(s"./tmp.${tool}/trn6.csv")
      .toUnit

    case class Sample500() extends Sampler[_2] {
      def select(cell: Cell[_2]): Boolean = cell.content.value gtr 500
    }

    sliced
      .subset(Sample500())
      .saveAsCSV(Over(_1), Default())(s"./tmp.${tool}/flt1.csv")
      .toUnit

    case class RemoveGreaterThanMean[D <: Nat : ToInt](dim: D)(implicit ev: LTEq[D, _2]) extends SamplerWithValue[_2] {
      type V = Map[Position[_1], Map[Position[_1], Content]]

      def selectWithValue(cell: Cell[_2], ext: V): Boolean =
        if (cell.content.schema.classification.isOfType(NumericType))
          cell.content.value leq ext(Position(cell.position(dim)))(Position("mean")).value
        else
          true
    }

    sliced
      .subsetWithValue(stats, RemoveGreaterThanMean(_2))
      .saveAsCSV(Over(_1), Default())(s"./tmp.${tool}/flt2.csv")
      .toUnit
  }

  def test18[
    U[_],
    E[_]
  ](
    ctx: Context[U, E],
    data: U[Cell[_3]],
    path: String,
    tool: String
  )(implicit
    ev1: Positions.NamesTuner[U, Default[NoParameters]],
    ev2: Matrix.SaveAsCSVTuner[U, Default[NoParameters]],
    ev3: Matrix.SliceTuner[U, Default[NoParameters]],
    ev4: Matrix.SquashTuner[U, Default[NoParameters]],
    ev5: Matrix.SummariseTuner[U, Default[NoParameters]],
    ev6: Matrix.WhichTuner[U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.matrix._
    import ctx.implicits.position._

    val ids = List(
      "iid:0064402",
      "iid:0066848",
      "iid:0076357",
      "iid:0216406",
      "iid:0221707",
      "iid:0262443",
      "iid:0364354",
      "iid:0375226",
      "iid:0444510",
      "iid:1004305"
    )

    val sliced = data
      .slice(Over(_1), Default())(true, ids)
      .slice(Over(_2), Default())(true, List("fid:A", "fid:B", "fid:C", "fid:D", "fid:E", "fid:F", "fid:G"))
      .squash(_3, PreservingMaximumPosition(), Default())

    val aggregators: List[Aggregator[_2, _1, _2]] = List(
      Counts().andThenRelocate(_.position.append("count").toOption),
      Mean().andThenRelocate(_.position.append("mean").toOption),
      Minimum().andThenRelocate(_.position.append("min").toOption),
      Maximum().andThenRelocate(_.position.append("max").toOption),
      MaximumAbsolute().andThenRelocate(_.position.append("max.abs").toOption)
    )

    val stats = sliced
      .summarise(Along(_1), Default())(aggregators)

    val rem = stats
      .whichByPosition(Over(_2), Default())(("count", (c: Cell[_2]) => c.content.value leq 2))
      .names(Over(_1), Default())

    sliced
      .slice(Over(_2), Default())(false, rem)
      .saveAsCSV(Over(_1), Default())(s"./tmp.${tool}/flt3.csv")
      .toUnit
  }

  def test19[
    U[_],
    E[_]
  ](
    ctx: Context[U, E],
    data: U[Cell[_3]],
    path: String,
    tool: String
  )(implicit
    ev1: Matrix.CompactTuner[U, Default[NoParameters]],
    ev2: Matrix.FillHomogeneousTuner[U, Default[NoParameters]],
    ev3: Positions.NamesTuner[U, Default[NoParameters]],
    ev4: Matrix.SaveAsCSVTuner[U, Default[NoParameters]],
    ev5: Matrix.SliceTuner[U, Default[NoParameters]],
    ev6: Matrix.SquashTuner[U, Default[NoParameters]],
    ev7: Matrix.SummariseTuner[U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.matrix._
    import ctx.implicits.partition._
    import ctx.implicits.position._

    val ids = List(
      "iid:0064402",
      "iid:0066848",
      "iid:0076357",
      "iid:0216406",
      "iid:0221707",
      "iid:0262443",
      "iid:0364354",
      "iid:0375226",
      "iid:0444510",
      "iid:1004305"
    )

    val raw = data
      .slice(Over(_1), Default())(true, ids)
      .slice(Over(_2), Default())(true, List("fid:A", "fid:B", "fid:C", "fid:D", "fid:E", "fid:F", "fid:G"))
      .squash(_3, PreservingMaximumPosition(), Default())

    case class CustomPartition[
      D <: Nat : ToInt
    ](
      dim: D,
      left: String,
      right: String
    )(implicit
      ev: LTEq[D, _2]
    ) extends Partitioner[_2, String] {
      val bhs = BinaryHashSplit(dim, 7, left, right, base = 10)

      def assign(cell: Cell[_2]): TraversableOnce[String] =
        if (cell.position(dim).toShortString == "iid:0364354")
          List(right)
        else
          bhs.assign(cell)
    }

    val parts = raw
      .split(CustomPartition(_1, "train", "test"))

    val aggregators: List[Aggregator[_2, _1, _2]] = List(
      Counts().andThenRelocate(_.position.append("count").toOption),
      MaximumAbsolute().andThenRelocate(_.position.append("max.abs").toOption)
    )

    val stats = parts
      .get("train")
      .summarise(Along(_1), Default())(aggregators)

    val rem = stats
      .which(c => (c.position(_2) equ "count") && (c.content.value leq 2))
      .names(Over(_1), Default())

    type W = Map[Position[_1], Map[Position[_1], Content]]

    val transforms: List[TransformerWithValue[_2, _2] { type V >: W }] = List(
      Indicator().andThenRelocate(Locate.RenameDimension(_2, "%1$s.ind")),
      Binarise(Locate.RenameDimensionWithContent(_2)),
      Normalise(ExtractWithDimensionAndKey[_2, Content](_2, "max.abs").andThenPresent(_.value.asDouble))
    )

    def cb(key: String, pipe: U[Cell[_2]]): U[Cell[_2]] = pipe
      .slice(Over(_2), Default())(false, rem)
      .transformWithValue(stats.compact(Over(_1), Default()), transforms)
      .fillHomogeneous(Content(ContinuousSchema[Long](), 0), Default())
      .saveAsCSV(Over(_1), Default())(s"./tmp.${tool}/pln_" + key + ".csv")

    parts
      .forEach(List("train", "test"), cb)
      .toUnit
  }

  def test20[
    U[_],
    E[_]
  ](
    ctx: Context[U, E],
    path: String,
    tool: String
  )(implicit
    ev: Persist.SaveAsTextTuner[U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.matrix._

    val (dictionary, _) = Dictionary.load(Source.fromFile(path + "/dict.txt"))

    ctx
      .loadText(path + "/ivoryInputfile1.txt", Cell.parse3DWithDictionary(dictionary, _2, third = DateCodec()))
      .data
      .saveAsText(s"./tmp.${tool}/ivr1.out", tuner = Default())
      .toUnit
  }

  def test21[
    U[_],
    E[_]
  ](
    ctx: Context[U, E],
    data: U[Cell[_3]],
    path: String,
    tool: String
  )(implicit
    ev1: Persist.SaveAsTextTuner[U, Default[NoParameters]],
    ev2: Matrix.ShapeTuner[U, Default[NoParameters]],
    ev3: Matrix.SizeTuner[U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.matrix._

    data
      .shape(Default())
      .saveAsText(s"./tmp.${tool}/siz0.out", tuner = Default())
      .toUnit

    data
      .size(_1, tuner = Default())
      .saveAsText(s"./tmp.${tool}/siz1.out", tuner = Default())
      .toUnit

    data
      .size(_2, tuner = Default())
      .saveAsText(s"./tmp.${tool}/siz2.out", tuner = Default())
      .toUnit

    data
      .size(_3, tuner = Default())
      .saveAsText(s"./tmp.${tool}/siz3.out", tuner = Default())
      .toUnit
  }

  def test22[
    U[_],
    E[_]
  ](
    ctx: Context[U, E],
    path: String,
    tool: String
  )(implicit
    ev1: Persist.SaveAsTextTuner[U, Default[NoParameters]],
    ev2: Matrix.SlideTuner[U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.matrix._

    val (data, _) = ctx.loadText(path + "/numericInputfile.txt", Cell.parse2D())

    case class Diff() extends Window[_2, _1, _1, _2] {
      type I = Option[Double]
      type T = (Option[Double], Position[_1])
      type O = (Double, Position[_1], Position[_1])

      def prepare(cell: Cell[_2]): I = cell.content.value.asDouble

      def initialise(rem: Position[_1], in: I): (T, TraversableOnce[O]) = ((in, rem), List())

      def update(rem: Position[_1], in: I, t: T): (T, TraversableOnce[O]) = ((in, rem), (in, t._1) match {
        case (Some(c), Some(l)) => List((c - l, rem,  t._2))
        case _ => List()
      })

      def present(pos: Position[_1], out: O): TraversableOnce[Cell[_2]] = List(
        Cell(
          pos.append(out._2.toShortString("") + "-" + out._3.toShortString("")),
          Content(ContinuousSchema[Double](), out._1)
        )
      )
    }

    data
      .slide(Over(_1), Default())(true, Diff())
      .saveAsText(s"./tmp.${tool}/dif1.out", tuner = Default())
      .toUnit

    data
      .slide(Over(_2), Default())(true, Diff())
      .permute(_2, _1)
      .saveAsText(s"./tmp.${tool}/dif2.out", tuner = Default())
      .toUnit
  }

  def test23[
    U[_],
    E[_]
  ](
    ctx: Context[U, E],
    path: String,
    tool: String
  )(implicit
    ev1: Matrix.PairwiseTuner[U, Default[NoParameters]],
    ev2: Persist.SaveAsTextTuner[U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.matrix._

    val (data, _) = ctx.loadText(path + "/somePairwise.txt", Cell.parse2D())

    case class DiffSquared() extends Operator[_2, _2] {
      def compute(left: Cell[_2], right: Cell[_2]): TraversableOnce[Cell[_2]] = {
        val xc = left.position(_2).toShortString
        val yc = right.position(_2).toShortString

        if (left.position(_1) == right.position(_1))
          List(
            Cell(
              right.position.remove(_2).append("(" + xc + "-" + yc + ")^2"),
              Content(
                ContinuousSchema[Double](),
                math.pow(left.content.value.asLong.get - right.content.value.asLong.get, 2)
              )
            )
          )
        else
          List()
      }
    }

    data
      .pairwise(Over(_2), Default())(Upper, DiffSquared())
      .saveAsText(s"./tmp.${tool}/pws1.out", tuner = Default())
      .toUnit
  }

  def test24[
    U[_],
    E[_]
  ](
    ctx: Context[U, E],
    path: String,
    tool: String
  )(implicit
    ev1: PairwiseDistance.CorrelationTuner[U, Default[NoParameters]],
    ev2: Persist.SaveAsTextTuner[U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.matrix._

    // see http://www.mathsisfun.com/data/correlation.html for data

    val schema = List(
      ("day", Content.parser(StringCodec, NominalSchema[String]())),
      ("temperature", Content.parser(DoubleCodec, ContinuousSchema[Double]())),
      ("sales", Content.parser(LongCodec, DiscreteSchema[Long]()))
    )
    val (data, _) = ctx.loadText(path + "/somePairwise2.txt", Cell.parseTable(schema, separator = "|"))

    def locate[P <: Nat] = (l: Position[P], r: Position[P]) => Option(
      Position(s"(${l.toShortString("|")}*${r.toShortString("|")})")
    )

    data
      .correlation(Over(_2), Default())(locate, true)
      .saveAsText(s"./tmp.${tool}/pws2.out", tuner = Default())
      .toUnit

    val schema2 = List(
      ("day", Content.parser(StringCodec, NominalSchema[String]())),
      ("temperature", Content.parser(DoubleCodec, ContinuousSchema[Double]())),
      ("sales", Content.parser(LongCodec, DiscreteSchema[Long]())),
      ("neg.sales", Content.parser(LongCodec, DiscreteSchema[Long]()))
    )
    val (data2, _) = ctx.loadText(path + "/somePairwise3.txt", Cell.parseTable(schema2, separator = "|"))

    data2
      .correlation(Over(_2), Default())(locate, true)
      .saveAsText(s"./tmp.${tool}/pws3.out", tuner = Default())
      .toUnit
  }

  def test25[
    U[_],
    E[_]
  ](
    ctx: Context[U, E],
    path: String,
    tool: String
  )(implicit
    ev1: PairwiseDistance.MutualInformationTuner[U, Default[NoParameters]],
    ev2: Persist.SaveAsTextTuner[U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.matrix._

    // see http://www.eecs.harvard.edu/cs286r/courses/fall10/papers/Chapter2.pdf example 2.2.1 for data

    def locate[P <: Nat] = (l: Position[P], r: Position[P]) => Option(
      Position(s"${r.toShortString("|")},${l.toShortString("|")}")
    )

    ctx
      .loadText(path + "/mutualInputfile.txt", Cell.parse2D())
      .data
      .mutualInformation(Over(_2), Default())(locate, true)
      .saveAsText(s"./tmp.${tool}/mi.out", tuner = Default())
      .toUnit

    ctx
      .loadText(path + "/mutualInputfile.txt", Cell.parse2D())
      .data
      .mutualInformation(Along(_1), Default())(locate, true)
      .saveAsText(s"./tmp.${tool}/im.out", tuner = Default())
      .toUnit
  }

  def test26[
    U[_],
    E[_]
  ](
    ctx: Context[U, E],
    path: String,
    tool: String
  )(implicit
    ev1: Matrix.PairwiseTuner[U, Default[NoParameters]],
    ev2: Persist.SaveAsTextTuner[U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.matrix._

    val (left, _) = ctx.loadText(path + "/algebraInputfile1.txt", Cell.parse2D())
    val (right, _) = ctx.loadText(path + "/algebraInputfile2.txt", Cell.parse2D())

    left
      .pairwiseBetween(Over(_1), Default())(
        All,
        right,
        Times(Locate.PrependPairwiseSelectedStringToRemainder(Over(_1), "(%1$s*%2$s)"))
      )
      .saveAsText(s"./tmp.${tool}/alg.out", tuner = Default())
      .toUnit
  }

  def test27[
    U[_],
    E[_]
  ](
    ctx: Context[U, E],
    path: String,
    tool: String
  )(implicit
    ev1: Persist.SaveAsTextTuner[U, Default[NoParameters]],
    ev2: Matrix.SlideTuner[U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.matrix._

    // http://www.statisticshowto.com/moving-average/

    ctx
      .loadText(path + "/simMovAvgInputfile.txt", Cell.parse2D(first = LongCodec))
      .data
      .slide(Over(_2), Default())(true, SimpleMovingAverage(5, Locate.AppendRemainderDimension(_1)))
      .saveAsText(s"./tmp.${tool}/sma1.out", tuner = Default())
      .toUnit

    ctx
      .loadText(path + "/simMovAvgInputfile.txt", Cell.parse2D(first = LongCodec))
      .data
      .slide(Over(_2), Default())(true, SimpleMovingAverage(5, Locate.AppendRemainderDimension(_1), all = true))
      .saveAsText(s"./tmp.${tool}/sma2.out", tuner = Default())
      .toUnit

    ctx
      .loadText(path + "/simMovAvgInputfile.txt", Cell.parse2D(first = LongCodec))
      .data
      .slide(Over(_2), Default())(true, CenteredMovingAverage(2, Locate.AppendRemainderDimension(_1)))
      .saveAsText(s"./tmp.${tool}/tma.out", tuner = Default())
      .toUnit

    ctx
      .loadText(path + "/simMovAvgInputfile.txt", Cell.parse2D(first = LongCodec))
      .data
      .slide(Over(_2), Default())(true, WeightedMovingAverage(5, Locate.AppendRemainderDimension(_1)))
      .saveAsText(s"./tmp.${tool}/wma1.out", tuner = Default())
      .toUnit

    ctx
      .loadText(path + "/simMovAvgInputfile.txt", Cell.parse2D(first = LongCodec))
      .data
      .slide(Over(_2), Default())(true, WeightedMovingAverage(5, Locate.AppendRemainderDimension(_1), all = true))
      .saveAsText(s"./tmp.${tool}/wma2.out", tuner = Default())
      .toUnit

    // http://stackoverflow.com/questions/11074665/how-to-calculate-the-cumulative-average-for-some-numbers

    ctx
      .loadText(path + "/cumMovAvgInputfile.txt", Cell.parse1D())
      .data
      .slide(Along(_1), Default())(true, CumulativeMovingAverage(Locate.AppendRemainderDimension(_1)))
      .saveAsText(s"./tmp.${tool}/cma.out", tuner = Default())
      .toUnit

    // http://www.incrediblecharts.com/indicators/exponential_moving_average.php

    ctx
      .loadText(path + "/expMovAvgInputfile.txt", Cell.parse1D())
      .data
      .slide(Along(_1), Default())(true, ExponentialMovingAverage(0.33, Locate.AppendRemainderDimension(_1)))
      .saveAsText(s"./tmp.${tool}/ema.out", tuner = Default())
      .toUnit
  }

  def test28[
    U[_],
    E[_]
  ](
    ctx: Context[U, E],
    rules: CutRules[E],
    tool: String
  )(implicit
    ev1: Matrix.CompactTuner[U, Default[NoParameters]],
    ev2: Persist.SaveAsTextTuner[U, Default[NoParameters]],
    ev3: Matrix.SummariseTuner[U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.matrix._

    val data = List
      .range(0, 16)
      .flatMap { case i =>
        List(
          ("iid:" + i, "fid:A", Content(ContinuousSchema[Long](), i)),
          ("iid:" + i, "fid:B", Content(NominalSchema[String](), i.toString))
        )
      }

    val aggregators: List[Aggregator[_2, _1, _2]] = List(
      Counts().andThenRelocate(_.position.append("count").toOption),
      Minimum().andThenRelocate(_.position.append("min").toOption),
      Maximum().andThenRelocate(_.position.append("max").toOption),
      Mean().andThenRelocate(_.position.append("mean").toOption),
      StandardDeviation(biased = true).andThenRelocate(_.position.append("sd").toOption),
      Skewness().andThenRelocate(_.position.append("skewness").toOption)
    )

    val stats = data
      .summarise(Along(_1), Default())(aggregators)
      .compact(Over(_1), Default())

    val extractor = ExtractWithDimension[_2, List[Double]](_2)

    data
      .transformWithValue(rules.fixed(stats, "min", "max", 4), Cut(extractor))
      .saveAsText(s"./tmp.${tool}/cut1.out", tuner = Default())
      .toUnit

    data
      .transformWithValue(
        rules.squareRootChoice(stats, "count", "min", "max"),
        Cut(extractor).andThenRelocate(Locate.RenameDimension(_2, "%s.square"))
      )
      .saveAsText(s"./tmp.${tool}/cut2.out", tuner = Default())
      .toUnit

    data
      .transformWithValue(
        rules.sturgesFormula(stats, "count", "min", "max"),
        Cut(extractor).andThenRelocate(Locate.RenameDimension(_2, "%s.sturges"))
      )
      .saveAsText(s"./tmp.${tool}/cut3.out", tuner = Default())
      .toUnit

    data
      .transformWithValue(
        rules.riceRule(stats, "count", "min", "max"),
        Cut(extractor).andThenRelocate(Locate.RenameDimension(_2, "%s.rice"))
      )
      .saveAsText(s"./tmp.${tool}/cut4.out", tuner = Default())
      .toUnit

    data
      .transformWithValue(
        rules.doanesFormula(stats, "count", "min", "max", "skewness"),
        Cut(extractor).andThenRelocate(Locate.RenameDimension(_2, "%s.doane"))
      )
      .saveAsText(s"./tmp.${tool}/cut5.out", tuner = Default())
      .toUnit

    data
      .transformWithValue(
        rules.scottsNormalReferenceRule(stats, "count", "min", "max", "sd"),
        Cut(extractor).andThenRelocate(Locate.RenameDimension(_2, "%s.scott"))
      )
      .saveAsText(s"./tmp.${tool}/cut6.out", tuner = Default())
      .toUnit

    data
      .transformWithValue(
        rules.breaks(Map("fid:A" -> List(-1, 4, 8, 12, 16))),
        Cut(extractor).andThenRelocate(Locate.RenameDimension(_2, "%s.break"))
      )
      .saveAsText(s"./tmp.${tool}/cut7.out", tuner = Default())
      .toUnit
  }

  def test29[
    U[_],
    E[_]
  ](
    ctx: Context[U, E],
    tool: String
  )(implicit
    ev: Persist.SaveAsTextTuner[U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.matrix._

    val schema = DiscreteSchema[Long]()
    val data = List(
      ("iid:A", Content(schema, 0)),
      ("iid:B", Content(schema, 1)),
      ("iid:C", Content(schema, 2)),
      ("iid:D", Content(schema, 3)),
      ("iid:E", Content(schema, 4)),
      ("iid:F", Content(schema, 5)),
      ("iid:G", Content(schema, 6)),
      ("iid:H", Content(schema, 7))
    )

    data
      .stream(
        "Rscript double.R",
        List("double.R"),
        Cell.toString(false, "|", true),
        Cell.parse2D("#", StringCodec, LongCodec),
        Reducers(1)
      )
      .data
      .saveAsText(s"./tmp.${tool}/strm.out", tuner = Default())
      .toUnit
  }

  def test30[
    U[_],
    E[_]
  ](
    ctx: Context[U, E],
    path: String,
    tool: String
  )(implicit
    ev: Persist.SaveAsTextTuner[U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.environment._
    import ctx.implicits.matrix._

    val (data, errors) = ctx.loadText(path + "/badInputfile.txt", Cell.parse3D(third = DateCodec()))

    data
      .saveAsText(s"./tmp.${tool}/yok.out", Cell.toString(verbose = true), Default())
      .toUnit

    errors
      .saveAsText(s"./tmp.${tool}/nok.out", Default())
      .toUnit
  }

  def test31[
    U[_],
    E[_]
  ](
    ctx: Context[U, E],
    tool: String
  )(implicit
    ev: Matrix.SaveAsIVTuner[U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.matrix._

    List(
      ("a", Content(ContinuousSchema[Double](), 3.14)),
      ("b", Content(DiscreteSchema[Long](), 42)),
      ("c", Content(NominalSchema[String](), "foo"))
    )
      .saveAsIV(s"./tmp.${tool}/iv1.out", tuner = Default())
      .toUnit

    List(
      ("a", "d", Content(ContinuousSchema[Double](), 3.14)),
      ("b", "c", Content(DiscreteSchema[Long](), 42)),
      ("c", "b", Content(NominalSchema[String](), "foo"))
    )
      .saveAsIV(s"./tmp.${tool}/iv2.out", tuner = Default())
      .toUnit

    List(
      ("a", "d", "c", Content(ContinuousSchema[Double](), 3.14)),
      ("b", "c", "d", Content(DiscreteSchema[Long](), 42)),
      ("c", "b", "e", Content(NominalSchema[String](), "foo"))
    )
      .saveAsIV(s"./tmp.${tool}/iv3.out", tuner = Default())
      .toUnit

    List(
      ("a", "d", "c", "d", Content(ContinuousSchema[Double](), 3.14)),
      ("b", "c", "d", "e", Content(DiscreteSchema[Long](), 42)),
      ("c", "b", "e", "f", Content(NominalSchema[String](), "foo"))
    )
      .saveAsIV(s"./tmp.${tool}/iv4.out", tuner = Default())
      .toUnit

    List(
      ("a", "d", "c", "d", "e", Content(ContinuousSchema[Double](), 3.14)),
      ("b", "c", "d", "e", "f", Content(DiscreteSchema[Long](), 42)),
      ("c", "b", "e", "f", "g", Content(NominalSchema[String](), "foo"))
    )
      .saveAsIV(s"./tmp.${tool}/iv5.out", tuner = Default())
      .toUnit

    List(
      ("a", "d", "c", "d", "e", "f", Content(ContinuousSchema[Double](), 3.14)),
      ("b", "c", "d", "e", "f", "g", Content(DiscreteSchema[Long](), 42)),
      ("c", "b", "e", "f", "g", "h", Content(NominalSchema[String](), "foo"))
    )
      .saveAsIV(s"./tmp.${tool}/iv6.out", tuner = Default())
      .toUnit

    List(
      ("a", "d", "c", "d", "e", "f", "g", Content(ContinuousSchema[Double](), 3.14)),
      ("b", "c", "d", "e", "f", "g", "h", Content(DiscreteSchema[Long](), 42)),
      ("c", "b", "e", "f", "g", "h", "i", Content(NominalSchema[String](), "foo"))
    )
      .saveAsIV(s"./tmp.${tool}/iv7.out", tuner = Default())
      .toUnit

    List(
      ("a", "d", "c", "d", "e", "f", "g", "h", Content(ContinuousSchema[Double](), 3.14)),
      ("b", "c", "d", "e", "f", "g", "h", "i", Content(DiscreteSchema[Long](), 42)),
      ("c", "b", "e", "f", "g", "h", "i", "j", Content(NominalSchema[String](), "foo"))
    )
      .saveAsIV(s"./tmp.${tool}/iv8.out", tuner = Default())
      .toUnit

    List(
      ("a", "d", "c", "d", "e", "f", "g", "h", "i", Content(ContinuousSchema[Double](), 3.14)),
      ("b", "c", "d", "e", "f", "g", "h", "i", "j", Content(DiscreteSchema[Long](), 42)),
      ("c", "b", "e", "f", "g", "h", "i", "j", "k", Content(NominalSchema[String](), "foo"))
    )
      .saveAsIV(s"./tmp.${tool}/iv9.out", tuner = Default())
      .toUnit
  }

  def test32[
    U[_],
    E[_]
  ](
    ctx: Context[U, E],
    tool: String
  )(implicit
    ev: Matrix.SaveAsVWTuner[U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.cell._
    import ctx.implicits.matrix._

    val data = List(
      ("a", "one", Content(ContinuousSchema[Double](), 3.14)),
      ("a", "two", Content(NominalSchema[String](), "foo")),
      ("a", "three", Content(DiscreteSchema[Long](), 42)),
      ("b", "one", Content(ContinuousSchema[Double](), 6.28)),
      ("b", "two", Content(DiscreteSchema[Long](), 123)),
      ("b", "three", Content(ContinuousSchema[Double](), 9.42)),
      ("c", "two", Content(NominalSchema[String](), "bar")),
      ("c", "three", Content(ContinuousSchema[Double](), 12.56))
    )

    val labels = List(
      Cell(Position("a"), Content(DiscreteSchema[Long](), 1)),
      Cell(Position("b"), Content(DiscreteSchema[Long](), 2))
    )

    val importance = List(
      Cell(Position("a"), Content(ContinuousSchema[Double](), 0.5)),
      Cell(Position("b"), Content(ContinuousSchema[Double](), 0.75))
    )

    data
      .saveAsVW(Over(_1), Default())(s"./tmp.${tool}/vw0.out", tag = false)
      .toUnit

    data
      .saveAsVW(Over(_1), Default())(s"./tmp.${tool}/vw1.out", tag = true)
      .toUnit

    data
      .saveAsVWWithLabels(Over(_1), Default())(s"./tmp.${tool}/vw2.out", labels, tag = false)
      .toUnit

    data
      .saveAsVWWithImportance(Over(_1), Default())(s"./tmp.${tool}/vw3.out", importance, tag = true)
      .toUnit

    data
      .saveAsVWWithLabelsAndImportance(Over(_1), Default())(s"./tmp.${tool}/vw4.out", labels, importance, tag = false)
      .toUnit
  }

  def test33[
    U[_],
    E[_]
  ](
    ctx: Context[U, E],
    tool: String
  )(implicit
    ev: Persist.SaveAsTextTuner[U, Redistribute]
  ): Unit = {
    import ctx.implicits.environment._
    import ctx.implicits.matrix._

    val data = List(
      ("a", "one", Content(ContinuousSchema[Double](), 3.14)),
      ("a", "two", Content(NominalSchema[String](), "foo")),
      ("a", "three", Content(DiscreteSchema[Long](), 42)),
      ("b", "one", Content(ContinuousSchema[Double](), 6.28)),
      ("b", "two", Content(NominalSchema[String](), "bar")),
      ("c", "one", Content(ContinuousSchema[Double](), 12.56)),
      ("c", "three", Content(DiscreteSchema[Long](), 123))
    )

    def writer(values: List[Option[Cell[_2]]]) = List(
      values.map(_.map(_.content.value.toShortString).getOrElse("")).mkString("|")
    )

    val (result, errors) = data
      .streamByPosition(Over(_1))("sh ./parrot.sh", List("parrot.sh"), writer, Cell.parse1D(), 5)

    errors
      .saveAsText(s"./tmp.${tool}/sbp.out", Redistribute(1))
      .toUnit
  }
}

