// Copyright 2017,2018 Commonwealth Bank of Australia
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
import commbank.grimlock.framework.distance._
import commbank.grimlock.framework.encoding._
import commbank.grimlock.framework.extract._
import commbank.grimlock.framework.environment._
import commbank.grimlock.framework.environment.implicits._
import commbank.grimlock.framework.environment.tuner._
import commbank.grimlock.framework.metadata._
import commbank.grimlock.framework.pairwise._
import commbank.grimlock.framework.partition._
import commbank.grimlock.framework.position._
import commbank.grimlock.framework.sample._
import commbank.grimlock.framework.window._

import commbank.grimlock.library.aggregate._
import commbank.grimlock.library.pairwise._
import commbank.grimlock.library.squash._
import commbank.grimlock.library.transform._
import commbank.grimlock.library.window._

import java.util.Date

import scala.io.Source
import scala.util.Try

import shapeless.{ ::, HList, HNil, Nat }
import shapeless.nat.{ _0, _1, _2, _3 }

object Shared {
  def test1[
    C <: Context[C]
  ](
    ctx: C,
    data: C#U[Cell[Coordinates3[String, String, Date]]],
    path: String,
    tool: String
  )(implicit
    ev1: Persist.SaveAsTextTuner[C#U, Default[NoParameters]],
    ev2: Matrix.SetTuner[C#U, Default[NoParameters]],
    ev3: Matrix.SelectTuner[C#U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.cell._
    import ctx.implicits.matrix._
    import ctx.implicits.position._

    implicit val c = ctx

    implicit def toDate(date: Date): Value[Date] = DateValue(date)

    data
      .saveAsText(ctx, s"./tmp.${tool}/dat1.out", (c) => List(c.toString), Default())
      .toUnit

    data
      .set(
        Cell(
          Position("iid:1548763", "fid:Y", DateCodec().decode("2014-04-26").get),
          Content(ContinuousSchema[Long](), 1234L)
        ),
        Default()
      )
      .select(Over(_0), Default())(true, "iid:1548763")
      .saveAsText(ctx, s"./tmp.${tool}/dat2.out", (c) => List(c.toString), Default())
      .toUnit

    ctx
      .loadText(
        path + "/smallInputfile.txt",
        Cell.shortStringParser(StringCodec :: StringCodec :: DateCodec() :: HNil, "|")
      )
      .data
      .saveAsText(ctx, s"./tmp.${tool}/dat3.out", (c) => List(c.toString), Default())
      .toUnit
  }

  def test2[
    C <: Context[C]
  ](
    ctx: C,
    data: C#U[Cell[Coordinates3[String, String, Date]]],
    path: String,
    tool: String
  )(implicit
    ev1: Positions.NamesTuner[C#U, Default[NoParameters]],
    ev2: Persist.SaveAsTextTuner[C#U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.environment._
    import ctx.implicits.matrix._
    import ctx.implicits.position._

    (
      data.names(Over(_0), Default()).map(_.toString) ++
      data.names(Over(_1), Default()).map(_.toString) ++
      data.names(Over(_2), Default()).map(_.toString)
    )
      .saveAsText(ctx, s"./tmp.${tool}/nm0.out", Default())
      .toUnit

    data
      .names(Over(_1), Default())
      .select(false, "fid:M")
      .saveAsText(ctx, s"./tmp.${tool}/nm2.out", (p) => List(p.toString), Default())
      .toUnit

    data
      .names(Over(_1), Default())
      .select(true, """.*[BCD]$""".r)
      .saveAsText(ctx, s"./tmp.${tool}/nm5.out", (p) => List(p.toString), Default())
      .toUnit
  }

  def test3[
    C <: Context[C]
  ](
    ctx: C,
    data: C#U[Cell[Coordinates3[String, String, Date]]],
    path: String,
    tool: String
  )(implicit
    ev1: Persist.SaveAsTextTuner[C#U, Default[NoParameters]],
    ev2: Matrix.TypesTuner[C#U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.environment._
    import ctx.implicits.matrix._

    (
      data.types(Over(_0), Default())(false).map(_.toString) ++
      data.types(Over(_1), Default())(false).map(_.toString) ++
      data.types(Over(_2), Default())(false).map(_.toString)
    )
      .saveAsText(ctx, s"./tmp.${tool}/typ1.out", Default())
      .toUnit

    (
      data.types(Over(_0), Default())(true).map(_.toString) ++
      data.types(Over(_1), Default())(true).map(_.toString) ++
      data.types(Over(_2), Default())(true).map(_.toString)
    )
      .saveAsText(ctx, s"./tmp.${tool}/typ2.out", Default())
      .toUnit
  }

  def test4[
    C <: Context[C]
  ](
    ctx: C,
    data: C#U[Cell[Coordinates3[String, String, Date]]],
    path: String,
    tool: String
  )(implicit
    ev1: Positions.NamesTuner[C#U, Default[NoParameters]],
    ev2: Persist.SaveAsTextTuner[C#U, Default[NoParameters]],
    ev3: Matrix.SelectTuner[C#U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.matrix._
    import ctx.implicits.position._

    implicit val c = ctx

    data
      .select(Over(_1), Default())(true, "fid:B")
      .saveAsText(ctx, s"./tmp.${tool}/scl0.out", (c) => List(c.toString), Default())
      .toUnit

    data
      .select(Over(_1), Default())(true, List("fid:A", "fid:B"))
      .select(Over(_0), Default())(true, "iid:0221707")
      .saveAsText(ctx, s"./tmp.${tool}/scl1.out", (c) => List(c.toString), Default())
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
      .select(Over(_1), Default())(false, data.names(Over(_1), Default()).select(false, rem))
      .saveAsText(ctx, s"./tmp.${tool}/scl2.out", (c) => List(c.toString), Default())
      .toUnit
  }

  def test5[
    C <: Context[C]
  ](
    ctx: C,
    data: C#U[Cell[Coordinates3[String, String, Date]]],
    path: String,
    tool: String
  )(implicit
    ev1: Positions.NamesTuner[C#U, Default[NoParameters]],
    ev2: Matrix.SaveAsCSVTuner[C#U, Default[NoParameters]],
    ev3: Persist.SaveAsTextTuner[C#U, Default[NoParameters]],
    ev4: Matrix.SelectTuner[C#U, Default[NoParameters]],
    ev5: Matrix.SquashTuner[C#U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.matrix._
    import ctx.implicits.position._

    implicit val c = ctx

    data
      .select(Over(_1), Default())(true, List("fid:A", "fid:B"))
      .select(Over(_0), Default())(true, "iid:0221707")
      .squash(_2, PreservingMaximumPosition(), Default())
      .saveAsText(ctx, s"./tmp.${tool}/sqs1.out", (c) => List(c.toString), Default())
      .toUnit

    data
      .squash(_2, PreservingMaximumPosition(), Default())
      .saveAsText(ctx, s"./tmp.${tool}/sqs2.out", (c) => List(c.toString), Default())
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
      .select(Over(_0), Default())(true, ids)
      .squash(_2, PreservingMaximumPosition(), Default())
      .saveAsCSV(Over(_1), Default())(ctx, s"./tmp.${tool}/sqs3.out")
      .toUnit

    data
      .select(Over(_0), Default())(true, ids)
      .select(Over(_1), Default())(true, List("fid:A", "fid:B", "fid:C", "fid:D", "fid:E", "fid:F", "fid:G"))
      .squash(_2, PreservingMaximumPosition(), Default())
      .saveAsCSV(Over(_1), Default())(ctx, s"./tmp.${tool}/sqs4.out")
      .toUnit
  }

  def test6[
    C <: Context[C]
  ](
    ctx: C,
    data: C#U[Cell[Coordinates3[String, String, Date]]],
    path: String,
    tool: String
  )(implicit
    ev1: Matrix.GetTuner[C#U, Default[NoParameters]],
    ev2: Persist.SaveAsTextTuner[C#U, Default[NoParameters]],
    ev3: Matrix.SelectTuner[C#U, Default[NoParameters]],
    ev4: Matrix.SquashTuner[C#U, Default[NoParameters]],
    ev5: Matrix.SummariseTuner[C#U, Default[NoParameters]],
    ev6: Matrix.WhichTuner[C#U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.matrix._
    import ctx.implicits.position._

    implicit val c = ctx

    data
      .which(c => c.content.classification.isOfType(NumericType))
      .saveAsText(ctx, s"./tmp.${tool}/whc1.out", (p) => List(p.toString), Default())
      .toUnit

    data
      .which(c => !c.content.value.isInstanceOf[StringValue])
      .saveAsText(ctx, s"./tmp.${tool}/whc2.out", (p) => List(p.toString), Default())
      .toUnit

    data
      .get(
        data.which(c => (c.content.value equ 666) || (c.content.value leq 11.0) || (c.content.value equ "KQUPKFEH")),
        Default()
      )
      .saveAsText(ctx, s"./tmp.${tool}/whc3.out", (c) => List(c.toString), Default())
      .toUnit

    data
      .which(c => c.content.value.isInstanceOf[LongValue])
      .saveAsText(ctx, s"./tmp.${tool}/whc4.out", (p) => List(p.toString), Default())
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
      .select(Over(_0), Default())(true, ids)
      .select(Over(_1), Default())(true, List("fid:A", "fid:B", "fid:C", "fid:D", "fid:E", "fid:F", "fid:G"))
      .squash(_2, PreservingMaximumPosition(), Default())
      .summarise(Along(_0), Default())(
        Counts().andThenRelocate(_.position.append("count").toOption),
        Mean().andThenRelocate(_.position.append("mean").toOption),
        Minimum().andThenRelocate(_.position.append("min").toOption),
        Maximum().andThenRelocate(_.position.append("max").toOption),
        MaximumAbsolute().andThenRelocate(_.position.append("max.abs").toOption)
      )
      .whichByPosition(
        Over(_1),
        Default()
      )(
        List(("count", c => c.content.value leq 2), ("min", c => c.content.value equ 107))
      )
      .saveAsText(ctx, s"./tmp.${tool}/whc5.out", (p) => List(p.toString), Default())
      .toUnit
  }

  def test7[
    C <: Context[C]
  ](
    ctx: C,
    data: C#U[Cell[Coordinates3[String, String, Date]]],
    path: String,
    tool: String
  )(implicit
    ev1: Matrix.GetTuner[C#U, Default[NoParameters]],
    ev2: Persist.SaveAsTextTuner[C#U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.matrix._
    import ctx.implicits.position._

    implicit val c = ctx

    implicit def toDate(date: Date): Value[Date] = DateValue(date)

    data
      .get(
        Position("iid:1548763", "fid:Y", DateCodec().decode("2014-04-26").get),
        Default()
      )
      .saveAsText(ctx, s"./tmp.${tool}/get1.out", (c) => List(c.toString), Default())
      .toUnit

    data
      .get(
        List(
          Position("iid:1548763", "fid:Y", DateCodec().decode("2014-04-26").get),
          Position("iid:1303823", "fid:A", DateCodec().decode("2014-05-05").get)
        ),
        Default()
      )
      .saveAsText(ctx, s"./tmp.${tool}/get2.out", (c) => List(c.toString), Default())
      .toUnit
  }

  def test8[
    C <: Context[C]
  ](
    ctx: C,
    data: C#U[Cell[Coordinates3[String, String, Date]]],
    path: String,
    tool: String
  )(implicit
    ev1: Matrix.GetTuner[C#U, Default[NoParameters]],
    ev2: Matrix.SaveAsCSVTuner[C#U, Default[NoParameters]],
    ev3: Persist.SaveAsTextTuner[C#U, Default[NoParameters]],
    ev4: Matrix.SelectTuner[C#U, Default[NoParameters]],
    ev5: Matrix.SquashTuner[C#U, Default[NoParameters]],
    ev6: Matrix.UniqueTuner[C#U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.content._
    import ctx.implicits.matrix._
    import ctx.implicits.position._

    implicit val c = ctx

    data
      .select(Over(_1), Default())(true, "fid:B")
      .squash(_2, PreservingMaximumPosition(), Default())
      .unique(Default())
      .saveAsText(ctx, s"./tmp.${tool}/uniq.out", (c) => List(c.toString), Default())
      .toUnit

    ctx
      .loadText(path + "/mutualInputfile.txt", Cell.shortStringParser(StringCodec :: StringCodec :: HNil, "|"))
      .data
      .uniqueByPosition(Over(_1), Default())
      .saveAsText(ctx, s"./tmp.${tool}/uni2.out", IndexedContents.toShortString(false, "|"), Default())
      .toUnit

    data
      .select(Over(_1), Default())(true, List("fid:A", "fid:B", "fid:Y", "fid:Z"))
      .select(Over(_0), Default())(true, List("iid:0221707", "iid:0364354"))
      .squash(_2, PreservingMaximumPosition(), Default())
      .saveAsCSV(Over(_0), Default())(ctx, s"./tmp.${tool}/test.csv")
      .saveAsCSV(Over(_1), Default())(ctx, s"./tmp.${tool}/tset.csv", writeHeader = false, separator = ",")
      .toUnit

    data
      .select(Over(_1), Default())(true, List("fid:A", "fid:B", "fid:Y", "fid:Z"))
      .select(Over(_0), Default())(true, List("iid:0221707", "iid:0364354"))
      .squash(_2, PreservingMaximumPosition(), Default())
      .permute(_1, _0)
      .saveAsText(ctx, s"./tmp.${tool}/trs1.out", (c) => List(c.toString), Default())
      .toUnit

    data
      .select(Over(_1), Default())(true, List("fid:A", "fid:B", "fid:Y", "fid:Z"))
      .select(Over(_0), Default())(true, List("iid:0221707", "iid:0364354"))
      .squash(_2, PreservingMaximumPosition(), Default())
      .saveAsText(ctx, s"./tmp.${tool}/data.txt", Cell.toShortString(true, "|"), Default())
      .toUnit
  }

  def test9[
    C <: Context[C]
  ](
    ctx: C,
    data: C#U[Cell[Coordinates3[String, String, Date]]],
    path: String,
    tool: String
  )(implicit
    ev1: Persist.SaveAsTextTuner[C#U, Default[NoParameters]],
    ev2: Matrix.SelectTuner[C#U, Default[NoParameters]],
    ev3: Matrix.SquashTuner[C#U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.matrix._
    import ctx.implicits.partition._
    import ctx.implicits.position._

    implicit val c = ctx

    case class StringPartitioner[
      P <: HList,
      D <: Nat
    ](
      dim: D
    )(implicit
      ev: Position.IndexConstraints.Aux[P, D, Value[String]]
    ) extends Partitioner[P, String] {
      def assign(cell: Cell[P]): TraversableOnce[String] = List(cell.position(dim).value match {
        case "fid:A" => "training"
        case "fid:B" => "testing"
      }, "scoring")
    }

    val prt1 = data
      .select(Over(_1), Default())(true, List("fid:A", "fid:B"))
      .select(Over(_0), Default())(true, List("iid:0221707", "iid:0364354"))
      .squash(_2, PreservingMaximumPosition(), Default())
      .split(StringPartitioner(_1))

    prt1
      .saveAsText(ctx, s"./tmp.${tool}/prt1.out", (p) => List(p.toString), Default())
      .toUnit

    case class IntTuplePartitioner[
      P <: HList,
      D <: Nat
    ](
      dim: D
    )(implicit
      ev: Position.IndexConstraints.Aux[P, D, Value[String]]
    ) extends Partitioner[P, (Int, Int, Int)] {
      def assign(cell: Cell[P]): TraversableOnce[(Int, Int, Int)] = List(cell.position(dim).value match {
        case "fid:A" => (1, 0, 0)
        case "fid:B" => (0, 1, 0)
      }, (0, 0, 1))
    }

    data
      .select(Over(_1), Default())(true, List("fid:A", "fid:B"))
      .select(Over(_0), Default())(true, List("iid:0221707", "iid:0364354"))
      .squash(_2, PreservingMaximumPosition(), Default())
      .split(IntTuplePartitioner(_1))
      .saveAsText(ctx, s"./tmp.${tool}/prt2.out", (p) => List(p.toString), Default())
      .toUnit

    prt1
      .get("training")
      .saveAsText(ctx, s"./tmp.${tool}/train.out", (c) => List(c.toString), Default())
      .toUnit

    prt1
      .get("testing")
      .saveAsText(ctx, s"./tmp.${tool}/test.out", (c) => List(c.toString), Default())
      .toUnit

    prt1
      .get("scoring")
      .saveAsText(ctx, s"./tmp.${tool}/score.out", (c) => List(c.toString), Default())
      .toUnit
  }

  def test10[
    C <: Context[C]
  ](
    ctx: C,
    data: C#U[Cell[Coordinates3[String, String, Date]]],
    path: String,
    tool: String
  )(implicit
    ev1: Matrix.SaveAsCSVTuner[C#U, Default[NoParameters]],
    ev2: Matrix.SelectTuner[C#U, Default[NoParameters]],
    ev3: Matrix.SquashTuner[C#U, Default[NoParameters]],
    ev4: Matrix.SummariseTuner[C#U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.matrix._
    import ctx.implicits.position._

    implicit val c = ctx

    data
      .summarise(Over(_1), Default())(Mean(false, true, true).andThenRelocate(_.position.append("mean").toOption))
      .saveAsCSV(Over(_0), Default())(ctx, s"./tmp.${tool}/agg1.csv")
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
      .select(Over(_0), Default())(true, ids)
      .squash(_2, PreservingMaximumPosition(), Default())
      .summarise(Along(_1), Default())(Counts().andThenRelocate(_.position.append("count").toOption))
      .saveAsCSV(Over(_0), Default())(ctx, s"./tmp.${tool}/agg2.csv")
      .toUnit

    data
      .select(Over(_0), Default())(true, ids)
      .squash(_2, PreservingMaximumPosition(), Default())
      .summarise(
        Along(_0),
        Default()
      )(
        Counts().andThenRelocate(_.position.append("count").toOption),
        Mean().andThenRelocate(_.position.append("mean").toOption),
        StandardDeviation(biased = true).andThenRelocate(_.position.append("sd").toOption),
        Skewness().andThenRelocate(_.position.append("skewness").toOption),
        Kurtosis().andThenRelocate(_.position.append("kurtosis").toOption),
        Minimum().andThenRelocate(_.position.append("min").toOption),
        Maximum().andThenRelocate(_.position.append("max").toOption),
        MaximumAbsolute().andThenRelocate(_.position.append("max.abs").toOption)
      )
      .saveAsCSV(Over(_0), Default())(ctx, s"./tmp.${tool}/agg3.csv")
      .toUnit
  }

  def test11[
    C <: Context[C]
  ](
    ctx: C,
    data: C#U[Cell[Coordinates3[String, String, Date]]],
    path: String,
    tool: String
  )(implicit
    ev1: Matrix.SaveAsCSVTuner[C#U, Default[NoParameters]],
    ev2: Persist.SaveAsTextTuner[C#U, Default[NoParameters]],
    ev3: Matrix.SelectTuner[C#U, Default[NoParameters]],
    ev4: Matrix.SquashTuner[C#U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.matrix._
    import ctx.implicits.position._

    implicit val c = ctx

    data
      .select(Over(_1), Default())(true, List("fid:A", "fid:B", "fid:Y", "fid:Z"))
      .select(Over(_0), Default())(true, List("iid:0221707", "iid:0364354"))
      .transform(
        Indicator().andThenRelocate(Locate.RenameDimension(_1, (c: Value[String]) => s"${c.toShortString}.ind"))
      )
      .saveAsText(ctx, s"./tmp.${tool}/trn2.out", (c) => List(c.toString), Default())
      .toUnit

    data
      .select(Over(_1), Default())(true, List("fid:A", "fid:B", "fid:Y", "fid:Z"))
      .select(Over(_0), Default())(true, List("iid:0221707", "iid:0364354"))
      .squash(_2, PreservingMaximumPosition(), Default())
      .transform(Binarise(Locate.RenameDimensionWithContent(_1, Value.concatenate[Value[String], Value[_]]("="))))
      .saveAsCSV(Over(_0), Default())(ctx, s"./tmp.${tool}/trn3.out")
      .toUnit
  }

  def test12[
    C <: Context[C]
  ](
    ctx: C,
    data: C#U[Cell[Coordinates3[String, String, Date]]],
    path: String,
    tool: String
  )(implicit
    ev1: Matrix.FillHomogeneousTuner[C#U, Default[NoParameters]],
    ev2: Matrix.SaveAsCSVTuner[C#U, Default[NoParameters]],
    ev3: Persist.SaveAsTextTuner[C#U, Default[NoParameters]],
    ev4: Matrix.SelectTuner[C#U, Default[NoParameters]],
    ev5: Matrix.SquashTuner[C#U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.matrix._
    import ctx.implicits.position._

    implicit val c = ctx

    val sliced = data
      .select(Over(_1), Default())(true, List("fid:A", "fid:B", "fid:Y", "fid:Z"))
      .select(Over(_0), Default())(true, List("iid:0221707", "iid:0364354"))

    sliced
      .squash(_2, PreservingMaximumPosition(), Default())
      .fillHomogeneous(Content(ContinuousSchema[Long](), 0L), Default())
      .saveAsCSV(Over(_0), Default())(ctx, s"./tmp.${tool}/fll1.out")
      .toUnit

    sliced
      .fillHomogeneous(Content(ContinuousSchema[Long](), 0L), Default())
      .saveAsText(ctx, s"./tmp.${tool}/fll3.out", (c) => List(c.toString), Default())
      .toUnit
  }

  def test13[
    C <: Context[C]
  ](
    ctx: C,
    data: C#U[Cell[Coordinates3[String, String, Date]]],
    path: String,
    tool: String
  )(implicit
    ev1: Matrix.FillHeterogeneousTuner[C#U, Default[NoParameters]],
    ev2: Matrix.FillHomogeneousTuner[C#U, Default[NoParameters]],
    ev3: Matrix.JoinTuner[C#U, Default[NoParameters]],
    ev4: Matrix.SaveAsCSVTuner[C#U, Default[NoParameters]],
    ev5: Persist.SaveAsTextTuner[C#U, Default[NoParameters]],
    ev6: Matrix.SelectTuner[C#U, Default[NoParameters]],
    ev7: Matrix.SquashTuner[C#U, Default[NoParameters]],
    ev8: Matrix.SummariseTuner[C#U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.matrix._
    import ctx.implicits.position._

    implicit val c = ctx

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
      .select(Over(_0), Default())(true, ids)
      .select(Over(_1), Default())(true, List("fid:A", "fid:B", "fid:C", "fid:D", "fid:E", "fid:F", "fid:G"))
      .squash(_2, PreservingMaximumPosition(), Default())

    val inds = sliced
      .transform(
        Indicator().andThenRelocate(Locate.RenameDimension(_1, (c: Value[String]) => s"${c.toShortString}.ind"))
      )
      .fillHomogeneous(Content(ContinuousSchema[Long](), 0L), Default())

    sliced
      .join(Over(_0), Default())(inds)
      .fillHomogeneous(Content(ContinuousSchema[Long](), 0L), Default())
      .saveAsCSV(Over(_0), Default())(ctx, s"./tmp.${tool}/fll2.out")
      .toUnit

    sliced
      .fillHeterogeneous(Over(_1), Default())(data.summarise(Over(_1), Default())(Mean(false, true, true)))
      .join(Over(_0), Default())(inds)
      .saveAsCSV(Over(_0), Default())(ctx, s"./tmp.${tool}/fll4.out")
      .toUnit
  }

  def test14[
    C <: Context[C]
  ](
    ctx: C,
    data: C#U[Cell[Coordinates3[String, String, Date]]],
    path: String,
    tool: String
  )(implicit
    ev1: Matrix.MutateTuner[C#U, Default[NoParameters]],
    ev2: Persist.SaveAsTextTuner[C#U, Default[NoParameters]],
    ev3: Matrix.SelectTuner[C#U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.matrix._
    import ctx.implicits.position._

    implicit val c = ctx

    data
      .select(Over(_1), Default())(true, List("fid:A", "fid:B", "fid:Y", "fid:Z"))
      .select(Over(_0), Default())(true, List("iid:0221707", "iid:0364354"))
      .mutate(
        Over(_1),
        Default()
      )(
        "fid:A",
        c => Content.decoder(LongCodec, NominalSchema[Long]())(c.content.value.toShortString)
      )
      .saveAsText(ctx, s"./tmp.${tool}/chg1.out", (c) => List(c.toString), Default())
      .toUnit
  }

  def test15[
    C <: Context[C]
  ](
    ctx: C,
    data: C#U[Cell[Coordinates3[String, String, Date]]],
    path: String,
    tool: String
  )(implicit
    ev1: Matrix.JoinTuner[C#U, Default[NoParameters]],
    ev2: Matrix.SaveAsCSVTuner[C#U, Default[NoParameters]],
    ev3: Matrix.SelectTuner[C#U, Default[NoParameters]],
    ev4: Matrix.SquashTuner[C#U, Default[NoParameters]],
    ev5: Matrix.SummariseTuner[C#U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.matrix._
    import ctx.implicits.position._

    implicit val c = ctx

    data
      .select(Over(_1), Default())(true, List("fid:A", "fid:C", "fid:E", "fid:G"))
      .select(Over(_0), Default())(true, List("iid:0221707", "iid:0364354"))
      .summarise(Along(_2), Default())(Sums().andThenRelocate(_.position.append("sum").toOption))
      .contract(_2, _1, Value.concatenate[Value[String], Value[String]]("."))
      .saveAsCSV(Over(_0), Default())(ctx, s"./tmp.${tool}/rsh1.out")
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
      .select(Over(_0), Default())(true, ids)
      .select(Over(_1), Default())(true, List("fid:A", "fid:B", "fid:C", "fid:D", "fid:E", "fid:F", "fid:G"))
      .squash(_2, PreservingMaximumPosition(), Default())
      .transform(
        Indicator().andThenRelocate(Locate.RenameDimension(_1, (c: Value[String]) => s"${c.toShortString}.ind"))
      )
      .saveAsCSV(Over(_0), Default())(ctx, s"./tmp.${tool}/trn1.csv")

    data
      .select(Over(_0), Default())(true, ids)
      .select(Over(_1), Default())(true, List("fid:A", "fid:B", "fid:C", "fid:D", "fid:E", "fid:F", "fid:G"))
      .squash(_2, PreservingMaximumPosition(), Default())
      .join(Over(_0), Default())(inds)
      .saveAsCSV(Over(_0), Default())(ctx, s"./tmp.${tool}/jn1.csv")
      .toUnit
  }

  def test16[
    C <: Context[C]
  ](
    ctx: C,
    data: C#U[Cell[Coordinates3[String, String, Date]]],
    path: String,
    tool: String
  )(implicit
    ev: Persist.SaveAsTextTuner[C#U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.matrix._

    case class HashSample[
      P <: HList
    ](
    )(implicit
      ev: Position.IndexConstraints[P, _0]
    ) extends Sampler[P] {
      def select(cell: Cell[P]): Boolean = {
        val c = cell.position(_0).toShortString

        (c == "iid:1548763") || (c == "iid:2813650") || (c == "iid:4330328")
      }
    }

    data
      .extract(HashSample())
      .saveAsText(ctx, s"./tmp.${tool}/smp1.out", Cell.toShortString(true, "|"), Default())
      .toUnit
  }

  def test17[
    C <: Context[C]
  ](
    ctx: C,
    data: C#U[Cell[Coordinates3[String, String, Date]]],
    path: String,
    tool: String
  )(implicit
    ev1: Matrix.GatherTuner[C#U, Default[NoParameters]],
    ev2: Matrix.SaveAsCSVTuner[C#U, Default[NoParameters]],
    ev3: Matrix.SelectTuner[C#U, Default[NoParameters]],
    ev4: Matrix.SquashTuner[C#U, Default[NoParameters]],
    ev5: Matrix.SummariseTuner[C#U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.matrix._
    import ctx.implicits.position._

    implicit val c = ctx

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
      .select(Over(_0), Default())(true, ids)
      .select(Over(_1), Default())(true, List("fid:A", "fid:B", "fid:C", "fid:D", "fid:E", "fid:F", "fid:G"))
      .squash(_2, PreservingMaximumPosition(), Default())

    val stats = sliced
      .summarise(
        Along(_0),
        Default()
      )(
        Counts().andThenRelocate(_.position.append("count").toOption),
        Mean().andThenRelocate(_.position.append("mean").toOption),
        Minimum().andThenRelocate(_.position.append("min").toOption),
        Maximum().andThenRelocate(_.position.append("max").toOption),
        MaximumAbsolute().andThenRelocate(_.position.append("max.abs").toOption)
      )
      .gatherByPosition(Over(_0), Default())

    def extractor[
      P <: HList,
      V <: Value[_]
    ](implicit
      ev: Position.IndexConstraints.Aux[P, _1, V]
    ) = ExtractWithDimensionAndKey[P, _1, V, String, Content](_1, "max.abs")
      .andThenPresent(_.value.as[Double])

    sliced
      .transformWithValue(stats, Normalise(extractor))
      .saveAsCSV(Over(_0), Default())(ctx, s"./tmp.${tool}/trn6.csv")
      .toUnit

    case class Sample500[P <: HList]() extends Sampler[P] {
      def select(cell: Cell[P]): Boolean = cell.content.value gtr 500
    }

    sliced
      .extract(Sample500())
      .saveAsCSV(Over(_0), Default())(ctx, s"./tmp.${tool}/flt1.csv")
      .toUnit

    case class RemoveGreaterThanMean[
      P <: HList,
      D <: Nat,
      K <: Value[_]
    ](
      dim: D
    )(implicit
      ev: Position.IndexConstraints.Aux[P, D, K]
    ) extends SamplerWithValue[P] {
      type V = Map[Position[K :: HNil], Map[Position[Coordinates1[String]], Content]]

      def selectWithValue(cell: Cell[P], ext: V): Boolean =
        if (cell.content.classification.isOfType(NumericType))
          cell.content.value leq ext(Position(cell.position(dim) :: HNil))(Position("mean")).value
        else
          true
    }

    sliced
      .extractWithValue(stats, RemoveGreaterThanMean(_1))
      .saveAsCSV(Over(_0), Default())(ctx, s"./tmp.${tool}/flt2.csv")
      .toUnit
  }

  def test18[
    C <: Context[C]
  ](
    ctx: C,
    data: C#U[Cell[Coordinates3[String, String, Date]]],
    path: String,
    tool: String
  )(implicit
    ev1: Positions.NamesTuner[C#U, Default[NoParameters]],
    ev2: Matrix.SaveAsCSVTuner[C#U, Default[NoParameters]],
    ev3: Matrix.SelectTuner[C#U, Default[NoParameters]],
    ev4: Matrix.SquashTuner[C#U, Default[NoParameters]],
    ev5: Matrix.SummariseTuner[C#U, Default[NoParameters]],
    ev6: Matrix.WhichTuner[C#U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.matrix._
    import ctx.implicits.position._

    implicit val c = ctx

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
      .select(Over(_0), Default())(true, ids)
      .select(Over(_1), Default())(true, List("fid:A", "fid:B", "fid:C", "fid:D", "fid:E", "fid:F", "fid:G"))
      .squash(_2, PreservingMaximumPosition(), Default())

    val stats = sliced
      .summarise(
        Along(_0),
        Default()
      )(
        Counts().andThenRelocate(_.position.append("count").toOption),
        Mean().andThenRelocate(_.position.append("mean").toOption),
        Minimum().andThenRelocate(_.position.append("min").toOption),
        Maximum().andThenRelocate(_.position.append("max").toOption),
        MaximumAbsolute().andThenRelocate(_.position.append("max.abs").toOption)
      )

    val rem = stats
      .whichByPosition(
        Over(_1),
        Default()
      )(
        ("count", (c: Cell[Coordinates2[String, String]]) => c.content.value leq 2)
      )
      .names(Over(_0), Default())

    sliced
      .select(Over(_1), Default())(false, rem)
      .saveAsCSV(Over(_0), Default())(ctx, s"./tmp.${tool}/flt3.csv")
      .toUnit
  }

  def test19[
    C <: Context[C]
  ](
    ctx: C,
    data: C#U[Cell[Coordinates3[String, String, Date]]],
    path: String,
    tool: String
  )(implicit
    ev1: Matrix.GatherTuner[C#U, Default[NoParameters]],
    ev2: Matrix.FillHomogeneousTuner[C#U, Default[NoParameters]],
    ev3: Positions.NamesTuner[C#U, Default[NoParameters]],
    ev4: Matrix.SaveAsCSVTuner[C#U, Default[NoParameters]],
    ev5: Matrix.SelectTuner[C#U, Default[NoParameters]],
    ev6: Matrix.SquashTuner[C#U, Default[NoParameters]],
    ev7: Matrix.SummariseTuner[C#U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.matrix._
    import ctx.implicits.partition._
    import ctx.implicits.position._

    implicit val c = ctx

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
      .select(Over(_0), Default())(true, ids)
      .select(Over(_1), Default())(true, List("fid:A", "fid:B", "fid:C", "fid:D", "fid:E", "fid:F", "fid:G"))
      .squash(_2, PreservingMaximumPosition(), Default())

    case class CustomPartition[
      P <: HList,
      D <: Nat
    ](
      dim: D,
      left: String,
      right: String
    )(implicit
      ev: Position.IndexConstraints[P, D]
    ) extends Partitioner[P, String] {
      def assign(cell: Cell[P]): TraversableOnce[String] = {
        val c = cell.position(dim).toShortString

        if (c == "iid:0216406" || c == "iid:0364354")
          List(right)
        else
          List(left)
      }
    }

    val parts = raw
      .split(CustomPartition(_0, "train", "test"))

    val stats = parts
      .get("train")
      .summarise(
        Along(_0),
        Default()
      )(
        Counts().andThenRelocate(_.position.append("count").toOption),
        MaximumAbsolute().andThenRelocate(_.position.append("max.abs").toOption)
      )

    val rem = stats
      .which(c => (c.position(_1) equ "count") && (c.content.value leq 2))
      .names(Over(_0), Default())

    def extractor[
      P <: HList,
      V <: Value[_]
    ](implicit
      ev: Position.IndexConstraints.Aux[P, _1, V]
    ) = ExtractWithDimensionAndKey[P, _1, V, String, Content](_1, "max.abs")
      .andThenPresent(_.value.as[Double])

    def cb(
      key: String,
      pipe: C#U[Cell[Coordinates2[String, String]]]
    ): C#U[Cell[Coordinates2[String, String]]] = pipe
      .select(Over(_1), Default())(false, rem)
      .transformWithValue(
        stats.gatherByPosition(Over(_0), Default()),
        Indicator().andThenRelocate(Locate.RenameDimension(_1, (c: Value[String]) => s"${c.toShortString}.ind")),
        Binarise(Locate.RenameDimensionWithContent(_1, Value.concatenate[Value[String], Value[_]]("="))),
        Normalise(extractor)
      )
      .fillHomogeneous(Content(ContinuousSchema[Long](), 0L), Default())
      .saveAsCSV(Over(_0), Default())(ctx, s"./tmp.${tool}/pln_" + key + ".csv")

    parts
      .forEach(List("train", "test"), cb)
      .toUnit
  }

  def test20[
    C <: Context[C]
  ](
    ctx: C,
    path: String,
    tool: String
  )(implicit
    ev: Persist.SaveAsTextTuner[C#U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.matrix._

    val (dictionary, _) = Dictionary.load(Source.fromFile(path + "/dict.txt"), "|")

    ctx
      .loadText(
        path + "/ivoryInputfile1.txt",
        Cell.shortStringParser(StringCodec :: StringCodec :: DateCodec() :: HNil, dictionary, _1, "|")
      )
      .data
      .saveAsText(ctx, s"./tmp.${tool}/ivr1.out", Cell.toShortString(true, "|"), Default())
      .toUnit
  }

  def test21[
    C <: Context[C]
  ](
    ctx: C,
    data: C#U[Cell[Coordinates3[String, String, Date]]],
    path: String,
    tool: String
  )(implicit
    ev1: Persist.SaveAsTextTuner[C#U, Default[NoParameters]],
    ev2: Matrix.MeasureTuner[C#U, Default[NoParameters]],
    ev3: Matrix.ShapeTuner[C#U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.matrix._

    data
      .shape(Default())
      .saveAsText(ctx, s"./tmp.${tool}/siz0.out", Cell.toShortString(true, "|"), Default())
      .toUnit

    data
      .measure(_0, tuner = Default())
      .saveAsText(ctx, s"./tmp.${tool}/siz1.out", Cell.toShortString(true, "|"), Default())
      .toUnit

    data
      .measure(_1, tuner = Default())
      .saveAsText(ctx, s"./tmp.${tool}/siz2.out", Cell.toShortString(true, "|"), Default())
      .toUnit

    data
      .measure(_2, tuner = Default())
      .saveAsText(ctx, s"./tmp.${tool}/siz3.out", Cell.toShortString(true, "|"), Default())
      .toUnit
  }

  def test22[
    C <: Context[C]
  ](
    ctx: C,
    path: String,
    tool: String
  )(implicit
    ev1: Persist.SaveAsTextTuner[C#U, Default[NoParameters]],
    ev2: Matrix.SlideTuner[C#U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.matrix._

    val (data, _) = ctx
      .loadText(path + "/numericInputfile.txt", Cell.shortStringParser(StringCodec :: StringCodec :: HNil, "|"))

    case class Diff[
      P <: HList,
      S <: HList,
      R <: HList,
      Q <: HList
    ](
    )(implicit
      ev1: Value.Box[String],
      ev2: Position.AppendConstraints.Aux[S, Value[String], Q]
    ) extends Window[P, S, R, Q] {
      type I = Option[Double]
      type T = (Option[Double], Position[R])
      type O = (Double, Position[R], Position[R])

      def prepare(cell: Cell[P]): I = cell.content.value.as[Double]

      def initialise(rem: Position[R], in: I): (T, TraversableOnce[O]) = ((in, rem), List())

      def update(rem: Position[R], in: I, t: T): (T, TraversableOnce[O]) = ((in, rem), (in, t._1) match {
        case (Some(c), Some(l)) => List((c - l, rem,  t._2))
        case _ => List()
      })

      def present(pos: Position[S], out: O): TraversableOnce[Cell[Q]] = List(
        Cell(
          pos.append(out._2.toShortString("") + "-" + out._3.toShortString("")),
          Content(ContinuousSchema[Double](), out._1)
        )
      )
    }

    data
      .slide(Over(_0), Default())(true, Diff())
      .saveAsText(ctx, s"./tmp.${tool}/dif1.out", Cell.toShortString(true, "|"), Default())
      .toUnit

    data
      .slide(Over(_1), Default())(true, Diff())
      .permute(_1, _0)
      .saveAsText(ctx, s"./tmp.${tool}/dif2.out", Cell.toShortString(true, "|"), Default())
      .toUnit
  }

  def test23[
    C <: Context[C]
  ](
    ctx: C,
    path: String,
    tool: String
  )(implicit
    ev1: Matrix.PairTuner[C#U, Default[NoParameters]],
    ev2: Persist.SaveAsTextTuner[C#U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.matrix._

    val (data, _) = ctx
      .loadText(path + "/somePairwise.txt", Cell.shortStringParser(StringCodec :: StringCodec :: HNil, "|"))

    case class DiffSquared[
      P <: HList,
      X <: HList,
      Q <: HList
    ](
    )(implicit
      ev1: Value.Box[String],
      ev2: Position.IndexConstraints[P, _0],
      ev3: Position.IndexConstraints[P, _1],
      ev4: Position.RemoveConstraints.Aux[P, _1, X],
      ev5: Position.AppendConstraints.Aux[X, Value[String], Q]
    ) extends Operator[P, Q] {
      def compute(left: Cell[P], right: Cell[P]): TraversableOnce[Cell[Q]] = {
        val xc = left.position(_1).toShortString
        val yc = right.position(_1).toShortString

        if (left.position(_0) == right.position(_0))
          List(
            Cell(
              right.position.remove(_1).append("(" + xc + "-" + yc + ")^2"),
              Content(
                ContinuousSchema[Double](),
                math.pow(left.content.value.as[Long].get - right.content.value.as[Long].get, 2)
              )
            )
          )
        else
          List()
      }
    }

    data
      .pair(Over(_1), Default())(Upper, DiffSquared())
      .saveAsText(ctx, s"./tmp.${tool}/pws1.out", Cell.toShortString(true, "|"), Default())
      .toUnit
  }

  def test24[
    C <: Context[C]
  ](
    ctx: C,
    path: String,
    tool: String
  )(implicit
    ev1: PairwiseDistance.CorrelationTuner[C#U, Default[NoParameters]],
    ev2: Persist.SaveAsTextTuner[C#U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.matrix._

    // see http://www.mathsisfun.com/data/correlation.html for data

    val pkey = KeyDecoder(_0, StringCodec) // "day"
    val columns = Set(
      ColumnDecoder(_1, "temperature", Content.decoder(DoubleCodec, ContinuousSchema[Double]())),
      ColumnDecoder(_2, "sales", Content.decoder(LongCodec, DiscreteSchema[Long]()))
    )

    val (data, _) = ctx.loadText(path + "/somePairwise2.txt", Cell.tableParser(pkey, columns, "|"))

    def locate[
      P <: HList
    ] = (l: Position[P], r: Position[P]) => Option(Position(s"(${l.toShortString("|")}*${r.toShortString("|")})"))

    data
      .correlation(Over(_1), Default())(locate, true)
      .saveAsText(ctx, s"./tmp.${tool}/pws2.out", Cell.toShortString(true, "|"), Default())
      .toUnit

    val columns2 = columns + ColumnDecoder(_3, "neg.sales", Content.decoder(LongCodec, DiscreteSchema[Long]()))

    val (data2, _) = ctx.loadText(path + "/somePairwise3.txt", Cell.tableParser(pkey, columns2, "|"))

    data2
      .correlation(Over(_1), Default())(locate, true)
      .saveAsText(ctx, s"./tmp.${tool}/pws3.out", Cell.toShortString(true, "|"), Default())
      .toUnit
  }

  def test25[
    C <: Context[C]
  ](
    ctx: C,
    path: String,
    tool: String
  )(implicit
    ev1: PairwiseDistance.MutualInformationTuner[C#U, Default[NoParameters]],
    ev2: Persist.SaveAsTextTuner[C#U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.matrix._

    // see http://www.eecs.harvard.edu/cs286r/courses/fall10/papers/Chapter2.pdf example 2.2.1 for data

    def locate[
      P <: HList
    ] = (l: Position[P], r: Position[P]) => Option(Position(s"${r.toShortString("|")},${l.toShortString("|")}"))

    ctx
      .loadText(path + "/mutualInputfile.txt", Cell.shortStringParser(StringCodec :: StringCodec :: HNil, "|"))
      .data
      .mutualInformation(Over(_1), Default())(locate, true)
      .saveAsText(ctx, s"./tmp.${tool}/mi.out", Cell.toShortString(true, "|"), Default())
      .toUnit

    ctx
      .loadText(path + "/mutualInputfile.txt", Cell.shortStringParser(StringCodec :: StringCodec :: HNil, "|"))
      .data
      .mutualInformation(Along(_0), Default())(locate, true)
      .saveAsText(ctx, s"./tmp.${tool}/im.out", Cell.toShortString(true, "|"), Default())
      .toUnit
  }

  def test26[
    C <: Context[C]
  ](
    ctx: C,
    path: String,
    tool: String
  )(implicit
    ev1: Matrix.PairTuner[C#U, Default[NoParameters]],
    ev2: Persist.SaveAsTextTuner[C#U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.matrix._

    val (left, _) = ctx
      .loadText(path + "/algebraInputfile1.txt", Cell.shortStringParser(StringCodec :: StringCodec :: HNil, "|"))
    val (right, _) = ctx
      .loadText(path + "/algebraInputfile2.txt", Cell.shortStringParser(StringCodec :: StringCodec :: HNil, "|"))

    left
      .pairBetween(Over(_0), Default())(
        All,
        right,
        Times(Locate.PrependPairwiseSelectedStringToRemainder(Over(_0), "(%1$s*%2$s)", false, "|"))
      )
      .saveAsText(ctx, s"./tmp.${tool}/alg.out", Cell.toShortString(true, "|"), Default())
      .toUnit
  }

  def test27[
    C <: Context[C]
  ](
    ctx: C,
    path: String,
    tool: String
  )(implicit
    ev1: Persist.SaveAsTextTuner[C#U, Default[NoParameters]],
    ev2: Matrix.SlideTuner[C#U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.matrix._

    // http://www.statisticshowto.com/moving-average/

    ctx
      .loadText(path + "/simMovAvgInputfile.txt", Cell.shortStringParser(LongCodec :: StringCodec :: HNil, "|"))
      .data
      .slide(Over(_1), Default())(true, SimpleMovingAverage(5, Locate.AppendRemainderDimension(_0)))
      .saveAsText(ctx, s"./tmp.${tool}/sma1.out", Cell.toShortString(true, "|"), Default())
      .toUnit

    ctx
      .loadText(path + "/simMovAvgInputfile.txt", Cell.shortStringParser(LongCodec :: StringCodec :: HNil, "|"))
      .data
      .slide(Over(_1), Default())(true, SimpleMovingAverage(5, Locate.AppendRemainderDimension(_0), all = true))
      .saveAsText(ctx, s"./tmp.${tool}/sma2.out", Cell.toShortString(true, "|"), Default())
      .toUnit

    ctx
      .loadText(path + "/simMovAvgInputfile.txt", Cell.shortStringParser(LongCodec :: StringCodec :: HNil, "|"))
      .data
      .slide(Over(_1), Default())(true, CenteredMovingAverage(2, Locate.AppendRemainderDimension(_0)))
      .saveAsText(ctx, s"./tmp.${tool}/tma.out", Cell.toShortString(true, "|"), Default())
      .toUnit

    ctx
      .loadText(path + "/simMovAvgInputfile.txt", Cell.shortStringParser(LongCodec :: StringCodec :: HNil, "|"))
      .data
      .slide(Over(_1), Default())(true, WeightedMovingAverage(5, Locate.AppendRemainderDimension(_0)))
      .saveAsText(ctx, s"./tmp.${tool}/wma1.out", Cell.toShortString(true, "|"), Default())
      .toUnit

    ctx
      .loadText(path + "/simMovAvgInputfile.txt", Cell.shortStringParser(LongCodec :: StringCodec :: HNil, "|"))
      .data
      .slide(Over(_1), Default())(true, WeightedMovingAverage(5, Locate.AppendRemainderDimension(_0), all = true))
      .saveAsText(ctx, s"./tmp.${tool}/wma2.out", Cell.toShortString(true, "|"), Default())
      .toUnit

    // http://stackoverflow.com/questions/11074665/how-to-calculate-the-cumulative-average-for-some-numbers

    ctx
      .loadText(path + "/cumMovAvgInputfile.txt", Cell.shortStringParser(StringCodec :: HNil, "|"))
      .data
      .slide(Along(_0), Default())(true, CumulativeMovingAverage(Locate.AppendRemainderDimension(_0)))
      .saveAsText(ctx, s"./tmp.${tool}/cma.out", Cell.toShortString(true, "|"), Default())
      .toUnit

    // http://www.incrediblecharts.com/indicators/exponential_moving_average.php

    ctx
      .loadText(path + "/expMovAvgInputfile.txt", Cell.shortStringParser(StringCodec :: HNil, "|"))
      .data
      .slide(Along(_0), Default())(true, ExponentialMovingAverage(0.33, Locate.AppendRemainderDimension(_0)))
      .saveAsText(ctx, s"./tmp.${tool}/ema.out", Cell.toShortString(true, "|"), Default())
      .toUnit
  }

  def test28[
    C <: Context[C]
  ](
    ctx: C,
    tool: String
  )(implicit
    ev1: Matrix.GatherTuner[C#U, Default[NoParameters]],
    ev2: Persist.SaveAsTextTuner[C#U, Default[NoParameters]],
    ev3: Matrix.SummariseTuner[C#U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.matrix._

    implicit val c = ctx

    val data = List
      .range(0, 16)
      .flatMap { case i =>
        List(
          ("iid:" + i, "fid:A", Content(ContinuousSchema[Long](), i.toLong)),
          ("iid:" + i, "fid:B", Content(NominalSchema[String](), i.toString))
        )
      }

    val stats = data
      .summarise(
        Along(_0),
        Default()
      )(
        Counts().andThenRelocate(_.position.append("count").toOption),
        Minimum().andThenRelocate(_.position.append("min").toOption),
        Maximum().andThenRelocate(_.position.append("max").toOption),
        Mean().andThenRelocate(_.position.append("mean").toOption),
        StandardDeviation(biased = true).andThenRelocate(_.position.append("sd").toOption),
        Skewness().andThenRelocate(_.position.append("skewness").toOption)
      )
      .gatherByPosition(Over(_0), Default())

    val extractor = ExtractWithDimension[Coordinates2[String, String], _1, List[Double]]

    data
      .transformWithValue(ctx.library.rules.fixed(stats, "min", "max", 4), Cut(extractor))
      .saveAsText(ctx, s"./tmp.${tool}/cut1.out", Cell.toShortString(true, "|"), Default())
      .toUnit

    data
      .transformWithValue(
        ctx.library.rules.squareRootChoice(stats, "count", "min", "max"),
        Cut(extractor).andThenRelocate(Locate.RenameDimension(_1, (c: Value[String]) => s"${c.toShortString}.square"))
      )
      .saveAsText(ctx, s"./tmp.${tool}/cut2.out", Cell.toShortString(true, "|"), Default())
      .toUnit

    data
      .transformWithValue(
        ctx.library.rules.sturgesFormula(stats, "count", "min", "max"),
        Cut(extractor).andThenRelocate(Locate.RenameDimension(_1, (c: Value[String]) => s"${c.toShortString}.sturges"))
      )
      .saveAsText(ctx, s"./tmp.${tool}/cut3.out", Cell.toShortString(true, "|"), Default())
      .toUnit

    data
      .transformWithValue(
        ctx.library.rules.riceRule(stats, "count", "min", "max"),
        Cut(extractor).andThenRelocate(Locate.RenameDimension(_1, (c: Value[String]) => s"${c.toShortString}.rice"))
      )
      .saveAsText(ctx, s"./tmp.${tool}/cut4.out", Cell.toShortString(true, "|"), Default())
      .toUnit

    data
      .transformWithValue(
        ctx.library.rules.doanesFormula(stats, "count", "min", "max", "skewness"),
        Cut(extractor).andThenRelocate(Locate.RenameDimension(_1, (c: Value[String]) => s"${c.toShortString}.doane"))
      )
      .saveAsText(ctx, s"./tmp.${tool}/cut5.out", Cell.toShortString(true, "|"), Default())
      .toUnit

    data
      .transformWithValue(
        ctx.library.rules.scottsNormalReferenceRule(stats, "count", "min", "max", "sd"),
        Cut(extractor).andThenRelocate(Locate.RenameDimension(_1, (c: Value[String]) => s"${c.toShortString}.scott"))
      )
      .saveAsText(ctx, s"./tmp.${tool}/cut6.out", Cell.toShortString(true, "|"), Default())
      .toUnit

    data
      .transformWithValue(
        ctx.library.rules.breaks(Map("fid:A" -> List(-1, 4, 8, 12, 16))),
        Cut(extractor).andThenRelocate(Locate.RenameDimension(_1, (c: Value[String]) => s"${c.toShortString}.break"))
      )
      .saveAsText(ctx, s"./tmp.${tool}/cut7.out", Cell.toShortString(true, "|"), Default())
      .toUnit
  }

  def test29[
    C <: Context[C]
  ](
    ctx: C,
    tool: String
  )(implicit
    ev: Persist.SaveAsTextTuner[C#U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.matrix._

    implicit val c = ctx

    val schema = DiscreteSchema[Long]()
    val data = List(
      ("iid:A", Content(schema, 0L)),
      ("iid:B", Content(schema, 1L)),
      ("iid:C", Content(schema, 2L)),
      ("iid:D", Content(schema, 3L)),
      ("iid:E", Content(schema, 4L)),
      ("iid:F", Content(schema, 5L)),
      ("iid:G", Content(schema, 6L)),
      ("iid:H", Content(schema, 7L))
    )

    data
      .stream(
        "Rscript double.R",
        List("double.R"),
        Cell.toShortString(true, "|"),
        Cell.shortStringParser(StringCodec :: LongCodec :: HNil, "#"),
        Reducers(1)
      )
      .data
      .saveAsText(ctx, s"./tmp.${tool}/strm.out", Cell.toShortString(true, "|"), Default())
      .toUnit
  }

  def test30[
    C <: Context[C]
  ](
    ctx: C,
    path: String,
    tool: String
  )(implicit
    ev: Persist.SaveAsTextTuner[C#U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.environment._
    import ctx.implicits.matrix._

    val (data, errors) = ctx
      .loadText(
        path + "/badInputfile.txt",
        Cell.shortStringParser(StringCodec :: StringCodec :: DateCodec() :: HNil, "|")
      )

    data
      .saveAsText(ctx, s"./tmp.${tool}/yok.out", (c) => List(c.toString), Default())
      .toUnit

    errors
      .map(_.getMessage)
      .saveAsText(ctx, s"./tmp.${tool}/nok.out", Default())
      .toUnit
  }

  def test31[
    C <: Context[C]
  ](
    ctx: C,
    tool: String
  )(implicit
    ev: Matrix.SaveAsIVTuner[C#U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.matrix._

    implicit val c = ctx

    List(
      ("a", Content(ContinuousSchema[Double](), 3.14)),
      ("b", Content(DiscreteSchema[Long](), 42L)),
      ("c", Content(NominalSchema[String](), "foo"))
    )
      .saveAsIV(ctx, s"./tmp.${tool}/iv1.out", "%1$s.dict.%2$d", "|", Default())
      .toUnit

    List(
      ("a", "d", Content(ContinuousSchema[Double](), 3.14)),
      ("b", "c", Content(DiscreteSchema[Long](), 42L)),
      ("c", "b", Content(NominalSchema[String](), "foo"))
    )
      .saveAsIV(ctx, s"./tmp.${tool}/iv2.out", "%1$s.dict.%2$d", "|", Default())
      .toUnit

    List(
      ("a", "d", "c", Content(ContinuousSchema[Double](), 3.14)),
      ("b", "c", "d", Content(DiscreteSchema[Long](), 42L)),
      ("c", "b", "e", Content(NominalSchema[String](), "foo"))
    )
      .saveAsIV(ctx, s"./tmp.${tool}/iv3.out", "%1$s.dict.%2$d", "|", Default())
      .toUnit

    List(
      ("a", "d", "c", "d", Content(ContinuousSchema[Double](), 3.14)),
      ("b", "c", "d", "e", Content(DiscreteSchema[Long](), 42L)),
      ("c", "b", "e", "f", Content(NominalSchema[String](), "foo"))
    )
      .saveAsIV(ctx, s"./tmp.${tool}/iv4.out", "%1$s.dict.%2$d", "|", Default())
      .toUnit

    List(
      ("a", "d", "c", "d", "e", Content(ContinuousSchema[Double](), 3.14)),
      ("b", "c", "d", "e", "f", Content(DiscreteSchema[Long](), 42L)),
      ("c", "b", "e", "f", "g", Content(NominalSchema[String](), "foo"))
    )
      .saveAsIV(ctx, s"./tmp.${tool}/iv5.out", "%1$s.dict.%2$d", "|", Default())
      .toUnit

    List(
      ("a", "d", "c", "d", "e", "f", Content(ContinuousSchema[Double](), 3.14)),
      ("b", "c", "d", "e", "f", "g", Content(DiscreteSchema[Long](), 42L)),
      ("c", "b", "e", "f", "g", "h", Content(NominalSchema[String](), "foo"))
    )
      .saveAsIV(ctx, s"./tmp.${tool}/iv6.out", "%1$s.dict.%2$d", "|", Default())
      .toUnit

    List(
      ("a", "d", "c", "d", "e", "f", "g", Content(ContinuousSchema[Double](), 3.14)),
      ("b", "c", "d", "e", "f", "g", "h", Content(DiscreteSchema[Long](), 42L)),
      ("c", "b", "e", "f", "g", "h", "i", Content(NominalSchema[String](), "foo"))
    )
      .saveAsIV(ctx, s"./tmp.${tool}/iv7.out", "%1$s.dict.%2$d", "|", Default())
      .toUnit

    List(
      ("a", "d", "c", "d", "e", "f", "g", "h", Content(ContinuousSchema[Double](), 3.14)),
      ("b", "c", "d", "e", "f", "g", "h", "i", Content(DiscreteSchema[Long](), 42L)),
      ("c", "b", "e", "f", "g", "h", "i", "j", Content(NominalSchema[String](), "foo"))
    )
      .saveAsIV(ctx, s"./tmp.${tool}/iv8.out", "%1$s.dict.%2$d", "|", Default())
      .toUnit

    List(
      ("a", "d", "c", "d", "e", "f", "g", "h", "i", Content(ContinuousSchema[Double](), 3.14)),
      ("b", "c", "d", "e", "f", "g", "h", "i", "j", Content(DiscreteSchema[Long](), 42L)),
      ("c", "b", "e", "f", "g", "h", "i", "j", "k", Content(NominalSchema[String](), "foo"))
    )
      .saveAsIV(ctx, s"./tmp.${tool}/iv9.out", "%1$s.dict.%2$d", "|", Default())
      .toUnit
  }

  def test32[
    C <: Context[C]
  ](
    ctx: C,
    tool: String
  )(implicit
    ev: Matrix.SaveAsVWTuner[C#U, Default[NoParameters]]
  ): Unit = {
    import ctx.implicits.cell._
    import ctx.implicits.matrix._

    implicit val c = ctx

    val data = List(
      ("a", "one", Content(ContinuousSchema[Double](), 3.14)),
      ("a", "two", Content(NominalSchema[String](), "foo")),
      ("a", "three", Content(DiscreteSchema[Long](), 42L)),
      ("b", "one", Content(ContinuousSchema[Double](), 6.28)),
      ("b", "two", Content(DiscreteSchema[Long](), 123L)),
      ("b", "three", Content(ContinuousSchema[Double](), 9.42)),
      ("c", "two", Content(NominalSchema[String](), "bar")),
      ("c", "three", Content(ContinuousSchema[Double](), 12.56))
    )

    val labels = List(
      Cell(Position("a"), Content(DiscreteSchema[Long](), 1L)),
      Cell(Position("b"), Content(DiscreteSchema[Long](), 2L))
    )

    val importance = List(
      Cell(Position("a"), Content(ContinuousSchema[Double](), 0.5)),
      Cell(Position("b"), Content(ContinuousSchema[Double](), 0.75))
    )

    data
      .saveAsVW(Over(_0), Default())(ctx, s"./tmp.${tool}/vw0.out", tag = false)
      .toUnit

    data
      .saveAsVW(Over(_0), Default())(ctx, s"./tmp.${tool}/vw1.out", tag = true)
      .toUnit

    data
      .saveAsVWWithLabels(Over(_0), Default())(ctx, s"./tmp.${tool}/vw2.out", labels, tag = false)
      .toUnit

    data
      .saveAsVWWithImportance(Over(_0), Default())(ctx, s"./tmp.${tool}/vw3.out", importance, tag = true)
      .toUnit

    data
      .saveAsVWWithLabelsAndImportance(
        Over(_0),
        Default()
      )(
        ctx,
        s"./tmp.${tool}/vw4.out",
        labels,
        importance,
        tag = false
      )
      .toUnit
  }

  def test33[
    C <: Context[C],
    T <: Tuner
  ](
    ctx: C,
    tool: String,
    tuner: T
  )(implicit
    ev: Persist.SaveAsTextTuner[C#U, T]
  ): Unit = {
    import ctx.implicits.environment._
    import ctx.implicits.matrix._

    implicit val c = ctx

    val data = List(
      ("a", "one", Content(ContinuousSchema[Double](), 3.14)),
      ("a", "two", Content(NominalSchema[String](), "foo")),
      ("a", "three", Content(DiscreteSchema[Long](), 42L)),
      ("b", "one", Content(ContinuousSchema[Double](), 6.28)),
      ("b", "two", Content(NominalSchema[String](), "bar")),
      ("c", "one", Content(ContinuousSchema[Double](), 12.56)),
      ("c", "three", Content(DiscreteSchema[Long](), 123L))
    )

    def writer[P <: HList](values: List[Option[Cell[P]]]) = List(
      values.map(_.map(_.content.value.toShortString).getOrElse("")).mkString("|")
    )

    val (_, errors) = data
      .streamByPosition(
        Over(_0)
      )(
        "sh ./parrot.sh",
        List("parrot.sh"),
        writer,
        Cell.shortStringParser(StringCodec :: HNil, "|"),
        5
      )

    errors
      .map(_.getMessage)
      .saveAsText(ctx, s"./tmp.${tool}/sbp.out", tuner)
      .toUnit
  }

  //TODO: Would nicer if we can move this elsewhere and retain the general structure of tests
  //Using optional type for wider compatibility between various frameworks/tools
  case class ParquetSample(a: Option[String], b: Int, c: Boolean)

  def test34[
    C <: Context[C]
  ](
     ctx: C,
     path: String,
     tool: String
   )(implicit
     ev1: ParquetConfig[ParquetSample, C],
     ev2: Persist.SaveAsTextTuner[C#U, Default[NoParameters]]
   ): Unit = {
    import ctx.implicits.matrix._

    def dummyParser(arg: ParquetSample): TraversableOnce[Try[Cell[Coordinates1[String]]]] = List(
      Try(Cell(Position("b"), Content(DiscreteSchema[Int](), arg.b)))
    )

    ctx
      .loadParquet[ParquetSample, Coordinates1[String]](path + "/test.parquet", dummyParser)
      .data
      .saveAsText(ctx, s"./tmp.${tool}/prq.out", Cell.toShortString(true, "|"), Default())
      .toUnit
  }
}

