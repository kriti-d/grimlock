// Copyright 2014,2015,2016 Commonwealth Bank of Australia
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
import commbank.grimlock.framework.content.metadata._
import commbank.grimlock.framework.encoding._
import commbank.grimlock.framework.pairwise._
import commbank.grimlock.framework.partition._
import commbank.grimlock.framework.position._
import commbank.grimlock.framework.sample._
import commbank.grimlock.framework.transform._
import commbank.grimlock.framework.Type._
import commbank.grimlock.framework.window._

import commbank.grimlock.library.aggregate._
import commbank.grimlock.library.pairwise._
import commbank.grimlock.library.partition._
import commbank.grimlock.library.squash._
import commbank.grimlock.library.transform._
import commbank.grimlock.library.window._

import commbank.grimlock.spark.environment._
import commbank.grimlock.spark.environment.Context._
import commbank.grimlock.spark.Matrix._
import commbank.grimlock.spark.transform._

import commbank.grimlock.test.TestSparkReader._

import org.apache.spark.{ SparkContext, SparkConf }
import org.apache.spark.rdd.RDD

import scala.io.Source

import shapeless.Nat
import shapeless.nat._
import shapeless.ops.nat.{ LTEq, ToInt }

object TestSparkReader {
  def load4TupleDataAddDate(ctx: Context, file: String): RDD[Cell[_3]] = {
    def hashDate(v: String) = {
      val cal = java.util.Calendar.getInstance()

      cal.setTime((new java.text.SimpleDateFormat("yyyy-MM-dd")).parse("2014-05-14"))
      cal.add(java.util.Calendar.DATE, -(v.hashCode % 21)) // Generate 3 week window prior to date

      DateValue(cal.getTime(), DateCodec())
    }

    ctx.context.textFile(file)
      .flatMap {
        _.trim.split(java.util.regex.Pattern.quote("|"), 4) match {
          case Array(i, f, e, v) =>
            val content = e match {
              case "string" => StringCodec.decode(v).map(c => Content(NominalSchema[String](), c))
              case _ => scala.util.Try(v.toLong).toOption match {
                case Some(_) => LongCodec.decode(v).map(c => Content(ContinuousSchema[Long](), c))
                case None => DoubleCodec.decode(v).map(c => Content(ContinuousSchema[Double](), c))
              }
            }

            content.map(c => Cell(Position(i, f, hashDate(v)), c))
          case _ => None
        }
      }
  }
}

object TestSpark1 {
  def main(args: Array[String]) {
    implicit val ctx = Context(new SparkContext(args(0), "Test Spark", new SparkConf()))
    val tool = "spark"
    val path = args(1)

    val data = load4TupleDataAddDate(ctx, path + "/someInputfile3.txt")

    data
      .saveAsText(ctx, s"./tmp.${tool}/dat1.out", Cell.toString(descriptive = true))
      .toUnit

    data
      .set(
        Cell(
          Position("iid:1548763", "fid:Y", DateCodec().decode("2014-04-26").get),
          Content(ContinuousSchema[Long](), 1234)
        )
      )
      .slice(Over(_1))("iid:1548763", true)
      .saveAsText(ctx, s"./tmp.${tool}/dat2.out", Cell.toString(descriptive = true))
      .toUnit

    loadText(ctx, path + "/smallInputfile.txt", Cell.parse3D(third = DateCodec()))
      .data
      .saveAsText(ctx, s"./tmp.${tool}/dat3.out", Cell.toString(descriptive = true))
      .toUnit
  }
}

object TestSpark2 {
  def main(args: Array[String]) {
    implicit val ctx = Context(new SparkContext(args(0), "Test Spark", new SparkConf()))
    val tool = "spark"
    val path = args(1)

    val data = load4TupleDataAddDate(ctx, path + "/someInputfile3.txt")

    (data.names(Over(_1)) ++ data.names(Over(_2)) ++ data.names(Over(_3)))
      .saveAsText(ctx, s"./tmp.${tool}/nm0.out", Position.toString(descriptive = true))
      .toUnit

    data
      .names(Over(_2))
      .slice("fid:M", false)
      .saveAsText(ctx, s"./tmp.${tool}/nm2.out", Position.toString(descriptive = true))
      .toUnit

    data
      .names(Over(_2))
      .slice(""".*[BCD]$""".r, true, "")
      .saveAsText(ctx, s"./tmp.${tool}/nm5.out", Position.toString(descriptive = true))
      .toUnit
  }
}

object TestSpark3 {
  def main(args: Array[String]) {
    implicit val ctx = Context(new SparkContext(args(0), "Test Spark", new SparkConf()))
    val tool = "spark"
    val path = args(1)

    val data = load4TupleDataAddDate(ctx, path + "/someInputfile3.txt")

    (data.types(Over(_1))(false) ++ data.types(Over(_2))(false) ++ data.types(Over(_3))(false))
      .saveAsText(ctx, s"./tmp.${tool}/typ1.out", Type.toString(descriptive = true))
      .toUnit

    (data.types(Over(_1))(true) ++ data.types(Over(_2))(true) ++ data.types(Over(_3))(true))
      .saveAsText(ctx, s"./tmp.${tool}/typ2.out", Type.toString(descriptive = true))
      .toUnit
  }
}

object TestSpark4 {
  def main(args: Array[String]) {
    implicit val ctx = Context(new SparkContext(args(0), "Test Spark", new SparkConf()))
    val tool = "spark"
    val path = args(1)

    val data = load4TupleDataAddDate(ctx, path + "/someInputfile3.txt")

    data
      .slice(Over(_2))("fid:B", true)
      .saveAsText(ctx, s"./tmp.${tool}/scl0.out", Cell.toString(descriptive = true))
      .toUnit

    data
      .slice(Over(_2))(List("fid:A", "fid:B"), true)
      .slice(Over(_1))("iid:0221707", true)
      .saveAsText(ctx, s"./tmp.${tool}/scl1.out", Cell.toString(descriptive = true))
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
      .slice(Over(_2))(data.names(Over(_2)).slice(rem, false), false)
      .saveAsText(ctx, s"./tmp.${tool}/scl2.out", Cell.toString(descriptive = true))
      .toUnit
  }
}

object TestSpark5 {
  def main(args: Array[String]) {
    implicit val ctx = Context(new SparkContext(args(0), "Test Spark", new SparkConf()))
    val tool = "spark"
    val path = args(1)

    val data = load4TupleDataAddDate(ctx, path + "/someInputfile3.txt")

    data
      .slice(Over(_2))(List("fid:A", "fid:B"), true)
      .slice(Over(_1))("iid:0221707", true)
      .squash(_3, PreservingMaxPosition())
      .saveAsText(ctx, s"./tmp.${tool}/sqs1.out", Cell.toString(descriptive = true))
      .toUnit

    data
      .squash(_3, PreservingMaxPosition())
      .saveAsText(ctx, s"./tmp.${tool}/sqs2.out", Cell.toString(descriptive = true))
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
      .slice(Over(_1))(ids, true)
      .squash(_3, PreservingMaxPosition())
      .saveAsCSV(Over(_2))(ctx, s"./tmp.${tool}/sqs3.out")
      .toUnit

    data
      .slice(Over(_1))(ids, true)
      .slice(Over(_2))(List("fid:A", "fid:B", "fid:C", "fid:D", "fid:E", "fid:F", "fid:G"), true)
      .squash(_3, PreservingMaxPosition())
      .saveAsCSV(Over(_2))(ctx, s"./tmp.${tool}/sqs4.out")
      .toUnit
  }
}

object TestSpark6 {
  def main(args: Array[String]) {
    implicit val ctx = Context(new SparkContext(args(0), "Test Spark", new SparkConf()))
    val tool = "spark"
    val path = args(1)

    val data = load4TupleDataAddDate(ctx, path + "/someInputfile3.txt")

    data
      .which(c => c.content.schema.kind.isSpecialisationOf(Numerical))
      .saveAsText(ctx, s"./tmp.${tool}/whc1.out", Position.toString(descriptive = true))
      .toUnit

    data
      .which(c => !c.content.value.isInstanceOf[StringValue])
      .saveAsText(ctx, s"./tmp.${tool}/whc2.out", Position.toString(descriptive = true))
      .toUnit

    data
      .get(data.which(c => (c.content.value equ 666) || (c.content.value leq 11.0) || (c.content.value equ "KQUPKFEH")))
      .saveAsText(ctx, s"./tmp.${tool}/whc3.out", Cell.toString(descriptive = true))
      .toUnit

    data
      .which(c => c.content.value.isInstanceOf[LongValue])
      .saveAsText(ctx, s"./tmp.${tool}/whc4.out", Position.toString(descriptive = true))
      .toUnit

    val aggregators: List[Aggregator[_2, _1, _2]] = List(
      Count().andThenRelocate(_.position.append("count").toOption),
      Mean().andThenRelocate(_.position.append("mean").toOption),
      Min().andThenRelocate(_.position.append("min").toOption),
      Max().andThenRelocate(_.position.append("max").toOption),
      MaxAbs().andThenRelocate(_.position.append("max.abs").toOption)
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

    load4TupleDataAddDate(ctx, path + "/someInputfile3.txt")
      .slice(Over(_1))(ids, true)
      .slice(Over(_2))(List("fid:A", "fid:B", "fid:C", "fid:D", "fid:E", "fid:F", "fid:G"), true)
      .squash(_3, PreservingMaxPosition())
      .summarise(Along(_1))(aggregators)
      .whichByPosition(Over(_2))(List(("count", c => c.content.value leq 2), ("min", c => c.content.value equ 107)))
      .saveAsText(ctx, s"./tmp.${tool}/whc5.out", Position.toString(descriptive = true))
      .toUnit
  }
}

object TestSpark7 {
  def main(args: Array[String]) {
    implicit val ctx = Context(new SparkContext(args(0), "Test Spark", new SparkConf()))
    val tool = "spark"
    val path = args(1)

    val data = load4TupleDataAddDate(ctx, path + "/someInputfile3.txt")

    data
      .get(Position("iid:1548763", "fid:Y", DateCodec().decode("2014-04-26").get))
      .saveAsText(ctx, s"./tmp.${tool}/get1.out", Cell.toString(descriptive = true))
      .toUnit

    data
      .get(
        List(
          Position("iid:1548763", "fid:Y", DateCodec().decode("2014-04-26").get),
          Position("iid:1303823", "fid:A", DateCodec().decode("2014-05-05").get)
        )
      )
      .saveAsText(ctx, s"./tmp.${tool}/get2.out", Cell.toString(descriptive = true))
      .toUnit
  }
}

object TestSpark8 {
  def main(args: Array[String]) {
    implicit val ctx = Context(new SparkContext(args(0), "Test Spark", new SparkConf()))
    val tool = "spark"
    val path = args(1)

    val data = load4TupleDataAddDate(ctx, path + "/someInputfile3.txt")

    data
      .slice(Over(_2))("fid:B", true)
      .squash(_3, PreservingMaxPosition())
      .unique()
      .saveAsText(ctx, s"./tmp.${tool}/uniq.out", Content.toString(descriptive = true))
      .toUnit

    loadText(ctx, path + "/mutualInputfile.txt", Cell.parse2D())
      .data
      .uniqueByPosition(Over(_2))
      .saveAsText(ctx, s"./tmp.${tool}/uni2.out", IndexedContent.toString(codec = false, schema = false))
      .toUnit

    data
      .slice(Over(_2))(List("fid:A", "fid:B", "fid:Y", "fid:Z"), true)
      .slice(Over(_1))(List("iid:0221707", "iid:0364354"), true)
      .squash(_3, PreservingMaxPosition())
      .saveAsCSV(Over(_1))(ctx, s"./tmp.${tool}/test.csv")
      .saveAsCSV(Over(_2))(ctx, s"./tmp.${tool}/tset.csv", writeHeader = false, separator = ",")
      .toUnit

    data
      .slice(Over(_2))(List("fid:A", "fid:B", "fid:Y", "fid:Z"), true)
      .slice(Over(_1))(List("iid:0221707", "iid:0364354"), true)
      .squash(_3, PreservingMaxPosition())
      .permute(_2, _1)
      .saveAsText(ctx, s"./tmp.${tool}/trs1.out", Cell.toString(descriptive = true))
      .toUnit

    data
      .slice(Over(_2))(List("fid:A", "fid:B", "fid:Y", "fid:Z"), true)
      .slice(Over(_1))(List("iid:0221707", "iid:0364354"), true)
      .squash(_3, PreservingMaxPosition())
      .saveAsText(ctx, s"./tmp.${tool}/data.txt")
      .toUnit
  }
}

object TestSpark9 {
  def main(args: Array[String]) {
    implicit val ctx = Context(new SparkContext(args(0), "Test Spark", new SparkConf()))
    val tool = "spark"
    val path = args(1)

    val data = load4TupleDataAddDate(ctx, path + "/someInputfile3.txt")

    case class StringPartitioner[D <: Nat : ToInt](dim: D)(implicit ev: LTEq[D, _2]) extends Partitioner[_2, String] {
      def assign(cell: Cell[_2]): TraversableOnce[String] = List(cell.position(dim) match {
        case StringValue("fid:A", _) => "training"
        case StringValue("fid:B", _) => "testing"
      }, "scoring")
    }

    val prt1 = data
      .slice(Over(_2))(List("fid:A", "fid:B"), true)
      .slice(Over(_1))(List("iid:0221707", "iid:0364354"), true)
      .squash(_3, PreservingMaxPosition())
      .split(StringPartitioner(_2))

    prt1
      .saveAsText(ctx, s"./tmp.${tool}/prt1.out", Partition.toString(descriptive = true))
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
      .slice(Over(_2))(List("fid:A", "fid:B"), true)
      .slice(Over(_1))(List("iid:0221707", "iid:0364354"), true)
      .squash(_3, PreservingMaxPosition())
      .split(IntTuplePartitioner(_2))
      .saveAsText(ctx, s"./tmp.${tool}/prt2.out", Partition.toString(descriptive = true))
      .toUnit

    prt1
      .get("training")
      .saveAsText(ctx, s"./tmp.${tool}/train.out", Cell.toString(descriptive = true))
      .toUnit

    prt1
      .get("testing")
      .saveAsText(ctx, s"./tmp.${tool}/test.out", Cell.toString(descriptive = true))
      .toUnit

    prt1
      .get("scoring")
      .saveAsText(ctx, s"./tmp.${tool}/score.out", Cell.toString(descriptive = true))
      .toUnit
  }
}

object TestSpark10 {
  def main(args: Array[String]) {
    implicit val ctx = Context(new SparkContext(args(0), "Test Spark", new SparkConf()))
    val tool = "spark"
    val path = args(1)

    val data = load4TupleDataAddDate(ctx, path + "/someInputfile3.txt")

    data
      .summarise(Over(_2))(Mean(false, true, true).andThenRelocate(_.position.append("mean").toOption))
      .saveAsCSV(Over(_1))(ctx, s"./tmp.${tool}/agg1.csv")
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
      .slice(Over(_1))(ids, true)
      .squash(_3, PreservingMaxPosition())
      .summarise(Along(_2))(Count().andThenRelocate(_.position.append("count").toOption))
      .saveAsCSV(Over(_1))(ctx, s"./tmp.${tool}/agg2.csv")
      .toUnit

    val aggregators: List[Aggregator[_2, _1, _2]] = List(
      Count().andThenRelocate(_.position.append("count").toOption),
      Mean().andThenRelocate(_.position.append("mean").toOption),
      StandardDeviation(biased = true).andThenRelocate(_.position.append("sd").toOption),
      Skewness().andThenRelocate(_.position.append("skewness").toOption),
      Kurtosis().andThenRelocate(_.position.append("kurtosis").toOption),
      Min().andThenRelocate(_.position.append("min").toOption),
      Max().andThenRelocate(_.position.append("max").toOption),
      MaxAbs().andThenRelocate(_.position.append("max.abs").toOption)
    )

    data
      .slice(Over(_1))(ids, true)
      .squash(_3, PreservingMaxPosition())
      .summarise(Along(_1))(aggregators)
      .saveAsCSV(Over(_1))(ctx, s"./tmp.${tool}/agg3.csv")
      .toUnit
  }
}

object TestSpark11 {
  def main(args: Array[String]) {
    implicit val ctx = Context(new SparkContext(args(0), "Test Spark", new SparkConf()))
    val tool = "spark"
    val path = args(1)

    val data = load4TupleDataAddDate(ctx, path + "/someInputfile3.txt")

    data
      .slice(Over(_2))(List("fid:A", "fid:B", "fid:Y", "fid:Z"), true)
      .slice(Over(_1))(List("iid:0221707", "iid:0364354"), true)
      .transform(Indicator().andThenRelocate(Locate.RenameDimension(_2, "%1$s.ind")))
      .saveAsText(ctx, s"./tmp.${tool}/trn2.out", Cell.toString(descriptive = true))
      .toUnit

    data
      .slice(Over(_2))(List("fid:A", "fid:B", "fid:Y", "fid:Z"), true)
      .slice(Over(_1))(List("iid:0221707", "iid:0364354"), true)
      .squash(_3, PreservingMaxPosition())
      .transform(Binarise(Locate.RenameDimensionWithContent(_2)))
      .saveAsCSV(Over(_1))(ctx, s"./tmp.${tool}/trn3.out")
      .toUnit
  }
}

object TestSpark12 {
  def main(args: Array[String]) {
    implicit val ctx = Context(new SparkContext(args(0), "Test Spark", new SparkConf()))
    val tool = "spark"
    val path = args(1)

    val data = load4TupleDataAddDate(ctx, path + "/someInputfile3.txt")
      .slice(Over(_2))(List("fid:A", "fid:B", "fid:Y", "fid:Z"), true)
      .slice(Over(_1))(List("iid:0221707", "iid:0364354"), true)

    data
      .squash(_3, PreservingMaxPosition())
      .fillHomogeneous(Content(ContinuousSchema[Long](), 0))
      .saveAsCSV(Over(_1))(ctx, s"./tmp.${tool}/fll1.out")
      .toUnit

    data
      .fillHomogeneous(Content(ContinuousSchema[Long](), 0))
      .saveAsText(ctx, s"./tmp.${tool}/fll3.out", Cell.toString(descriptive = true))
      .toUnit
  }
}

object TestSpark13 {
  def main(args: Array[String]) {
    implicit val ctx = Context(new SparkContext(args(0), "Test Spark", new SparkConf()))
    val tool = "spark"
    val path = args(1)

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

    val all = load4TupleDataAddDate(ctx, path + "/someInputfile3.txt")
    val data = all
      .slice(Over(_1))(ids, true)
      .slice(Over(_2))(List("fid:A", "fid:B", "fid:C", "fid:D", "fid:E", "fid:F", "fid:G"), true)
      .squash(_3, PreservingMaxPosition())

    val inds = data
      .transform(Indicator().andThenRelocate(Locate.RenameDimension(_2, "%1$s.ind")))
      .fillHomogeneous(Content(ContinuousSchema[Long](), 0))

    data
      .join(Over(_1))(inds)
      .fillHomogeneous(Content(ContinuousSchema[Long](), 0))
      .saveAsCSV(Over(_1))(ctx, s"./tmp.${tool}/fll2.out")
      .toUnit

    data
      .fillHeterogeneous(Over(_2))(all.summarise(Over(_2))(Mean(false, true, true)))
      .join(Over(_1))(inds)
      .saveAsCSV(Over(_1))(ctx, s"./tmp.${tool}/fll4.out")
      .toUnit
  }
}

object TestSpark14 {
  def main(args: Array[String]) {
    implicit val ctx = Context(new SparkContext(args(0), "Test Spark", new SparkConf()))
    val tool = "spark"
    val path = args(1)

    val data = load4TupleDataAddDate(ctx, path + "/someInputfile3.txt")
      .slice(Over(_2))(List("fid:A", "fid:B", "fid:Y", "fid:Z"), true)
      .slice(Over(_1))(List("iid:0221707", "iid:0364354"), true)

    data
      .change(Over(_2))("fid:A", Content.parser(LongCodec, NominalSchema[Long]()))
      .data
      .saveAsText(ctx, s"./tmp.${tool}/chg1.out", Cell.toString(descriptive = true))
      .toUnit
  }
}

object TestSpark15 {
  def main(args: Array[String]) {
    implicit val ctx = Context(new SparkContext(args(0), "Test Spark", new SparkConf()))
    val tool = "spark"
    val path = args(1)

    val data = load4TupleDataAddDate(ctx, path + "/someInputfile3.txt")

    data
      .slice(Over(_2))(List("fid:A", "fid:C", "fid:E", "fid:G"), true)
      .slice(Over(_1))(List("iid:0221707", "iid:0364354"), true)
      .summarise(Along(_3))(Sum().andThenRelocate(_.position.append("sum").toOption))
      .melt(_3, _2, Value.concatenate("."))
      .saveAsCSV(Over(_1))(ctx, s"./tmp.${tool}/rsh1.out")
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
      .slice(Over(_1))(ids, true)
      .slice(Over(_2))(List("fid:A", "fid:B", "fid:C", "fid:D", "fid:E", "fid:F", "fid:G"), true)
      .squash(_3, PreservingMaxPosition())
      .transform(Indicator().andThenRelocate(Locate.RenameDimension(_2, "%1$s.ind")))
      .saveAsCSV(Over(_1))(ctx, s"./tmp.${tool}/trn1.csv")

    data
      .slice(Over(_1))(ids, true)
      .slice(Over(_2))(List("fid:A", "fid:B", "fid:C", "fid:D", "fid:E", "fid:F", "fid:G"), true)
      .squash(_3, PreservingMaxPosition())
      .join(Over(_1))(inds)
      .saveAsCSV(Over(_1))(ctx, s"./tmp.${tool}/jn1.csv")
      .toUnit
  }
}

object TestSpark16 {
  def main(args: Array[String]) {
    implicit val ctx = Context(new SparkContext(args(0), "Test Spark", new SparkConf()))
    val tool = "spark"
    val path = args(1)

    val data = load4TupleDataAddDate(ctx, path + "/someInputfile3.txt")

    case class HashSample() extends Sampler[_3] {
      def select(cell: Cell[_3]): Boolean = (cell.position(_1).toString.hashCode % 25) == 0
    }

    data
      .subset(HashSample())
      .saveAsText(ctx, s"./tmp.${tool}/smp1.out")
      .toUnit
  }
}

object TestSpark17 {
  def main(args: Array[String]) {
    implicit val ctx = Context(new SparkContext(args(0), "Test Spark", new SparkConf()))
    val tool = "spark"
    val path = args(1)

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

    val data = load4TupleDataAddDate(ctx, path + "/someInputfile3.txt")
      .slice(Over(_1))(ids, true)
      .slice(Over(_2))(List("fid:A", "fid:B", "fid:C", "fid:D", "fid:E", "fid:F", "fid:G"), true)
      .squash(_3, PreservingMaxPosition())

    val aggregators: List[Aggregator[_2, _1, _2]] = List(
      Count().andThenRelocate(_.position.append("count").toOption),
      Mean().andThenRelocate(_.position.append("mean").toOption),
      Min().andThenRelocate(_.position.append("min").toOption),
      Max().andThenRelocate(_.position.append("max").toOption),
      MaxAbs().andThenRelocate(_.position.append("max.abs").toOption)
    )

    val stats = data
      .summarise(Along(_1))(aggregators)
      .compact(Over(_1))

    data
      .transformWithValue(
        stats,
        Normalise(ExtractWithDimensionAndKey[_2, Content](_2, "max.abs").andThenPresent(_.value.asDouble))
      )
      .saveAsCSV(Over(_1))(ctx, s"./tmp.${tool}/trn6.csv")
      .toUnit

    case class Sample500() extends Sampler[_2] {
      def select(cell: Cell[_2]): Boolean = cell.content.value gtr 500
    }

    data
      .subset(Sample500())
      .saveAsCSV(Over(_1))(ctx, s"./tmp.${tool}/flt1.csv")
      .toUnit

    case class RemoveGreaterThanMean[D <: Nat : ToInt](dim: D)(implicit ev: LTEq[D, _2]) extends SamplerWithValue[_2] {
      type V = Map[Position[_1], Map[Position[_1], Content]]

      def selectWithValue(cell: Cell[_2], ext: V): Boolean =
        if (cell.content.schema.kind.isSpecialisationOf(Numerical))
          cell.content.value leq ext(Position(cell.position(dim)))(Position("mean")).value
        else
          true
    }

    data
      .subsetWithValue(stats, RemoveGreaterThanMean(_2))
      .saveAsCSV(Over(_1))(ctx, s"./tmp.${tool}/flt2.csv")
      .toUnit
  }
}

object TestSpark18 {
  def main(args: Array[String]) {
    implicit val ctx = Context(new SparkContext(args(0), "Test Spark", new SparkConf()))
    val tool = "spark"
    val path = args(1)

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

    val data = load4TupleDataAddDate(ctx, path + "/someInputfile3.txt")
      .slice(Over(_1))(ids, true)
      .slice(Over(_2))(List("fid:A", "fid:B", "fid:C", "fid:D", "fid:E", "fid:F", "fid:G"), true)
      .squash(_3, PreservingMaxPosition())

    val aggregators: List[Aggregator[_2, _1, _2]] = List(
      Count().andThenRelocate(_.position.append("count").toOption),
      Mean().andThenRelocate(_.position.append("mean").toOption),
      Min().andThenRelocate(_.position.append("min").toOption),
      Max().andThenRelocate(_.position.append("max").toOption),
      MaxAbs().andThenRelocate(_.position.append("max.abs").toOption)
    )

    val stats = data
      .summarise(Along(_1))(aggregators)

    val rem = stats
      .whichByPosition(Over(_2))(("count", (c: Cell[_2]) => c.content.value leq 2))
      .names(Over(_1))

    data
      .slice(Over(_2))(rem, false)
      .saveAsCSV(Over(_1))(ctx, s"./tmp.${tool}/flt3.csv")
      .toUnit
  }
}

object TestSpark19 {
  def main(args: Array[String]) {
    implicit val ctx = Context(new SparkContext(args(0), "Test Spark", new SparkConf()))
    val tool = "spark"
    val path = args(1)

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

    val raw = load4TupleDataAddDate(ctx, path + "/someInputfile3.txt")
      .slice(Over(_1))(ids, true)
      .slice(Over(_2))(List("fid:A", "fid:B", "fid:C", "fid:D", "fid:E", "fid:F", "fid:G"), true)
      .squash(_3, PreservingMaxPosition())

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
      Count().andThenRelocate(_.position.append("count").toOption),
      MaxAbs().andThenRelocate(_.position.append("max.abs").toOption)
    )

    val stats = parts
      .get("train")
      .summarise(Along(_1))(aggregators)

    val rem = stats
      .which(c => (c.position(_2) equ "count") && (c.content.value leq 2))
      .names(Over(_1))

    type W = Map[Position[_1], Map[Position[_1], Content]]

    val transforms: List[TransformerWithValue[_2, _2] { type V >: W }] = List(
      Indicator().andThenRelocate(Locate.RenameDimension(_2, "%1$s.ind")),
      Binarise(Locate.RenameDimensionWithContent(_2)),
      Normalise(ExtractWithDimensionAndKey[_2, Content](_2, "max.abs").andThenPresent(_.value.asDouble))
    )

    def cb(key: String, pipe: RDD[Cell[_2]]): RDD[Cell[_2]] = pipe
      .slice(Over(_2))(rem, false)
      .transformWithValue(stats.compact(Over(_1)), transforms)
      .fillHomogeneous(Content(ContinuousSchema[Long](), 0))
      .saveAsCSV(Over(_1))(ctx, s"./tmp.${tool}/pln_" + key + ".csv")

    parts
      .forEach(List("train", "test"), cb)
      .toUnit
  }
}

object TestSpark20 {
  def main(args: Array[String]) {
    implicit val ctx = Context(new SparkContext(args(0), "Test Spark", new SparkConf()))
    val tool = "spark"
    val path = args(1)

    val (dictionary, _) = Dictionary.load(Source.fromFile(path + "/dict.txt"))

    loadText(ctx, path + "/ivoryInputfile1.txt", Cell.parse3DWithDictionary(dictionary, _2, third = DateCodec()))
      .data
      .saveAsText(ctx, s"./tmp.${tool}/ivr1.out")
      .toUnit
  }
}

object TestSpark21 {
  def main(args: Array[String]) {
    implicit val ctx = Context(new SparkContext(args(0), "Test Spark", new SparkConf()))
    val tool = "spark"
    val path = args(1)

    val data = load4TupleDataAddDate(ctx, path + "/someInputfile3.txt")

    data
      .shape()
      .saveAsText(ctx, s"./tmp.${tool}/siz0.out")
      .toUnit

    data
      .size(_1)
      .saveAsText(ctx, s"./tmp.${tool}/siz1.out")
      .toUnit

    data
      .size(_2)
      .saveAsText(ctx, s"./tmp.${tool}/siz2.out")
      .toUnit

    data
      .size(_3)
      .saveAsText(ctx, s"./tmp.${tool}/siz3.out")
      .toUnit
  }
}

object TestSpark22 {
  def main(args: Array[String]) {
    implicit val ctx = Context(new SparkContext(args(0), "Test Spark", new SparkConf()))
    val tool = "spark"
    val path = args(1)

    val (data, _) = loadText(ctx, path + "/numericInputfile.txt", Cell.parse2D())

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
      .slide(Over(_1))(true, Diff())
      .saveAsText(ctx, s"./tmp.${tool}/dif1.out")
      .toUnit

    data
      .slide(Over(_2))(true, Diff())
      .permute(_2, _1)
      .saveAsText(ctx, s"./tmp.${tool}/dif2.out")
      .toUnit
  }
}

object TestSpark23 {
  def main(args: Array[String]) {
    implicit val ctx = Context(new SparkContext(args(0), "Test Spark", new SparkConf()))
    val tool = "spark"
    val path = args(1)

    val (data, _) = loadText(ctx, path + "/somePairwise.txt", Cell.parse2D())

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
      .pairwise(Over(_2))(Upper, DiffSquared())
      .saveAsText(ctx, s"./tmp.${tool}/pws1.out")
      .toUnit
  }
}

object TestSpark24 {
  def main(args: Array[String]) {
    implicit val ctx = Context(new SparkContext(args(0), "Test Spark", new SparkConf()))
    val tool = "spark"
    val path = args(1)

    // see http://www.mathsisfun.com/data/correlation.html for data

    val schema = List(
      ("day", Content.parser(StringCodec, NominalSchema[String]())),
      ("temperature", Content.parser(DoubleCodec, ContinuousSchema[Double]())),
      ("sales", Content.parser(LongCodec, DiscreteSchema[Long]()))
    )
    val (data, _) = loadText(ctx, path + "/somePairwise2.txt", Cell.parseTable(schema, separator = "|"))

    data
      .correlation(Over(_2))
      .saveAsText(ctx, s"./tmp.${tool}/pws2.out")
      .toUnit

    val schema2 = List(
      ("day", Content.parser(StringCodec, NominalSchema[String]())),
      ("temperature", Content.parser(DoubleCodec, ContinuousSchema[Double]())),
      ("sales", Content.parser(LongCodec, DiscreteSchema[Long]())),
      ("neg.sales", Content.parser(LongCodec, DiscreteSchema[Long]()))
    )
    val (data2, _) = loadText(ctx, path + "/somePairwise3.txt", Cell.parseTable(schema2, separator = "|"))

    data2
      .correlation(Over(_2))
      .saveAsText(ctx, s"./tmp.${tool}/pws3.out")
      .toUnit
  }
}

object TestSpark25 {
  def main(args: Array[String]) {
    implicit val ctx = Context(new SparkContext(args(0), "Test Spark", new SparkConf()))
    val tool = "spark"
    val path = args(1)

    loadText(ctx, path + "/mutualInputfile.txt", Cell.parse2D())
      .data
      .mutualInformation(Over(_2))
      .saveAsText(ctx, s"./tmp.${tool}/mi.out")
      .toUnit

    loadText(ctx, path + "/mutualInputfile.txt", Cell.parse2D())
      .data
      .mutualInformation(Along(_1))
      .saveAsText(ctx, s"./tmp.${tool}/im.out")
      .toUnit
  }
}

object TestSpark26 {
  def main(args: Array[String]) {
    implicit val ctx = Context(new SparkContext(args(0), "Test Spark", new SparkConf()))
    val tool = "spark"
    val path = args(1)

    val (left, _) = loadText(ctx, path + "/algebraInputfile1.txt", Cell.parse2D())
    val (right, _) = loadText(ctx, path + "/algebraInputfile2.txt", Cell.parse2D())

    left
      .pairwiseBetween(Over(_1))(
        All,
        right,
        Times(Locate.PrependPairwiseSelectedStringToRemainder[_1, _2](Over(_1), "(%1$s*%2$s)"))
      )
      .saveAsText(ctx, s"./tmp.${tool}/alg.out")
      .toUnit
  }
}

object TestSpark27 {
  def main(args: Array[String]) {
    implicit val ctx = Context(new SparkContext(args(0), "Test Spark", new SparkConf()))
    val tool = "spark"
    val path = args(1)

    // http://www.statisticshowto.com/moving-average/

    loadText(ctx, path + "/simMovAvgInputfile.txt", Cell.parse2D(first = LongCodec))
      .data
      .slide(Over(_2))(true, SimpleMovingAverage(5, Locate.AppendRemainderDimension(_1)))
      .saveAsText(ctx, s"./tmp.${tool}/sma1.out")
      .toUnit

    loadText(ctx, path + "/simMovAvgInputfile.txt", Cell.parse2D(first = LongCodec))
      .data
      .slide(Over(_2))(true, SimpleMovingAverage(5, Locate.AppendRemainderDimension(_1), all = true))
      .saveAsText(ctx, s"./tmp.${tool}/sma2.out")
      .toUnit

    loadText(ctx, path + "/simMovAvgInputfile.txt", Cell.parse2D(first = LongCodec))
      .data
      .slide(Over(_2))(true, CenteredMovingAverage(2, Locate.AppendRemainderDimension(_1)))
      .saveAsText(ctx, s"./tmp.${tool}/tma.out")
      .toUnit

    loadText(ctx, path + "/simMovAvgInputfile.txt", Cell.parse2D(first = LongCodec))
      .data
      .slide(Over(_2))(true, WeightedMovingAverage(5, Locate.AppendRemainderDimension(_1)))
      .saveAsText(ctx, s"./tmp.${tool}/wma1.out")
      .toUnit

    loadText(ctx, path + "/simMovAvgInputfile.txt", Cell.parse2D(first = LongCodec))
      .data
      .slide(Over(_2))(true, WeightedMovingAverage(5, Locate.AppendRemainderDimension(_1), all = true))
      .saveAsText(ctx, s"./tmp.${tool}/wma2.out")
      .toUnit

    // http://stackoverflow.com/questions/11074665/how-to-calculate-the-cumulative-average-for-some-numbers

    loadText(ctx, path + "/cumMovAvgInputfile.txt", Cell.parse1D())
      .data
      .slide(Along(_1))(true, CumulativeMovingAverage(Locate.AppendRemainderDimension(_1)))
      .saveAsText(ctx, s"./tmp.${tool}/cma.out")
      .toUnit

    // http://www.incrediblecharts.com/indicators/exponential_moving_average.php

    loadText(ctx, path + "/expMovAvgInputfile.txt", Cell.parse1D())
      .data
      .slide(Along(_1))(true, ExponentialMovingAverage(0.33, Locate.AppendRemainderDimension(_1)))
      .saveAsText(ctx, s"./tmp.${tool}/ema.out")
      .toUnit
  }
}

object TestSpark28 {
  def main(args: Array[String]) {
    implicit val ctx = Context(new SparkContext(args(0), "Test Spark", new SparkConf()))
    val tool = "spark"

    val data = List
      .range(0, 16)
      .flatMap { case i =>
        List(
          ("iid:" + i, "fid:A", Content(ContinuousSchema[Long](), i)),
          ("iid:" + i, "fid:B", Content(NominalSchema[String](), i.toString))
        )
      }

    val aggregators: List[Aggregator[_2, _1, _2]] = List(
      Count().andThenRelocate(_.position.append("count").toOption),
      Min().andThenRelocate(_.position.append("min").toOption),
      Max().andThenRelocate(_.position.append("max").toOption),
      Mean().andThenRelocate(_.position.append("mean").toOption),
      StandardDeviation(biased = true).andThenRelocate(_.position.append("sd").toOption),
      Skewness().andThenRelocate(_.position.append("skewness").toOption)
    )

    val stats = data
      .summarise(Along(_1))(aggregators)
      .compact(Over(_1))

    val extractor = ExtractWithDimension[_2, List[Double]](_2)

    data
      .transformWithValue(CutRules.fixed(stats, "min", "max", 4), Cut(extractor))
      .saveAsText(ctx, s"./tmp.${tool}/cut1.out")
      .toUnit

    data
      .transformWithValue(
        CutRules.squareRootChoice(stats, "count", "min", "max"),
        Cut(extractor).andThenRelocate(Locate.RenameDimension(_2, "%s.square"))
      )
      .saveAsText(ctx, s"./tmp.${tool}/cut2.out")
      .toUnit

    data
      .transformWithValue(
        CutRules.sturgesFormula(stats, "count", "min", "max"),
        Cut(extractor).andThenRelocate(Locate.RenameDimension(_2, "%s.sturges"))
      )
      .saveAsText(ctx, s"./tmp.${tool}/cut3.out")
      .toUnit

    data
      .transformWithValue(
        CutRules.riceRule(stats, "count", "min", "max"),
        Cut(extractor).andThenRelocate(Locate.RenameDimension(_2, "%s.rice"))
      )
      .saveAsText(ctx, s"./tmp.${tool}/cut4.out")
      .toUnit

    data
      .transformWithValue(
        CutRules.doanesFormula(stats, "count", "min", "max", "skewness"),
        Cut(extractor).andThenRelocate(Locate.RenameDimension(_2, "%s.doane"))
      )
      .saveAsText(ctx, s"./tmp.${tool}/cut5.out")
      .toUnit

    data
      .transformWithValue(
        CutRules.scottsNormalReferenceRule(stats, "count", "min", "max", "sd"),
        Cut(extractor).andThenRelocate(Locate.RenameDimension(_2, "%s.scott"))
      )
      .saveAsText(ctx, s"./tmp.${tool}/cut6.out")
      .toUnit

    data
      .transformWithValue(
        CutRules.breaks(Map("fid:A" -> List(-1, 4, 8, 12, 16))),
        Cut(extractor).andThenRelocate(Locate.RenameDimension(_2, "%s.break"))
      )
      .saveAsText(ctx, s"./tmp.${tool}/cut7.out")
      .toUnit
  }
}

object TestSpark29 {
  def main(args: Array[String]) {
    implicit val ctx = Context(new SparkContext(args(0), "Test Spark", new SparkConf()))
    val tool = "spark"

    val schema = DiscreteSchema[Long]()
    val data = List(
      ("mod:123", "iid:A", Content(schema, 1)),
      ("mod:123", "iid:B", Content(schema, 1)),
      ("mod:123", "iid:C", Content(schema, 0)),
      ("mod:123", "iid:D", Content(schema, 1)),
      ("mod:123", "iid:E", Content(schema, 1)),
      ("mod:123", "iid:G", Content(schema, 0)),
      ("mod:123", "iid:H", Content(schema, 1)),
      ("mod:456", "iid:A", Content(schema, 1)),
      ("mod:456", "iid:B", Content(schema, 1)),
      ("mod:456", "iid:C", Content(schema, 1)),
      ("mod:456", "iid:E", Content(schema, 1)),
      ("mod:456", "iid:F", Content(schema, 0)),
      ("mod:456", "iid:G", Content(schema, 1)),
      ("mod:456", "iid:H", Content(schema, 0))
    )

    data
      .gini(Over(_1))
      .saveAsText(ctx, s"./tmp.${tool}/gini.out")
      .toUnit

    data
      .map { case (a, b, c) => (b, a, c) }
      .gini(Along(_1))
      .saveAsText(ctx, s"./tmp.${tool}/inig.out")
      .toUnit
  }
}

object TestSpark30 {
  def main(args: Array[String]) {
    implicit val ctx = Context(new SparkContext(args(0), "Test Spark", new SparkConf()))
    val tool = "spark"

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
        Cell.toString(false, "|", true, true),
        Cell.parse2D("#", StringCodec, LongCodec)
      )
      .data
      .saveAsText(ctx, s"./tmp.${tool}/strm.out")
      .toUnit
  }
}

object TestSpark31 {
  def main(args: Array[String]) {
    implicit val ctx = Context(new SparkContext(args(0), "Test Spark", new SparkConf()))
    val tool = "spark"
    val path = args(1)

    val (data, errors) = loadText(ctx, path + "/badInputfile.txt", Cell.parse3D(third = DateCodec()))

    data
      .saveAsText(ctx, s"./tmp.${tool}/yok.out", Cell.toString(descriptive = true))
      .toUnit

    errors.saveAsTextFile(s"./tmp.${tool}/nok.out")
  }
}

object TestSpark32 {
  def main(args: Array[String]) {
    implicit val ctx = Context(new SparkContext(args(0), "Test Spark", new SparkConf()))
    val tool = "spark"

    List(
      ("a", Content(ContinuousSchema[Double](), 3.14)),
      ("b", Content(DiscreteSchema[Long](), 42)),
      ("c", Content(NominalSchema[String](), "foo"))
    )
      .saveAsIV(ctx, s"./tmp.${tool}/iv1.out")
      .toUnit

    List(
      ("a", "d", Content(ContinuousSchema[Double](), 3.14)),
      ("b", "c", Content(DiscreteSchema[Long](), 42)),
      ("c", "b", Content(NominalSchema[String](), "foo"))
    )
      .saveAsIV(ctx, s"./tmp.${tool}/iv2.out")
      .toUnit

    List(
      ("a", "d", "c", Content(ContinuousSchema[Double](), 3.14)),
      ("b", "c", "d", Content(DiscreteSchema[Long](), 42)),
      ("c", "b", "e", Content(NominalSchema[String](), "foo"))
    )
      .saveAsIV(ctx, s"./tmp.${tool}/iv3.out")
      .toUnit

    List(
      ("a", "d", "c", "d", Content(ContinuousSchema[Double](), 3.14)),
      ("b", "c", "d", "e", Content(DiscreteSchema[Long](), 42)),
      ("c", "b", "e", "f", Content(NominalSchema[String](), "foo"))
    )
      .saveAsIV(ctx, s"./tmp.${tool}/iv4.out")
      .toUnit

    List(
      ("a", "d", "c", "d", "e", Content(ContinuousSchema[Double](), 3.14)),
      ("b", "c", "d", "e", "f", Content(DiscreteSchema[Long](), 42)),
      ("c", "b", "e", "f", "g", Content(NominalSchema[String](), "foo"))
    )
      .saveAsIV(ctx, s"./tmp.${tool}/iv5.out")
      .toUnit

    List(
      ("a", "d", "c", "d", "e", "f", Content(ContinuousSchema[Double](), 3.14)),
      ("b", "c", "d", "e", "f", "g", Content(DiscreteSchema[Long](), 42)),
      ("c", "b", "e", "f", "g", "h", Content(NominalSchema[String](), "foo"))
    )
      .saveAsIV(ctx, s"./tmp.${tool}/iv6.out")
      .toUnit

    List(
      ("a", "d", "c", "d", "e", "f", "g", Content(ContinuousSchema[Double](), 3.14)),
      ("b", "c", "d", "e", "f", "g", "h", Content(DiscreteSchema[Long](), 42)),
      ("c", "b", "e", "f", "g", "h", "i", Content(NominalSchema[String](), "foo"))
    )
      .saveAsIV(ctx, s"./tmp.${tool}/iv7.out")
      .toUnit

    List(
      ("a", "d", "c", "d", "e", "f", "g", "h", Content(ContinuousSchema[Double](), 3.14)),
      ("b", "c", "d", "e", "f", "g", "h", "i", Content(DiscreteSchema[Long](), 42)),
      ("c", "b", "e", "f", "g", "h", "i", "j", Content(NominalSchema[String](), "foo"))
    )
      .saveAsIV(ctx, s"./tmp.${tool}/iv8.out")
      .toUnit

    List(
      ("a", "d", "c", "d", "e", "f", "g", "h", "i", Content(ContinuousSchema[Double](), 3.14)),
      ("b", "c", "d", "e", "f", "g", "h", "i", "j", Content(DiscreteSchema[Long](), 42)),
      ("c", "b", "e", "f", "g", "h", "i", "j", "k", Content(NominalSchema[String](), "foo"))
    )
      .saveAsIV(ctx, s"./tmp.${tool}/iv9.out")
      .toUnit
  }
}

object TestSpark33 {
  def main(args: Array[String]) {
    implicit val ctx = Context(new SparkContext(args(0), "Test Spark", new SparkConf()))
    val tool = "spark"

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

    val labels = ctx.context.parallelize(
      List(
        Cell(Position("a"), Content(DiscreteSchema[Long](), 1)),
        Cell(Position("b"), Content(DiscreteSchema[Long](), 2))
      )
    )

    val importance = ctx.context.parallelize(
      List(
        Cell(Position("a"), Content(ContinuousSchema[Double](), 0.5)),
        Cell(Position("b"), Content(ContinuousSchema[Double](), 0.75))
      )
    )

    data
      .saveAsVW(Over(_1))(ctx, s"./tmp.${tool}/vw0.out", tag = false)
      .toUnit

    data
      .saveAsVW(Over(_1))(ctx, s"./tmp.${tool}/vw1.out", tag = true)
      .toUnit

    data
      .saveAsVWWithLabels(Over(_1))(ctx, s"./tmp.${tool}/vw2.out", labels, tag = false)
      .toUnit

    data
      .saveAsVWWithImportance(Over(_1))(ctx, s"./tmp.${tool}/vw3.out", importance, tag = true)
      .toUnit

    data
      .saveAsVWWithLabelsAndImportance(Over(_1))(ctx, s"./tmp.${tool}/vw4.out", labels, importance, tag = false)
      .toUnit
  }
}

