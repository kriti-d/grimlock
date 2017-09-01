grimlock
========

[![Build Status](https://travis-ci.org/CommBank/grimlock.svg?branch=master)](https://travis-ci.org/CommBank/grimlock)
[![Gitter](https://badges.gitter.im/Join Chat.svg)](https://gitter.im/CommBank/grimlock?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

Overview
--------

The grimlock library can be used for performing data-science and machine learning related data preparation, aggregation, manipulation and querying tasks. It makes it easy to perform such tasks as:

* Normalisation/Standardisation/Bucketing of numeric variables;
* Binarisation of categorical variables;
* Creating indicator variables;
* Computing statistics in all dimensions;
* Generating data for a variety of machine learning tools;
* Partitioning and sampling of data;
* Derived features such as gradients and moving averages;
* Text analysis (tf-idf/LDA);
* Computing pairwise distances.

The library contains default implementations for many of the above tasks. It also has a number of useful properties:

* Supports wide variety of variable types;
* Is easily extensible;
* Can operate in multiple dimensions (currently up to 9);
* Supports heterogeneous data;
* Can be used in the Scalding/Spark REPL;
* Supports basic as well as structured data types.

Getting Started
-------------

Simply add the following lines to your build file:

```
libraryDependencies += "au.com.cba.omnia" %% "grimlock-core" % "0.4.4"
resolvers += "commbank-releases" at "http://commbank.artifactoryonline.com/commbank/ext-releases-local"
```

For information on the API, see the [scaladoc](https://commbank.github.io/grimlock/latest/api/index.html). Links to detailed examples are included at the end of the Scalding and Spark sections below.

Concepts
--------

### Data Structures

The basic data structure in grimlock is a N-dimensional sparse __Matrix__ (N=1..9). Each __Cell__ in a Matrix consists of a __Position__ and __Content__.

```
          Matrix
            ^ 1
            |
            | M
  Cell(Position, Content)
```

The Position is, essentially, a list of N coordinates (where each coordinate is stored in a __Value__). The Content consists of a __Schema__ together with a Value. The Value contains the data (Double, Sting, Date, etc.) of the cell, while the Schema defines the variable type of the data, and (optionally) what it's legal values are.

```
   Position              Content
       ^ 1                  ^ 1
       |                    |
       | N           +------+------+
     Value           | 1           | 1
                  Schema         Value
```

Lastly, a __Codec__ can be used to parse and write the basic data types used in the values.

```
  Value
    ^ 1
    |
    | 1
  Codec
```

### Working with dimensions

Performing operations along any of the dimensions of the matrix is supported through a __Slice__. There are two realisations of Slice: __Along__ and __Over__. Both are constructed with a single dimension (__shapeless.Nat__), but differ in how the dimension is interpreted. When using Over, all data in the matrix is grouped by the dimension and operations, such as aggregation, are applied to the resulting groups. When using Along, the data is grouped by all dimensions *except* the dimension used when constructing the Slice. The differences between Over and Along are graphically presented below for a three dimensional matrix. Note that in 2 dimensions, Along and Over are each other's inverse.

```
        Over(_2)          Along(_3)

     +----+------+      +-----------+
    /    /|     /|     /     _     /|
   /    / |    / |    /    /|_|   / |
  +----+------+  |   +-----------+  |
  |    |  |   |  |   |   /_/ /   |  |
  |    |  +   |  +   |   |_|/    |  +
  |    | /    | /    |           | /
  |    |/     |/     |           |/
  +----+------+      +-----------+
```

### Data Format

The basic data format used by grimlock (though others are supported) is a column-oriented pipe separated file (each row is a single cell). The first N fields are the coordinates, optionally followed by the variable type and codec (again pipe separated). If the variable type and codec are omitted from the data then they have to be provided by a __Dictionary__. The last field of each row is the value.

In the example below the first field is a coordinate identifying an instance, the second field is a coordinate identifying a feature. The third and fourth columns are the codec and variable type respectively. The last column has the actual value.

```
> head <path to>/grimlock/examples/src/main/scala/commbank/grimlock/data/exampleInput.txt
iid:0064402|fid:B|string|nominal|H
iid:0064402|fid:E|long|continuous|219
iid:0064402|fid:H|string|nominal|C
iid:0066848|fid:A|long|continuous|371
iid:0066848|fid:B|string|nominal|H
iid:0066848|fid:C|long|continuous|259
iid:0066848|fid:D|string|nominal|F
iid:0066848|fid:E|long|continuous|830
iid:0066848|fid:F|string|nominal|G
iid:0066848|fid:H|string|nominal|B
...
```

If the type and codec were omitted then the data would look as follows:

```
iid:0064402|fid:B|H
iid:0064402|fid:E|219
iid:0064402|fid:H|C
iid:0066848|fid:A|371
iid:0066848|fid:B|H
iid:0066848|fid:C|259
iid:0066848|fid:D|F
iid:0066848|fid:E|830
iid:0066848|fid:F|G
iid:0066848|fid:H|B
...
```

An external dictionary will then have to be provided to correctly decode and validate the values:

```
fid:A|long|continuous
fid:B|string|nominal
fid:C|long|continuous
fid:D|string|nominal
fid:E|long|continuous
fid:F|string|nominal
fid:H|string|nominal
...
```

Usage - Scalding
-----

### Setting up REPL

The examples below are executed in the Scalding REPL. To use grimlock in the REPL follow the following steps:

1. Install Scalding; follow [these](https://github.com/twitter/scalding/wiki/Getting-Started) instructions.
2. Check out tag (0.16); `git checkout 0.16`.
3. Update `build.sbt` of the scalding project. For the module `scaldingRepl`, add `grimlock` as a dependency under the `libraryDependencies`:
    `"au.com.cba.omnia" %% "grimlock-core" % "<version-string>"`;
4. Update `project/plugins.sbt` to add the 'commbank-ext' repository to the `resolvers`:
    `"commbank-ext" at "http://commbank.artifactoryonline.com/commbank/ext-releases-local-ivy"`
5. Start REPL; `./sbt scalding-repl/console`.

After the last command, the console should appear as follows:

```
> ./sbt scalding-repl/console
...
[info] Starting scala interpreter...
[info] 
import com.twitter.scalding._
import com.twitter.scalding.ReplImplicits._
import com.twitter.scalding.ReplImplicitContext._
Welcome to Scala version 2.11.7 (Java HotSpot(TM) 64-Bit Server VM, Java 1.8.0_92).
Type in expressions to have them evaluated.
Type :help for more information.

scala>
```

Note, for readability, the REPL info is suppressed from now on.

### Getting started

When at the Scalding REPL console, the first step is to import grimlock's functionality (be sure to press ctrl-D after the last import statement):

```
> scala> :paste
// Entering paste mode (ctrl-D to finish)

import commbank.grimlock.framework._
import commbank.grimlock.framework.position._
import commbank.grimlock.library.aggregate._
import commbank.grimlock.scalding.environment._

import shapeless.nat.{ _1, _2 }

```

Next, for convenience, set up grimlock's Context as an implicit:

```
scala> implicit val context = Context()
```

The next step is to read in data (be sure to change <path to> to the correct path to the grimlock repo):

```
scala> val (data, _) = context.loadText(
  "<path to>/grimlock/examples/src/main/scala/commbank/grimlock/data/exampleInput.txt",
  Cell.parse2D()
)
```

The returned `data` is a 2 dimensional matrix. To investigate it's content Scalding's `dump` command can be used in the REPL, use grimlock's `saveAsText` API for writing to disk:

```
scala> data.dump
Cell(Position(StringValue(iid:0064402,StringCodec),StringValue(fid:B,StringCodec)),Content(NominalSchema[String](),StringValue(H,StringCodec)))
Cell(Position(StringValue(iid:0064402,StringCodec),StringValue(fid:E,StringCodec)),Content(ContinuousSchema[Long](),LongValue(219,LongCodec)))
Cell(Position(StringValue(iid:0064402,StringCodec),StringValue(fid:H,StringCodec)),Content(NominalSchema[String](),StringValue(C,StringCodec)))
Cell(Position(StringValue(iid:0066848,StringCodec),StringValue(fid:A,StringCodec)),Content(ContinuousSchema[Long](),LongValue(371,LongCodec)))
Cell(Position(StringValue(iid:0066848,StringCodec),StringValue(fid:B,StringCodec)),Content(NominalSchema[String](),StringValue(H,StringCodec)))
Cell(Position(StringValue(iid:0066848,StringCodec),StringValue(fid:C,StringCodec)),Content(ContinuousSchema[Long](),LongValue(259,LongCodec)))
Cell(Position(StringValue(iid:0066848,StringCodec),StringValue(fid:D,StringCodec)),Content(NominalSchema[String](),StringValue(F,StringCodec)))
Cell(Position(StringValue(iid:0066848,StringCodec),StringValue(fid:E,StringCodec)),Content(ContinuousSchema[Long](),LongValue(830,LongCodec)))
Cell(Position(StringValue(iid:0066848,StringCodec),StringValue(fid:F,StringCodec)),Content(NominalSchema[String](),StringValue(G,StringCodec)))
Cell(Position(StringValue(iid:0066848,StringCodec),StringValue(fid:H,StringCodec)),Content(NominalSchema[String](),StringValue(B,StringCodec)))
...
```

The following shows a number of basic operations (get number of rows, get type of features, perform simple query):

```
scala> data.size(_1).dump
Cell(Position(LongValue(1,LongCodec)),Content(DiscreteSchema[Long](),LongValue(9,LongCodec)))

scala> data.types(Over(_2))(false).dump
Cell(Position(StringValue(fid:A,StringCodec)),Content(NominalSchema[Type](),TypeValue(Numeric,TypeCodec)))
Cell(Position(StringValue(fid:B,StringCodec)),Content(NominalSchema[Type](),TypeValue(Categorical,TypeCodec)))
Cell(Position(StringValue(fid:C,StringCodec)),Content(NominalSchema[Type](),TypeValue(Numeric,TypeCodec)))
Cell(Position(StringValue(fid:D,StringCodec)),Content(NominalSchema[Type](),TypeValue(Categorical,TypeCodec)))
Cell(Position(StringValue(fid:E,StringCodec)),Content(NominalSchema[Type](),TypeValue(Numeric,TypeCodec)))
Cell(Position(StringValue(fid:F,StringCodec)),Content(NominalSchema[Type](),TypeValue(Categorical,TypeCodec)))
Cell(Position(StringValue(fid:G,StringCodec)),Content(NominalSchema[Type](),TypeValue(Numeric,TypeCodec)))
Cell(Position(StringValue(fid:H,StringCodec)),Content(NominalSchema[Type](),TypeValue(Categorical,TypeCodec)))

scala> data.which((cell: Cell[_2]) => (cell.content.value gtr 995) || (cell.content.value equ "F")).dump
Position(StringValue(iid:0066848,StringCodec),StringValue(fid:D,StringCodec))
Position(StringValue(iid:0216406,StringCodec),StringValue(fid:E,StringCodec))
Position(StringValue(iid:0444510,StringCodec),StringValue(fid:D,StringCodec))
```

Now for something a little more interesting. Let's compute the number of features for each instance and then compute the moments of the distribution of counts:

```
scala> val counts = data.summarise(Over(_1))(Counts())

scala> counts.dump
Cell(Position(StringValue(iid:0064402,StringCodec)),Content(DiscreteSchema[Long](),LongValue(3,LongCodec)))
Cell(Position(StringValue(iid:0066848,StringCodec)),Content(DiscreteSchema[Long](),LongValue(7,LongCodec)))
Cell(Position(StringValue(iid:0216406,StringCodec)),Content(DiscreteSchema[Long](),LongValue(5,LongCodec)))
Cell(Position(StringValue(iid:0221707,StringCodec)),Content(DiscreteSchema[Long](),LongValue(4,LongCodec)))
Cell(Position(StringValue(iid:0262443,StringCodec)),Content(DiscreteSchema[Long](),LongValue(2,LongCodec)))
Cell(Position(StringValue(iid:0364354,StringCodec)),Content(DiscreteSchema[Long](),LongValue(5,LongCodec)))
Cell(Position(StringValue(iid:0375226,StringCodec)),Content(DiscreteSchema[Long](),LongValue(3,LongCodec)))
Cell(Position(StringValue(iid:0444510,StringCodec)),Content(DiscreteSchema[Long](),LongValue(5,LongCodec)))
Cell(Position(StringValue(iid:1004305,StringCodec)),Content(DiscreteSchema[Long](),LongValue(2,LongCodec)))

scala> counts.summarise(Along(_1))(
  Mean().andThenRelocate(_.position.append("mean").toOption),
  StandardDeviation().andThenRelocate(_.position.append("sd").toOption),
  Skewness().andThenRelocate(_.position.append("skewness").toOption),
  Kurtosis().andThenRelocate(_.position.append("kurtosis").toOption)
).dump
Cell(Position(StringValue(mean,StringCodec)),Content(ContinuousSchema[Double](),DoubleValue(4.0,DoubleCodec)))
Cell(Position(StringValue(sd,StringCodec)),Content(ContinuousSchema[Double](),DoubleValue(1.6583123951777,DoubleCodec)))
Cell(Position(StringValue(skewness,StringCodec)),Content(ContinuousSchema[Double](),DoubleValue(0.348873899490999,DoubleCodec)))
Cell(Position(StringValue(kurtosis,StringCodec)),Content(ContinuousSchema[Double](),DoubleValue(2.194214876033058,DoubleCodec)))
```

Computing the moments can also be achieved more concisely as follows:

```
scala> counts.summarise(Along(_1))(
  Moments(
    _.append("mean").toOption,
    _.append("sd").toOption,
    _.append("skewness").toOption,
    _.append("kurtosis").toOption
  )
).dump

```

For more examples see [BasicOperations.scala](https://github.com/CommBank/grimlock/blob/master/examples/src/main/scala/commbank/grimlock/scalding/BasicOperations.scala), [Conditional.scala](https://github.com/CommBank/grimlock/blob/master/examples/src/main/scala/commbank/grimlock/scalding/Conditional.scala), [DataAnalysis.scala](https://github.com/CommBank/grimlock/blob/master/examples/src/main/scala/commbank/grimlock/scalding/DataAnalysis.scala), [DerivedData.scala](https://github.com/CommBank/grimlock/blob/master/examples/src/main/scala/commbank/grimlock/scalding/DerivedData.scala), [Ensemble.scala](https://github.com/CommBank/grimlock/blob/master/examples/src/main/scala/commbank/grimlock/scalding/Ensemble.scala), [Event.scala](https://github.com/CommBank/grimlock/blob/master/examples/src/main/scala/commbank/grimlock/scalding/Event.scala), [LabelWeighting.scala](https://github.com/CommBank/grimlock/blob/master/examples/src/main/scala/commbank/grimlock/scalding/LabelWeighting.scala), [MutualInformation.scala](https://github.com/CommBank/grimlock/blob/master/examples/src/main/scala/commbank/grimlock/scalding/MutualInformation.scala), [PipelineDataPreparation.scala](https://github.com/CommBank/grimlock/blob/master/examples/src/main/scala/commbank/grimlock/scalding/PipelineDataPreparation.scala) or [Scoring.scala](https://github.com/CommBank/grimlock/blob/master/examples/src/main/scala/commbank/grimlock/scalding/Scoring.scala).

Usage - Spark
-----

### Setting up REPL

The examples below are executed in the Spark REPL. To use grimlock in the REPL follow the following steps:

1. Download the latest source code release for Spark from [here](http://spark.apache.org/downloads.html).
2. You can, optionally, suppress much of the console INFO output. Follow [these](http://stackoverflow.com/questions/28189408/how-to-reduce-the-verbosity-of-sparks-runtime-output) instructions.
3. Update the shapeless jar (in the `jars` folder) to version: `shapeless_2.11-2.3.2.jar`.
4. Start REPL; `./bin/spark-shell --master local --jars <path to>/grimlock-core_2.11-assembly.jar`.

After the last command, the console should appear as follows:

```
> ./bin/spark-shell --master local --jars <path to>/grimlock-core_2.11-assembly.jar
...
Spark context available as sc (master = local, ...).
Spark session available as 'spark'
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 2.1.1
      /_/

Using Scala version 2.11.8 (OpenJDK 64-Bit Server VM, Java 1.8.0_92)
Type in expressions to have them evaluated.
Type :help for more information.

scala>
```

Note, for readability, the REPL info is suppressed from now on.

### Getting started

When at the Spark REPL console, the first step is to import grimlock's functionality (be sure to press ctrl-D after the last import statement):

```
> scala> :paste
// Entering paste mode (ctrl-D to finish)

import commbank.grimlock.framework._
import commbank.grimlock.framework.position._
import commbank.grimlock.library.aggregate._
import commbank.grimlock.spark.environment._

import shapeless.nat.{ _1, _2 }

```

Next, for convenience, set up grimlock's Context as an implicit:

```
scala> implicit val context = Context(sc)
```

The next step is to read in data (be sure to change <path to> to the correct path to the grimlock repo):

```
scala> val (data, _) = context.loadText(
  "<path to>/grimlock/examples/src/main/scala/commbank/grimlock/data/exampleInput.txt",
  Cell.parse2D()
)
```

The returned `data` is a 2 dimensional matrix. To investigate it's content Spark's `foreach` command can be used in the REPL, use the grimlock's `saveAsText` API for writing to disk:

```
scala> data.foreach(println)
Cell(Position(StringValue(iid:0064402,StringCodec),StringValue(fid:B,StringCodec)),Content(NominalSchema[String](),StringValue(H,StringCodec)))
Cell(Position(StringValue(iid:0064402,StringCodec),StringValue(fid:E,StringCodec)),Content(ContinuousSchema[Long](),LongValue(219,LongCodec)))
Cell(Position(StringValue(iid:0064402,StringCodec),StringValue(fid:H,StringCodec)),Content(NominalSchema[String](),StringValue(C,StringCodec)))
Cell(Position(StringValue(iid:0066848,StringCodec),StringValue(fid:A,StringCodec)),Content(ContinuousSchema[Long](),LongValue(371,LongCodec)))
Cell(Position(StringValue(iid:0066848,StringCodec),StringValue(fid:B,StringCodec)),Content(NominalSchema[String](),StringValue(H,StringCodec)))
Cell(Position(StringValue(iid:0066848,StringCodec),StringValue(fid:C,StringCodec)),Content(ContinuousSchema[Long](),LongValue(259,LongCodec)))
Cell(Position(StringValue(iid:0066848,StringCodec),StringValue(fid:D,StringCodec)),Content(NominalSchema[String](),StringValue(F,StringCodec)))
Cell(Position(StringValue(iid:0066848,StringCodec),StringValue(fid:E,StringCodec)),Content(ContinuousSchema[Long](),LongValue(830,LongCodec)))
Cell(Position(StringValue(iid:0066848,StringCodec),StringValue(fid:F,StringCodec)),Content(NominalSchema[String](),StringValue(G,StringCodec)))
Cell(Position(StringValue(iid:0066848,StringCodec),StringValue(fid:H,StringCodec)),Content(NominalSchema[String](),StringValue(B,StringCodec)))
...
```

The following shows a number of basic operations (get number of rows, get type of features, perform simple query):

```
scala> data.size(_1).foreach(println)
Cell(Position(LongValue(1,LongCodec)),Content(DiscreteSchema[Long](),LongValue(9,LongCodec)))

scala> data.types(Over(_2))(false).foreach(println)
Cell(Position(StringValue(fid:A,StringCodec)),Content(NominalSchema[Type](),TypeValue(Numeric,TypeCodec)))
Cell(Position(StringValue(fid:B,StringCodec)),Content(NominalSchema[Type](),TypeValue(Categorical,TypeCodec)))
Cell(Position(StringValue(fid:C,StringCodec)),Content(NominalSchema[Type](),TypeValue(Numeric,TypeCodec)))
Cell(Position(StringValue(fid:D,StringCodec)),Content(NominalSchema[Type](),TypeValue(Categorical,TypeCodec)))
Cell(Position(StringValue(fid:E,StringCodec)),Content(NominalSchema[Type](),TypeValue(Numeric,TypeCodec)))
Cell(Position(StringValue(fid:F,StringCodec)),Content(NominalSchema[Type](),TypeValue(Categorical,TypeCodec)))
Cell(Position(StringValue(fid:G,StringCodec)),Content(NominalSchema[Type](),TypeValue(Numeric,TypeCodec)))
Cell(Position(StringValue(fid:H,StringCodec)),Content(NominalSchema[Type](),TypeValue(Categorical,TypeCodec)))

scala> data.which((cell: Cell[_2]) => (cell.content.value gtr 995) || (cell.content.value equ "F")).foreach(println)
Position(StringValue(iid:0066848,StringCodec),StringValue(fid:D,StringCodec))
Position(StringValue(iid:0216406,StringCodec),StringValue(fid:E,StringCodec))
Position(StringValue(iid:0444510,StringCodec),StringValue(fid:D,StringCodec))
```

Now for something a little more interesting. Let's compute the number of features for each instance and then compute the moments of the distribution of counts:

```
scala> val counts = data.summarise(Over(_1))(Counts())

scala> counts.foreach(println)
Cell(Position(StringValue(iid:0064402,StringCodec)),Content(DiscreteSchema[Long](),LongValue(3,LongCodec)))
Cell(Position(StringValue(iid:0066848,StringCodec)),Content(DiscreteSchema[Long](),LongValue(7,LongCodec)))
Cell(Position(StringValue(iid:0216406,StringCodec)),Content(DiscreteSchema[Long](),LongValue(5,LongCodec)))
Cell(Position(StringValue(iid:0221707,StringCodec)),Content(DiscreteSchema[Long](),LongValue(4,LongCodec)))
Cell(Position(StringValue(iid:0262443,StringCodec)),Content(DiscreteSchema[Long](),LongValue(2,LongCodec)))
Cell(Position(StringValue(iid:0364354,StringCodec)),Content(DiscreteSchema[Long](),LongValue(5,LongCodec)))
Cell(Position(StringValue(iid:0375226,StringCodec)),Content(DiscreteSchema[Long](),LongValue(3,LongCodec)))
Cell(Position(StringValue(iid:0444510,StringCodec)),Content(DiscreteSchema[Long](),LongValue(5,LongCodec)))
Cell(Position(StringValue(iid:1004305,StringCodec)),Content(DiscreteSchema[Long](),LongValue(2,LongCodec)))

scala> counts.summarise(Along(_1))(
  Mean().andThenRelocate(_.position.append("mean").toOption),
  StandardDeviation().andThenRelocate(_.position.append("sd").toOption),
  Skewness().andThenRelocate(_.position.append("skewness").toOption),
  Kurtosis().andThenRelocate(_.position.append("kurtosis").toOption)
).foreach(println)
Cell(Position(StringValue(mean,StringCodec)),Content(ContinuousSchema[Double](),DoubleValue(4.0,DoubleCodec)))
Cell(Position(StringValue(sd,StringCodec)),Content(ContinuousSchema[Double](),DoubleValue(1.6583123951777,DoubleCodec)))
Cell(Position(StringValue(skewness,StringCodec)),Content(ContinuousSchema[Double](),DoubleValue(0.348873899490999,DoubleCodec)))
Cell(Position(StringValue(kurtosis,StringCodec)),Content(ContinuousSchema[Double](),DoubleValue(2.194214876033058,DoubleCodec)))
```

Computing the moments can also be achieved more concisely as follows:

```
scala> counts.summarise(Along(_1))(
  Moments(
    _.append("mean").toOption,
    _.append("sd").toOption,
    _.append("skewness").toOption,
    _.append("kurtosis").toOption
  )
).foreach(println)
```

For more examples see [BasicOperations.scala](https://github.com/CommBank/grimlock/blob/master/examples/src/main/scala/commbank/grimlock/spark/BasicOperations.scala), [Conditional.scala](https://github.com/CommBank/grimlock/blob/master/examples/src/main/scala/commbank/grimlock/spark/Conditional.scala), [DataAnalysis.scala](https://github.com/CommBank/grimlock/blob/master/examples/src/main/scala/commbank/grimlock/spark/DataAnalysis.scala), [DerivedData.scala](https://github.com/CommBank/grimlock/blob/master/examples/src/main/scala/commbank/grimlock/spark/DerivedData.scala), [Ensemble.scala](https://github.com/CommBank/grimlock/blob/master/examples/src/main/scala/commbank/grimlock/spark/Ensemble.scala), [Event.scala](https://github.com/CommBank/grimlock/blob/master/examples/src/main/scala/commbank/grimlock/spark/Event.scala), [LabelWeighting.scala](https://github.com/CommBank/grimlock/blob/master/examples/src/main/scala/commbank/grimlock/spark/LabelWeighting.scala), [MutualInformation.scala](https://github.com/CommBank/grimlock/blob/master/examples/src/main/scala/commbank/grimlock/spark/MutualInformation.scala), [PipelineDataPreparation.scala](https://github.com/CommBank/grimlock/blob/master/examples/src/main/scala/commbank/grimlock/spark/PipelineDataPreparation.scala) or [Scoring.scala](https://github.com/CommBank/grimlock/blob/master/examples/src/main/scala/commbank/grimlock/spark/Scoring.scala).

Acknowledgement
---------------
We would like to thank the YourKit team for their support in providing us with their excellent [Java Profiler](https://www.yourkit.com/java/profiler/index.jsp). ![YourKit Logo](yk_logo.png)

