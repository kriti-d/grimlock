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

import sbt._
import sbt.Keys._
import sbtassembly.AssemblyKeys._

import au.com.cba.omnia.uniform.assembly.UniformAssemblyPlugin._
import au.com.cba.omnia.uniform.core.standard.StandardProjectPlugin.uniform
import au.com.cba.omnia.uniform.dependency.UniformDependencyPlugin._
import au.com.cba.omnia.uniform.thrift.UniformThriftPlugin._

object build extends Build {
  lazy val all = Project(
    id = "all",
    base = file("."),
    settings = uniform.project("grimlock-all", "commbank.grimlock.all") ++
      common ++
      uniform.ghsettings ++
      Seq(assembly := file(""), publishArtifact := false),
    aggregate = Seq(core, examples)
  )

  lazy val core = Project(
    id = "core",
    base = file("core"),
    settings = uniform.project("grimlock-core", "commbank.grimlock") ++
      common ++
      dependencies ++
      overrides ++
      uniformAssemblySettings ++
      Seq(test in assembly := {}, parallelExecution in Test := false)
   )

  lazy val examples = Project(
    id = "examples",
    base = file("examples"),
    settings = uniform.project("grimlock-examples", "commbank.grimlock.examples") ++
      common ++
      uniformAssemblySettings ++
      Seq(libraryDependencies ++= depend.hadoopClasspath)
  ).dependsOn(core % "test->test;compile->compile")

  lazy val common: Seq[Setting[_]] = uniformDependencySettings ++
    strictDependencySettings ++
    uniform.docSettings("https://github.com/CommBank/grimlock")

  lazy val dependencies: Seq[Setting[_]] = Seq(libraryDependencies <++=
    scalaVersion.apply(scalaVersion => depend.hadoopClasspath ++
      depend.scalding() ++
      depend.parquet() ++
      depend.omnia("ebenezer", "0.22.2-20160619063420-4eb964f") ++
      depend.shapeless("2.3.0") ++
      Seq(
        noHadoop("org.apache.spark" %% "spark-core" % "1.6.2")
          exclude("com.twitter", "chill-java")
          exclude("com.twitter", "chill_2.11"),
        "com.tdunning"  %  "t-digest"  % "3.2-20160726-OMNIA",
        "org.scalatest" %% "scalatest" % "2.2.4" % "test"
      )
    )
  )

  lazy val overrides: Seq[Setting[_]] = Seq(
    dependencyOverrides ++= Set(
      "org.scala-lang.modules" %% "scala-parser-combinators" % "1.0.3",
      "org.scala-lang.modules" %% "scala-xml"                % "1.0.3",
      "org.apache.commons"     %  "commons-lang3"            % "3.3.2"
    )
  )
}

