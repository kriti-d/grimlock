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

import com.typesafe.sbt.SbtSite.SiteKeys.makeSite

import sbtassembly.MergeStrategy

import sbtunidoc.Plugin.UnidocKeys.unidoc

lazy val all = Project(
  id = "grimlock-all",
  base = file("."),
  settings = Seq(assembly := file(""), publishArtifact := false) ++ standardSettings ++ websiteSettings,
  aggregate = Seq(core, examples)
)

lazy val core = Project(
  id = "grimlock-core",
  base = file("grimlock-core"),
  settings = Seq(
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % "2.3.2"
        exclude("com.google.code.findbugs",   "jsr305")                   // 1.3.9         -> 3.0.2   [spark-sql]
        exclude("com.google.guava",           "guava")                    // 11.0.2        -> 14.0.1  [scalding-core]
        exclude("commons-codec",              "commons-codec")            // 1.4, 1.11     -> 1.10    [spark-sql] !!
        exclude("org.apache.avro",            "avro")                     // 1.7.7         -> 1.8.0   [parquet-avro]
        exclude("org.apache.commons",         "commons-compress")         // 1.4.1         -> 1.8.1   [parquet-avro]
        exclude("org.apache.httpcomponents",  "httpclient")               // 4.2.4         -> 4.4.1   [spark-sql]
        ,
      "org.apache.spark" %% "spark-sql" % "2.3.2"
        exclude("com.fasterxml.jackson.core", "jackson-core")             // 2.7.9         -> 2.6.7.1 [spark-core] !!
        exclude("org.slf4j",                  "slf4j-api")                // 1.7.5, 1.7.25 -> 1.7.16  [spark-core] !!
        ,
      "org.apache.parquet" % "parquet-avro" % "1.8.3"
        exclude("org.slf4j",                  "slf4j-api")                // 1.7.7         -> 1.7.16  [spark-core]
        exclude("org.xerial.snappy",          "snappy-java")              // 1.1.1.3       -> 1.1.2.6 [spark-core]
        ,
      "com.typesafe.play" %% "play-json" % "2.6.10"
        exclude("com.fasterxml.jackson.core", "jackson-core")             // 2.8.11        -> 2.6.7   [spark-core] !!
        exclude("com.fasterxml.jackson.core", "jackson-annotations")      // 2.8.11        -> 2.6.7   [spark-core] !!
        exclude("com.fasterxml.jackson.core", "jackson-databind")         // 2.8.11.1      -> 2.6.7.1 [spark-core] !!
        ,
      "com.tdunning" % "t-digest" % "3.2"
        ,
      "com.chuusai" %% "shapeless" % "2.3.3"
        ,
      "com.twitter" %% "scalding-core" % "0.17.4"
        exclude("org.codehaus.janino",        "janino")                   // 2.7.5         -> 3.0.8   [spark-sql]
        exclude("org.slf4j",                  "slf4j-api")                // 1.6.6         -> 1.7.16  [spark-core]
        ,
      "com.twitter" %% "scalding-parquet-scrooge" % "0.17.4"
        exclude("commons-lang",               "commons-lang")             // 2.5           -> 2.6     [spark-core]
        exclude("org.apache.httpcomponents",  "httpclient")               // 4.0.1         -> 4.4.1   [spark-sql]
        exclude("org.apache.parquet",         "parquet-column")           // 1.8.1         -> 1.8.3   [spark-sql]
        exclude("org.apache.parquet",         "parquet-hadoop")           // 1.8.1         -> 1.8.3   [spark-sql]
        exclude("org.apache.parquet",         "parquet-jackson")          // 1.8.1         -> 1.8.3   [spark-sql]
        exclude("org.slf4j",                  "slf4j-api")                // 1.6.6         -> 1.7.16  [spark-core]
        ,
      "com.twitter" %% "scalding-parquet" % "0.17.4"
        exclude("commons-lang",               "commons-lang")             // 2.5           -> 2.6     [spark-core]
        exclude("org.apache.httpcomponents",  "httpclient")               // 4.0.1         -> 4.4.1   [spark-sql]
        exclude("org.apache.parquet",         "parquet-column")           // 1.8.1         -> 1.8.3   [spark-sql]
        exclude("org.apache.parquet",         "parquet-hadoop")           // 1.8.1         -> 1.8.3   [spark-sql]
        exclude("org.apache.parquet",         "parquet-jackson")          // 1.8.1         -> 1.8.3   [spark-sql]
        exclude("org.slf4j",                  "slf4j-api")                // 1.6.6         -> 1.7.16  [spark-core]
        ,
      "org.scalatest" %% "scalatest" % "3.0.5" % "test"
        exclude("org.scala-lang.modules",     "scala-xml_2.11")           // 1.0.6         -> 1.0.5   [scala] !!
    )
  ) ++
  standardSettings ++
  resolverSettings ++
  assemblySettings ++
  documentationSettings
)

lazy val examples = Project(
  id = "grimlock-examples",
  base = file("grimlock-examples"),
  settings = standardSettings ++ assemblySettings ++ shadeShapelessSettings
).dependsOn(core % "test->test;compile->compile")

lazy val assemblySettings = Seq(
  assemblyMergeStrategy in assembly := { // TODO: Clean up this implementation
    case "META-INF/LICENSE" => MergeStrategy.rename
    case "META-INF/license" => MergeStrategy.rename
    case "META-INF/NOTICE.txt" => MergeStrategy.rename
    case "META-INF/LICENSE.txt" => MergeStrategy.rename
    case "META-INF/MANIFEST.MF" => MergeStrategy.discard
    case "application.conf" => MergeStrategy.concat
    case "reference.conf" => MergeStrategy.concat
    case PathList("META-INF", xs) if xs.toLowerCase.endsWith(".dsa") => MergeStrategy.discard
    case PathList("META-INF", xs) if xs.toLowerCase.endsWith(".rsa") => MergeStrategy.discard
    case PathList("META-INF", xs) if xs.toLowerCase.endsWith(".sf") => MergeStrategy.discard
    case _ => MergeStrategy.first
  },
  artifact in (Compile, assembly) ~= { _.copy(`classifier` = Some("assembly")) }
) ++
addArtifact(artifact in (Compile, assembly), assembly)

lazy val shadeShapelessSettings = Seq(
  assemblyShadeRules in assembly := Seq(
    // Shading shapeless, as it conflicts when running with spark's shapeless prior to spark-2.2.
    ShadeRule.rename("shapeless.**" -> "shapeless233.@1").inAll
  )
)

lazy val compilerSettings = Seq(
  scalaVersion := "2.11.12",
  scalacOptions ++= Seq(
    "-Xfatal-warnings",
    "-Ywarn-unused-import",
    "-Ywarn-unused",
    "-Ywarn-dead-code",
    "-deprecation",
    "-unchecked",
    "-Xlint",
    "-feature",
    "-language:_"
  ),
  scalacOptions in (Compile, console) ~= (_.filterNot(Set("-Xfatal-warnings", "-Ywarn-unused-import"))),
  scalacOptions in (Compile, doc) ~= (_.filterNot(Set("-Xfatal-warnings"))),
  scalacOptions in (Test, console) := (scalacOptions in (Compile, console)).value,
  javacOptions ++= Seq("-Xlint:unchecked")
)

lazy val dependencySettings = Seq(
  conflictManager := ConflictManager.strict,
  dependencyOverrides ++= Set(
    "com.thoughtworks.paranamer" % "paranamer"            % "2.6",        // 2.3           -> 2.6     [spark-core]
    "javax.activation"           % "activation"           % "1.1.1",      // 1.1           -> 1.1.1   [spark-core]

    // sbt version conflict warnings
    "commons-net"                % "commons-net"          % "2.2",        // 3.1           -> 2.2     [spark-core] !!
    "io.netty"                   % "netty"                % "3.9.9.Final" // 3.6.2, 3.7.0  -> 3.9.9   [spark-core]
  )
)

lazy val documentationSettings = Seq(
  autoAPIMappings := true,
  apiURL := Some(url("https://github.com/CommBank/grimlock"))
)

lazy val resolverSettings = Seq(
  resolvers += "Concurrent Maven Repo" at "http://conjars.org/repo"         // cascading jars
)

lazy val standardSettings = Defaults.coreDefaultSettings ++ Seq(
  licenses := Seq("Apache License, Version 2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0.txt")),
  organization := "au.com.cba.omnia"
) ++
dependencySettings ++
compilerSettings ++
testSettings

lazy val testSettings = Seq(test in assembly := {}, parallelExecution in Test := false)

lazy val websiteSettings = unidocSettings ++ site.settings ++ Seq(
  site.addMappingsToSiteDir(mappings in (ScalaUnidoc, packageDoc), "latest/api"),
  includeFilter in makeSite := "*.html" | "*.css" | "*.png" | "*.jpg" | "*.gif" | "*.js" | "*.swf" | "*.md" | "*.yml",
  apiURL := Some(url(s"https://commbank.github.io/${baseDirectory.value.getName}/latest/api")),
  scalacOptions in (ScalaUnidoc, unidoc) ++= Seq(
    "-sourcepath",
    baseDirectory.value.getAbsolutePath,
    "-doc-source-url",
    s"https://github.com/CommBank/${baseDirectory.value.getName}/blob/master/€{FILE_PATH}.scala"
  )
)

