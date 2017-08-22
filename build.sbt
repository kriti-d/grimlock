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

import com.typesafe.sbt.SbtSite.SiteKeys.makeSite

import sbtassembly.MergeStrategy

import sbtunidoc.Plugin.UnidocKeys.unidoc

val jvmVersion = "1.7"

lazy val all = Project(
  id = "grimlock-all",
  base = file("."),
  settings = Seq(assembly := file(""), publishArtifact := false) ++ standardSettings ++ websiteSettings,
  aggregate = Seq(core, examples)
)

lazy val core = Project(
  id = "grimlock-core",
  base = file("core"),
  settings = Seq(
    libraryDependencies ++= noDerby(
      Seq(
        "org.apache.spark" %% "spark-core" % "2.1.1"
          exclude("com.google.guava",           "guava")                        // 11.0.2  -> 14.0.1  [scalding]
          exclude("com.twitter",                "chill_2.11")                   // 0.8.0   -> 0.8.4   [scalding]
          exclude("com.twitter",                "chill-java")                   // 0.8.0   -> 0.8.4   [scalding]
          ,
        "com.typesafe.play" %% "play-json" % "2.3.10"
          exclude("com.fasterxml.jackson.core", "jackson-annotations")          // 2.3.2   ->  2.6.5  [spark]
          exclude("com.fasterxml.jackson.core", "jackson-core")                 // 2.3.2   ->  2.6.5  [spark]
          exclude("com.fasterxml.jackson.core", "jackson-databind")             // 2.3.2   ->  2.6.5  [spark]
          ,
        "com.tdunning" %  "t-digest" % "3.2"
          ,
        "com.chuusai" %% "shapeless" % "2.3.2"
          ,
        "com.twitter" %% "scalding-core" % "0.17.2"
          exclude("org.slf4j",                  "slf4j-api")                    // 1.6.6   -> 1.7.16  [spark]
          ,
        "com.twitter" %% "scalding-parquet-scrooge" % "0.17.2"
          exclude("commons-lang",               "commons-lang")                 // 2.5     -> 2.6     [spark]
          exclude("commons-codec",              "commons-codec")                // 1.4     -> 1.5     [spark]
          exclude("org.codehaus.jackson",       "jackson-core-asl")             // 1.9.11  -> 1.9.13  [spark]
          exclude("org.codehaus.jackson",       "jackson-mapper-asl")           // 1.9.11  -> 1.9.13  [spark]
          exclude("org.slf4j",                  "slf4j-api")                    // 1.6.6   -> 1.7.16  [spark]
          exclude("org.xerial.snappy",          "snappy-java")                  // 1.1.1.6 -> 1.1.2.6 [spark]
          ,
        "com.esotericsoftware" % "kryo" % "3.0.3" % "test"                      // Needed by Spark unit tests
          ,
        "org.scalatest" %% "scalatest" % "3.0.1" % "test"
          exclude("org.scala-lang.modules",     "scala-xml_2.11")               // 1.0.5   -> 1.0.4   [scala] !
      )
    )
  ) ++
  standardSettings ++
  resolverSettings ++
  assemblySettings ++
  documentationSettings
)

lazy val examples = Project(
  id = "grimlock-examples",
  base = file("examples"),
  settings = standardSettings ++ assemblySettings
).dependsOn(core % "test->test;compile->compile")

lazy val assemblySettings = Seq(
  assemblyMergeStrategy in assembly := { // TODO: Clean up this implementation
    case "META-INF/LICENSE" => MergeStrategy.rename
    case "META-INF/license" => MergeStrategy.rename
    case "META-INF/NOTICE.txt" => MergeStrategy.rename
    case "META-INF/LICENSE.txt" => MergeStrategy.rename
    case "META-INF/MANIFEST.MF" => MergeStrategy.discard
    case "application.conf" => MergeStrategy.concat
    case "reference.conf"   => MergeStrategy.concat
    case PathList("META-INF", xs) if xs.toLowerCase.endsWith(".dsa") => MergeStrategy.discard
    case PathList("META-INF", xs) if xs.toLowerCase.endsWith(".rsa") => MergeStrategy.discard
    case PathList("META-INF", xs) if xs.toLowerCase.endsWith(".sf") => MergeStrategy.discard
    case _ => MergeStrategy.first
  },
  artifact in (Compile, assembly) ~= { _.copy(`classifier` = Some("assembly")) }
) ++
addArtifact(artifact in (Compile, assembly), assembly)

lazy val compilerSettings = Seq(
  scalaVersion := "2.11.8",
  scalacOptions ++= Seq("-Xfatal-warnings", "-deprecation", "-unchecked", "-Xlint", "-feature", "-language:_"),
  scalacOptions in (Compile, console) ~= (_.filterNot(Set("-Xfatal-warnings"))),
  scalacOptions in (Compile, doc) ~= (_.filterNot(Set("-Xfatal-warnings"))),
  scalacOptions in (Test, console) := (scalacOptions in (Compile, console)).value,
  javacOptions ++= Seq("-Xlint:unchecked", "-source", jvmVersion, "-target", jvmVersion)
)

lazy val dependencySettings = Seq(
  conflictManager := ConflictManager.strict,
  dependencyOverrides ++= Set(
    "com.thoughtworks.paranamer" %  "paranamer"     % "2.6",                    // 2.3     -> 2.6     [spark]
    "commons-codec"              %  "commons-codec" % "1.8",                    // 1.4     -> 1.8     [spark]
    "javax.activation"           %  "activation"    % "1.1.1",                  // 1.1     -> 1.1.1   [spark]
    "org.apache.httpcomponents"  %  "httpclient"    % "4.3.6",                  // 4.2.5   -> 4.3.6   [spark]
    "org.apache.httpcomponents"  %  "httpcore"      % "4.3.3"                   // 4.3.2   -> 4.3.3   [spark]
  )
)

lazy val documentationSettings = Seq(
  autoAPIMappings := true,
  apiURL := Some(url("https://github.com/CommBank/grimlock"))
)

lazy val resolverSettings = Seq(
  resolvers += "Concurrent Maven Repo" at "http://conjars.org/repo"             // cascading jars
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

def noDerby(deps: Seq[ModuleID]) = deps.map(_ exclude("org.apache.derby", "derby")) // Exclude for Spark REPL

