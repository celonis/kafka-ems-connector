import Dependencies.commonResolvers
import Dependencies.testConfigurationsMap
import sbt._
import Dependencies._

import scala.collection.immutable

object Dependencies {

  // scala versions
  val scalaOrganization      = "org.scala-lang"
  val scala212Version        = "2.12.13"
  val scala213Version        = "2.13.5"
  val supportedScalaVersions = List(scala212Version, scala213Version)


  val FunctionalTest: Configuration = config("fun") extend Test describedAs "Runs functional tests"
  val ItTest:         Configuration = config("it").extend(Test).describedAs("Runs integration tests")
  val E2ETest:        Configuration = config("e2e").extend(Test).describedAs("Runs E2E tests")

  val testConfigurationsMap =
    Map(Test.name -> Test, FunctionalTest.name -> FunctionalTest, ItTest.name -> ItTest, E2ETest.name -> E2ETest)


  val commonResolvers = Seq(
    Resolver sonatypeRepo "public",
    Resolver typesafeRepo "releases",
    Resolver.mavenLocal,
    "confluent" at "https://packages.confluent.io/maven/",
   "typesafe" at "https://repo.typesafe.com/typesafe/releases/",
     "cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/" ,
      "conjars" at "https://conjars.org/repo" ,
      "jitpack" at "https://jitpack.io"
  )

  object Versions {
    // libraries versions
    val scalatestVersion               = "3.1.0"
    val scalaCheckPlusVersion = "3.1.0.0"
    val scalatestPlusScalaCheckVersion = "3.1.0.0-RC2"
    val scalaCheckVersion              = "1.14.3"
    val randomDataGeneratorVersion     = "2.8"

    val enumeratumVersion         = "1.7.0"

    val kafkaVersion                   = "2.8.0"

    val confluentVersion = "6.2.0"

    val avroVersion                    = "1.9.2"
     val avro4sVersion = "3.1.1"

    val catsVersion           = "2.6.1"
    val catsEffectVersion     = "2.5.1"
    val `cats-effect-testing` = "0.5.4"
    val jacksonVersion        = "2.10.5"

    val circeVersion              = "0.14.1"
    val circeGenericExtrasVersion = "0.14.1"
    val circeJsonSchemaVersion    = "0.2.0"

    // build plugins versions
    val silencerVersion         = "1.7.1"
    val kindProjectorVersion    = "0.10.3"
    val betterMonadicForVersion = "0.3.1"

    val slf4jVersion          = "1.7.25"

    val logbackVersion = "1.2.3"

    val classGraphVersions    = "4.4.12"
    val scalaCollectionCompat = "2.4.2"
    val quicklens             = "1.6.1"

    val parquetVersion = "1.11.0"
    val hadoopVersion = "2.10.0"
  }

  val scalaCollectionCompat = "org.scala-lang.modules" %% "scala-collection-compat" % Versions.scalaCollectionCompat
  val scalaCollectionMigrations =
    "org.scala-lang.modules" %% "scala-collection-migrations" % Versions.scalaCollectionCompat

  val scalafixDeps = Seq(
    scalaCollectionMigrations
  )

  // functional libraries
  val cats           = "org.typelevel" %% "cats-core" % Versions.catsVersion
  val catsLaws       = "org.typelevel" %% "cats-laws" % Versions.catsVersion
  val catsEffect     = "org.typelevel" %% "cats-effect" % Versions.catsEffectVersion
  val catsEffectLaws = "org.typelevel" %% "cats-effect-laws" % Versions.catsEffectVersion
  lazy val catsFree  = "org.typelevel" %% "cats-free" % Versions.catsVersion

  val circeGeneric      = "io.circe" %% "circe-generic" % Versions.circeVersion
  val circeParser       = "io.circe" %% "circe-parser" % Versions.circeVersion
  val circeRefined      = "io.circe" %% "circe-refined" % Versions.circeVersion
  val circe             = Seq(circeGeneric, circeParser,  circeRefined)

  // logging
  val logback          = "ch.qos.logback" % "logback-classic" % Versions.logbackVersion
  lazy val logbackCore = "ch.qos.logback" % "logback-core" % Versions.logbackVersion

  // testing
  val scalatest = "org.scalatest" %% "scalatest" % Versions.scalatestVersion
  val scalatestPlusScalaCheck =
    "org.scalatestplus" %% "scalatestplus-scalacheck" % Versions.scalatestPlusScalaCheckVersion
  val scalaCheck = "org.scalacheck" %% "scalacheck" % Versions.scalaCheckVersion
  val `mockito-scala` = "org.scalatestplus" %% "mockito-3-2" % Versions.scalaCheckPlusVersion

  val randomDataGeneratorMagnolia =
    "com.danielasfregola" %% "random-data-generator-magnolia" % Versions.randomDataGeneratorVersion
  lazy val pegDown               = "org.pegdown" % "pegdown" % "1.6.0"

  val catsEffectScalatest        = "com.codecommit" %% "cats-effect-testing-scalatest" % Versions.`cats-effect-testing`

  val enumeratumCore  = "com.beachape" %% "enumeratum" % Versions.enumeratumVersion
  val enumeratumCirce = "com.beachape" %% "enumeratum-circe" % Versions.enumeratumVersion
  val enumeratum      = Seq(enumeratumCore, enumeratumCirce)

  val classGraph = "io.github.classgraph" % "classgraph" % Versions.classGraphVersions

  lazy val slf4j = "org.slf4j" % "slf4j-api" % Versions.slf4jVersion

  lazy val kafkaConnectJson        = "org.apache.kafka" %  "connect-json" % Versions.kafkaVersion
  lazy val confluentAvroConverter = ("io.confluent" % "kafka-connect-avro-converter" % Versions.confluentVersion)
    .exclude("org.slf4j", "slf4j-log4j12")
    .exclude("org.apache.kafka", "kafka-clients")
    .exclude("javax.ws.rs", "javax.ws.rs-api")
    .excludeAll(ExclusionRule(organization = "io.swagger"))
    .excludeAll(ExclusionRule(organization = "com.fasterxml.jackson.core"))

  lazy val confluentSchemaRegistry = ("io.confluent" % "kafka-schema-registry" % Versions.confluentVersion)
    .exclude("org.slf4j", "slf4j-log4j12")
    .exclude("com.google.guava", "guava")
    .exclude("javax.ws.rs", "javax.ws.rs-api")
    .excludeAll(ExclusionRule(organization = "io.swagger"))
    .excludeAll(ExclusionRule(organization = "com.fasterxml.jackson.core"))


  val jacksonCore          = "com.fasterxml.jackson.core" % "jackson-core" % Versions.jacksonVersion
  val jacksonAnnotations   = "com.fasterxml.jackson.core" % "jackson-annotations" % Versions.jacksonVersion
  val jacksonDatabind      = "com.fasterxml.jackson.core" % "jackson-databind" % Versions.jacksonVersion
  val jacksonModuleScala   = "com.fasterxml.jackson.module" %% "jackson-module-scala" % Versions.jacksonVersion
  val jacksonDataformatXml = "com.fasterxml.jackson.dataformat" % "jackson-dataformat-xml" % Versions.jacksonVersion
  val jackson              = Seq(jacksonCore, jacksonAnnotations, jacksonDatabind, jacksonModuleScala, jacksonDataformatXml)

  lazy val avro   = "org.apache.avro" % "avro" % Versions.avroVersion
  lazy val avro4s = "com.sksamuel.avro4s" %% "avro4s-core" % Versions.avro4sVersion

  val quicklens = "com.softwaremill.quicklens" %% "quicklens" % Versions.quicklens

  lazy val parquetAvro = "org.apache.parquet" % "parquet-avro" % Versions.parquetVersion
  lazy val parquetHadoop = "org.apache.parquet" % "parquet-hadoop" % Versions.parquetVersion
  lazy val hadoopCommon =  "org.apache.hadoop"% "hadoop-common"  % Versions.hadoopVersion
  //compile("org.apache.hadoop:hadoop-mapreduce-client-core:$hadoopVersion")
}

trait Dependencies {

  val scalaOrganizationUsed: String = scalaOrganization
  val scalaVersionUsed: String = scala213Version
  val supportedScalaVersionsUsed: immutable.Seq[String] = supportedScalaVersions

  // resolvers
  val projectResolvers: Seq[MavenRepository] = commonResolvers

  val mainDeps =
    Seq(cats, catsEffect, logback, logbackCore)

  val baseTestDeps: Seq[ModuleID] = Seq(
    catsLaws,
    catsEffectLaws,
    scalatest,
    catsEffectScalatest,
    scalatestPlusScalaCheck,
    scalaCheck,
    `mockito-scala`,
    randomDataGeneratorMagnolia,
  ).map(_ exclude ("org.slf4j", "slf4j-log4j12")).map(_ % testConfigurationsMap.keys.mkString(","))

  //Specific modules dependencies
  val emsSinkDeps: Seq[ModuleID] = (Seq(
    kafkaConnectJson,
    confluentAvroConverter,
    confluentSchemaRegistry,
    avro4s,
    catsFree,
    parquetAvro,
    parquetHadoop,
    hadoopCommon
  ) ++ enumeratum ++ circe).map(_.exclude("org.slf4j", "slf4j-log4j12"))
    .map(_.exclude("org.apache.logging.log4j", "log4j-slf4j-impl"))
    .map(_.exclude("com.sun.jersey", "*"))

  // build plugins
  val kindProjectorPlugin = addCompilerPlugin("org.typelevel" %% "kind-projector" % Versions.kindProjectorVersion)
  val betterMonadicFor    = addCompilerPlugin("com.olegpy" %% "better-monadic-for" % Versions.betterMonadicForVersion)

  implicit final class ProjectRoot(project: Project) {

    def root: Project = project in file(".")
  }

  implicit final class ProjectFrom(project: Project) {

    private val commonDir = "modules"

    def from(dir: String): Project = project in file(s"$commonDir/$dir")
  }

  implicit final class DependsOnProject(project: Project) {
    private def findCompileAndTestConfigs(p: Project) =
      findTestConfigs(p) + "compile"

    private def findTestConfigs(p: Project) =
      p.configurations.map(_.name).toSet.intersect(testConfigurationsMap.keys.toSet)

    private val thisProjectsConfigs = findCompileAndTestConfigs(project)
    private def generateDepsForProject(p: Project, withCompile: Boolean) =
      p % thisProjectsConfigs
          .intersect(if (withCompile) findCompileAndTestConfigs(p) else findTestConfigs(p))
          .map(c => s"$c->$c")
          .mkString(            ";")

    def compileAndTestDependsOn(projects: Project*): Project =
      project.dependsOn(projects.map(generateDepsForProject(_, true)): _*)

    def testsDependOn(projects: Project*): Project =
      project.dependsOn(projects.map(generateDepsForProject(_, false)): _*)
  }
}
