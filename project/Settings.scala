import de.heikoseeberger.sbtheader.HeaderPlugin.autoImport._
import sbt.Keys._
import sbt.TestFrameworks.ScalaTest
import sbt._
import scalafix.sbt.ScalafixPlugin.autoImport.scalafixConfigSettings
import scalafix.sbt.ScalafixPlugin.autoImport.scalafixSemanticdb
import scoverage._

import java.util.Calendar

object Settings extends Dependencies {
  // keep the SNAPSHOT version numerically higher than the latest release.
  val majorVersion        = "1.0"
  val nextSnapshotVersion = "1.1"

  val artifactVersion: String = {
    val maybeGithubRunId = sys.env.get("github_run_id")
    val maybeVersion     = sys.env.get("VERSION")
    val snapshotTag      = sys.env.get("SNAPSHOT_TAG")
    (maybeVersion, maybeGithubRunId) match {
      case (_, Some(patchVersion)) => majorVersion + "." + patchVersion
      case (Some(v), _)            => v
      case _                       => s"$nextSnapshotVersion-${snapshotTag.fold("SNAPSHOT")(t => s"$t-SNAPSHOT")}"
    }
  }

  val manifestSection: Package.JarManifest = {
    import java.util.jar.Attributes
    import java.util.jar.Manifest
    val manifest      = new Manifest
    val newAttributes = new Attributes()
    newAttributes.put(new Attributes.Name("version"), majorVersion)
    manifest.getEntries.put("celonis", newAttributes)
    Package.JarManifest(manifest)
  }

  val licenseHeader: String = {
    val currentYear = Calendar.getInstance().get(Calendar.YEAR)
    s"Copyright 2017-$currentYear Celonis Ltd"
  }

  object ScalacFlags {
    val availableProcessors: String = java.lang.Runtime.getRuntime.availableProcessors.toString

    val commonOptions = Seq(
      // standard settings
      "-target:jvm-1.8",
      "-encoding",
      "UTF-8",
      "-unchecked",
      "-deprecation",
      "-explaintypes",
      "-feature",
      // language features
      "-language:existentials",
      "-language:higherKinds",
      "-language:implicitConversions",
      "-language:postfixOps",
      // private options
      "-Ybackend-parallelism",
      availableProcessors,
      "-Yrangepos",                 // required by SemanticDB compiler plugin
      "-P:semanticdb:synthetics:on",// required by scala-collection-migrations
    )

    val lintings = List(
      "-Xlint:adapted-args", //TODO kept commented when streaming was merged in. Review.
      "-Xlint:constant",
      "-Xlint:delayedinit-select",
      "-Xlint:doc-detached",
      "-Xlint:inaccessible",
      "-Xlint:infer-any",
      "-Xlint:missing-interpolator",
      "-Xlint:nullary-unit",
      "-Xlint:option-implicit",
      //"-Xlint:package-object-classes", //TODO kept commented when streaming was merged in. Review.
      "-Xlint:poly-implicit-overload",
      "-Xlint:private-shadow",
      "-Xlint:stars-align",
      "-Xlint:type-parameter-shadow",
    )

    object Scala212 {
      val WarnUnusedImports = "-Ywarn-unused:imports"
      val FatalWarnings     = "-Xfatal-warnings"
      val ValueDiscard      = "-Ywarn-value-discard"

      val warnings = List(
        FatalWarnings,
        ValueDiscard,
        WarnUnusedImports,
        "-Ywarn-dead-code",
        "-Ywarn-extra-implicit",
        "-Ywarn-self-implicit",
        "-Ywarn-infer-any",
        "-Ywarn-macros:after",
        "-Ywarn-nullary-override",
        //"-Ywarn-nullary-unit",
        "-Ywarn-numeric-widen",
        "-Ywarn-unused:implicits",
        "-Ywarn-unused:locals",
        //    "-Ywarn-unused:params", //todo this is more pain than it's worth right now
        "-Ywarn-unused:patvars",
        "-Ywarn-unused:privates",
      )

      val options: Seq[String] = commonOptions ++ List(
        "-Ypartial-unification",
        // advanced options
        "-Xcheckinit",
        "-Yno-adapted-args",
        "-Xlint:by-name-right-associative",
        "-Xlint:unsound-match",
        "-Xlint:nullary-override",
      ) ++ warnings ++ lintings
    }

    object Scala213 {
      val WarnUnusedImports = "-Wunused:imports"
      val FatalWarnings     = "-Werror"
      val ValueDiscard      = "-Wvalue-discard"

      val warnings = List(
        FatalWarnings,
        ValueDiscard,
        WarnUnusedImports,
        "-Wdead-code",
        "-Wextra-implicit",
        "-Wmacros:after",
        "-Wnumeric-widen",
        "-Wunused:implicits",
        "-Wunused:locals",
        "-Wunused:patvars",
        "-Wunused:privates",
        //    "-Wself-implicit"
        //    "-Wunused:params", //todo this is more pain than it's worth right now
      )

      val options: Seq[String] = commonOptions ++ List(
        // advanced options
        "-Xcheckinit",
        // TODO Verify whether this is right...
        //"-Wconf:msg=import scala\\.collection\\.compat\\._:s"
      ) ++ warnings ++ lintings
    }
  }

  private val commonSettings: Seq[Setting[_]] = Seq(
    organization := "com.celonis.kafka.connect",
    version := artifactVersion,
    scalaOrganization := scalaOrganizationUsed,
    scalaVersion := scalaVersionUsed,
    headerLicense := Some(HeaderLicense.Custom(licenseHeader)),
    headerEmptyLine := false,
    isSnapshot := artifactVersion.contains("SNAPSHOT"),
    //publishTo := artifactoryRepo,
    kindProjectorPlugin,
    betterMonadicFor,
    semanticdbEnabled := true,
    semanticdbVersion := scalafixSemanticdb.revision,
    libraryDependencies ++= Seq(Dependencies.scalaCollectionCompat),
  )

  val rootSettings: Seq[Setting[_]] = commonSettings ++ Seq(
    crossScalaVersions := Nil,
    publish / skip := true,
    publishArtifact := false,
  )

  val modulesSettings: Seq[Setting[_]] = commonSettings ++ Seq(
    scalacOptions ++= {
      CrossVersion.partialVersion(scalaVersion.value) match {
        case Some((2, n)) if n <= 12 =>
          ScalacFlags.Scala212.options
        case _ =>
          ScalacFlags.Scala213.options
      }
    },
    Compile / console / scalacOptions := ScalacFlags.commonOptions,
    Global / cancelable := true,
    Compile / fork := true,
    Compile / trapExit := false,
    Compile / connectInput := true,
    Compile / outputStrategy := Some(StdoutOutput),
    resolvers ++= projectResolvers,
    //libraryDependencies ++= mainDeps,
    crossScalaVersions := supportedScalaVersionsUsed,
    /*Global / concurrentRestrictions := {
      val max = java.lang.Runtime.getRuntime.availableProcessors
      Seq(
        Tags.limit(Tags.Test, 4),
        Tags.limitAll(if (parallelExecution.value) math.max(max - 2, 1) else 1)
      )
    }*/
  )

  val ItTest:         Configuration = config("it").extend(Test).describedAs("Runs integration tests")
  val FunctionalTest: Configuration = config("fun") extend Test describedAs "Runs functional tests"
  val E2ETest:        Configuration = config("e2e").extend(Test).describedAs("Runs E2E tests")

  val testConfigurationsMap =
    Map(Test.name -> Test, ItTest.name -> ItTest, FunctionalTest.name -> FunctionalTest, E2ETest.name -> E2ETest)

  sealed abstract class TestConfigurator(
    project:         Project,
    config:          Configuration,
    defaultSettings: Seq[Def.Setting[_]] = Defaults.testSettings,
  ) {

    protected def configure(
      requiresFork:              Boolean,
      requiresParallelExecution: Boolean,
      frameworks:                Seq[TestFramework],
      dependencies:              Seq[ModuleID],
    ): Project =
      project
        .configs(config)
        .settings(
          libraryDependencies ++= dependencies.map(
            _ % config,
          ),
          inConfig(config)(
            defaultSettings ++ Seq(
              fork := requiresFork,
              parallelExecution := requiresParallelExecution,
              testFrameworks := frameworks,
              classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.Flat,
            ) ++ scalafixConfigSettings(config),
          ),
        )
        .enablePlugins(ScoverageSbtPlugin)

  }

  implicit final class UnitTestConfigurator(project: Project) extends TestConfigurator(project, Test) {

    def configureTests(
      requiresFork:              Boolean            = false,
      requiresParallelExecution: Boolean            = false,
      frameworks:                Seq[TestFramework] = Seq(ScalaTest),
    ): Project =
      configure(requiresFork, requiresParallelExecution, frameworks, scalaTestFunSuiteDeps)
  }

  implicit final class IntegrationTestConfigurator(project: Project) extends TestConfigurator(project, ItTest) {

    def configureIntegrationTests(
      requiresFork:              Boolean            = false,
      requiresParallelExecution: Boolean            = false,
      frameworks:                Seq[TestFramework] = Seq(ScalaTest),
    ): Project =
      configure(requiresFork, requiresParallelExecution, frameworks, scalaTestFunSuiteDeps)
  }

  implicit final class FunctionalTestConfigurator(project: Project) extends TestConfigurator(project, FunctionalTest) {

    def configureFunctionalTests(
      requiresFork:              Boolean            = false,
      requiresParallelExecution: Boolean            = true,
      frameworks:                Seq[TestFramework] = Seq(ScalaTest),
    ): Project =
      configure(requiresFork, requiresParallelExecution, frameworks, scalaTestFunSuiteDeps)
  }

  implicit final class E2ETestConfigurator(project: Project) extends TestConfigurator(project, E2ETest) {

    def configureE2ETests(
      requiresFork:              Boolean            = false,
      requiresParallelExecution: Boolean            = true,
      frameworks:                Seq[TestFramework] = Seq(ScalaTest),
    ): Project =
      configure(requiresFork, requiresParallelExecution, frameworks, scalaTestFunSuiteDeps)
  }
}
