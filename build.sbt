import Settings._
import sbt._

ThisBuild / scalafixDependencies ++= Dependencies.scalafixDeps

val generateManifest = taskKey[Seq[File]]("generateManifest")

Compile / generateManifest := {
  val content = IO.read((Compile / baseDirectory).value / "release/manifest.json")
  val out     = (Compile / baseDirectory).value / "connector/target/manifest.json"
  IO.write(out, content.replace("<project.version>", artifactVersion))
  Seq(out)
}

Compile / resourceGenerators += (Compile / generateManifest)

lazy val root = Project("kafka-ems-connector", file("."))
  .settings(rootSettings)
  .settings(
    name := "kafka-ems-connector",
  )
  .aggregate(
    connector,
    `test-common`,
  )
  .dependsOn(`test-common` % "e2e->compile")
  .dependsOn(connector)
  .configureE2ETests(requiresParallelExecution = false)
  .disablePlugins(sbtassembly.AssemblyPlugin)

lazy val `test-common` = project.in(file("test-common"))
  .disablePlugins(sbtassembly.AssemblyPlugin)
  .settings(
    modulesSettings ++
      Seq(
        name        := "test-common",
        description := "Provides common utilities for testing",
        libraryDependencies ++= testCommonDeps,
        excludeDependencies += ExclusionRule("javax.ws.rs", "javax.ws.rs-api"),
        libraryDependencies += "jakarta.ws.rs" % "jakarta.ws.rs-api" % "2.1.6",
        publish / skip                        := true,
      ),
  )

lazy val connector = project.in(file("connector"))
  .enablePlugins(BuildInfoPlugin)
  .settings(
    modulesSettings ++
      Seq(
        name        := "kafka-ems-sink",
        description := "Provides a Kafka Connect sink for Celonis EMS",
        libraryDependencies ++= emsSinkDeps,
        dependencyOverrides ++= emsSinkOverrides,
        excludeDependencies += ExclusionRule("javax.ws.rs", "javax.ws.rs-api"),
        libraryDependencies += "jakarta.ws.rs" % "jakarta.ws.rs-api" % "2.1.6",
        publish / skip                        := true,
        buildInfoKeys := Seq[BuildInfoKey](
          name,
          version,
          BuildInfoKey(git.gitHeadCommit),
        ),
        buildInfoPackage := "com.celonis.kafka.connect",
      ),
  )
  .dependsOn(`test-common` % "test->compile;it->compile")
  .configureTests()
  .configureIntegrationTests()
  .configureAssembly()

addCommandAlias(
  "validateAll",
  ";headerCheck;test:headerCheck;scalafmtCheckAll;scalafmtSbtCheck",
)

addCommandAlias(
  "formatAll",
  ";headerCreate;test:headerCreate;scalafmtAll;scalafmtSbt",
)

addCommandAlias("fullTest", ";test;it:test;fun:test;e2e:test")
addCommandAlias("fullCoverageTest", ";coverage;test;it:test;coverageReport;coverageAggregate")

dependencyCheckFormats                  := Seq("XML", "HTML")
dependencyCheckNodeAnalyzerEnabled      := Some(false)
dependencyCheckNodeAuditAnalyzerEnabled := Some(false)
dependencyCheckNPMCPEAnalyzerEnabled    := Some(false)
dependencyCheckRetireJSAnalyzerEnabled  := Some(false)
