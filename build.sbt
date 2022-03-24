import Settings._
import sbt._

ThisBuild / scalafixDependencies ++= Dependencies.scalafixDeps
// This line ensures that sources are downloaded for dependencies, when using Bloop
bloopExportJarClassifiers in Global := Some(Set("sources"))

lazy val generateManifest = Def.task {
  val content = IO.read((Compile / baseDirectory).value / "release/manifest.json")
  val out = (Compile / baseDirectory ).value / "connector/target/manifest.json"
  IO.write(out, content.replace("<project.version>", artifactVersion))
  Seq(out)
}

Compile / resourceGenerators += generateManifest.taskValue

lazy val root = Project("kafka-ems-connector", file("."))
  .settings(rootSettings)
  .settings(
    name := "kafka-ems-connector",
  )
  .aggregate(
    connector,
    testcontainers,
  )
  .dependsOn(testcontainers)
  .dependsOn(connector)
  .configureE2ETests()
  .disablePlugins(sbtassembly.AssemblyPlugin)

lazy val testcontainers = project.in(file("testcontainers"))
  .disablePlugins(sbtassembly.AssemblyPlugin)
  .settings(
    modulesSettings ++
      Seq(
        name := "kafka-ems-testcontainers",
        description := "Provides a testing environment for EMS connector",
        libraryDependencies ++= testcontainersDeps,
        publish / skip := true,
      ),
  )

lazy val connector = project.in(file("connector"))
  .settings(
    modulesSettings ++
      Seq(
        name := "kafka-ems-sink",
        description := "Provides a Kafka Connect sink for Celonis EMS",
        libraryDependencies ++= emsSinkDeps,
        publish / skip := true,
      )
  )
  .configureTests()
  .configureAssembly()

addCommandAlias(
  "validateAll",
  ";headerCheck;test:headerCheck;scalafmtCheckAll;scalafmtSbtCheck",
)

addCommandAlias(
  "formatAll",
  ";headerCreate;test:headerCreate;scalafmtAll;scalafmtSbt",
)

addCommandAlias("fullTest", ";test;fun:test;it:test;e2e:test")
addCommandAlias("fullCoverageTest", ";coverage;test;it:test;coverageReport;coverageAggregate")

dependencyCheckFormats := Seq("XML", "HTML")
dependencyCheckNodeAnalyzerEnabled := Some(false)
dependencyCheckNodeAuditAnalyzerEnabled := Some(false)
dependencyCheckNPMCPEAnalyzerEnabled := Some(false)
dependencyCheckRetireJSAnalyzerEnabled := Some(false)
