lazy val Scala3   = "3.2.0"
lazy val Scala213 = "2.13.10"
lazy val Scala212 = "2.12.17"

lazy val ParadiseVer     = "2.1.1"
lazy val LogbackVer      = "1.4.4"
lazy val ScalaLoggingVer = "3.9.5"
lazy val ScalaTestVer    = "3.2.14"
lazy val AkkaVer         = "2.6.20"
lazy val CatsVer         = "2.8.0"
lazy val ZIOVer          = "2.0.2"
lazy val ZIOLoggingVer   = "2.1.2"
lazy val ZIOJsonVer      = "0.3.0"
lazy val ZIOHttpVer      = "2.0.0-RC10"
lazy val ZIOK8sVer       = "2.0.1"
lazy val SttpVer         = "3.8.3"
lazy val UpickleVer      = "2.0.0"
lazy val PPrintVer       = "0.8.0"
lazy val QuicklensVer    = "1.9.0"
lazy val HoconVer        = "1.4.2"
lazy val MinioVer        = "8.4.5"

lazy val FlinkDefaultVer = Flink115Ver
lazy val Flink116Ver     = "1.16.0"
lazy val Flink115Ver     = "1.15.2"
lazy val Flink114Ver     = "1.14.6"
lazy val Flink113Ver     = "1.13.6"

lazy val commonSettings = Seq(
  ThisBuild / organization := "com.github.potamois",
  ThisBuild / version      := "0.1.0-SNAPSHOT",
  ThisBuild / developers := List(
    Developer(
      id = "Al-assad",
      name = "Linying Assad",
      email = "assad.dev@outlook.com",
      url = new URL("https://github.com/Al-assad")
    )
  ),
  Compile / javacOptions ++= Seq("-source", "17", "-target", "17"),
  Compile / scalacOptions ++= (if (scalaBinaryVersion.value == "2.13") Seq("-Ymacro-annotations") else Nil),
  libraryDependencies ++= (
    if (scalaBinaryVersion.value == "2.12") Seq(compilerPlugin("org.scalamacros" % "paradise" % ParadiseVer cross CrossVersion.patch))
    else Nil
  )
)

lazy val root = (project in file("."))
  .settings(name := "potamoi")
  .aggregate(
    Seq(
      potaCommon,
      potaFs,
      potaK8s,
      potaServer,
      potaFlinkOperator,
      potaFlinkSqlQueryBase,
      potaFlinkSqlQuery116,
      potaFlinkSqlQuery115,
      potaFlinkSqlQuery114,
      potaFlinkSqlQuery113).flatMap(_.projectRefs): _*)

lazy val commonDeps = Seq(
  "ch.qos.logback"              % "logback-classic"          % LogbackVer,
  "com.typesafe.scala-logging" %% "scala-logging"            % ScalaLoggingVer,
  "org.scalatest"              %% "scalatest"                % ScalaTestVer % Test,
  "com.typesafe.akka"          %% "akka-actor-testkit-typed" % AkkaVer      % Test
)

/**
 * Common module.
 */
lazy val potaCommon = (projectMatrix in file("pota-common"))
  .settings(commonSettings)
  .settings(
    name := "potamoi-common",
    libraryDependencies ++= commonDeps ++ Seq(
      "com.typesafe"                   % "config"                     % HoconVer,
      "com.typesafe.akka"             %% "akka-actor-typed"           % AkkaVer,
      "com.typesafe.akka"             %% "akka-cluster-typed"         % AkkaVer,
      "com.typesafe.akka"             %% "akka-serialization-jackson" % AkkaVer,
      "dev.zio"                       %% "zio"                        % ZIOVer,
      "dev.zio"                       %% "zio-concurrent"             % ZIOVer,
      "dev.zio"                       %% "zio-macros"                 % ZIOVer,
      "dev.zio"                       %% "zio-logging-slf4j"          % ZIOLoggingVer,
      "dev.zio"                       %% "zio-json"                   % ZIOJsonVer,
      "org.typelevel"                 %% "cats-core"                  % CatsVer,
      "com.softwaremill.quicklens"    %% "quicklens"                  % QuicklensVer,
      "com.lihaoyi"                   %% "upickle"                    % UpickleVer,
      "com.lihaoyi"                   %% "pprint"                     % PPrintVer,
      "com.softwaremill.sttp.client3" %% "core"                       % SttpVer,
      "com.softwaremill.sttp.client3" %% "zio"                        % SttpVer,
      "com.softwaremill.sttp.client3" %% "zio-json"                   % SttpVer,
      "com.softwaremill.sttp.client3" %% "slf4j-backend"              % SttpVer
    )
  )
  .jvmPlatform(scalaVersions = Seq(Scala213, Scala212))

/**
 * File system handling module, such as s3 storage, local fs.
 */
lazy val potaFs = (projectMatrix in file("pota-fs"))
  .settings(commonSettings)
  .settings(
    name := "potamoi-fs",
    libraryDependencies ++= commonDeps ++ Seq(
      "io.minio" % "minio" % MinioVer excludeAll (ExclusionRule(organization = "com.fasterxml.jackson.core"))
    )
  )
  .jvmPlatform(scalaVersions = Seq(Scala213, Scala212))
  .dependsOn(potaCommon)

/**
 * kubernetes system interaction module.
 */
lazy val potaK8s = (projectMatrix in file("pota-k8s"))
  .settings(commonSettings)
  .settings(
    name := "potamoi-k8s",
    libraryDependencies ++= commonDeps ++ Seq(
      "com.coralogix" %% "zio-k8s-client" % ZIOK8sVer
    )
  )
  .jvmPlatform(scalaVersions = Seq(Scala213, Scala212))
  .dependsOn(potaCommon)

/**
 * Potamoi server module as an entry point for external interactions.
 */
lazy val potaServer = (projectMatrix in file("pota-server"))
  .settings(commonSettings)
  .settings(
    name := "potamoi-server",
    libraryDependencies ++= commonDeps ++ Seq(
      "io.d11" %% "zhttp" % ZIOHttpVer
    )
  )
  .jvmPlatform(scalaVersions = Seq(Scala213))
  .dependsOn(potaCommon, potaFs, potaK8s)

/**
 * Flink operator module.
 */
lazy val potaFlinkOperator = (projectMatrix in file("pota-flink-operator"))
  .settings(commonSettings)
  .settings(
    name := "potamoi-flink-operator",
    libraryDependencies ++= commonDeps ++ Seq(
      "org.apache.flink" % "flink-clients"    % FlinkDefaultVer,
      "org.apache.flink" % "flink-kubernetes" % FlinkDefaultVer
    )
  )
  .jvmPlatform(scalaVersions = Seq(Scala213))
  .dependsOn(potaCommon, potaFs, potaK8s)

/**
 * Basic implementation of the standalone flink sql interactive query service module.
 */
lazy val potaFlinkSqlQueryBase = (projectMatrix in file("pota-flink-sql-query/flink-base"))
  .settings(commonSettings)
  .settings(
    name := "potamoi-flink-sql-query-base",
    libraryDependencies ++= commonDeps ++ Seq(
      "org.apache.flink"  % "flink-clients"       % FlinkDefaultVer % Provided,
      "org.apache.flink" %% "flink-table-planner" % FlinkDefaultVer % Provided
    )
  )
  .jvmPlatform(scalaVersions = Seq(Scala212))
  .dependsOn(potaCommon)

def flinkSqlQueryDeps(flinkVer: String) = {
  val shortVer = flinkVer.split('.').slice(0, 2).mkString("").toInt
  if (shortVer >= 115) {
    Seq(
      "org.apache.flink"  % "flink-clients"       % flinkVer,
      "org.apache.flink" %% "flink-table-planner" % flinkVer
    )
  } else if (shortVer >= 114) {
    Seq(
      "org.apache.flink" %% "flink-clients"       % flinkVer,
      "org.apache.flink" %% "flink-table-planner" % flinkVer
    )
  } else {
    Seq(
      "org.apache.flink" %% "flink-clients"             % flinkVer,
      "org.apache.flink" %% "flink-table-planner-blink" % flinkVer
    )
  }
}

/**
 * Flink-1.16 implementation of flink-sql-query
 */
lazy val potaFlinkSqlQuery116 = (projectMatrix in file(s"pota-flink-sql-query/flink-116"))
  .settings(commonSettings)
  .settings(
    name := s"potamoi-flink-sql-query-116",
    libraryDependencies ++= commonDeps ++ flinkSqlQueryDeps(Flink116Ver)
  )
  .jvmPlatform(scalaVersions = Seq(Scala212))
  .dependsOn(potaFlinkSqlQueryBase)

/**
 * Flink-1.15 implementation of flink-sql-query
 */
lazy val potaFlinkSqlQuery115 = (projectMatrix in file(s"pota-flink-sql-query/flink-115"))
  .settings(commonSettings)
  .settings(
    name := s"potamoi-flink-sql-query-115",
    libraryDependencies ++= commonDeps ++ flinkSqlQueryDeps(Flink115Ver)
  )
  .jvmPlatform(scalaVersions = Seq(Scala212))
  .dependsOn(potaFlinkSqlQueryBase)

/**
 * Flink-1.14 implementation of flink-sql-query
 */
lazy val potaFlinkSqlQuery114 = (projectMatrix in file(s"pota-flink-sql-query/flink-114"))
  .settings(commonSettings)
  .settings(
    name := s"potamoi-flink-sql-query-114",
    libraryDependencies ++= commonDeps ++ flinkSqlQueryDeps(Flink114Ver)
  )
  .jvmPlatform(scalaVersions = Seq(Scala212))
  .dependsOn(potaFlinkSqlQueryBase)

/**
 * Flink-1.13 implementation of flink-sql-query
 */
lazy val potaFlinkSqlQuery113 = (projectMatrix in file(s"pota-flink-sql-query/flink-113"))
  .settings(commonSettings)
  .settings(
    name := s"potamoi-flink-sql-query-113",
    libraryDependencies ++= commonDeps ++ flinkSqlQueryDeps(Flink113Ver)
  )
  .jvmPlatform(scalaVersions = Seq(Scala212))
  .dependsOn(potaFlinkSqlQueryBase)
