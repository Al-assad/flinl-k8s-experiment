lazy val Scala3      = "3.2.0"
lazy val Scala213    = "2.13.10"
lazy val Scala212    = "2.12.17"
lazy val ParadiseVer = "2.1.1"

lazy val LogbackVer      = "1.4.3"
lazy val ScalaLoggingVer = "3.9.5"
lazy val ScalaTestVer    = "3.2.14"

lazy val AkkaVer      = "2.6.20"
lazy val CatsVer      = "2.8.0"
lazy val ZIOVer       = "2.0.2"
lazy val ZIOJsonVer   = "0.3.0"
lazy val ZIOHttpVer   = "2.0.0-RC10"
lazy val ZIOK8sVer    = "2.0.1"
lazy val SttpVer      = "3.8.3"
lazy val UpickleVer   = "2.0.0"
lazy val QuicklensVer = "1.9.0"
lazy val HoconVer     = "1.4.2"
lazy val MinioVer     = "8.4.5"

lazy val FlinkDefaultVer = Flink15Ver
lazy val Flink15Ver      = "1.15.2"
lazy val Flink14Ver      = "1.14.6"
lazy val Flink13Ver      = "1.13.6"

lazy val commonSettings = List(
  Compile / javacOptions ++= List("-source", "11", "-target", "11"),
  Compile / scalaSource       := baseDirectory.value / "src",
  Compile / javaSource        := baseDirectory.value / "src",
  Compile / resourceDirectory := baseDirectory.value / "resources",
  Test / scalaSource          := baseDirectory.value / "test" / "src",
  Test / javaSource           := baseDirectory.value / "test" / "src",
  Test / resourceDirectory    := baseDirectory.value / "test" / "resources"
) ++ macroSettings

lazy val macroSettings = List(
  Compile / scalacOptions ++= (
    if (scalaBinaryVersion.value == "2.13") List("-Ymacro-annotations")
    else Nil
  ),
  libraryDependencies ++= (
    if (scalaBinaryVersion.value == "2.12")
      List(compilerPlugin("org.scalamacros" % "paradise" % ParadiseVer cross CrossVersion.patch))
    else Nil
  )
)

lazy val root = (project in file("."))
  .settings(commonSettings)
  .settings(name := "k8s-calc-experiment")
  .aggregate(
    kceCommon_212,
    kceCommon_213,
    kceServer,
    flinkOperatorBase,
    flinkOperator115,
    flinkOperator114,
    flinkOperator113
  )

lazy val commonDeps = Seq(
  "ch.qos.logback"              % "logback-classic"          % LogbackVer,
  "com.typesafe.scala-logging" %% "scala-logging"            % ScalaLoggingVer,
  "org.scalatest"              %% "scalatest"                % ScalaTestVer % Test,
  "com.typesafe.akka"          %% "akka-actor-testkit-typed" % AkkaVer      % Test
)

lazy val kceCommon = (project in file("kce-common"))
  .settings(commonSettings)
  .settings(
    name         := "kce-common",
    scalaVersion := Scala213,
    libraryDependencies ++= commonDeps ++ List(
      "com.typesafe"                   % "config"                     % HoconVer,
      "com.typesafe.akka"             %% "akka-actor-typed"           % AkkaVer,
      "com.typesafe.akka"             %% "akka-cluster-typed"         % AkkaVer,
      "com.typesafe.akka"             %% "akka-serialization-jackson" % AkkaVer,
      "dev.zio"                       %% "zio"                        % ZIOVer,
      "dev.zio"                       %% "zio-macros"                 % ZIOVer,
      "dev.zio"                       %% "zio-json"                   % ZIOJsonVer,
      "org.typelevel"                 %% "cats-core"                  % CatsVer,
      "com.softwaremill.quicklens"    %% "quicklens"                  % QuicklensVer,
      "com.lihaoyi"                   %% "upickle"                    % UpickleVer,
      "com.softwaremill.sttp.client3" %% "core"                       % SttpVer,
      "com.softwaremill.sttp.client3" %% "zio"                        % SttpVer,
      "com.softwaremill.sttp.client3" %% "zio-json"                   % SttpVer,
      "com.softwaremill.sttp.client3" %% "slf4j-backend"              % SttpVer,
      "com.coralogix"                 %% "zio-k8s-client"             % ZIOK8sVer,
      "io.minio"                       % "minio"                      % MinioVer
    )
  )
  .cross

lazy val kceCommon_213 = kceCommon(Scala213)
lazy val kceCommon_212 = kceCommon(Scala212)

lazy val kceServer = (project in file("kce-server"))
  .settings(commonSettings)
  .settings(
    name         := "kce-server",
    scalaVersion := Scala213,
    libraryDependencies ++= commonDeps
  )
  .dependsOn(kceCommon_213)

lazy val flinkOperatorBase = (project in file("flink-operator-base"))
  .settings(commonSettings)
  .settings(
    name         := "flink-operator-base",
    scalaVersion := Scala212,
    libraryDependencies ++= commonDeps ++ List(
      "org.apache.flink"  % "flink-clients"       % FlinkDefaultVer % Provided,
      "org.apache.flink"  % "flink-kubernetes"    % FlinkDefaultVer % Provided,
      "org.apache.flink" %% "flink-table-planner" % FlinkDefaultVer % Provided
    )
  )
  .dependsOn(kceCommon_212)

lazy val flinkOperator115 = (project in file("flink-operator-115"))
  .settings(commonSettings)
  .settings(
    name         := "flink-operator-115",
    scalaVersion := Scala212,
    libraryDependencies ++= commonDeps ++ List(
      "org.apache.flink"  % "flink-clients"       % Flink15Ver,
      "org.apache.flink"  % "flink-kubernetes"    % Flink15Ver,
      "org.apache.flink" %% "flink-table-planner" % Flink15Ver
    )
  )
  .dependsOn(flinkOperatorBase)

lazy val flinkOperator114 = (project in file("flink-operator-114"))
  .settings(commonSettings)
  .settings(
    name         := "flink-operator-114",
    scalaVersion := Scala212,
    libraryDependencies ++= commonDeps ++ List(
      "org.apache.flink" %% "flink-clients"       % Flink14Ver,
      "org.apache.flink" %% "flink-kubernetes"    % Flink14Ver,
      "org.apache.flink" %% "flink-table-planner" % Flink14Ver
    ))
  .dependsOn(flinkOperatorBase)

lazy val flinkOperator113 = (project in file("flink-operator-113"))
  .settings(commonSettings)
  .settings(
    name         := "flink-operator-113",
    scalaVersion := Scala212,
    libraryDependencies ++= commonDeps ++ List(
      "org.apache.flink" %% "flink-clients"             % Flink13Ver,
      "org.apache.flink" %% "flink-kubernetes"          % Flink13Ver,
      "org.apache.flink" %% "flink-table-planner-blink" % Flink13Ver
    )
  )
  .dependsOn(flinkOperatorBase)
