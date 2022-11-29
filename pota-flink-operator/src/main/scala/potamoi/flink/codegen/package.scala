package potamoi.flink

import zio.IO

package object codegen {

  /**
   * Generate flink metrics case class base on rest api mapping.
   */
  def genMetricCode(caseClzName: String, listKeys: IO[_, Set[String]], getMetrics: Set[String] => IO[_, Map[String, String]]): IO[Any, String] = {
    for {
      keys    <- listKeys
      metrics <- getMetrics(keys)
      // _       <- printLine("key list: " + keys.toList.sorted.toPrettyStr)
      // _       <- printLine(metrics.toPrettyStr)

      token = metrics
        .map { case (key, value) =>
          var fName = key.split('.').mkString("")
          if (fName.startsWith("Status")) fName = fName.drop(6)
          fName = fName.replace("JVM", "jvm").replace("CPU", "Cpu")
          val fType = if (value.contains('.')) "Double" else "Long"
          (key, fName, fType)
        }
        .toVector
        .sortBy(e => e._2)

      // case class
      fieldToken = token.map { case (_, fName, fType) => s"$fName: Option[$fType] = None" }
      caseClassCode = s"""case class $caseClzName (
                         |${fieldToken.map(e => "\t" + e).mkString(",\n")},
                         |  ts: Long = curTs
                         |)
                         |""".stripMargin

      // companion object
      mappingToken = token.map { case (key, fName, fType) => s"""$fName = raw.get("${key}").map(_.to${fType})""" }
      companionCode = s"""object $caseClzName {
                         |  implicit val codec: JsonCodec[$caseClzName] = DeriveJsonCodec.gen[$caseClzName]
                         |
                         |  val metricsRawKeys: Set[String] = Set(
                         |${keys.toList.sorted.map(e => s"""\t\t"$e"""").mkString(", \n")}
                         |  )
                         |
                         |  def fromRaw(raw: Map[String, String]): $caseClzName = $caseClzName(
                         |${mappingToken.map(e => "\t\t" + e).mkString(",\n")}
                         |  )
                         |}
                         |""".stripMargin
    } yield caseClassCode + "\n" + companionCode
  }
}
