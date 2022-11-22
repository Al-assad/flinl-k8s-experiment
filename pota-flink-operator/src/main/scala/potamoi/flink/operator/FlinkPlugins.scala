package potamoi.flink.operator

import potamoi.flink.share.model.FlinkVer

/**
 * Flink build in plugins resolver.
 */
object FlinkPlugins {

  val s3Hadoop      = Desc("flink-s3-fs-hadoop", scalaFree)
  val s3HadoopGS    = Desc("flink-gs-fs-hadoop", scalaFree)
  val s3HadoopOSS   = Desc("flink-oss-fs-hadoop", scalaFree)
  val s3HadoopAzure = Desc("flink-azure-fs-hadoop", scalaFree)
  val s3Presto      = Desc("flink-s3-fs-presto", scalaFree)
  val cep           = Desc("flink-cep", scalaBind)
  val gelly         = Desc("flink-gelly", scalaBind)
  val pyFlink       = Desc("flink-python", scalaBind)

  /**
   * All built-in plugins.
   */
  val plugins = Set(s3Hadoop, s3HadoopGS, s3HadoopOSS, s3HadoopAzure, s3Presto, cep, gelly, pyFlink)

  /**
   * All s3 built-in plugins.
   */
  lazy val s3Plugins  = s3aPlugins ++ Set(s3Presto)
  lazy val s3aPlugins = Set(s3Hadoop, s3HadoopGS, s3HadoopOSS, s3HadoopAzure)

  /**
   * All HDFS built-in plugins
   */
  lazy val hadoopPlugins = Set(s3Hadoop, s3HadoopGS, s3HadoopOSS, s3HadoopAzure)

  /**
   * Default Hadoop plugin.
   */
  lazy val defaultHadoopPlugin = s3Hadoop

  /**
   * Flink plugin name descriptor.
   *
   * @param name    plugin identify name.
   * @param jarName function to generate jar name according to Flink version.
   */
  case class Desc(name: String, private val jarNameFunc: (String, FlinkVer) => String) {
    val jarName: FlinkVer => String = jarNameFunc(name, _)
  }

  private lazy val scalaFree = (name: String, v: FlinkVer) => s"${name}-${v.ver}.jar"
  private lazy val scalaBind = (name: String, v: FlinkVer) => s"${name}_${v.scalaVer}-${v.ver}.jar"

}
