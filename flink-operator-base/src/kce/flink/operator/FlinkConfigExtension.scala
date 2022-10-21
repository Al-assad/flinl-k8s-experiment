package kce.flink.operator

import kce.flink.operator.entity.FlinkRawConf
import org.apache.flink.configuration.Configuration

import scala.collection.immutable.Iterable
import scala.language.implicitConversions

/**
 * Extension for Flink [[Configuration]]
 */
object FlinkConfigExtension {

  /**
   * Create empty [[Configuration]] instance.
   */
  def EmptyConfiguration(): Configuration = new Configuration()

  implicit def configurationToPF(conf: Configuration): ConfigurationPF = new ConfigurationPF(conf)
  implicit def pfToConfiguration(pf: ConfigurationPF): Configuration   = pf.value

  /**
   * Partition function for [[Configuration]].
   */
  class ConfigurationPF(conf: Configuration) {

    def append(key: String, value: Any): ConfigurationPF = {
      val rawValue = value match {
        case v: Iterable[_]         => v.map(_.toString).mkString(";")
        case v: Array[_]            => v.map(_.toString).mkString(";")
        case v: Map[String, String] => v.map(kv => s"${kv._1}=${kv._2}").mkString(";")
        case v                      => v.toString
      }
      conf.setString(key, rawValue)
      this
    }
    def appendWhen(cond: => Boolean)(key: String, value: Any): ConfigurationPF = if (cond) append(key, value) else this

    def append(rawConf: FlinkRawConf): ConfigurationPF         = rawConf.injectRaw(this)
    def append(rawConf: Option[FlinkRawConf]): ConfigurationPF = rawConf.map(_.injectRaw(this)).getOrElse(this)

    def tap(f: Configuration => Any): Configuration                                        = { f(this.conf); this }
    def pipe(f: ConfigurationPF => ConfigurationPF): ConfigurationPF                       = f(this)
    def pipeWhen(cond: => Boolean)(f: ConfigurationPF => ConfigurationPF): ConfigurationPF = if (cond) f(this) else this

    def merge(anotherConf: Map[String, String]): ConfigurationPF = {
      anotherConf.foreach { case (k, v) => conf.setString(k, v) }
      this
    }

    def value: Configuration = conf
  }

}
