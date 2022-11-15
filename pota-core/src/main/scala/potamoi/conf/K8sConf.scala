package potamoi.conf

import zio.json.{DeriveJsonCodec, JsonCodec}

/**
 * Kubernetes configuration.
 */
case class K8sConf(debug: Boolean = false) extends Resolvable

object K8sConf {
  implicit val codec: JsonCodec[K8sConf] = DeriveJsonCodec.gen[K8sConf]
}
