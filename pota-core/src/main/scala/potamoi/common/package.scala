package potamoi

import zio.json.{JsonCodec, JsonDecoder, JsonEncoder}

import scala.concurrent.duration.Duration

package object common {

  implicit val scalaDurationCodec: JsonCodec[Duration] = JsonCodec(
    encoder = JsonEncoder[(Long, String)].contramap(d => d._1 -> d._2.toString),
    decoder = JsonDecoder[(Long, String)].map(t => Duration(t._1, t._2))
  )

}
