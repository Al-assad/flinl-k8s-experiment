package potamoi.flink

import zio.IO

package object share {

  type JobId     = String
  type JarId     = String
  type TriggerId = String

  type FlinkIO[A] = IO[FlinkOprErr, A]

}
