package potamoi.flink

import zio.IO

package object share {

  type JobId     = String
  type JarId     = String
  type TriggerId = String

  /**
   * Flink operation IO
   */
  type FlinkIO[A] = IO[FlinkOprErr, A]

}
