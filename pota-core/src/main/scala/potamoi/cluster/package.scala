package potamoi

import akka.cluster.ddata.Replicator
import akka.cluster.ddata.Replicator.{ReadMajorityPlus, WriteMajorityPlus}
import akka.cluster.ddata.typed.scaladsl.Replicator.{ReadAll, ReadLocal, WriteAll, WriteLocal}
import potamoi.config.{DDataReadLevel, DDataWriteLevel}

import scala.concurrent.duration.FiniteDuration

package object cluster {

  /**
   * Conversion from [[potamoi.config.DDataWriteLevel]] to [[akka.cluster.ddata.typed.scaladsl.Replicator.WriteConsistency]].
   */
  implicit class DDataWriteLevelWrapper(level: DDataWriteLevel) {
    def asAkka: Replicator.WriteConsistency = level match {
      case DDataWriteLevel.WriteLocal                                 => WriteLocal
      case DDataWriteLevel.WriteMajority(timeout, additional, minCap) => WriteMajorityPlus(FiniteDuration(timeout._1, timeout._2), additional, minCap)
      case DDataWriteLevel.WriteAll(timeout)                          => WriteAll(FiniteDuration(timeout._1, timeout._2))
    }
  }

  /**
   * Conversion from [[potamoi.config.DDataReadLevel]] to [[akka.cluster.ddata.typed.scaladsl.Replicator.ReadConsistency]].
   */
  implicit class DDataReadLevelWrapper(level: DDataReadLevel) {
    def asAkka: Replicator.ReadConsistency = level match {
      case DDataReadLevel.ReadLocal                                 => ReadLocal
      case DDataReadLevel.ReadMajority(timeout, additional, minCap) => ReadMajorityPlus(FiniteDuration(timeout._1, timeout._2), additional, minCap)
      case DDataReadLevel.ReadAll(timeout)                          => ReadAll(FiniteDuration(timeout._1, timeout._2))
    }
  }

}
