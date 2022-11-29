package potamoi.flink.share.model

import potamoi.curTs
import zio.json.{DeriveJsonCodec, JsonCodec}

/**
 * Flink task manager details.
 */
case class FlinkTmDetail(
    id: String,
    path: String,
    dataPort: Int,
    slotsNumber: Int,
    freeSlots: Int,
    totalResource: TmResource,
    freeResource: TmResource,
    hardware: TmHardware,
    memoryConfiguration: TmMemoryConfig,
    ts: Long = curTs)

case class TmResource(
    cpuCores: Float,
    taskHeapMemory: Long,
    taskOffHeapMemory: Long,
    managedMemory: Long,
    networkMemory: Long)

case class TmHardware(
    cpuCores: Float,
    physicalMemory: Long,
    freeMemory: Long,
    managedMemory: Long)

case class TmMemoryConfig(
    frameworkHeap: Long,
    taskHeap: Long,
    frameworkOffHeap: Long,
    taskOffHeap: Long,
    networkMemory: Long,
    managedMemory: Long,
    jvmMetaspace: Long,
    jvmOverhead: Long,
    totalFlinkMemory: Long,
    totalProcessMemory: Long)

object FlinkTmDetail {
  implicit val tmResourceCodec: JsonCodec[TmResource]         = DeriveJsonCodec.gen[TmResource]
  implicit val tmHardwareCodec: JsonCodec[TmHardware]         = DeriveJsonCodec.gen[TmHardware]
  implicit val tmMemoryConfigCodec: JsonCodec[TmMemoryConfig] = DeriveJsonCodec.gen[TmMemoryConfig]
  implicit val tmDetailCodec: JsonCodec[FlinkTmDetail]        = DeriveJsonCodec.gen[FlinkTmDetail]
  implicit val ordering: Ordering[FlinkTmDetail]              = Ordering.by(_.id)
}
