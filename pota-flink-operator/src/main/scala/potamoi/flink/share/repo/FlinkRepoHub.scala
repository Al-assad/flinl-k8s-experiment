package potamoi.flink.share.repo

import potamoi.db.QuillCtx
import zio.ZLayer

/**
 * Flink cluster information repositories.
 */
case class FlinkRepoHub(jobOverview: FlinkJobOverviewRepo)

object FlinkRepoHub {
  val live = ZLayer.service[QuillCtx].project { ctx =>
    FlinkRepoHub(jobOverview = FlinkJobOverviewRepo.persist(ctx))
  }
}
