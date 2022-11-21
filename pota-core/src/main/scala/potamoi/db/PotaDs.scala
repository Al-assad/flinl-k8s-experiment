package potamoi.db

import io.getquill.SnakeCase
import io.getquill.jdbczio.Quill
import potamoi.config.PotaConf
import zio.ZLayer

/**
 * Potamoi persist storage datasource for Quill.
 */
object PotaDs {

  val live: ZLayer[PotaConf, Throwable, QuillCtx] = {
    ZLayer.service[PotaConf].flatMap { conf =>
      Quill.DataSource.fromDataSource(conf.get.db.toHikariDs)
    } >>>
    Quill.Postgres.fromNamingStrategy(SnakeCase)
  }

}
