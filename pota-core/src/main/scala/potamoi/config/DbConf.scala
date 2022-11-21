package potamoi.config

import com.zaxxer.hikari.HikariDataSource
import potamoi.syntax._
import zio.config.magnolia.name
import zio.json.{DeriveJsonCodec, JsonCodec}

/**
 * Potamoi database configuration.
 * see: https://github.com/brettwooldridge/HikariCP#gear-configuration-knobs-baby
 */
case class DbConf(
    host: String,
    port: Int,
    database: String,
    user: String,
    password: Option[String] = None,
    @name("max-pool-size") maximumPoolSize: Option[Int] = Some(20),
    @name("connection-timeout") connectionTimeout: Option[Long] = Some(30000),
    @name("idle-timeout") idleTimeout: Option[Long] = None,
    @name("min-idle") minimumIdle: Option[Int] = None,
    @name("max-life-time") maxLifetime: Option[Long] = None)
    extends Resolvable {

  lazy val jdbcUrl = s"jdbc:postgresql://$host:$port/$database"

  def toHikariDs: HikariDataSource = new HikariDataSource().contra { ds =>
    ds.setJdbcUrl(jdbcUrl)
    ds.setUsername(user)
    password.foreach(ds.setPassword)
    maximumPoolSize.foreach(ds.setMaximumPoolSize)
    connectionTimeout.foreach(ds.setConnectionTimeout)
    idleTimeout.foreach(ds.setIdleTimeout)
    minimumIdle.foreach(ds.setMinimumIdle)
    maxLifetime.foreach(ds.setMaxLifetime)
    ds.setAutoCommit(true)
    ds
  }
}

object DbConf {
  implicit val codec: JsonCodec[DbConf] = DeriveJsonCodec.gen[DbConf]
}


