package potamoi

import io.getquill.SnakeCase
import io.getquill.jdbczio.Quill
import zio.ZIO

import java.sql.SQLException

package object db {

  val defaultPageLimit: Int = 10

  type UQIO[A] = ZIO[Any, SQLException, A]

  type QuillCtx = Quill.Postgres[SnakeCase]

}
