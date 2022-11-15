package potamoi.config

import potamoi.common.ComplexEnum

/**
 * Potamoi cluster node role.
 */
object NodeRole extends ComplexEnum {
  type NodeRole = Value

  val Server             = Value("server")
  val FlinkOperator      = Value("flink-operator")
  val FlinkSqlInteractor = Value("flink-sql-interactor")
}
