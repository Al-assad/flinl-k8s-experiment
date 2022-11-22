package potamoi.flink

import potamoi.common.Syntax.GenericPF
import potamoi.flink.share.model.Fcid

package object observer {

  // marshall/unmarshall between fcid and cluster-sharding entity id.
  def marshallFcid(fcid: Fcid): String   = s"${fcid.clusterId}@${fcid.namespace}"
  def unMarshallFcid(fcid: String): Fcid = fcid.split('@').contra { arr => Fcid(arr(0), arr(1)) }

}
