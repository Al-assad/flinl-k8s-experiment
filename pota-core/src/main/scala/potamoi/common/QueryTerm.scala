package potamoi.common

import potamoi.db.defaultPageLimit

import scala.reflect.ClassTag

/**
 * Pagination query parameters.
 *
 * @param pagNum  page index number, form 1 on
 * @param pagSize page size limit
 */
case class PageReq(pagNum: Int = 1, pagSize: Int = defaultPageLimit) {
  def safety: PageReq = PageReq(
    pagNum = if (pagNum <= 0) 1 else pagNum,
    pagSize = if (pagSize <= 0) defaultPageLimit else pagSize
  )
  def isHead: Boolean    = pagNum == 1
  def offsetRows: Long   = (pagNum - 1) * pagSize
  def offsetRowsInt: Int = (pagNum - 1) * pagSize
}

/**
 * Pagination query result.
 *
 * @param totalEle total rows count
 * @param totalPage    total page count
 * @param size         current result size
 * @param pagNum       current page index number
 * @param pagSize      current page size
 * @param data         result data sequence
 */
case class PageRsp[T](totalEle: Int, totalPage: Int, size: Int, pagNum: Int, pagSize: Int, data: List[T]) {
  // whether has next page
  val hasNext: Boolean = pagNum < totalPage
  // whether has previous page
  val hasPrev: Boolean = pagNum > 1
}

object PageRsp {
  def apply[T: ClassTag](pageReq: PageReq, totalElement: Int, data: List[T]): PageRsp[T] = PageRsp(
    totalEle = totalElement,
    totalPage = (totalElement.toDouble / pageReq.pagSize.toDouble).ceil.toInt,
    size = data.size,
    pagNum = pageReq.pagNum,
    pagSize = pageReq.pagSize,
    data = data
  )
}

/**
 * Sorting order terms.
 */
object Order extends ComplexEnum {
  type Order = Value
  val asc  = Value(1, "asc")
  val desc = Value(-1, "desc")
}

/**
 * Timestamp range.
 */
case class TsRange(begin: Option[Long] = None, end: Option[Long] = None) {
  def isUnlimited: Boolean        = begin.isEmpty && end.isEmpty
  def isLimited: Boolean          = begin.isDefined || end.isDefined
  def judge(value: Long): Boolean = begin.forall(_ <= value) && end.forall(_ > value)
}
