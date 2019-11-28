import java.util.UUID

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.UserVisitAction
import commons.utils.{DateUtils, ParamUtils}
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.mutable



/**
  * @Autor sc
  * @DATE 2019/9/12 13:18
  */
object PageConvertStat {

  def getUserVisitAction(sparkSession: SparkSession, taskParam: JSONObject) = {
    val startDate: String = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE)
    val endDate: String = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE)

    val sql: String = "select * from user_visit_action where date >='" + startDate + "'and date <= '" + endDate + "'"

    import sparkSession.implicits._

    sparkSession.sql(sql).as[UserVisitAction].rdd.map(item => (item.session_id, item))
  }

  def getPageConvert(sparkSession: SparkSession, taskUUID: String, targetPageSplit: Array[String], startPageCount: Long, pageSplitCountMap: collection.Map[String, Long]): Unit = {
    val pageSplitRatio = new mutable.HashMap[String,Double]()

    var lastPageCount = startPageCount.toDouble

    for (pageSplit <- targetPageSplit){
      val currentPageSplitCount: Double = pageSplitCountMap.get(pageSplit).get.toDouble
      val ratio  = currentPageSplitCount / lastPageCount
      pageSplitRatio.put(pageSplit,ratio)
      lastPageCount = currentPageSplitCount
    }
    val convertStr: String = pageSplitRatio.map {
      case (pageSplit, ratio) => pageSplit + "=" + ratio
    }.mkString("|")
    val pageSplit = PageSplitConvertRate(taskUUID,convertStr)
    val pageSplitRatioRDD: RDD[PageSplitConvertRate] = sparkSession.sparkContext.makeRDD(Array(pageSplit))

    import sparkSession.implicits._

    pageSplitRatioRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable", "page_split_convert_rate_0912")
      .mode(SaveMode.Append)
      .save()

  }

  def main(args: Array[String]): Unit = {
    val jsonStr: String = ConfigurationManager.config.getString(Constants.TASK_PARAMS)
    val taskParam: JSONObject = JSONObject.fromObject(jsonStr)

    val taskUUID: String = UUID.randomUUID().toString

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("PageConvertStat")

    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

    //获取用户行为数据
    val sessionId2ActionRDD: RDD[(String, UserVisitAction)] = getUserVisitAction(sparkSession, taskParam)

    // pageFlowStr: "1,2,3,4,5,6,7"
    val pageFlowStr = ParamUtils.getParam(taskParam, Constants.PARAM_TARGET_PAGE_FLOW)
    // pageFlowArray: Array[Long]  [1,2,3,4,5,6,7]
    val pageFlowArray: Array[String] = pageFlowStr.split(",")
    // pageFlowArray.slice(0, pageFlowArray.length - 1): [1,2,3,4,5,6]
    // pageFlowArray.tail: [2,3,4,5,6,7]
    // pageFlowArray.slice(0, pageFlowArray.length - 1).zip(pageFlowArray.tail): [(1,2), (2,3) , ..]
    // targetPageSplit: [1_2, 2_3, 3_4, ...]
    val targetPageSplit = pageFlowArray.slice(0, pageFlowArray.length - 1).zip(pageFlowArray.tail).map {
      case (page1, page2) => page1 + "_" + page2
    }
    // sessionId2ActionRDD: RDD[(sessionId, action)]
    val sessionId2GroupRDD = sessionId2ActionRDD.groupByKey()

    // pageSplitNumRDD: RDD[(String, 1L)]
    val pageSplitNumRDD: RDD[(String, Long)] = sessionId2GroupRDD.flatMap {
      case (sessionId, iterableAction) =>
        // item1: action
        // item2: action
        // sortList: List[UserVisitAction]
        val sortList = iterableAction.toList.sortWith((item1, item2) => {
          DateUtils.parseTime(item1.action_time).getTime < DateUtils.parseTime(item2.action_time).getTime
        })

        // pageList: List[Long]  [1,2,3,4,...]
        val pageList = sortList.map {
          case action => action.page_id
        }

        // pageList.slice(0, pageList.length - 1): [1,2,3,..,N-1]
        // pageList.tail: [2,3,4,..,N]
        // pageList.slice(0, pageList.length - 1).zip(pageList.tail): [(1,2), (2,3), ...]
        // pageSplit: [1_2, 2_3, ...]
        val pageSplit = pageList.slice(0, pageList.length - 1).zip(pageList.tail).map {
          case (page1, page2) => page1 + "_" + page2
        }

        val pageSplitFilter: List[String] = pageSplit.filter {
          case pageSplit => targetPageSplit.contains(pageSplit)
        }

        pageSplitFilter.map {
          case pageSplit => (pageSplit, 1L)
        }
    }

    // pageSplitCountMap: Map[(pageSplit, count)]
    val pageSplitCountMap = pageSplitNumRDD.countByKey()

    val startPage = pageFlowArray(0).toLong

    val startPageCount = sessionId2ActionRDD.filter {
      case (sessionId, action) => action.page_id == startPage
    }.count()


    getPageConvert(sparkSession,taskUUID,targetPageSplit,startPageCount,pageSplitCountMap)
  }
}


