import java.util.{Date, Random, UUID}

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.model.{UserInfo, UserVisitAction}
import commons.utils.{DateUtils, NumberUtils, ParamUtils, StringUtils, ValidUtils}
import net.sf.json.JSONObject
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}

import scala.collection.immutable.StringOps
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}


/**
  * @Autor sc
  * @DATE 2019/9/10 13:28
  */
object SessionStat {

  def getOriActionRDD(sc: SparkSession, taskParam: JSONObject) = {

    val startDate: String = ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE)
    val endDate: String = ParamUtils.getParam(taskParam, Constants.PARAM_END_DATE)

    val sql: String = "select * from user_visit_action where date >='" + startDate + "' and date <= '" + endDate + "'"
    import sc.implicits._
    sc.sql(sql).as[UserVisitAction].rdd
  }

  def getSessionFullInfo(sc: SparkSession, session2GroupActionRDD: RDD[(String, Iterable[UserVisitAction])]) = {
    //[(userId,aggrInfo)]
    val userId2AggrInfoRDD: RDD[(Long, String)] = session2GroupActionRDD.map {
      case (sessionId, iterableAction) => {
        var userId: Long = -1L

        var startTime: Date = null
        var endTime: Date = null

        var stepLength = 0

        val searchKeywords = new StringBuffer("")
        val clickCategories = new StringBuffer("")

        for (action <- iterableAction) {
          if (userId == -1L) {
            userId = action.user_id
          }

          //获得一个session的开始时间和结束时间
          val actionTime: Date = DateUtils.parseTime(action.action_time)
          if (startTime == null || startTime.after(actionTime)) {
            startTime = actionTime
          }
          if (endTime == null || endTime.before(actionTime)) {
            endTime = actionTime
          }

          val search_keyword: String = action.search_keyword
          if (StringUtils.isNotEmpty(search_keyword) && !searchKeywords.toString.contains(search_keyword)) {
            searchKeywords.append(search_keyword + ",")

          }

          val clickCategoryId: Long = action.click_category_id
          if (clickCategoryId != -1 && !clickCategories.toString.contains(clickCategoryId)) {
            clickCategories.append(clickCategoryId + ",")

          }
          stepLength += 1
        }

        val searchKw: String = StringUtils.trimComma(searchKeywords.toString)
        val clickCg: String = StringUtils.trimComma(clickCategories.toString)

        val visitLength: Long = (endTime.getTime - startTime.getTime) / 1000

        val aggrInfo = Constants.FIELD_SESSION_ID + "=" + sessionId + "|" +
          Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKw + "|" +
          Constants.FIELD_CLICK_CATEGORY_IDS + "=" + clickCg + "|" +
          Constants.FIELD_VISIT_LENGTH + "=" + visitLength + "|" +
          Constants.FIELD_STEP_LENGTH + "=" + stepLength + "|" +
          Constants.FIELD_START_TIME + "=" + DateUtils.formatTime(startTime) + "|"

        (userId, aggrInfo)
      }
    }
    val sql = "select * from user_info"
    import sc.implicits._
    val userId2InfoRDD: RDD[(Long, UserInfo)] = sc.sql(sql).as[UserInfo].rdd.map(item => (item.user_id, item))
    val SessionId2FullInfoRDD: RDD[(String, String)] = userId2AggrInfoRDD.join(userId2InfoRDD).map {
      case (userId, (aggrInfo, userInfo)) => {
        val age: Int = userInfo.age
        val professional: String = userInfo.professional
        val sex: String = userInfo.sex
        val city: String = userInfo.city

        val fullInfo = aggrInfo + "|" +
          Constants.FIELD_AGE + "=" + age + "|" +
          Constants.FIELD_PROFESSIONAL + "=" + professional + "|" +
          Constants.FIELD_SEX + "=" + sex + "|" +
          Constants.FIELD_CITY + "=" + city

        val sessionId: String = StringUtils.getFieldFromConcatString(aggrInfo, "\\|", Constants.FIELD_SESSION_ID)
        (sessionId, fullInfo)
      }
    }
    SessionId2FullInfoRDD
  }

  def calculateVisitLength(visitLength: Long, sessionAccumulator: SessionAccumulator) = {
    if (visitLength >= 1 && visitLength <= 3)
      sessionAccumulator.add(Constants.TIME_PERIOD_1s_3s)
    else if (visitLength > 4 && visitLength <= 6)
      sessionAccumulator.add(Constants.TIME_PERIOD_4s_6s)
    else if (visitLength > 7 && visitLength <= 9)
      sessionAccumulator.add(Constants.TIME_PERIOD_7s_9s)
    else if (visitLength > 10 && visitLength <= 30)
      sessionAccumulator.add(Constants.TIME_PERIOD_10s_30s)
    else if (visitLength > 30 && visitLength <= 60)
      sessionAccumulator.add(Constants.TIME_PERIOD_30s_60s)
    else if (visitLength > 60 && visitLength <= 180)
      sessionAccumulator.add(Constants.TIME_PERIOD_1m_3m)
    else if (visitLength > 180 && visitLength <= 600)
      sessionAccumulator.add(Constants.TIME_PERIOD_3m_10m)
    else if (visitLength > 600 && visitLength <= 1800)
      sessionAccumulator.add(Constants.TIME_PERIOD_10m_30m)
    else if (visitLength > 1800)
      sessionAccumulator.add(Constants.TIME_PERIOD_30m)
  }

  def calculateStepLength(stepLength: Long, sessionAccumulator: SessionAccumulator) = {
    if (stepLength > 1 && stepLength <= 3)
      sessionAccumulator.add(Constants.STEP_PERIOD_1_3)
    else if (stepLength > 4 && stepLength <= 6)
      sessionAccumulator.add(Constants.STEP_PERIOD_4_6)
    else if (stepLength > 7 && stepLength <= 9)
      sessionAccumulator.add(Constants.STEP_PERIOD_7_9)
    else if (stepLength > 10 && stepLength <= 30)
      sessionAccumulator.add(Constants.STEP_PERIOD_10_30)
    else if (stepLength > 30 && stepLength <= 60)
      sessionAccumulator.add(Constants.STEP_PERIOD_30_60)
    else if (stepLength > 60)
      sessionAccumulator.add(Constants.STEP_PERIOD_60)
  }

  def getSessionFiteredRDD(taskParam: JSONObject, sessionId2FullInfoRDD: RDD[(String, String)], sessionAccumulator: SessionAccumulator) = {

    val startAge: String = ParamUtils.getParam(taskParam, Constants.PARAM_START_AGE)
    val endAge: String = ParamUtils.getParam(taskParam, Constants.PARAM_END_AGE)
    val professionals: String = ParamUtils.getParam(taskParam, Constants.PARAM_PROFESSIONALS)
    val citys: String = ParamUtils.getParam(taskParam, Constants.PARAM_CITIES)
    val sex: String = ParamUtils.getParam(taskParam, Constants.PARAM_SEX)
    val keywords: String = ParamUtils.getParam(taskParam, Constants.PARAM_KEYWORDS)
    val categoryIds: String = ParamUtils.getParam(taskParam, Constants.PARAM_CATEGORY_IDS)

    var filterInfo: String = (if (startAge != null) Constants.PARAM_START_AGE + "=" + startAge + "|" else "") +
      (if (endAge != null) Constants.PARAM_END_AGE + "=" + endAge + "|" else "") +
      (if (professionals != null) Constants.PARAM_PROFESSIONALS + "=" + professionals + "|" else "") +
      (if (citys != null) Constants.PARAM_CITIES + "=" + citys + "|" else "") +
      (if (sex != null) Constants.PARAM_SEX + "=" + sex + "|" else "") +
      (if (keywords != null) Constants.PARAM_KEYWORDS + "=" + keywords + "|" else "") +
      (if (categoryIds != null) Constants.PARAM_CATEGORY_IDS + "=" + categoryIds else "")

    if (filterInfo.endsWith("\\|"))
      filterInfo = filterInfo.substring(0, filterInfo.length - 1)

    val sessionId2FiteredRDD: RDD[(String, String)] = sessionId2FullInfoRDD.filter {
      case (sessionId, fullInfo) =>
        var success = true

        if (!ValidUtils.between(fullInfo, Constants.FIELD_AGE, filterInfo, Constants.PARAM_START_AGE, Constants.PARAM_END_AGE))
          success = false
        else if (!ValidUtils.in(fullInfo, Constants.FIELD_PROFESSIONAL, filterInfo, Constants.PARAM_PROFESSIONALS))
          success = false
        else if (!ValidUtils.in(fullInfo, Constants.FIELD_CITY, filterInfo, Constants.PARAM_CITIES))
          success = false
        else if (!ValidUtils.equal(fullInfo, Constants.FIELD_SEX, filterInfo, Constants.PARAM_SEX))
          success = false
        else if (!ValidUtils.in(fullInfo, Constants.FIELD_SEARCH_KEYWORDS, filterInfo, Constants.PARAM_KEYWORDS))
          success = false
        else if (!ValidUtils.in(fullInfo, Constants.FIELD_CATEGORY_ID, filterInfo, Constants.PARAM_CATEGORY_IDS))
          success = false

        if (success) {
          sessionAccumulator.add(Constants.SESSION_COUNT)

          val visitLength: Long = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_VISIT_LENGTH).toLong
          val stepLength: Long = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_STEP_LENGTH).toLong


          calculateVisitLength(visitLength, sessionAccumulator)

          calculateStepLength(stepLength, sessionAccumulator)


        }
        success
    }
    sessionId2FiteredRDD
  }

  //获得session步长和session时间范围百分比
  def getSessionRatio(sc: SparkSession, taskUUID: String, value: mutable.HashMap[String, Int]) = {

    val session_count: Double = value.getOrElse(Constants.SESSION_COUNT, 1).toDouble

    val visit_length_1s_3s: Int = value.getOrElse(Constants.TIME_PERIOD_1s_3s, 0)
    val visit_length_4s_6s: Int = value.getOrElse(Constants.TIME_PERIOD_4s_6s, 0)
    val visit_length_7s_9s: Int = value.getOrElse(Constants.TIME_PERIOD_7s_9s, 0)
    val visit_length_10s_30s: Int = value.getOrElse(Constants.TIME_PERIOD_10s_30s, 0)
    val visit_length_30s_60s: Int = value.getOrElse(Constants.TIME_PERIOD_30s_60s, 0)
    val visit_length_1m_3m: Int = value.getOrElse(Constants.TIME_PERIOD_1m_3m, 0)
    val visit_length_3m_10m: Int = value.getOrElse(Constants.TIME_PERIOD_3m_10m, 0)
    val visit_length_10m_30m: Int = value.getOrElse(Constants.TIME_PERIOD_10m_30m, 0)
    val visit_length_30m: Int = value.getOrElse(Constants.TIME_PERIOD_30m, 0)

    val step_length_1_3: Int = value.getOrElse(Constants.STEP_PERIOD_1_3, 0)
    val step_length_4_6: Int = value.getOrElse(Constants.STEP_PERIOD_4_6, 0)
    val step_length_7_9: Int = value.getOrElse(Constants.STEP_PERIOD_7_9, 0)
    val step_length_10_30: Int = value.getOrElse(Constants.STEP_PERIOD_10_30, 0)
    val step_length_30_60: Int = value.getOrElse(Constants.STEP_PERIOD_30_60, 0)
    val step_length_60: Int = value.getOrElse(Constants.STEP_PERIOD_60, 0)

    val visit_length_1s_3s_ratio: Double = NumberUtils.formatDouble(visit_length_1s_3s / session_count, 2)
    val visit_length_4s_6s_ratio: Double = NumberUtils.formatDouble(visit_length_4s_6s / session_count, 2)
    val visit_length_7s_9s_ratio: Double = NumberUtils.formatDouble(visit_length_7s_9s / session_count, 2)
    val visit_length_10s_30s_ratio: Double = NumberUtils.formatDouble(visit_length_10s_30s / session_count, 2)
    val visit_length_30s_60s_ratio: Double = NumberUtils.formatDouble(visit_length_30s_60s / session_count, 2)
    val visit_length_1m_3m_ratio: Double = NumberUtils.formatDouble(visit_length_1m_3m / session_count, 2)
    val visit_length_3m_10m_ratio: Double = NumberUtils.formatDouble(visit_length_3m_10m / session_count, 2)
    val visit_length_10m_30m_ratio: Double = NumberUtils.formatDouble(visit_length_10m_30m / session_count, 2)
    val visit_length_30m_ratio: Double = NumberUtils.formatDouble(visit_length_30m / session_count, 2)

    val step_length_1_3_ratio: Double = NumberUtils.formatDouble(step_length_1_3 / session_count, 2)
    val step_length_4_6_ratio: Double = NumberUtils.formatDouble(step_length_4_6 / session_count, 2)
    val step_length_7_9_ratio: Double = NumberUtils.formatDouble(step_length_7_9 / session_count, 2)
    val step_length_10_30_ratio: Double = NumberUtils.formatDouble(step_length_10_30 / session_count, 2)
    val step_length_30_60_ratio: Double = NumberUtils.formatDouble(step_length_30_60 / session_count, 2)
    val step_length_60_ratio: Double = NumberUtils.formatDouble(step_length_60 / session_count, 2)


    val stat = SessionAggrStat(taskUUID, session_count.toLong, visit_length_1s_3s_ratio, visit_length_4s_6s_ratio, visit_length_7s_9s_ratio,
      visit_length_10s_30s_ratio, visit_length_30s_60s_ratio, visit_length_1m_3m_ratio,
      visit_length_3m_10m_ratio, visit_length_10m_30m_ratio, visit_length_30m_ratio,
      step_length_1_3_ratio, step_length_4_6_ratio, step_length_7_9_ratio,
      step_length_10_30_ratio, step_length_30_60_ratio, step_length_60_ratio)

    val sessionRatioRDD = sc.sparkContext.makeRDD(Array(stat))
    import sc.implicits._
    sessionRatioRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable", "session_stat_ratio_0911")
      .mode(SaveMode.Append)
      .save()

  }

  def generateRandomIndexList(extractPerDay: Long,
                              daySessionCount: Long,
                              hourCountMap: mutable.HashMap[String, Long],
                              hourListMap: mutable.HashMap[String, ListBuffer[Int]]) = {
    for ((hour, count) <- hourCountMap) {
      //获取一个小时要抽取多少数据   (每小时数量/一天数量)*每天的抽取数量
      var hourExrCount: Int = ((count / daySessionCount.toDouble) * extractPerDay).toInt
      //避免一个小时要抽取的数量超过这个小时的总数
      if (hourExrCount > count) {
        hourExrCount = count.toInt
      }

      val random = new Random()
      hourListMap.get(hour) match {
        case None => hourListMap(hour) = new ListBuffer[Int]
          for (i <- 0 until hourExrCount) {
            var index: Int = random.nextInt(count.toInt)
            while (hourListMap(hour).contains(index)) {
              index = random.nextInt(count.toInt)
            }
            hourListMap(hour).append(index)
          }
        case Some(list) =>
          for (i <- 0 until hourExrCount) {
            var index: Int = random.nextInt(count.toInt)
            while (hourListMap(hour).contains(index)) {
              index = random.nextInt(count.toInt)
            }
            hourListMap(hour).append(index)

          }
      }
    }
  }

  def sessionRandomExtract(sc: SparkSession,
                           taskUUID: String,
                           sessionId2FiteredRDD: RDD[(String, String)]): Unit = {
    val dateHour2FullInforRDD: RDD[(String, String)] = sessionId2FiteredRDD.map {
      case (sid, fullInfo) => {
        val startTime: String = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_START_TIME)
        val dateHour: String = DateUtils.getDateHour(startTime)
        (dateHour, fullInfo)
      }

    }
    val hourCountMap: collection.Map[String, Long] = dateHour2FullInforRDD.countByKey()

    val dateHourCountMap = new mutable.HashMap[String, mutable.HashMap[String, Long]]()

    for ((dateHour, count) <- hourCountMap) {
      val date: String = dateHour.split("_")(0)
      val hour: String = dateHour.split("_")(1)

      dateHourCountMap.get(date) match {
        case None => dateHourCountMap(date) = new mutable.HashMap[String, Long]()
          dateHourCountMap(date) += (hour -> count)
        case Some(map) => dateHourCountMap(date) += (hour -> count)
      }
    }
    //解决问题:一天抽取多少数量
    val extractPerDay = 100 / dateHourCountMap.size

    //随机index   list map
    val dateHourExtractIndexListMap = new mutable.HashMap[String, mutable.HashMap[String, ListBuffer[Int]]]()
    //
    for ((date, hourCountMap) <- dateHourCountMap) {
      val dateSessionCount: Long = hourCountMap.values.sum

      dateHourExtractIndexListMap.get(date) match {
        case None => dateHourExtractIndexListMap(date) = new mutable.HashMap[String, ListBuffer[Int]]()
          generateRandomIndexList(extractPerDay, dateSessionCount, hourCountMap, dateHourExtractIndexListMap(date))
        case Some(map) =>
          generateRandomIndexList(extractPerDay, dateSessionCount, hourCountMap, dateHourExtractIndexListMap(date))
      }
    }
    val dateHourExtractIndexListMapBd: Broadcast[mutable.HashMap[String, mutable.HashMap[String, ListBuffer[Int]]]] = sc.sparkContext.broadcast(dateHourExtractIndexListMap)

    val dateHour2GroupRDD: RDD[(String, Iterable[String])] = dateHour2FullInforRDD.groupByKey()

    val extractSessionRDD = dateHour2GroupRDD.flatMap {
      case (dateHour, iterableFullInfo) =>
        val date: String = dateHour.split("_")(0)
        val hour: String = dateHour.split("_")(1)

        val extractList: ListBuffer[Int] = dateHourExtractIndexListMapBd.value.get(date).get(hour)

        val extractSessionArrayBuffer = new ArrayBuffer[SessionRandomExtract]()
        var index = 0
        for (fullInfo <- iterableFullInfo) {
          if (extractList.contains(index)) {
            val sessionId = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_SESSION_ID)
            val startTime: String = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_START_TIME)
            val searchKeywords: String = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_SEARCH_KEYWORDS)
            val clickCategories: String = StringUtils.getFieldFromConcatString(fullInfo, "\\|", Constants.FIELD_CLICK_CATEGORY_IDS)

            val extractSession = SessionRandomExtract(taskUUID, sessionId, startTime, searchKeywords, clickCategories)
            extractSessionArrayBuffer += extractSession
          }
          index += 1

        }
        extractSessionArrayBuffer

    }
    import sc.implicits._
    extractSessionRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable", "session_extract_0911")
      .mode(SaveMode.Append)
      .save()
  }

  //第二步:统计品类的点击次数
  def getClickCount(sessionId2FilterActionRDD: RDD[(String, UserVisitAction)]) = {
    val clickFilterRDD: RDD[(String, UserVisitAction)] = sessionId2FilterActionRDD.filter {
      case (sessionId, action) => action.click_category_id != -1L
    }
    val clickNumRDD: RDD[(Long, Long)] = clickFilterRDD.map {
      case (sessionId, action) => (action.click_category_id, 1L)
    }
    clickNumRDD.reduceByKey(_ + _)

  }

  def getOrderCount(sessionId2FilterActionRDD: RDD[(String, UserVisitAction)]) = {
    val orderFilterRDD: RDD[(String, UserVisitAction)] = sessionId2FilterActionRDD.filter {
      case (sessionId, action) => action.order_category_ids != null
    }

    val orderNumRDD = orderFilterRDD.flatMap {
      case (sessionId, action) => action.order_category_ids.split(",")
        .map(item => (item.toLong, 1L))
    }
    orderNumRDD.reduceByKey(_ + _)
  }

  def getPayCount(sessionId2FilterActionRDD: RDD[(String, UserVisitAction)]) = {
    val payFilterRDD: RDD[(String, UserVisitAction)] = sessionId2FilterActionRDD.filter {
      case (sessionId, action) => action.pay_category_ids != null

    }
    val payNumRDD = payFilterRDD.flatMap {
      case (sessionId, action) => action.pay_category_ids.split(",").map(item => (item.toLong, 1L))
    }
    payNumRDD.reduceByKey(_ + _)
  }

  def getFullCount(cid2CidRDD: RDD[(Long, Long)], cid2ClickCountRDD: RDD[(Long, Long)], cid2OrderCountRDD: RDD[(Long, Long)], cid2PayCountRDD: RDD[(Long, Long)]) = {
    val cid2ClickInfoRDD: RDD[(Long, String)] = cid2CidRDD.leftOuterJoin(cid2ClickCountRDD).map {
      case (cid, (categoryId, option)) =>
        val clickCount = if (option.isDefined) option.get else 0
        val aggrCount: String = Constants.FIELD_CATEGORY_ID + "=" + cid + "|" +
          Constants.FIELD_CLICK_COUNT + "=" + clickCount
        (cid, aggrCount)
    }
    val cid2OrderInfoRDD: RDD[(Long, String)] = cid2ClickInfoRDD.leftOuterJoin(cid2OrderCountRDD).map {
      case (cid, (clickInfo, option)) =>
        val orderCount = if (option.isDefined) option.get else 0
        val aggrInfo = clickInfo + "|" + Constants.FIELD_ORDER_COUNT + "=" + orderCount

        (cid, aggrInfo)

    }
    val cid2PayInfoRDD: RDD[(Long, String)] = cid2OrderInfoRDD.leftOuterJoin(cid2PayCountRDD).map {
      case (cid, (orderInfo, option)) =>
        val payCount = if (option.isDefined) option.get else 0
        val aggrInfo = orderInfo + "|" + Constants.FIELD_PAY_COUNT + "=" + payCount
        (cid, aggrInfo)
    }
    cid2PayInfoRDD

  }

  def top10PopularCategories(sc: SparkSession, taskUUID: String, sessionId2FilterActionRDD: RDD[(String, UserVisitAction)]) = {
    var cid2CidRDD: RDD[(Long, Long)] = sessionId2FilterActionRDD.flatMap {
      case (sid, action) =>
        val categoryBuffer: ArrayBuffer[(Long, Long)] = new ArrayBuffer[(Long, Long)]()
        //点击行为
        if (action.click_category_id != -1) {
          categoryBuffer += ((action.click_category_id, action.click_category_id))
        } else if (action.order_category_ids != null) {
          for (orderCid <- action.order_category_ids.split(","))
            categoryBuffer += ((orderCid.toLong, orderCid.toLong))
        } else if (action.pay_category_ids != null) {
          for (payCid <- action.pay_category_ids.split(","))
            categoryBuffer += ((payCid.toLong, payCid.toLong))
        }
        categoryBuffer
    }
    cid2CidRDD = cid2CidRDD.distinct()

    //第二步:统计品类的点击次数
    val cid2ClickCountRDD: RDD[(Long, Long)] = getClickCount(sessionId2FilterActionRDD)
    //cid2ClickCountRDD.foreach(println(_))

    //第三部;统计品类订单数
    val cid2OrderCountRDD = getOrderCount(sessionId2FilterActionRDD)

    //第四部:统计品类下单数
    val cid2PayCountRDD = getPayCount(sessionId2FilterActionRDD)

    val cid2FullCountRDD: RDD[(Long, String)] = getFullCount(cid2CidRDD, cid2ClickCountRDD, cid2OrderCountRDD, cid2PayCountRDD)
    //实现二次排序
    val sortKey2CountInfoRDD: RDD[(SortKey, String)] = cid2FullCountRDD.map {
      case (cid, countInfo) =>
        val clickCount: Long = StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_CLICK_COUNT).toLong
        val orderCount: Long = StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_ORDER_COUNT).toLong
        val payCount: Long = StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_PAY_COUNT).toLong

        val sortKey = SortKey(clickCount, orderCount, payCount)
        (sortKey, countInfo)
    }
    val top10CategorayArray: Array[(SortKey, String)] = sortKey2CountInfoRDD.sortByKey(false).take(10)
    val top10CategoryRDD: RDD[Top10Category] = sc.sparkContext.makeRDD(top10CategorayArray)
      .map {
        case (sortKey, countInfo) =>
          val cid = StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_CATEGORY_ID).toLong
          val clickCount: Long = sortKey.clickCount
          val orderCount: Long = sortKey.orderCount
          val payCount: Long = sortKey.payCount

          Top10Category(taskUUID, cid, clickCount, orderCount, payCount)
      }


    import sc.implicits._

    top10CategoryRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable", "session_top10_0912")
      .mode(SaveMode.Append)
      .save()

    top10CategorayArray
  }

  def top10ActiveSession(sc: SparkSession, taskUUID: String, sessionId2FilterActionRDD: RDD[(String, UserVisitAction)], top10CategoryArray: Array[(SortKey, String)] ): Unit = {
    //第一种方法
//    val cid2CountInfoRDD: RDD[(Long, String)] = sc.sparkContext.makeRDD(top10CategoryArray).map {
//      case (sortKey, countInfo) =>
//        val cid: Long = StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_CATEGORY_ID).toLong
//        (cid, countInfo)
//    }
//    val cid2ActionRDD: RDD[(Long, UserVisitAction)] = sessionId2FilterActionRDD.map {
//      case (sessionId, action) =>
//        val cid: Long = action.click_category_id
//        (cid, action)
//    }
//
//    cid2CountInfoRDD.join(cid2ActionRDD).map{
//      case (cid,(countInfo,action)) =>
//        val sid: String = action.session_id
//        (sid,action)
//    }

    //第二种方法
    val cidArray: Array[Long] = top10CategoryArray.map {
      case (sortKey, countInfo) =>
        val cid: Long = StringUtils.getFieldFromConcatString(countInfo, "\\|", Constants.FIELD_CATEGORY_ID).toLong
        cid
    }
    //所有点击过top10热门品类的action
    val sessionId2ActionRDD: RDD[(String, UserVisitAction)] = sessionId2FilterActionRDD.filter {
      case (sessionId, action) =>
        cidArray.contains(action.click_category_id)
    }
    val sessionId2GroupRDD: RDD[(String, Iterable[UserVisitAction])] = sessionId2ActionRDD.groupByKey()
    val cid2SessionIdCountRDD: RDD[(Long, String)] = sessionId2GroupRDD.flatMap {
      case (sessionId, iterableAction) =>
        val categoryCountMap: mutable.HashMap[Long, Long] = new mutable.HashMap[Long, Long]()

        for (action <- iterableAction) {
          val cid: Long = action.click_category_id
          if (!categoryCountMap.contains(cid))
            categoryCountMap += (cid -> 0)
          categoryCountMap.update(cid, categoryCountMap(cid) + 1)
        }

        //记录了一个session对于他所有点击过的品类次数
        for ((cid, count) <- categoryCountMap)
          yield (cid, sessionId + "=" + count)
    }
    val cid2GroupRDD: RDD[(Long, Iterable[String])] = cid2SessionIdCountRDD.groupByKey()
    val top10SessionRDD: RDD[Top10Session] = cid2GroupRDD.flatMap {
      case (cid, iterableSessionCount) =>
        val sortList: List[String] = iterableSessionCount.toList.sortWith((item1, item2) => {
          item1.split("=")(1).toLong > item2.split("=")(1).toLong
        }).take(10)
        sortList.map {
          case item =>
            val sessionId: String = item.split("=")(0)
            val count: Long = item.split("=")(1).toLong
            Top10Session(taskUUID, cid, sessionId, count)

        }
    }

    import sc.implicits._
    top10SessionRDD.toDF().write
      .format("jdbc")
      .option("url", ConfigurationManager.config.getString(Constants.JDBC_URL))
      .option("user", ConfigurationManager.config.getString(Constants.JDBC_USER))
      .option("password", ConfigurationManager.config.getString(Constants.JDBC_PASSWORD))
      .option("dbtable", "session_top10session_0912")
      .mode(SaveMode.Append)
      .save()
  }

  def main(args: Array[String]): Unit = {
    val jsonStr: String = ConfigurationManager.config.getString(Constants.TASK_PARAMS)

    val taskParam: JSONObject = JSONObject.fromObject(jsonStr)

    val taskUUID: String = UUID.randomUUID().toString

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("session")

    val sc: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    //获得RDD并形成map
    val actionRDD: RDD[UserVisitAction] = getOriActionRDD(sc, taskParam)
    val sessionID2ActionRDD: RDD[(String, UserVisitAction)] = actionRDD.map(item => (item.session_id, item))
    //把相同sessionID的数据,聚合在一条数据里面
    val session2GroupActionRDD: RDD[(String, Iterable[UserVisitAction])] = sessionID2ActionRDD.groupByKey()


    //将user_visit和user_info合并
    val sessionId2FullInfoRDD: RDD[(String, String)] = getSessionFullInfo(sc, session2GroupActionRDD)

    //根据限制条件过滤并累加
    val sessionAccumulator = new SessionAccumulator
    sc.sparkContext.register(sessionAccumulator)
    val sessionId2FiteredRDD: RDD[(String, String)] = getSessionFiteredRDD(taskParam, sessionId2FullInfoRDD, sessionAccumulator)

    //sessionId2FiteredRDD.foreach(println(_))
    //获取session所在范围的百分比,并写入mysql
    getSessionRatio(sc, taskUUID, sessionAccumulator.value)

    //需求二;session随机抽取
    sessionRandomExtract(sc, taskUUID, sessionId2FiteredRDD)

    val sessionId2FilterActionRDD: RDD[(String, UserVisitAction)] = sessionID2ActionRDD.join(sessionId2FiteredRDD).map {
      case (sessionId, (action, fullInfo)) =>
        (sessionId, action)
    }
    //需求三:热门商品top10统计
    val top10CategoryArray = top10PopularCategories(sc, taskUUID, sessionId2FilterActionRDD)
    //需求四:top10热门商品的top10活跃session统计
    top10ActiveSession(sc,taskUUID,sessionId2FilterActionRDD,top10CategoryArray)
  }

}

