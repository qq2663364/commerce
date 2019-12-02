import java.util.Date
import java.{io, lang}

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.utils.DateUtils
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.expressions.Minute
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Duration, Minutes, Seconds, StreamingContext}

import scala.collection.mutable.ArrayBuffer

/**
  * @Autor sc
  * @DATE 0001 15:22
  */
object AdverStat {


  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("adver")
    val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
    //时间间隔根据数据量设置
    val streamingContext = new StreamingContext(sparkSession.sparkContext, Seconds(5))
    //获取kafka集群的参数
    val kafka_brokers: String = ConfigurationManager.config.getString(Constants.KAFKA_BROKERS)
    val kafka_topics: String = ConfigurationManager.config.getString(Constants.KAFKA_TOPICS)
    // kafka消费者配置
    val kafkaParam = Map(
      "bootstrap.servers" -> kafka_brokers, //用于初始化链接到集群的地址
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      //用于标识这个消费者属于哪个消费团体
      "group.id" -> "group1",
      //如果没有初始化偏移量或者当前的偏移量不存在任何服务器上，可以使用这个配置属性
      //可以使用这个配置，latest自动重置偏移量为最新的偏移量
      "auto.offset.reset" -> "latest",
      //如果是true，则这个消费者的偏移量会在后台自动提交
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    // 创建DStream，返回接收到的输入数据
    // LocationStrategies：根据给定的主题和集群地址创建consumer
    // LocationStrategies.PreferConsistent：持续的在所有Executor之间分配分区
    // ConsumerStrategies：选择如何在Driver和Executor上创建和配置Kafka Consumer
    // ConsumerStrategies.Subscribe：订阅一系列主题
    val adRealTimeDStream = KafkaUtils.createDirectStream[String, String](streamingContext,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Array(kafka_topics), kafkaParam)
    )

    val adRealTimeValueDStream = adRealTimeDStream.map(item => item.value())

    val adRealTimeFilterDStream: DStream[String] = adRealTimeValueDStream.transform {
      logRDD =>

        val blackListArray: Array[AdBlacklist] = AdBlacklistDAO.findAll()

        val userIdArray: Array[Long] = blackListArray.map(item => item.userid)

        logRDD.filter {
          case log =>
            val logSplit = log.split(" ")
            val userId = logSplit(3).toLong
            !userIdArray.contains(userId)
        }
    }
    streamingContext.checkpoint("./spark-streaming")

    adRealTimeFilterDStream.checkpoint(Duration(10000))

    //需求一:实时维护黑名单
    generateBlackList(adRealTimeFilterDStream)

    //需求二:各省各城市一天中的广告点击量(累计统计)
    val key2ProvinceCityCountDStream: DStream[(String, Long)] = provinceCityClickStat(adRealTimeFilterDStream)

    //需求三:统计各省top3热门广告
    provinceTop3Adver(sparkSession, key2ProvinceCityCountDStream)

    //需求四:最近一个小时广告点击量统计
    getRecentHourClickCount(adRealTimeFilterDStream)

    streamingContext.start()
    streamingContext.awaitTermination()
  }

  def generateBlackList(adRealTimeFilterDStream: DStream[String]) = {
    val key2NumDStream = adRealTimeFilterDStream.map {
      case log => {
        val logSplit: Array[String] = log.split(" ")
        val timeStamp: Long = logSplit(0).toLong
        //yyyyMMdd
        val dateKey: String = DateUtils.formatDateKey(new Date(timeStamp))

        val userId = logSplit(3)
        val adid: String = logSplit(4)

        val key = dateKey + "_" + userId + "_" + adid

        (key, 1L)

      }

    }
    val key2CountDStream: DStream[(String, Long)] = key2NumDStream.reduceByKey(_ + _)

    key2CountDStream.foreachRDD {
      rdd =>
        rdd.foreachPartition {
          items =>
            val clickCountArray = new ArrayBuffer[AdUserClickCount]()

            for ((key, count) <- items) {
              val keySplit = key.split("_")
              val date = keySplit(0)
              val userId = keySplit(1).toLong
              val adid = keySplit(2).toLong

              clickCountArray += AdUserClickCount(date, userId, adid, count)
            }
            AdUserClickCountDAO.updateBatch(clickCountArray.toArray)
        }
    }
    val key2BlackListDStream = key2CountDStream.filter {
      case (key, count) =>
        val keySplit = key.split("_")
        val date = keySplit(0)
        val userId = keySplit(1).toLong
        val adid = keySplit(2).toLong

        val clickCount: Int = AdUserClickCountDAO.findClickCountByMultiKey(date, userId, adid)

        if (clickCount > 100) {
          true
        } else {
          false
        }
    }
    val userIdDStream = key2BlackListDStream.map {
      case (key, count) => key.split("_")(1).toLong
    }.transform(rdd => rdd.distinct())

    userIdDStream.foreachRDD {
      rdd =>
        rdd.foreachPartition {
          items =>
            val userIdArray = new ArrayBuffer[AdBlacklist]()

            for (userId <- items) {
              userIdArray += AdBlacklist(userId)
            }

            AdBlacklistDAO.insertBatch(userIdArray.toArray)
        }
    }

  }

  def provinceCityClickStat(adRealTimeFilterDStream: DStream[String]) = {
    val key2ProvinceCityDStream = adRealTimeFilterDStream.map {
      case log =>
        val logSplit: Array[String] = log.split(" ")
        val timeStamp: Long = logSplit(0).toLong
        val dateKey: String = DateUtils.formatDateKey(new Date(timeStamp))

        val province: String = logSplit(1)
        val city: String = logSplit(2)
        val adid: String = logSplit(4)


        val key = dateKey + "_" + province + "_" + city + "_" + adid
        (key, 1L)
    }
    // 在这个dstream中，就相当于，有每个batch rdd累加的各个key（各天各省份各城市各广告的点击次数）
    // 每次计算出最新的值，就在aggregatedDStream中的每个batch rdd中反应出来
    val key2StateDStream = key2ProvinceCityDStream.updateStateByKey[Long] {
      (values: Seq[Long], state: Option[Long]) =>
        // 举例来说
        // 对于每个key，都会调用一次这个方法
        // 比如key是<20151201_Jiangsu_Nanjing_10001,1>，就会来调用一次这个方法7
        // 10个
        // values，(1,1,1,1,1,1,1,1,1,1)
        // 首先根据optional判断，之前这个key，是否有对应的状态
        var newValue = 0L
        if (state.isDefined)
          newValue = state.get
        for (value <- values) {
          newValue += value
        }
        Some(newValue)
    }

    key2StateDStream.foreachRDD {
      rdd =>
        rdd.foreachPartition {
          items =>
            val adStatArray = new ArrayBuffer[AdStat]()
            for ((key, count) <- items) {
              val keySplit = key.split("_")
              val date = keySplit(0)
              val province = keySplit(1)
              val city = keySplit(2)
              val adid = keySplit(3).toLong

              adStatArray += AdStat(date, province, city, adid, count)
            }
            AdStatDAO.updateBatch(adStatArray.toArray)
        }
    }
    key2StateDStream
  }

  def provinceTop3Adver(sparkSession: SparkSession, key2ProvinceCityCountDStream: DStream[(String, Long)]) = {

    val key2ProvinceCountDStream: DStream[(String, Long)] = key2ProvinceCityCountDStream.map {
      case (key, count) => {
        val keySplit: Array[String] = key.split("_")
        val date: String = keySplit(0)
        val province: String = keySplit(1)
        val adid: String = keySplit(3)

        val newKey = date + "_" + province + "_" + adid

        (newKey, count)
      }
    }
    val key2ProvinceAggrCountDStream: DStream[(String, Long)] = key2ProvinceCountDStream.reduceByKey(_ + _)

    val top3DStream: DStream[Row] = key2ProvinceAggrCountDStream.transform {
      rdd =>
        //rdd:RDD[(key,count)]
        val basicDateRDD = rdd.map {
          case (key, count) => {
            val keySplit: Array[String] = key.split("_")
            val date: String = keySplit(0)
            val province: String = keySplit(1)
            val adid = keySplit(2).toLong
            (date, province, adid, count)

          }
        }
        import sparkSession.implicits._
        basicDateRDD.toDF("date", "province", "adid", "count").createOrReplaceTempView("tmp_basic_info")

        val sql = "select date,province,adid,count from(" +
          "select date,province,adid,count," +
          "row_number() over(partition by date,province order by count desc) rank from tmp_basic_info) t " +
          "where rank <= 3"

        sparkSession.sql(sql).rdd
    }
    top3DStream.foreachRDD{
      rdd => rdd.foreachPartition{
        items =>
          val top3Array = new ArrayBuffer[AdProvinceTop3]()
          for(item <- items){
            val date = item.getAs[String]("date")
            val province = item.getAs[String]("province")
            val adid = item.getAs[Long]("adid")
            val count = item.getAs[Long]("count")
            top3Array += AdProvinceTop3(date,province,adid,count)
          }
          AdProvinceTop3DAO.updateBatch(top3Array.toArray)
      }

    }
  }

  def getRecentHourClickCount(adRealTimeFilterDStream: DStream[String]) = {
    val key2TimeMinuteDStream: DStream[(String, Long)] = adRealTimeFilterDStream.map {
      case log =>
        val logSplit = log.split(" ")
        val timeStamp: Long = logSplit(0).toLong

        val timeMinute = DateUtils.formatTimeMinute(new Date(timeStamp))

        val adid = logSplit(4).toLong

        val key: String = timeMinute + "_" + adid

        (key, 1L)
    }
    val key2WindowDStream: DStream[(String, Long)] = key2TimeMinuteDStream.reduceByKeyAndWindow((a:Long,b:Long)=>(a+b),Minutes(60),Minutes(1))


    key2WindowDStream.foreachRDD{
      rdd => rdd.foreachPartition({
        items =>
          val trendArray = new ArrayBuffer[AdClickTrend]()

          for ((key,count) <- items){
            val keySplit = key.split("_")

            val timeMinute = keySplit(0)
            val date: String = timeMinute.substring(0,8)
            val hour: String = timeMinute.substring(8,10)
            val minute: String = timeMinute.substring(10)
            val adid = keySplit(1).toLong
            trendArray += AdClickTrend(date,hour,minute,adid,count)
          }
          AdClickTrendDAO.updateBatch(trendArray.toArray)
      })
    }


  }
}
