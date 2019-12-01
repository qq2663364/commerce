import java.util.Date
import java.{io, lang}

import commons.conf.ConfigurationManager
import commons.constant.Constants
import commons.utils.DateUtils
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
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
    val streamingContext = new StreamingContext(sparkSession.sparkContext,Seconds(5))
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
    //需求一:实时维护黑名单

    generateBlackList(adRealTimeFilterDStream)

    streamingContext.start()
    streamingContext.awaitTermination()
  }
  def generateBlackList(adRealTimeFilterDStream: DStream[String]) = {
    val key2NumDStream = adRealTimeFilterDStream.map{
      case log=>{
        val logSplit: Array[String] = log.split(" ")
        val timeStamp: Long = logSplit(0).toLong
        //yyyyMMdd
        val dateKey: String = DateUtils.formatDateKey(new Date(timeStamp))

        val userId = logSplit(3)
        val adid: String = logSplit(4)

        val key = dateKey +"_" + userId + "_" + adid

        (key,1L)

      }

    }
    val key2CountDStream: DStream[(String, Long)] = key2NumDStream.reduceByKey(_+_)

    key2CountDStream.foreachRDD{
      rdd => rdd.foreachPartition{
        items =>
          val clickCountArray = new ArrayBuffer[AdUserClickCount]()

          for((key,count) <- items){
            val keySplit = key.split("_")
            val date = keySplit(0)
            val userId = keySplit(1).toLong
            val adid = keySplit(2).toLong

            clickCountArray += AdUserClickCount(date,userId,adid,count)
          }
          AdUserClickCountDAO.updateBatch(clickCountArray.toArray)
      }
    }
    val key2BlackListDStream = key2CountDStream.filter{
      case(key,count) =>
        val keySplit = key.split("_")
        val date = keySplit(0)
        val userId = keySplit(1).toLong
        val adid = keySplit(2).toLong

        val clickCount: Int = AdUserClickCountDAO.findClickCountByMultiKey(date,userId,adid)

        if (clickCount > 100){
          true
        }else{
          false
      }
    }
    val userIdDStream = key2BlackListDStream.map{
      case (key,count) => key.split("_")(1).toLong
    }.transform(rdd => rdd.distinct())
    userIdDStream.foreachRDD{
      rdd => rdd.collect()
    }

  }
}
