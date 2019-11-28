import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

/**
  * @Autor sc
  * @DATE 2019/9/11 10:33
  */
class SessionAccumulator extends AccumulatorV2[String, mutable.HashMap[String, Int]] {
  private val countMap = new mutable.HashMap[String, Int]()

  override def isZero: Boolean = {
    countMap.isEmpty
  }

  override def copy(): AccumulatorV2[String, mutable.HashMap[String, Int]] = {
    val acc = new SessionAccumulator
    acc.countMap ++= this.countMap
    acc
  }

  override def reset(): Unit = {
    countMap.clear()
  }

  override def add(v: String): Unit = {
    if (!countMap.contains(v)) {
      this.countMap += (v -> 0)
    }
    this.countMap.update(v, countMap(v) + 1)
  }

  override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Int]]): Unit = {
    other match {
      case acc: SessionAccumulator => acc.countMap.foldLeft(this.countMap) {
        //getOrElse就是如果存在k的值为原来的value值,如果不存在则k的value为0
        //分区进行聚合
        //map代表this.countMap分区,(k, v)代表两一个分区,然后把(k, v)的值进行往map里追加
        case (map, (k, v)) => map += (k -> (map.getOrElse(k, 0) + v))
      }
    }
  }


  override def value: mutable.HashMap[String, Int] = {
    this.countMap
  }
}
