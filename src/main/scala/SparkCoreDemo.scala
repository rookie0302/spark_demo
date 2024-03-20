import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ListBuffer

object SparkCoreDemo {

  val log: Logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

    //初始化spark环境
    val sc = SparkUtil.getSession("spark_core_demo", isLocal = true)

    //用户指定sizeNum  Given a size number, for example 3.
    val sizeNum = 7

    //数据转换
    log.info(s"数据转换开始,当前输入num为 : $sizeNum")
    dataBatch(sc, sizeNum)

    //关闭资源
    sc.close()
  }

  /**
   *
   * @param sparkSession 上下文
   * @param sizeNum 用户指定sizeNum
   */
  def dataBatch(sparkSession: SparkSession, sizeNum: Int): Unit = {

    //数据加载  需求要求num=3使用第一份数据,num=5,7使用第二份数据
    val dataRdd = loadData(sparkSession, sizeNum)
    val resRdd = dataRdd.groupBy(_.getString(0))
      .flatMap {
        case (peerId, dataIter) =>
          val buffer = new ListBuffer[Row]
          var flag = true
          //1.	For each peer_id, get the year when peer_id contains id_2
          val headYear = dataIter.filter(f => peerId.contains(f.getString(2))).head.getInt(3)
          val filterRows = dataIter.filter(f => f.getInt(3) <= headYear)
          //Order the value in step 2 by year
          val yearNumSeq = filterRows.groupBy(f => f.getInt(3)).map(f => (f._1, f._2.size)).toSeq.sortBy(_._1)(Ordering[Int].reverse)
          var sum = 0
          yearNumSeq.foreach {
            case (year, num) =>
              sum += num
              if (sum >= sizeNum && flag) {
                // the count number of the first year is bigger or equal than the given size number
                buffer.append(Row(peerId, year))
                flag = false
              } else {
                //plus the count number from the biggest year to next year until the count number is bigger or equal than the given number
                if (flag) {
                  buffer.append(Row(peerId, year))
                }
              }
          }
          buffer
      }

    //定义输出结果schema
    val schema = StructType(Seq(
      StructField("peer_id", StringType, nullable = false),
      StructField("year", IntegerType, nullable = false)
    ))

    sparkSession.createDataFrame(resRdd,schema).show()
    log.info(s"===================数据转换完成,当前展示为num = ${sizeNum}测试结果==================")

  }

  /**
   *
   * @param sparkSession 上下文
   * @param num 用户指定sizeNum
   * @return 加载数据RDD[Row]
   */
  def loadData(sparkSession: SparkSession,num: Int): RDD[Row] = {
    //加载数据
    val tuples = if (num == 3) {
      Seq(
        ("ABC17969(AB)", "1", "ABC17969", 2022),
        ("ABC17969(AB)", "2", "CDC52533", 2022),
        ("ABC17969(AB)", "3", "DEC59161", 2023),
        ("ABC17969(AB)", "4", "F43874", 2022),
        ("ABC17969(AB)", "5", "MY06154", 2021),
        ("ABC17969(AB)", "6", "MY4387", 2022),
        ("AE686(AE)", "7", "AE686", 2023),
        ("AE686(AE)", "8", "BH2740", 2021),
        ("AE686(AE)", "9", "EG999", 2021),
        ("AE686(AE)", "10", "AE0908", 2021),
        ("AE686(AE)", "11", "QA402", 2022),
        ("AE686(AE)", "12", "OM691", 2022)
      )
    } else if (Array(5, 7).contains(num)) {
      Seq(
        ("AE686(AE)", "7", "AE686", 2022),
        ("AE686(AE)", "8", "BH2740", 2021),
        ("AE686(AE)", "9", "EG999", 2021),
        ("AE686(AE)", "10", "AE0908", 2023),
        ("AE686(AE)", "11", "QA402", 2022),
        ("AE686(AE)", "12", "OA691", 2022),
        ("AE686(AE)", "12", "OB691", 2022),
        ("AE686(AE)", "12", "OC691", 2019),
        ("AE686(AE)", "12", "OD691", 2017)
      )
    } else {
      null
    }
    //初始化数据
    sparkSession.sparkContext.parallelize(tuples.map(Row.fromTuple))
  }
}
