
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import scala.collection.mutable.ListBuffer


object App {
  def main(args: Array[String]): Unit = {

    //初始化
    val sc = SparkUtil.getSession("spark_demo", isLocal = true)

    //加载数据
    val data = Seq(
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

    val data2 = Seq(
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

    //定义数据schema
    val schema = StructType(Seq(
      StructField("peer_id", StringType, nullable = false),
      StructField("id_1", StringType, nullable = false),
      StructField("id_2", StringType, nullable = false),
      StructField("year", IntegerType, nullable = false)
    ))

    val rdd = sc.sparkContext.parallelize(data2.map(Row.fromTuple))
    val df = sc.createDataFrame(rdd, schema)

    df.createOrReplaceTempView("tbl_demo_temp")

    //验证数据
    df.show()

    //2.	Given a size number, for example 3.
    val num = 7

    val sql =
      """
        |
        |with tmp1 as (
        |select
        |  peer_id,
        |  year,
        |  if(instr(peer_id,id_2) > 0,1,0) flag
        |from tbl_demo_temp
        |)
        |
        |select
        |  peer_id,
        |  year,
        |  cnt,
        |  sum(cnt) over(partition by peer_id order by year desc) sum_cnt
        |from
        |(
        |select
        |  A.peer_id,
        |  A.year,
        |  count(1) cnt
        |from tbl_demo_temp A
        |join tmp1 B on B.flag = 1 and A.peer_id = B.peer_id and A.year <= B.year
        |group by A.peer_id,A.year
        |) t1
        |""".stripMargin

    sc.sql(sql).show()

    val res = sc.sql(sql).rdd.groupBy(f => f.getString(0))
      .flatMap {
        case (peerId, dataIter) =>
          val buffer = new ListBuffer[Row]
          var flag = true;
          dataIter.foreach {
            data =>
              val sum = data.getAs[java.lang.Long]("sum_cnt")
              if (sum >= num && flag) {
                buffer.append(Row(peerId,data.getAs[Int]("year")))
                flag = false
              } else {
                if (flag) {
                  buffer.append(Row(peerId, data.getAs[Int]("year")))
                }
              }
          }
          buffer
      }

    //定义输出结果schema
    val schema2 = StructType(Seq(
      StructField("peer_id", StringType, nullable = false),
      StructField("year", IntegerType, nullable = false)
    ))

    sc.createDataFrame(res,schema2).show()






  }

}
