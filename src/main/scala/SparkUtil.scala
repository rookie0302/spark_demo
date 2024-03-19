
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory


object SparkUtil {
  private val log = LoggerFactory.getLogger("SparkUtil")

  def getSession(appName: String, isLocal: Boolean):SparkSession={
    log.info(s"初始化sparkSession开始,appName: [$appName], isLocal: [$isLocal]")
    val sparkSession = if (isLocal) {
      val cpuProcessorCount = Runtime.getRuntime.availableProcessors()
      val sparkProcessorCount = cpuProcessorCount - 1
      log.info(s"本地模式，当前服务器CPU核数[$cpuProcessorCount],spark分配cpu核数[$sparkProcessorCount]")
      SparkSession.builder()
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .appName(appName)
        .master(s"local[$sparkProcessorCount]")
        .getOrCreate()
    }else {
      SparkSession.builder()

        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .appName(appName)
        .getOrCreate()
    }
    log.info(s"初始化sparkSession结束,appName: [$appName], isLocal: [$isLocal]")
    sparkSession
  }


}
