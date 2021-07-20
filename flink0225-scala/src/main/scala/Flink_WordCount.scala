import org.apache.flink.streaming.api.scala._

object Flink_WordCount {
  def main(args: Array[String]): Unit = {
    //1.获取流的执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    //2.从端口获取数据
    env.socketTextStream("localhost",9999)
      .flatMap(_.split(" "))
      .map((_,1))
      .keyBy(_._1)
      .sum(1)
      .print()

    env.execute()



  }

}
