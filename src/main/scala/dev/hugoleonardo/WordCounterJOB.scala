package dev.hugoleonardo

import dev.hugoleonardo.conters.Counter
import org.apache.spark.sql.SparkSession

object WordCounterJOB {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName(this.getClass.getSimpleName)
      .master("local")
      .getOrCreate()

    val bookDataset = spark.read.textFile("contrib/book.txt")

    val wordCountDF = Counter.wordCount(spark, bookDataset)

    import spark.implicits._

    wordCountDF.orderBy($"count".desc).limit(20).collect.foreach(println)

    spark.stop()
  }
}
