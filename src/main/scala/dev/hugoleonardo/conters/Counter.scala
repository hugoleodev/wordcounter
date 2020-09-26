package dev.hugoleonardo.conters

import org.apache.spark.ml.feature.StopWordsRemover
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object Counter {

  private def cleanData(line: String): String = {
    line
      .toLowerCase
      .replaceAll("[,.]"," ")
      .replaceAll("[^a-z0-9\\s-]","")
      .replaceAll("\\s+"," ")
      .trim
  }

  private def tokenize(line: String): List[String] = {
    line.split("\\s").toList
  }

  private def keyValue(word: String): (String, Int) = {
    (word, 1)
  }

  def wordCount(spark: SparkSession, dataset: Dataset[String]): DataFrame = {
    import spark.implicits._

    val stopWords = StopWordsRemover.loadDefaultStopWords("english")

    val words: Dataset[String] = dataset
      .map(line => cleanData(line))
      .flatMap(line => tokenize(line))
      .filter(word => !stopWords.contains(word))
      .filter(_.nonEmpty)
      .filter(word => word.length > 5)

    val wordFrequencies: DataFrame = words
      .map(keyValue)
      .rdd.reduceByKey(_ + _)
      .toDF("word", "count")

    wordFrequencies

  }
}
