package dev.hugoleonardo.conters

import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import dev.hugoleonardo.tests.SparkSessionTestWrapper
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}
import org.scalatest.funspec.AnyFunSpec

class CounterTest
  extends AnyFunSpec
    with DataFrameComparer
    with BeforeAndAfterAll
    with SparkSessionTestWrapper {

  describe("Counter Object") {
    import spark.implicits._

    it("should count words") {
      val dataset = Seq(
        "Writing Business",
        "Business Description Vision"
      ).toDS()

      val actualDF = Counter.wordCount(spark, dataset)

      val expectedDF = Seq(
        ("business", 2),
        ("writing", 1),
        ("description", 1),
        ("vision", 1),
      ).toDF("word", "count")

      assertSmallDataFrameEquality(actualDF, expectedDF)
    }

    it("should count words ignoring punctuation") {
      val dataset = Seq(
        "Writing Business",
        "Business Description Vision"
      ).toDS()

      val actualDF = Counter.wordCount(spark, dataset)

      val expectedDF = Seq(
        ("business", 2),
        ("writing", 1),
        ("description", 1),
        ("vision", 1),
      ).toDF("word", "count")

      assertSmallDataFrameEquality(actualDF, expectedDF)
    }

    it("should count words with more than 5 characters") {
      val dataset = Seq(
        "Writing Your Business Plan",
        "Business Description and Vision"
      ).toDS()

      val actualDF = Counter.wordCount(spark, dataset)

      val expectedDF = Seq(
        ("business", 2),
        ("writing", 1),
        ("description", 1),
        ("vision", 1),
      ).toDF("word", "count")

      assertSmallDataFrameEquality(actualDF, expectedDF)
    }

    it("should count words without stopwords") {
      val dataset = Seq(
        "Writing Your Business Plan",
        "Business Description and Vision",
        "There are several questions you should ask yourself"
      ).toDS()

      val actualDF = Counter.wordCount(spark, dataset)

      val expectedDF = Seq(
        ("business", 2),
        ("writing", 1),
        ("several", 1),
        ("questions", 1),
        ("description", 1),
        ("vision", 1),
      ).toDF("word", "count")

      assertSmallDataFrameEquality(actualDF, expectedDF)
    }
  }

  override def afterAll(): Unit = {
    super.afterAll()

    spark.stop()
  }

}
