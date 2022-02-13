package com.deans

import org.apache.spark.sql.SparkSession


object TokenizerTest {
  def assert(assertion: Boolean) {
    if (!assertion)
      throw new java.lang.AssertionError("assertion failed")
  }

  def main(args: Array[String]): Unit = {
    val dummyData = Seq("Today is a good day", "some sort of sentence")

    val spark:SparkSession = SparkSession.builder()
      .appName("TokenizerTest")
      .getOrCreate()

    import spark.implicits._

    val dummyDF = spark.sparkContext.parallelize(dummyData).toDF

    val processedInSeq: Seq[String] = Tokenizer
      .process(dummyDF, 1, 2, 1, spark)
      .collect()
      .flatMap(row => row.toSeq)
      .map(elem => elem.toString)

    assert(processedInSeq.size == 16)
    assert(processedInSeq.contains("good day"))
    assert(processedInSeq.contains("sort of"))
    assert(processedInSeq.contains("Today"))
    assert(processedInSeq.contains("is"))
    assert(!processedInSeq.contains("a good day"))
  }
}
