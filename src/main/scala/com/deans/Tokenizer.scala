package com.deans

import org.apache.spark.sql.{DataFrame, SparkSession}

object Tokenizer {
  def main(args: Array[String]): Unit = {
    // define file path on the system to read from
    val inputFile: String = args(0)
    val numExecutor: Int = args(1).toInt
    val minTokenLens: Int = args(2).toInt
    val maxTokenLens: Int = args(3).toInt

    // create a spark session
    val spark = SparkSession.builder.appName("Tokenizer").getOrCreate()

    import spark.implicits._

    // read source file and repartition
    val sourceData: DataFrame = spark.read.load(inputFile).repartition(numExecutor)

    // for each line, split tokens
    val processedData: DataFrame = process(sourceData, minTokenLens, maxTokenLens, numExecutor, spark)

    // store as output
    processedData.write.format("csv")
      .save("users_with_options.orc")

    spark.stop()
  }

  def split_to_tokens(sourceStr: String, tokenLens: Int): Seq[String] = {
    val allSingleTokens: Array[String] = sourceStr.split(" ")
    (0 until allSingleTokens.length - tokenLens).map(
      startingInd => {
        val endingInd = startingInd + tokenLens
        allSingleTokens.slice(startingInd, endingInd).mkString(" ")
      }
    )
  }

  def split_to_all_token_lens(sourceStr: String, minTokenLens: Int, maxTokenLens: Int): Seq[Seq[String]] = {
    (minTokenLens to maxTokenLens).map(
      tokenLens => {
        split_to_tokens(sourceStr, tokenLens)
      }
    )
  }

  def process(sourceData: DataFrame, minTokenLens: Int, maxTokenLens: Int, numExecutor: Int, spark: SparkSession): DataFrame = {
    import spark.implicits._
    sourceData.flatMap(row => split_to_all_token_lens(row.get(0).toString, minTokenLens, maxTokenLens)).toDF()
      .repartition(numExecutor)
  }
}
