package com.example

import java.io.{File, IOException}
import java.util.{Locale, TimeZone, UUID}

import org.scalatest.Assertions.fail

import org.apache.spark.network.util.JavaUtils
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.util.{sideBySide, stackTraceToString}
import org.apache.spark.sql.execution.columnar.InMemoryRelation

trait TestUtils {
  // Timezone is fixed to America/Los_Angeles for those timezone sensitive tests (timestamp_*)
  TimeZone.setDefault(TimeZone.getTimeZone("GMT"))
  // Add Locale setting
  Locale.setDefault(Locale.ENGLISH)

  /**
    * Runs the plan and makes sure the answer contains all of the keywords.
    */
  def checkKeywordsExist(df: DataFrame, keywords: String*): Unit = {
    val outputs = df.collect().map(_.mkString).mkString
    for (key <- keywords) {
      assert(outputs.contains(key), s"Failed for $df ($key doesn't exist in result)")
    }
  }

  /**
    * Runs the plan and makes sure the answer does NOT contain any of the keywords.
    */
  def checkKeywordsNotExist(df: DataFrame, keywords: String*): Unit = {
    val outputs = df.collect().map(_.mkString).mkString
    for (key <- keywords) {
      assert(!outputs.contains(key), s"Failed for $df ($key existed in the result)")
    }
  }

  /**
    * Evaluates a dataset to make sure that the result of calling collect matches the given
    * expected answer.
    */
  protected def checkDataset[T](
                                 ds: => Dataset[T],
                                 expectedAnswer: T*): Unit = {
    val result = getResult(ds)

    if (!compare(result.toSeq, expectedAnswer)) {
      fail(
        s"""
           |Decoded objects do not match expected objects:
           |expected: $expectedAnswer
           |actual:   ${result.toSeq}
         """.stripMargin)
    }
  }

  /**
    * Evaluates a dataset to make sure that the result of calling collect matches the given
    * expected answer, after sort.
    */
  protected def checkDatasetUnorderly[T : Ordering](
                                                     ds: => Dataset[T],
                                                     expectedAnswer: T*): Unit = {
    val result = getResult(ds)

    if (!compare(result.toSeq.sorted, expectedAnswer.sorted)) {
      fail(
        s"""
           |Decoded objects do not match expected objects:
           |expected: $expectedAnswer
           |actual:   ${result.toSeq}
         """.stripMargin)
    }
  }

  private def getResult[T](ds: => Dataset[T]): Array[T] = {
    val analyzedDS = try ds catch {
      case ae: AnalysisException =>
        if (ae.plan.isDefined) {
          fail(
            s"""
               |Failed to analyze query: $ae
               |${ae.plan.get}
               |
               |${stackTraceToString(ae)}
             """.stripMargin)
        } else {
          throw ae
        }
    }
    assertEmptyMissingInput(analyzedDS)

    try ds.collect() catch {
      case e: Exception =>
        fail(
          s"""
             |Exception collecting dataset as objects
             |${ds.queryExecution}
           """.stripMargin, e)
    }
  }

  private def compare(obj1: Any, obj2: Any): Boolean = (obj1, obj2) match {
    case (null, null) => true
    case (null, _) => false
    case (_, null) => false
    case (a: Array[_], b: Array[_]) =>
      a.length == b.length && a.zip(b).forall { case (l, r) => compare(l, r)}
    case (a: Iterable[_], b: Iterable[_]) =>
      a.size == b.size && a.zip(b).forall { case (l, r) => compare(l, r)}
    case (a, b) => a == b
  }

  /**
    * Runs the plan and makes sure the answer matches the expected result.
    *
    * @param df the [[DataFrame]] to be executed
    * @param expectedAnswer the expected result in a [[Seq]] of [[Row]]s.
    */
  protected def checkAnswer(df: => DataFrame, expectedAnswer: Seq[Row]): Unit = {
    val analyzedDF = try df catch {
      case ae: AnalysisException =>
        if (ae.plan.isDefined) {
          fail(
            s"""
               |Failed to analyze query: $ae
               |${ae.plan.get}
               |
               |${stackTraceToString(ae)}
               |""".stripMargin)
        } else {
          throw ae
        }
    }

    assertEmptyMissingInput(analyzedDF)

    TestUtils.checkAnswer(analyzedDF, expectedAnswer) match {
      case Some(errorMessage) => fail(errorMessage)
      case None =>
    }
  }

  protected def checkAnswer(df: => DataFrame, expectedAnswer: Row): Unit = {
    checkAnswer(df, Seq(expectedAnswer))
  }

  protected def checkAnswer(df: => DataFrame, expectedAnswer: DataFrame): Unit = {
    checkAnswer(df, expectedAnswer.collect())
  }

  /**
    * Runs the plan and makes sure the answer is within absTol of the expected result.
    *
    * @param dataFrame the [[DataFrame]] to be executed
    * @param expectedAnswer the expected result in a [[Seq]] of [[Row]]s.
    * @param absTol the absolute tolerance between actual and expected answers.
    */
  protected def checkAggregatesWithTol(dataFrame: DataFrame,
                                       expectedAnswer: Seq[Row],
                                       absTol: Double): Unit = {
    // TODO: catch exceptions in data frame execution
    val actualAnswer = dataFrame.collect()
    require(actualAnswer.length == expectedAnswer.length,
      s"actual num rows ${actualAnswer.length} != expected num of rows ${expectedAnswer.length}")

    actualAnswer.zip(expectedAnswer).foreach {
      case (actualRow, expectedRow) =>
        TestUtils.checkAggregatesWithTol(actualRow, expectedRow, absTol)
    }
  }

  protected def checkAggregatesWithTol(dataFrame: DataFrame,
                                       expectedAnswer: Row,
                                       absTol: Double): Unit = {
    checkAggregatesWithTol(dataFrame, Seq(expectedAnswer), absTol)
  }

  /**
    * Asserts that a given [[Dataset]] will be executed using the given number of cached results.
    */
  def assertCached(query: Dataset[_], numCachedTables: Int = 1): Unit = {
    val planWithCaching = query.queryExecution.withCachedData
    val cachedData = planWithCaching collect {
      case cached: InMemoryRelation => cached
    }

    assert(
      cachedData.size == numCachedTables,
      s"Expected query to contain $numCachedTables, but it actually had ${cachedData.size}\n" +
        planWithCaching)
  }

  /**
    * Asserts that a given [[Dataset]] does not have missing inputs in all the analyzed plans.
    */
  def assertEmptyMissingInput(query: Dataset[_]): Unit = {
    assert(query.queryExecution.analyzed.missingInput.isEmpty,
      s"The analyzed logical plan has missing inputs:\n${query.queryExecution.analyzed}")
    assert(query.queryExecution.optimizedPlan.missingInput.isEmpty,
      s"The optimized logical plan has missing inputs:\n${query.queryExecution.optimizedPlan}")
    assert(query.queryExecution.executedPlan.missingInput.isEmpty,
      s"The physical plan has missing inputs:\n${query.queryExecution.executedPlan}")
  }

  /**
    * Returns full path to the given file in the resource folder
    */
  protected def testFile(fileName: String): String = {
    Thread.currentThread().getContextClassLoader.getResource(fileName).toString
  }


  /**
    * Creates a temporary directory, which is then passed to `f` and will be deleted after `f`
    * returns.
    *
    */
  protected def withTempDir(f: File => Unit): Unit = {
    val root: String = System.getProperty("java.io.tmpdir")
    val namePrefix: String = "spark"
    val dir = createDirectory(root, namePrefix).getCanonicalFile
    val hook = new Thread(new Runnable {
      override def run(): Unit = {
        JavaUtils.deleteRecursively(dir)
      }
    })
    Runtime.getRuntime.addShutdownHook(hook)
    try f(dir) finally {
      if (dir != null) {
        JavaUtils.deleteRecursively(dir)
        Runtime.getRuntime.removeShutdownHook(hook)
      }
    }
  }

  /**
    * Create a directory inside the given parent directory. The directory is guaranteed to be
    * newly created, and is not marked for automatic deletion.
    */
  private def createDirectory(root: String, namePrefix: String = "spark"): File = {
    var attempts = 0
    val maxAttempts = 3
    var dir: File = null
    while (dir == null) {
      attempts += 1
      if (attempts > maxAttempts) {
        throw new IOException("Failed to create a temp directory (under " + root + ") after " +
          maxAttempts + " attempts!")
      }
      try {
        dir = new File(root, namePrefix + "-" + UUID.randomUUID.toString)
        if (dir.exists() || !dir.mkdirs()) {
          dir = null
        }
      } catch { case e: SecurityException => dir = null; }
    }

    dir.getCanonicalFile
  }
}

object TestUtils {
  /**
    * Runs the plan and makes sure the answer matches the expected result.
    * If there was exception during the execution or the contents of the DataFrame does not
    * match the expected result, an error message will be returned. Otherwise, a [[None]] will
    * be returned.
    *
    * @param df the [[DataFrame]] to be executed
    * @param expectedAnswer the expected result in a [[Seq]] of [[Row]]s.
    * @param checkToRDD whether to verify deserialization to an RDD. This runs the query twice.
    */
  def checkAnswer(
                   df: DataFrame,
                   expectedAnswer: Seq[Row],
                   checkToRDD: Boolean = true): Option[String] = {
    if (checkToRDD) {
      df.rdd.count()  // Also attempt to deserialize as an RDD [SPARK-15791]
    }

    val sparkAnswer = try df.collect().toSeq catch {
      case e: Exception =>
        val errorMessage =
          s"""
             |Exception thrown while executing query:
             |${df.queryExecution}
             |== Exception ==
             |$e
             |${org.apache.spark.sql.catalyst.util.stackTraceToString(e)}
          """.stripMargin
        return Some(errorMessage)
    }

    sameRows(expectedAnswer, sparkAnswer, false).map { results =>
      s"""
         |Results do not match for query:
         |Timezone: ${TimeZone.getDefault}
         |Timezone Env: ${sys.env.getOrElse("TZ", "")}
         |
        |${df.queryExecution}
         |== Results ==
         |$results
       """.stripMargin
    }
  }


  def prepareAnswer(answer: Seq[Row], isSorted: Boolean): Seq[Row] = {
    // Converts data to types that we can do equality comparison using Scala collections.
    // For BigDecimal type, the Scala type has a better definition of equality test (similar to
    // Java's java.math.BigDecimal.compareTo).
    // For binary arrays, we convert it to Seq to avoid of calling java.util.Arrays.equals for
    // equality test.
    val converted: Seq[Row] = answer.map(prepareRow)
    if (!isSorted) converted.sortBy(_.toString()) else converted
  }

  // We need to call prepareRow recursively to handle schemas with struct types.
  def prepareRow(row: Row): Row = {
    Row.fromSeq(row.toSeq.map {
      case null => null
      case d: java.math.BigDecimal => BigDecimal(d)
      // Equality of WrappedArray differs for AnyVal and AnyRef in Scala 2.12.2+
      case seq: Seq[_] => seq.map {
        case b: java.lang.Byte => b.byteValue
        case s: java.lang.Short => s.shortValue
        case i: java.lang.Integer => i.intValue
        case l: java.lang.Long => l.longValue
        case f: java.lang.Float => f.floatValue
        case d: java.lang.Double => d.doubleValue
        case x => x
      }
      // Convert array to Seq for easy equality check.
      case b: Array[_] => b.toSeq
      case r: Row => prepareRow(r)
      case o => o
    })
  }

  private def genError(
                        expectedAnswer: Seq[Row],
                        sparkAnswer: Seq[Row],
                        isSorted: Boolean = false): String = {
    val getRowType: Option[Row] => String = row =>
      row.map(row =>
        if (row.schema == null) {
          "struct<>"
        } else {
          s"${row.schema.catalogString}"
        }).getOrElse("struct<>")

    s"""
       |== Results ==
       |${
      sideBySide(
        s"== Correct Answer - ${expectedAnswer.size} ==" +:
          getRowType(expectedAnswer.headOption) +:
          prepareAnswer(expectedAnswer, isSorted).map(_.toString()),
        s"== Spark Answer - ${sparkAnswer.size} ==" +:
          getRowType(sparkAnswer.headOption) +:
          prepareAnswer(sparkAnswer, isSorted).map(_.toString())).mkString("\n")
    }
    """.stripMargin
  }

  def includesRows(
                    expectedRows: Seq[Row],
                    sparkAnswer: Seq[Row]): Option[String] = {
    if (!prepareAnswer(expectedRows, true).toSet.subsetOf(prepareAnswer(sparkAnswer, true).toSet)) {
      return Some(genError(expectedRows, sparkAnswer, true))
    }
    None
  }

  def sameRows(
                expectedAnswer: Seq[Row],
                sparkAnswer: Seq[Row],
                isSorted: Boolean = false): Option[String] = {
    if (prepareAnswer(expectedAnswer, isSorted) != prepareAnswer(sparkAnswer, isSorted)) {
      return Some(genError(expectedAnswer, sparkAnswer, isSorted))
    }
    None
  }

  /**
    * Runs the plan and makes sure the answer is within absTol of the expected result.
    *
    * @param actualAnswer the actual result in a [[Row]].
    * @param expectedAnswer the expected result in a[[Row]].
    * @param absTol the absolute tolerance between actual and expected answers.
    */
  protected def checkAggregatesWithTol(actualAnswer: Row, expectedAnswer: Row, absTol: Double) = {
    require(actualAnswer.length == expectedAnswer.length,
      s"actual answer length ${actualAnswer.length} != " +
        s"expected answer length ${expectedAnswer.length}")

    // TODO: support other numeric types besides Double
    // TODO: support struct types?
    actualAnswer.toSeq.zip(expectedAnswer.toSeq).foreach {
      case (actual: Double, expected: Double) =>
        assert(math.abs(actual - expected) < absTol,
          s"actual answer $actual not within $absTol of correct answer $expected")
      case (actual, expected) =>
        assert(actual == expected, s"$actual did not equal $expected")
    }
  }

  def checkAnswer(df: DataFrame, expectedAnswer: java.util.List[Row]): String = {
    import scala.collection.JavaConverters._
    checkAnswer(df, expectedAnswer.asScala) match {
      case Some(errorMessage) => errorMessage
      case None => null
    }
  }
}