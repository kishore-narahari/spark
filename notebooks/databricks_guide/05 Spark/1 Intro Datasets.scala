// Databricks notebook source exported at Fri, 18 Mar 2016 07:07:18 UTC
// MAGIC %md ## Datasets
// MAGIC 
// MAGIC Datasets API, added in Spark 1.6, provides the benefits of RDDs (strong typing, ability to use powerful lambda functions) with the benefits of Spark SQL's
// MAGIC optimized execution engine that leverages project [Tungsten](https://databricks.com/blog/2015/04/28/project-tungsten-bringing-spark-closer-to-bare-metal.html).  A Dataset can be constructed from JVM objects and then manipulated using functional transformations (map, flatMap, filter, etc.) similar to RDD. The benefits is that, unlike RDD, these transformations are now applied on a _structured and strongly typed_ distributed collection that allows Spark to leverage Spark SQL's execution engine for optimization.

// COMMAND ----------

// MAGIC %md Verify that the notebook is attached to a Spark 1.6+ cluster as Datasets API is introduced only from 1.6

// COMMAND ----------

require(sc.version.replace(".", "").substring(0,3).toInt >= 160, "Spark 1.6.0+ is required to run this notebook. Please attach it to a Spark 1.6.0+ cluster.")

// COMMAND ----------

// MAGIC %md ### Creating Datasets
// MAGIC 
// MAGIC **Creating Datasets from a Sequence**
// MAGIC 
// MAGIC You can simply call `.toDS()` on a sequence to convert the sequence to a Dataset.

// COMMAND ----------

val dataset = Seq(1, 2, 3).toDS()
dataset.show()

// COMMAND ----------

// MAGIC %md If you have a sequence of case classes, calling `.toDS()` will provide a dataset with all the necessary fields in the dataset.

// COMMAND ----------

case class Person(name: String, age: Int)
val personDS = Seq(Person("Max", 33), Person("Adam", 32), Person("Muller", 62)).toDS()
personDS.show()

// COMMAND ----------

// MAGIC %md ** Creating Datasets from a RDD **
// MAGIC 
// MAGIC You can call `rdd.toDS()` to convert an RDD into a DataSet.

// COMMAND ----------

val rdd = sc.parallelize(Seq((1, "Spark"), (2, "Databricks")))
val integerDS = rdd.toDS()
integerDS.show()

// COMMAND ----------

// MAGIC %md ** Creating Datasets from a DataFrame **
// MAGIC 
// MAGIC You can call `df.as[SomeCaseClass]` to convert the dataframe to a dataset.

// COMMAND ----------

case class Company(name: String, foundingYear: Int, numEmployees: Int)
val inputSeq = Seq(Company("ABC", 1998, 310), Company("XYZ", 1983, 904), Company("NOP", 2005, 83))
val df = sc.parallelize(inputSeq).toDF()

val companyDS = df.as[Company]
companyDS.show()

// COMMAND ----------

// MAGIC %md You can also deal with tuples while converting a dataframe to dataset without using a case class

// COMMAND ----------

val rdd = sc.parallelize(Seq((1, "Spark"), (2, "Databricks"), (3, "Notebook")))
val df = rdd.toDF("Id", "Name")

val dataset = df.as[(Int, String)]
dataset.show()

// COMMAND ----------

// MAGIC %md ### Working with Datasets
// MAGIC 
// MAGIC ** Word Count Example **

// COMMAND ----------

val wordsDataset = sc.parallelize(Seq("Spark I am your father", "May the spark be with you", "Spark I am your father")).toDS()
val groupedDataset = wordsDataset.flatMap(_ split (" "))
                                 .filter(_ != "")
                                 .groupBy(_.toLowerCase())
val countsDataset = groupedDataset.count()
countsDataset.show()

// COMMAND ----------

// MAGIC %md **Joining in Datasets**
// MAGIC 
// MAGIC The following example demonstrates the following:
// MAGIC * Union multiple datasets
// MAGIC * Doing an inner join on a condition
// MAGIC * Group by a specific column
// MAGIC * Doing a custom aggregation (average) on the grouped dataset
// MAGIC 
// MAGIC The examples uses only Datasets API to demonstrate all the operations available. In reality, using dataframes for doing aggregation would be simpler and faster than doing custom aggregation with `mapGroups`. The next section covers the details of converting datasets to dataframes and using Dataframes API for doing aggregations.

// COMMAND ----------

case class Employee(name: String, age: Int, departmentId: Int, salary: Double)
case class Department(id: Int, name: String)

case class Record(name: String, age: Int, salary: Double, departmentId: Int, departmentName: String)
case class ResultSet(departmentId: Int, departmentName: String, avgSalary: Double)

val employeeDataSet1 = sc.parallelize(Seq(Employee("Max", 22, 1, 100000.0), Employee("Adam", 33, 2, 93000.0), Employee("Eve", 35, 2, 89999.0), Employee("Muller", 39, 3, 120000.0))).toDS()
val employeeDataSet2 = sc.parallelize(Seq(Employee("John", 26, 1, 990000.0), Employee("Joe", 38, 3, 115000.0))).toDS()
val departmentDataSet = sc.parallelize(Seq(Department(1, "Engineering"), Department(2, "Marketing"), Department(3, "Sales"))).toDS()

val employeeDataset = employeeDataSet1.union(employeeDataSet2)

def averageSalary(key: (Int, String), iterator: Iterator[Record]): ResultSet = {
  val (total, count) = iterator.foldLeft(0.0, 0.0) {
      case ((total, count), x) => (total + x.salary, count + 1)
  }
  ResultSet(key._1, key._2, total/count)
}

val averageSalaryDataset = employeeDataset.joinWith(departmentDataSet, $"departmentId" === $"id", "inner")
                                          .map(record => Record(record._1.name, record._1.age, record._1.salary, record._1.departmentId, record._2.name))
                                          .filter(record => record.age > 25)
                                          .groupBy(record => (record.departmentId, record.departmentName))
                                          .mapGroups((key, iter) => averageSalary(key, iter))
averageSalaryDataset.show()

// COMMAND ----------

// MAGIC %md ### Converting Datasets to Dataframes
// MAGIC 
// MAGIC The above 2 examples dealt with using pure Datasets APIs. You can also easily move from Datasets to Dataframes and leverage the Dataframes APIs. The below example shows the word count example that uses both Datasets and Dataframes APIs.

// COMMAND ----------

import org.apache.spark.sql.functions._

val wordsDataset = sc.parallelize(Seq("Spark I am your father", "May the spark be with you", "Spark I am your father")).toDS()
val result = wordsDataset
              .flatMap(_.split(" "))               // Split on whitespace
              .filter(_ != "")                     // Filter empty words
              .map(_.toLowerCase())
              .toDF()                              // Convert to DataFrame to perform aggregation / sorting
              .groupBy($"value")                   // Count number of occurences of each word
              .agg(count("*") as "numOccurances")
              .orderBy($"numOccurances" desc)      // Show most common words first
result.show()
