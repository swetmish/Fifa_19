import org.apache.spark.sql.{SparkSession, functions}

object Main_Fifa {

  def main(args: Array[String]): Unit = {


    val spark = SparkSession
      .builder().master("local")
      .appName("Spark SQL basic example")
      .config("spark.some.config.option", "some-value")
      .getOrCreate()
    //start - a. Which club has the most number of left footed midfielders under 30 years of age?
    val df = spark.read.format("csv").option("header", "true").load("C://fifa_data//data.csv")
    df.createOrReplaceTempView("data")
    var data1 = spark.sql("select Club, count(Position) as count_pos from data where Club is not null and Position in ('LM','LW') and Age < 30 group by Club")
    data1.createOrReplaceTempView("data1")
    val data2 = spark.sql("select Club, count_pos, rank() over (order by count_pos desc) as rank from data1")
    data2.filter(data2("rank") === 1).select("Club").show()
    //end - a. Which club has the most number of left footed midfielders under 30 years of age


    //start - Which position pays the highest wage in average?

    var data3 = df.withColumn("Wage", functions.regexp_replace(df.col("Wage"), "\\D", ""))
    data3.createOrReplaceTempView("data3")
    var data4 = spark.sql("select Position, avg(Wage) as wg from data3 group by Position")
    var max_avg_wage = data4.groupBy().max("wg")
    data4.filter(data4("wg") === max_avg_wage.first.get(0)).select("Position").show()


       //end - Which position pays the highest wage in average?

    //start - Which team has the most expensive squad value in the world? Does that team also have the largest wage bill ?

    var data_exp = df.withColumn("Value", functions.regexp_replace(df.col("Value"), "\\D", ""))
    import org.apache.spark.sql.types._
    val max_Value = data_exp.withColumn("Value", data_exp("Value").cast(IntegerType)).groupBy().max("Value")
    println("max Value of Club:", max_Value.first.get(0).toString)
    println("Club having max value")
    data_exp.filter(data_exp("Value") === max_Value.first.get(0).toString).select("Club").show()
    var data_wage = df.withColumn("Wage", functions.regexp_replace(df.col("Wage"), "\\D", ""))
    val max_Wage_Value = data_wage.withColumn("Wage", data_wage("Wage").cast(IntegerType)).groupBy().max("Wage")
    println("max Wage of Club:", max_Wage_Value.first.get(0).toString)
    println("Club having max Wage")
    data_wage.filter(data_wage("Wage") === max_Wage_Value.first.get(0).toString).select("Club").show()
    if(max_Wage_Value.first.get(0).toString.equals(max_Value.first.get(0).toString)){
      println("yes the team has also the largest wage bill")
    }else{
      println("No the team does not largest wage bill")
    }

    //end - Which team has the most expensive squad value in the world? Does that team also have the largest wage bill ?


  }
}