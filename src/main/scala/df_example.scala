import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Encoders
import org.apache.spark.sql.functions.{approx_count_distinct, col}

object df_example {



  def main(args: Array[String]): Unit = {
    case class emp_schema (EmpID:String, EmpName:String,DepartID:Int , Salary:Double  )
    val spark=SparkSession.builder.config("spark.master","local").appName("DFexamples")
              .enableHiveSupport().getOrCreate()
    import spark.implicits._
    val df_emp=spark.read.option("delimiter",",").csv("file:///home/mymachine/employee.csv")
    val df_dept=spark.read.option("delimiter",",").csv("file:///home/mymachine/department.csv")

    df_emp.show()
    df_dept.show()


    
    var all_cols=Seq("EmpID","EMPName","Dept","Salary")

    var df_emp_new=df_emp.toDF(all_cols:_*)
     df_emp_new.printSchema()
    //val empencoder = Encoders.bean(classOf[emp_schema])
    //val ds_emp=df_emp.select(

    //ds_emp.printSchema()
    //ds_emp.show()

    var df_updt=df_emp_new.select(col("EmpID"),col("Salary").cast("INT"))

    df_updt.printSchema()

    df_updt.show()

    println("approx_count_distinct: "+
      df_updt.select(approx_count_distinct("Salary")).collect()(0)(0))


      df_updt.select(approx_count_distinct("Salary")).collect().foreach(println)

   





  }
}


