import org.postgresql.Driver
import java.sql.{Connection, DriverManager, Statement}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.UserDefinedFunction
import java.net.{URL, URLDecoder}
import scala.util.Try

def get_age_type: UserDefinedFunction = udf((age: Int) => {
    if (age >= 18 && age <= 24) {
        "18-24"
    } else if (age >= 25 && age <= 34) {
        "25-34"
    } else if (age >= 35 && age <= 44) {
        "35-44"
    } else if (age >= 45 && age <= 54) {
        "45-54"
    } else if (age >= 55) {
        ">=55"
    } else {
        ""
    }
})

def decodeUrlAndGetDomain: UserDefinedFunction = udf((url: String) => {
    Try {
      new URL(URLDecoder.decode(url, "UTF-8")).getHost
    }.getOrElse("")
})

val spark: SparkSession = SparkSession.builder()
    .config("spark.cassandra.connection.host", "10.0.0.31")
    .config("spark.cassandra.connection.port", "9042")
    .getOrCreate()


val cats: DataFrame = spark.read
    .format("jdbc")
    .option("url", "jdbc:postgresql://10.0.0.31:5432/labdata")
    .option("dbtable", "domain_cats")
    .option("user", "maksim_strometskiy")
    .option("password", "Wecpp2vQ")
    .option("driver", "org.postgresql.Driver")
    .load()
    .select('domain, concat(lit("web_"), regexp_replace(lower('category), "[ -]", "_")).as("category"))

val visits: DataFrame = spark.read
    .format("org.elasticsearch.spark.sql")
    .options(Map("es.read.metadata" -> "true",
      "es.nodes.wan.only" -> "true",
      "es.nodes" -> "10.0.0.31:9200",
      "es.net.ssl" -> "false"))
    .load("visits")
    .select('uid, concat(lit("shop_"), regexp_replace(lower('category), "[ -]", "_")).as("category"))
    .filter('uid.isNotNull and 'category.isNotNull)
    .groupBy('uid).pivot('category).count()


val clients: DataFrame = spark.read
    .format("org.apache.spark.sql.cassandra")
    .options(Map("table" -> "clients", "keyspace" -> "labdata"))
    .load()
    .filter('age >= 18)
    .select('uid, 'gender, get_age_type('age).as("age_cat"))
    .filter('uid.isNotNull)

val logs: DataFrame = spark.read.json("hdfs:///labs/laba03/weblogs.json")
    .select('uid, explode('visits).as("visit"))
    .select('uid, regexp_replace(decodeUrlAndGetDomain(col("visit.url")), "www.", "").as("domain"))
    .filter('uid.isNotNull and 'domain.isNotNull)

clients.join(
    visits,
    Seq("uid"),
    "left"
).join(
    logs.join(cats, Seq("domain")).groupBy('uid).pivot('category).count(),
    Seq("uid"),
    "left"
).na.fill(0)
    .write
    .format("jdbc")
    .option("url", "jdbc:postgresql://10.0.0.31:5432/maksim_strometskiy") //как в логине к личному кабинету но _ вместо .
    .option("dbtable", "clients")
    .option("user", "maksim_strometskiy")
    .option("password", "Wecpp2vQ")
    .option("createTableOptions", "PRIMARY KEY (uid)")
    .option("driver", "org.postgresql.Driver")
    .option("truncate", value = true) //позволит не терять гранты на таблицу
    .mode("overwrite") //очищает данные в таблице перед записью
    .save()

val driverClass: Class[Driver] = classOf[org.postgresql.Driver]
val driver: Any = Class.forName("org.postgresql.Driver").newInstance()
val url = "jdbc:postgresql://10.0.0.31:5432/maksim_strometskiy?user=maksim_strometskiy&password=Wecpp2vQ"
val connection: Connection = DriverManager.getConnection(url)
val statement: Statement = connection.createStatement()
val bool: Boolean = statement.execute("GRANT SELECT ON clients TO labchecker2")
connection.close()
