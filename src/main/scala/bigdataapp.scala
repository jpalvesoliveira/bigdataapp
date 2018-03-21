import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.io.Source
import collection.JavaConverters._
import java.io.{FileOutputStream, InputStream}
import java.nio.file.Path
import java.util.zip.ZipInputStream
import java.io.ByteArrayInputStream
import java.nio.charset.CodingErrorAction
import java.util.Properties
import java.util.zip.{GZIPInputStream, ZipInputStream}
import javax.activation.DataSource
import javax.mail.Session
import javax.mail.internet.MimeMessage
import scala.collection.JavaConverters._
import java.io._
import java.util.jar._
import java.net._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType};

object SpedingWatcherApp {
	def downloadFile(ano: String, mes: String) {
		var out: OutputStream = null
		var in: InputStream = null
		val url = new URL( "http://arquivos.portaldatransparencia.gov.br/downloads.asp?a=".concat(ano).concat("&m=").concat(mes).concat("&consulta=FavorecidosTransferencias"))
		val connection = url.openConnection().asInstanceOf[HttpURLConnection]
		connection.setRequestMethod("GET")
		in = connection.getInputStream
		val localfile = "source.zip"
		out = new BufferedOutputStream(new FileOutputStream(localfile))
		val byteArray = Stream.continually(in.read).takeWhile(-1 !=).map(_.toByte).toArray
		out.write(byteArray)
	}

	def copyStream(istream : InputStream, ostream : OutputStream) : Unit = {
	  var bytes =  new Array[Byte](1024)
	  var len = -1
	  while({ len = istream.read(bytes, 0, 1024); len != -1 })
	    ostream.write(bytes, 0, len)
	}

	def extractJar(file : File) : Unit = {
	  val basename = file.getName.substring(0, file.getName.lastIndexOf("."))
	  val todir = new File(file.getParentFile, basename)
	  todir.mkdirs()
	      
	  println("Extracting " + file + " to " + todir)
	  val jar = new JarFile(file)
	  val enu = jar.entries
	  while(enu.hasMoreElements){
	    val entry = enu.nextElement
	    val entryPath = 
	      if(entry.getName.startsWith(basename)) entry.getName.substring(basename.length) 
	      else entry.getName
	      
	    println("Extracting to " + todir + "/" + entryPath)
	    if(entry.isDirectory){
	      new File(todir, entryPath).mkdirs
	    }else{
	      val istream = jar.getInputStream(entry)
	      val ostream = new FileOutputStream(new File(todir, entryPath))
	      copyStream(istream, ostream)
	      ostream.close
	      istream.close
	     }
	  }
	}

	def getListOfFiles(dir: String):List[File] = {
	    val d = new File(dir)
	    if (d.exists && d.isDirectory) {
		d.listFiles.filter(_.isFile).toList
	    } else {
		List[File]()
	    }
	}

	def main( args: Array[String] ) {
		val conf = new SparkConf().setAppName("SpedingWatcherApp").setMaster("local[8]")
		val sc = new SparkContext(conf)
		val spark = SparkSession.
				builder().
				appName("Spark SQL - Exemplo 1").
				getOrCreate()

		//--packages mysql:mysql-connector-java:6.0.5 ...
		downloadFile(args(0), args(1))
	
		extractJar(new java.io.File("source.zip"))

		println("Creating schemas")

		val customSchema1 = StructType(Array(
		    StructField("CNPJ", StringType, true),
		    StructField("RAZAOSOCIAL", StringType, true),
		    StructField("FANTASIA", StringType, true),
		    StructField("CNAE", IntegerType, true),
		    StructField("CODIGO", IntegerType, true)))

		val customSchema2 = StructType(Array(
		    StructField("CODIGO", IntegerType, true),
		    StructField("NATUREZA", StringType, true)))

		val cnpjdf = spark.read.format("csv").     // Use "csv" regardless of TSV or CSV.
		                option("header", "true").  // Does the file have a header line?
                		option("delimiter", "\t"). // Set delimiter to tab or comma.
				option("encoding", "ISO-8859-1").
			        schema(customSchema1).
                		load("source/".concat(args(0)).concat(args(1)).concat("_CNPJ.csv"))

		val natjdf = spark.read.format("csv").     // Use "csv" regardless of TSV or CSV.
		                option("header", "true").  // Does the file have a header line?
                		option("delimiter", "\t"). // Set delimiter to tab or comma.
				option("encoding", "ISO-8859-1").
			        schema(customSchema2).
                		load("source/".concat(args(0)).concat(args(1)).concat("_NaturezaJuridica.csv"))

		val df = cnpjdf.join(natjdf, Seq("CODIGO"))
		df.createOrReplaceTempView("TOTAIS")

		var sqlDF = spark.sql("SELECT NATUREZA, Count(*) AS TOTAL FROM TOTAIS GROUP BY NATUREZA")

		println("Storing data")

		//create properties object
		val prop = new java.util.Properties
		prop.setProperty("driver", "com.mysql.jdbc.Driver")
		prop.setProperty("user", "root")
		prop.setProperty("password", "Juliana2") 
		 
		//jdbc mysql url - destination database is named "data"
		val url = "jdbc:mysql://localhost:3306/bigdata"
		 
		//destination database table 
		val table = "TOTAIS"
		 
		//write data from spark dataframe to database
		//df.write.mode(SaveMode.Overwrite).jdbc(url, table, prop)

		sqlDF.write.mode(SaveMode.Overwrite).jdbc(url, table, prop)

		df.printSchema
		df.show(5)

		sqlDF.printSchema
		sqlDF.show(5)

		sc.stop()
	}
}
