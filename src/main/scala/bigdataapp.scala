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

object SpedingWatcherApp {
	def downloadFile(url: String, newfile: String) {
	    val src = scala.io.Source.fromURL(url)("ISO-8859-1")
	    val out = new java.io.FileWriter(newfile)
	    out.write(src.mkString)
	    out.close
	}

	def downloadFile() {
		var out: OutputStream = null;
		var in: InputStream = null;
		val url = new URL( """http://arquivos.portaldatransparencia.gov.br/downloads.asp?a=2017&m=12&consulta=FavorecidosTransferencias""")
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
		val spark = SparkSession.builder().appName("Spark SQL - Exemplo 1").getOrCreate()

		downloadFile()
	
		extractJar(new java.io.File("source.zip"))

		val cnpjdf = spark.read.option("header","true").csv("source/201712_CNPJ.csv")
		val natjdf = spark.read.option("header","true").csv("source/201712_NaturezaJuridica.csv")

		//create properties object
		val prop = new java.util.Properties
		prop.setProperty("driver", "com.mysql.jdbc.Driver")
		prop.setProperty("user", "root")
		prop.setProperty("password", "Juliana2") 
		 
		//jdbc mysql url - destination database is named "data"
		val url = "jdbc:mysql://localhost:3306/bigdata"
		 
		//destination database table 
		val table = "NATJURIDICA"
		 
		//write data from spark dataframe to database
		natjdf.write.mode(SaveMode.Append).jdbc("jdbc:mysql://localhost:3306/bigdata", "NATJURIDICA", prop)

		sc.stop()
	}
}
