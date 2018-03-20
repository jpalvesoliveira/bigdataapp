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

object SpedingWatcherApp {
	def downloadFile(url: String, newfile: String) {
	    val src = scala.io.Source.fromURL(url)("ISO-8859-1")
	    val out = new java.io.FileWriter(newfile)
	    out.write(src.mkString)
	    out.close
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

	def main( args: Array[String] ) {
		val conf = new SparkConf().setAppName("SpedingWatcherApp").setMaster("local[8]")
		val sc = new SparkContext(conf)
		/*val html = scala.io.Source.fromURL("http://arquivos.portaldatransparencia.gov.br/downloads.asp?a=2017&m=12&consulta=FavorecidosTransferencias")("ISO-8859-1").mkString
		println(html)
*/

		downloadFile("http://arquivos.portaldatransparencia.gov.br/downloads.asp?a=2017&m=12&consulta=FavorecidosTransferencias", "source.zip")
	//readFile("source.zip",sc.defaultMinPartitions)
	extractJar(new java.io.File("source.zip"))
	sc.stop()
	}
}
