package com.persist
import java.io.File
import java.io.BufferedReader
import java.io.FileReader
import java.io.BufferedWriter
import java.io.FileWriter

object Convert {
  
  def isNumber(s:String)= {
    val r = """^[-]?\d*[.]?\d+$""".r
    r.findFirstIn(s) match {
      case Some(s1) => true
      case None => false
     }
  }
  
  var line0 = ""
  def getLine(reader:BufferedReader):String = {
    val line1 = reader.readLine()
    if (line1 == null || line1.startsWith("\"")) {
      val result = line0
      line0 = line1
      result
    } else {
      line0 = line0 + "\\n" + line1
      getLine(reader)
    }
  }

  def main(args: Array[String]): Unit = {
    val name = args(0)
    val in = "/git/ostore/core/config/beer/openbeerdb_csv/" + name + ".csv"
    val out = "/git/ostore/core/config/beer/" + name + ".data"
    val inFile = new File(in)
    val outFile = new File(out)
    val reader = new BufferedReader(new FileReader(inFile))
    val writer = new BufferedWriter(new FileWriter(outFile))
    line0 = reader.readLine()
    val line = getLine(reader)
    val names = line.split(",")
    var fields = List[String]()
    for (name<-names.tail) {
      if (name != "") {
        val name1 = name.replaceAll("\"", "")
        fields = name1 +: fields
      }
    }
    fields = fields.reverse
    println(fields)
    var done = false
    while (! done) {
      val line1 = getLine(reader)
      if (line1 == null) {
        done = true
      } else {
        val line2 = line1.replaceAll(", ","@@@")
        val vals = line2.split(",")
        val sb = new StringBuilder()
        sb.append("{")
        var first = true
        for ((f,v)<-fields.zip(vals.tail)) {
          if (v != "") {
            val v1 = v.replaceAll("@@@",", ").replaceAll("\"","")
            val v2 = if (isNumber(v1)) v1 else "\"" + v1 + "\""
            if (first) first = false else sb.append(",")
            sb.append("\""+f+"\"")
            sb.append(":"+v2)
          }
        }
        sb.append("}")
        var key = vals.head.replaceAll("\"","")
        val key1 = if (isNumber(key)) key else "\"" + key + "\""
        writer.write(key1 +"\t"+sb.toString()+"\n")
      }
    }
    reader.close()
    writer.close()
    println("DONE")
  }

}