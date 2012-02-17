package org.ea

import scala.io._
import scala.collection.mutable.ArrayBuffer

class Statement {
  var name = ""
  var sqltype = ""
  var notNullable = false
  var default = ""
  var autoincrement = false
  var sqlStr = ""
  var havePrimeKey = false
    
  override def toString :String = {
    if(sqlStr == "") {
    	val typeString = if(autoincrement && sqltype.contains("int")) "integer primary key autoincrement" else sqltype
    	val nullString = if(notNullable) " NOT NULL" else ""
    	val defaultString = if(default != "") " DEFAULT "+default else ""
    	name + typeString + nullString + defaultString 
    } else {
    	sqlStr
    }
  }
}

object MySql2Sqlite extends Application {
//  val path = "/home/woden/sqlite/";
  val path = "c:/temp/sqltest/";
//  val s = Source.fromFile(path+"dump2.sql", "UTF-8")
//  val s = Source.fromFile(path+"dump3.sql")
  val s = Source.fromFile(path+"dump3.sql", "latin1")
  var createArr = new Array[String](3)
  var createStatArr = new ArrayBuffer[Statement]
  var createStatement = false  
  var insertStatement = false  
  val out = new java.io.FileWriter(path+"createtables.sql")
  var dataout = new java.io.FileWriter(path+"update.sql")
  var outfile = 1;
  var filenr = 1;
  var table = ""
  var havePrime = false

  s.getLines.foreach( (line) => {
    
    if(line.trim.toLowerCase.startsWith("insert")) {
      insertStatement = true      
    }
    
    if(insertStatement) {
      val i = line.indexOf(" VALUES (")+8;
      val insStatement = line.substring(0, i);
      
      val changeEscapes = line.replaceAll("\\\\'", "''").replaceAll("\\\\''", "\\'");
      
      val arrValues = changeEscapes.substring(i+1).split("\\),\\(");
      for(value <- arrValues) {
    	  dataout.write(insStatement+"("+(if(value.endsWith(");")) value.replace(");", "") else value)+");\n");       
      }      
      dataout.flush();
      
      if(outfile == 100) {
        dataout.close();
        dataout = new java.io.FileWriter(path+"update"+filenr+".sql");
        outfile = 1;
        filenr = filenr + 1;
      }
      outfile = outfile + 1;
    }    

    if(line.contains(");")) {
      insertStatement = false      
    }               
    
    if(line.trim.toLowerCase.startsWith("create")) {
      createStatement = true
//      createArr = line.split("`")
      createArr = line.split("\"")
      if(createArr.length > 1) {
         table = createArr.apply(1)
         //out.write("drop table if exists '"+table+"';\n")
         out.write("create table "+table+" (\n")
      }      
    }
    
    if(createStatement && line.trim.startsWith(")")) {
      createStatement = false
      var first = true
      var havePrimeKey = false
      createStatArr.foreach( (stat) => {
         if(!first)
           out.write(",\n")
         out.write("\t"+stat.toString())
         first = false
      })            
      out.write("\n);\n\n")
      createStatArr = new ArrayBuffer[Statement]
      havePrime = false
    }
    
    if(createStatement) {
      if(!(line.trim.toLowerCase.startsWith("create")  || 
          line.trim.toLowerCase.startsWith("primary key") || 
          line.trim.toLowerCase.startsWith("unique key") ||  
          line.trim.toLowerCase.startsWith("key") || 
          line.trim.toLowerCase.startsWith("constraint"))
          ) {
        
         createArr = line.trim.split(' ')
         var stat = new Statement
                  
         if(createArr.length > 1) {
            stat.name = createArr.apply(0)
            var linestr = createArr.apply(1)
            if(linestr.endsWith(","))
               linestr = linestr.substring(0, linestr.length()-1);
            stat.sqltype = linestr
              
            if(createArr.apply(1).startsWith("enum"))
               stat.sqltype = "varchar(9)"
            if(line.indexOf("NOT NULL") != -1)
              stat.notNullable = true
            if(line.toLowerCase().indexOf("auto_increment") != -1) {              
              stat.autoincrement = true
              havePrime = true
            }
                            
            for(i <- 0 until createArr.length) {                 
              if(createArr.apply(i).compareToIgnoreCase("default") == 0) {
                var linestr = createArr.apply(i+1)
                if(linestr.endsWith(","))
                   linestr = linestr.substring(0, linestr.length()-1);
                if(linestr.compareTo("CURRENT_TIMESTAMP") == 0)
                   linestr = "'0000-00-00 00:00:00'"
                if(linestr.compareTo("'0000-00-00") == 0)
                   linestr = "'0000-00-00 00:00:00'"
                stat.default = linestr
              }
            }
            createStatArr += stat         
         }        
      }
      
      if(
          line.trim.toLowerCase.startsWith("primary key") || 
          line.trim.toLowerCase.startsWith("constraint")
          ) {
         var stat = new Statement
         var linestr = line.trim

         stat.sqlStr = linestr
         if(linestr.endsWith(",")) {
            stat.sqlStr = linestr.substring(0, linestr.length()-1);
         }
         if(!havePrime || linestr.indexOf("PRIMARY") == -1)
            createStatArr += stat                          
      }
    }
        
  })

  out.close  
}