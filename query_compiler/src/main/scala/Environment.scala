package DunceCap

import net.liftweb.json._

import scala.io._

import net.liftweb.json.Serialization.{read, write, writePretty}
import scala.collection.mutable.Stack

/*
  Stores information about what is in the database and 
  current configuration of the database. This is necessary
  for code generation information.
*/

case class RelationNotFoundException(what:String)  extends Exception

object Environment {
  var config:DatabaseConfig = null
  var schemaStack:Stack[Map[String, Schema]] = Stack[Map[String, Schema]]()

  def fromJSON(filename:String) = {
    val fileContents = Source.fromFile(filename).getLines.mkString
    implicit val formats = DefaultFormats
    config = parse(fileContents).extract[DatabaseConfig]
    schemaStack.push(config.schemas)
  }
  def toJSON() = {
    val filename = config.database+"/config.json"
    implicit val formats = Serialization.formats(NoTypeHints)
    scala.tools.nsc.io.File(filename).writeAll(writePretty(config))
  }


  /**
   * @param queryRelation find the query relation with this name and attrs and set it's annotation as in the config
   * @return boolean indicates whether this was successful (may not be if queryRelation does not exist)
   */
  def setAnnotationAccordingToConfig(queryRelation: QueryRelation): Boolean = {
    val schema = schemaStack.find(schema => schema.get(queryRelation.name)
      .find(schema => schema.attributes.length == queryRelation.attrs.length).isDefined)
    schema.map(schema => queryRelation.annotationType = schema.get(queryRelation.name).get.annotation)
    return schema.isDefined
  }

  def startScope(): Unit = {
    schemaStack.push(Map[String, Schema]())
  }

  def endScope(): Unit = {
    schemaStack.pop()
  }
}
