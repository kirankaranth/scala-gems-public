package io.prophecy.scalagems.custom

import io.prophecy.gems._
import io.prophecy.gems.uiSpec._
import io.prophecy.gems.componentSpec._
import io.prophecy.gems.dataTypes._
import io.prophecy.gems.diagnostics._
import org.apache.spark.sql.{DataFrame, SparkSession}
import play.api.libs.json.{Format, Json}

class SQLStatement extends ComponentSpec {
  val name: String = "SQLStatement"
  val category: String = "Custom"
  val gemDescription: String =
    "Creates one or more DataFrame(s) based on provided SQL queries to run against one or more input DataFrame(s)."
  val docUrl: String = "https://docs.prophecy.io/low-code-spark/gems/custom/sql-statement"

  type PropertiesType = SQLStatementProperties
  override def optimizeCode: Boolean = true

  case class SQLStatementProperties(
    @Property("Files", "")
    fileTabs: List[FileTab] = Nil,
    @Property("Input Port Names", "")
    inputPortNames: List[String] = Nil
  ) extends ComponentProperties

  @Property("Helper")
  case class FileTab(
    @Property("path", "") path: String,
    @Property("id", "") id: String,
    @Property("language", "") language: String = "sql",
    @Property("content", "") content: SString
  )

  implicit val fileTabFormat: Format[FileTab] = Json.format
  implicit val SQLStatementPropertiesFormat: Format[SQLStatementProperties] = Json.format

  def dialog: Dialog = {
    val filePlaceholder = "select * from in0"
    Dialog("SQLStatement")
      .addElement(
        ColumnsLayout(gap = Some("1rem"), height = Some("100%"))
          .addColumn(
            PortSchemaTabs(editableInput = Some(true), allowOutportRename = true, allowOutportAddDelete = true)
              .importSchema()
          )
          .addColumn(
            FileEditor(newFilePrefix = Some("out"), newFileLanguage = Some("sql"), minFiles = Some(1))
              .withSchemaSuggestions()
              .bindScalaPlaceholder(filePlaceholder)
              .bindPythonPlaceholder(filePlaceholder)
              .bindSQLPlaceholder(filePlaceholder)
              .bindProperty("fileTabs"),
            "2fr"
          )
      )
  }

  def validate(component: Component)(implicit context: WorkflowContext): List[Diagnostic] = {
    import scala.collection.mutable.ListBuffer
    val diagnostics = ListBuffer[Diagnostic]()
    if (component.ports.outputs.size != component.properties.fileTabs.size)
      diagnostics += Diagnostic(
        s"properties.fileTabs",
        "Number of output ports have to be the same as the number of file tabs.",
        SeverityLevel.Error
      )
    component.properties.fileTabs.zipWithIndex.foreach {
      case (_tab, idx) ⇒
        if (_tab.content.isEmpty)
          diagnostics += Diagnostic(s"properties.fileTabs[$idx]", s"${_tab.path} is empty", SeverityLevel.Error)
    }
    diagnostics.toList
  }

  def onChange(oldState: Component, newState: Component)(implicit context: WorkflowContext): Component = {
    val currentTabs = newState.properties.fileTabs
    val revisedFileTabs = newState.ports.outputs.map { port ⇒
      val language = currentTabs.head.language
      val tab = currentTabs.find(_.path == port.slug)
      val newFileTab = FileTab(port.slug, port.id, language, SString(""))
      tab match {
        case None ⇒ newFileTab
        case Some(t) ⇒ newFileTab.copy(content = t.content)
      }
    }
    val revisedInputPortNames = newState.ports.inputs.map { port ⇒
      port.slug
    }

    newState.copy(
      properties = newState.properties.copy(fileTabs = revisedFileTabs, inputPortNames = revisedInputPortNames)
    )
  }

  override def deserializeProperty(props: String): PropertiesType = Json.parse(props).as[PropertiesType]

  override def serializeProperty(props: PropertiesType): String = Json.stringify(Json.toJson(props))

  class SQLStatementCode(props: PropertiesType) extends ComponentCode {
    def apply(spark: SparkSession, in: DataFrame*): (DataFrame, List[DataFrame]) = {
      in.zip(props.inputPortNames).foreach { dfWithSlugName ⇒
        dfWithSlugName._1.createOrReplaceTempView(dfWithSlugName._2)
      }

      val head :: tail = props.fileTabs.map { _tab ⇒
        spark.sql(_tab.content)
      }
      (head, tail)
    }
  }
}
