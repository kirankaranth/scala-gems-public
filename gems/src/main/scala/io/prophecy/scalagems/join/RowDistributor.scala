package io.prophecy.scalagems.join

import io.prophecy.gems._
import io.prophecy.gems.uiSpec._
import io.prophecy.gems.componentSpec._
import io.prophecy.gems.dataTypes._
import io.prophecy.gems.diagnostics._
import org.apache.spark.sql.{DataFrame, SparkSession}
import play.api.libs.json.{Format, Json}

class RowDistributor extends ComponentSpec {

  val name: String = "RowDistributor"
  val category: String = "Join/Split"
  val gemDescription: String = "Splits one Dataframe into multiple Dataframes by rows based on filter conditions."
  val docUrl: String = "https://docs.prophecy.io/low-code-spark/gems/join-split/row-distributor"

  type PropertiesType = RowDistributorProperties
  override def optimizeCode: Boolean = true

  case class RowDistributorProperties(
    @Property("", "")
    outports: List[FileTab] = Nil
  ) extends ComponentProperties

  @Property("Helper")
  case class FileTab(
    @Property("path", "") path: String,
    @Property("id", "") id: String,
    @Property("model", "") model: SColumn
  )

  implicit val fileTabPropertiesFormat: Format[FileTab] = Json.format
  implicit val rowDistributorPropertiesFormat: Format[RowDistributorProperties] = Json.format

  def dialog: Dialog = Dialog("RowDistributor")
    .addElement(
      ColumnsLayout(gap = Some("1rem"), height = Some("100%"))
        .addColumn(PortSchemaTabs(allowOutportRename = true, allowOutportAddDelete = true).importSchema())
        .addColumn(
          FileEditor(
            newFilePrefix = Some("out"),
            newFileLanguage = Some("${$.workflow.metainfo.frontEndLanguage}"),
            minFiles = Some(2)
          )
            .withSchemaSuggestions()
            .withExpressionMode()
            .bindScalaPlaceholder("""col("some_column") > lit(10)""")
            .bindPythonPlaceholder("""(col("some_column") > lit(10))""")
            .bindSQLPlaceholder("some_column>10")
            .bindProperty("outports"),
          "2fr"
        )
    )

  def validate(component: Component)(implicit context: WorkflowContext): List[Diagnostic] = {
    import scala.collection.mutable._
    val diagnostics = ListBuffer[Diagnostic]()

    if (component.ports.outputs.size < 2)
      diagnostics += Diagnostic(
        s"properties.outports",
        "Number of output ports cannot be less than two.",
        SeverityLevel.Error
      )
    if (component.ports.outputs.size != component.properties.outports.size)
      diagnostics += Diagnostic(
        s"properties.outports",
        "Number of output ports have to be the same as the number of file tabs.",
        SeverityLevel.Error
      )

    component.properties.outports.zipWithIndex.foreach {
      case (outport, idx) ⇒
        if (outport.model.expression.trim.isEmpty)
          diagnostics += Diagnostic(s"properties.outports[$idx]", s"${outport.path} is empty", SeverityLevel.Error)
        else if (outport.model.expression.isEmpty)
          diagnostics += Diagnostic(
            s"properties.outports[$idx]",
            s"Unsupported expression ${outport.model.expression} ",
            SeverityLevel.Error
          )
    }
    diagnostics.toList
  }

  def onChange(oldState: Component, newState: Component)(implicit context: WorkflowContext): Component = {

    // following logic is required to migrate the old row distributor which
    // do not have that correct associated port id in the fileTab
    // it can be removed after we have proper migration process in place
    val migratedFileTabs = newState.properties.outports.map { fileTab ⇒
      if (fileTab.id.length < 20)
        oldState.ports.outputs.find(_.slug == fileTab.path).map(port ⇒ fileTab.copy(id = port.id)).getOrElse(fileTab)
      else fileTab
    }
    val migratedNewState = newState.copy(properties = newState.properties.copy(outports = migratedFileTabs))

    val currentTabs = migratedNewState.properties.outports
    val revisedFileTabs = migratedNewState.ports.outputs.map { port ⇒
      val tab = currentTabs.find(_.id == port.id)
      val newFileTab = FileTab(port.slug, port.id, SColumn(""))
      tab match {
        case None ⇒ newFileTab
        case Some(t) ⇒ newFileTab.copy(model = t.model)
      }
    }
    migratedNewState.copy(
      properties = migratedNewState.properties.copy(outports = revisedFileTabs)
    )
  }

  override def deserializeProperty(props: String): PropertiesType = Json.parse(props).as[PropertiesType]

  override def serializeProperty(props: PropertiesType): String = Json.stringify(Json.toJson(props))

  class RowDistributorCode(props: PropertiesType) extends ComponentCode {

    def apply(spark: SparkSession, in: DataFrame): (DataFrame, DataFrame, List[DataFrame]) = {
      val first :: second :: rest = props.outports.map { outport ⇒
        in.filter(outport.model.column)
      }
      (first, second, rest)
    }
  }
}
