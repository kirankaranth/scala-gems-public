package io.prophecy.scalagems.transform

import io.prophecy.gems._
import io.prophecy.gems.componentSpec._
import io.prophecy.gems.dataTypes._
import io.prophecy.gems.diagnostics._
import io.prophecy.gems.uiSpec._
import org.apache.spark.sql.{DataFrame, SparkSession}
import play.api.libs.json.{Format, Json}

class Filter extends ComponentSpec {

    val name: String = "Filter"
  val category: String = "Transform"
  val gemDescription: String = "Filters rows of the DataFrame based on the provided filter conditions"
  val docUrl: String = "https://docs.prophecy.io/low-code-spark/gems/transform/filter/"

  type PropertiesType = FilterProperties

  override def optimizeCode: Boolean = true

  case class FilterProperties(
    @Property("Columns selector")
    columnsSelector: List[String] = Nil,
    @Property("Filter", "Predicate expression to filter rows of incoming dataframe")
    condition: SColumn = SColumn("lit(true)")
  ) extends ComponentProperties

  implicit val filterPropertiesFormat: Format[FilterProperties] = Json.format

  def dialog: Dialog = Dialog("Filter")
    .addElement(
      ColumnsLayout(height = Some("100%"))
        .addColumn(
          PortSchemaTabs(selectedFieldsProperty = Some("columnsSelector")).importSchema(),
          "2fr"
        )
        .addColumn(
          StackLayout(height = Some("100%"))
            .addElement(TitleElement("Filter Condition"))
            .addElement(
              Editor(height = Some("100%"))
                .withSchemaSuggestions()
                .bindProperty("condition.expression")
            ),
          "5fr"
        )
    )

  def validate(component: Component)(implicit context: WorkflowContext): List[Diagnostic] = {
    val diagnostics =
      validateSColumn(component.properties.condition, "condition", component)
    diagnostics.toList
  }

  def onChange(oldState: Component, newState: Component)(implicit context: WorkflowContext): Component = {
    val newProps = newState.properties
    val portId = newState.ports.inputs.head.id

    val expressions = getColumnsToHighlight(List(newProps.condition), newState)

    newState.copy(properties = newProps.copy(columnsSelector = expressions))
  }

  override def deserializeProperty(props: String): PropertiesType = Json.parse(props).as[PropertiesType]

  override def serializeProperty(props: PropertiesType): String = Json.stringify(Json.toJson(props))

  class FilterCode(props: PropertiesType)(implicit context: WorkflowContext) extends ComponentCode {

    def apply(spark: SparkSession, in: DataFrame): DataFrame = {
      val out = in.filter(props.condition.column)
      out
    }

  }

}
