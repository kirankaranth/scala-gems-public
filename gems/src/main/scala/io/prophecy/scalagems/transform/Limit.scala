package io.prophecy.scalagems.transform

import io.prophecy.gems._
import io.prophecy.gems.componentSpec._
import io.prophecy.gems.dataTypes._
import io.prophecy.gems.diagnostics._
import io.prophecy.gems.uiSpec._
import org.apache.spark.sql.{DataFrame, SparkSession}
import play.api.libs.json.{Format, Json}

class Limit extends ComponentSpec {

  val name: String = "Limit"
  val category: String = "Transform"
  val gemDescription: String = "Limits the number of rows in the input data"
  val docUrl: String = "https://docs.prophecy.io/low-code-spark/gems/transform/limit/"

  type PropertiesType = LimitProperties
  override def optimizeCode: Boolean = true

  case class LimitProperties(
    @Property("Limit", "Number of rows to limit the incoming DataFrame to")
    limit: SInt = SInt("10")
  ) extends ComponentProperties

  implicit val limitPropertiesFormat: Format[LimitProperties] = Json.format

  def dialog: Dialog = Dialog("Limit")
    .addElement(
      ColumnsLayout(gap = Some("1rem"), height = Some("100%"))
        .addColumn(PortSchemaTabs().importSchema(), "2fr")
        .addColumn(
          ExpressionBox("Limit")
            .bindProperty("limit")
            .bindPlaceholder("10")
            .withFrontEndLanguage,
          "5fr"
        )
    )

  def validate(component: Component)(implicit context: WorkflowContext): List[Diagnostic] = {
    import scala.collection.mutable.ListBuffer

    val diagnostics = ListBuffer[Diagnostic]()

    val (diag, limit) = (component.properties.limit.diagnostics, component.properties.limit.value)
    diagnostics ++= diag

    val limitDiagMsg = "Limit has to be an integer between [0, (2**31)-1]"
    if (limit.isDefined) {
      if (limit.get < 0)
        diagnostics += Diagnostic("properties.limit", limitDiagMsg, SeverityLevel.Error)
    }
    diagnostics.toList
  }

  def onChange(oldState: Component, newState: Component)(implicit context: WorkflowContext): Component = newState

  override def deserializeProperty(props: String): PropertiesType = Json.parse(props).as[PropertiesType]

  override def serializeProperty(props: PropertiesType): String = Json.stringify(Json.toJson(props))
  class LimitCode(props: PropertiesType) extends ComponentCode {

    def apply(spark: SparkSession, in: DataFrame): DataFrame = {
      val out = in.limit(props.limit)
      out
    }

  }
}
