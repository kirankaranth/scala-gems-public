package io.prophecy.scalagems.transform

import io.prophecy.gems._
import io.prophecy.gems.componentSpec._
import io.prophecy.gems.dataTypes._
import io.prophecy.gems.diagnostics._
import io.prophecy.gems.uiSpec._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import play.api.libs.json.{Format, Json}

class SampleRows extends ComponentSpec {

  val name: String = "SampleRows"
  val category: String = "Transform"
  val gemDescription: String = "Samples Rows based on certain conditions"

  type PropertiesType = SampleRowsProperties

  override def optimizeCode: Boolean = true

  case class SampleRowsProperties(
    @Property("Ratio", "Sampling ratio")
    samplingRatio: Double = 0.5d,
    @Property("WithReplacement", "With Replacement")
    withReplacement: Boolean = false,
    @Property("Seed", "Random seed for sampling (optional)")
    seed: Option[Long] = None
  ) extends ComponentProperties

  implicit val sampleRowsPropertiesFormat: Format[SampleRowsProperties] = Json.format

  def dialog: Dialog = Dialog("Sample Rows")
    .addElement(
      ColumnsLayout(gap = Some("1rem"), height = Some("100%"))
        .addColumn(PortSchemaTabs().importSchema(), "2fr")
        .addColumn(
          StackLayout(gap = Some("1rem"))
            .addElement(
              TextBox("Sampling Ratio")
                .bindPlaceholder("""0.5""")
                .bindProperty("samplingRatio")
            )
            .addElement(
              Checkbox("With Replacement")
                .bindProperty("withReplacement")
            )
            .addElement(
              TextBox("Random seed for sampling (optional)")
                .bindPlaceholder("""long""")
                .bindProperty("seed")
            ),
          "5fr"
        )
    )

  def validate(component: Component)(implicit context: WorkflowContext): List[Diagnostic] = {
    import scala.collection.mutable._
    val diagnostics = ListBuffer[Diagnostic]()

    if (component.properties.samplingRatio < 0) {
      diagnostics += Diagnostic(
        "properties.samplingRatio",
        "Sampling ratio must be greater than zero",
        SeverityLevel.Error
      )
    } else if (component.properties.samplingRatio > 1 && !component.properties.withReplacement) {
      diagnostics += Diagnostic(
        "properties.samplingRatio",
        "Sampling ratio must be in the range [0, 1] without replacement",
        SeverityLevel.Error
      )
    }

    diagnostics.toList
  }

  override def deserializeProperty(props: String): PropertiesType = Json.parse(props).as[PropertiesType]

  override def serializeProperty(props: PropertiesType): String = Json.stringify(Json.toJson(props))

  def onChange(oldState: Component, newState: Component)(implicit context: WorkflowContext): Component =
    newState

  class SampleRowsCode(props: PropertiesType)(implicit context: WorkflowContext) extends ComponentCode {
    def apply(spark: SparkSession, in: DataFrame): DataFrame = {
      if (props.seed.nonEmpty) {
        in.sample(props.withReplacement, props.samplingRatio, props.seed.get)
      } else {
        in.sample(props.withReplacement, props.samplingRatio)
      }
    }
  }

}
