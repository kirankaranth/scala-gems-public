package io.prophecy.scalagems.join

import io.prophecy.gems._
import io.prophecy.gems.componentSpec._
import io.prophecy.gems.dataTypes._
import io.prophecy.gems.diagnostics._
import io.prophecy.gems.uiSpec._
import org.apache.spark.sql.{DataFrame, SparkSession}
import play.api.libs.json.{Format, Json}

class RandomSplit extends ComponentSpec {

  val name: String = "RandomSplit"
  val category: String = "Join/Split"
  val gemDescription: String = "Splits data randomly"

  type PropertiesType = RandomSplitProperties
  override def optimizeCode: Boolean = true

  case class RandomSplitProperties(
    @Property("weights")
    weights: List[WeightWrapper] = Nil,
    @Property("Random Seed")
    seed: Option[Long] = None
  ) extends ComponentProperties

  @Property("Weight Wrapper")
  case class WeightWrapper(
    @Property("Weight") weight: Double
  )

  implicit val weightWrapperFormat: Format[WeightWrapper] = Json.format
  implicit val randomSplitPropertiesFormat: Format[RandomSplitProperties] = Json.format

  def dialog: Dialog = Dialog("Random Split")
    .addElement(
      ColumnsLayout(gap = Some("1rem"), height = Some("100%"))
        .addColumn(PortSchemaTabs().importSchema(), "2fr")
        .addColumn(
          StackLayout(gap = Some("1rem"))
            .addElement(
              BasicTable(
                "Weights",
                height = Some("200px"),
                columns = List(
                  Column(
                    "Weights",
                    "weight",
                    Some(
                      TextBox("").bindPlaceholder("weight")
                    )
                  )
                )
              )
                .bindProperty("weights")
            )
            .addElement(
              TextBox("Seed for Random split")
                // .makeFieldOptional()
                .bindPlaceholder("""long""")
                .bindProperty("seed")
            ),
          "5fr"
        )
    )

  def validate(component: Component)(implicit context: WorkflowContext): List[Diagnostic] = {
    import scala.collection.mutable._
    val diagnostics = ListBuffer[Diagnostic]()

    if (component.ports.outputs.size < 1) {
      diagnostics += Diagnostic(
        s"properties.weights",
        "Number of output ports cannot be less than one.",
        SeverityLevel.Error
      )
    }

    /*val indexWeights = component.properties.weights.map({ w =>
      Try {
        w.weight.toDouble
      }
    })
    var parseError = false
    indexWeights.zipWithIndex.foreach {
      case (Failure(_), idx) => {
        parseError = true
        diagnostics += Diagnostic(
          s"properties.weights[$idx]",
          s"'${component.properties.weights(idx).weight}' is not a valid floating-point number",
          SeverityLevel.Error
        )
      }
      case _ =>
    }*/

    val totalWeight = component.properties.weights.foldLeft(0.0)((acc, w) => acc + w.weight)
    if (component.ports.outputs.size >= 1 && totalWeight <= 0) {
      diagnostics += Diagnostic(
        "properties.weights",
        "The sum of the weights must be > 0",
        SeverityLevel.Error
      )
    }

    diagnostics.toList
  }

  def onChange(oldState: Component, newState: Component)(implicit context: WorkflowContext): Component = {
    val revisedOutputs = (0 until (newState.properties.weights.size + 1)).map { i =>
      NodePort(s"out${i}", s"out${i}")
    }
    newState.copy(ports = newState.ports.copy(outputs = revisedOutputs.toList))
  }

  override def deserializeProperty(props: String): PropertiesType = Json.parse(props).as[PropertiesType]

  override def serializeProperty(props: PropertiesType): String = Json.stringify(Json.toJson(props))

  class RandomSplitCode(props: PropertiesType)(implicit context: WorkflowContext) extends ComponentCode {

    def apply(spark: SparkSession, in: DataFrame): List[DataFrame] = {
      val weights = props.weights.map(_.weight).toArray
      if (props.seed.isEmpty) {
        in.randomSplit(weights).toList
      } else {
        in.randomSplit(weights, props.seed.get).toList
      }
    }
  }
}
