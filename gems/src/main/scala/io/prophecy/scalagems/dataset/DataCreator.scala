package io.prophecy.scalagems.dataset

import io.prophecy.gems._
import io.prophecy.gems.dataTypes._
import io.prophecy.gems.datasetSpec._
import io.prophecy.gems.diagnostics._
import io.prophecy.gems.uiSpec._
import org.apache.spark.sql.functions.{col, monotonically_increasing_id}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import play.api.libs.json.{Format, Json}

class DataCreator extends DatasetSpec {

  val name: String = "DataCreator"
  val datasetType: String = "File"

  type PropertiesType = RandomDataCreatorProperties
  case class RandomDataCreatorProperties(
    @Property("Schema")
    schema: Option[StructType] = None,
    @Property("numRows")
    numRows: Option[String] = Some("1000000"),
    @Property("dataConfig")
    dataConfig: Option[String] = Some("""{"numeric":2, "string(256)":2, "rand_int(1,100)":["id1", "id2"] }"""),
    @Property("Description")
    description: Option[String] = Some("Random Data Creator")
  ) extends DatasetProperties

  implicit val randomDataCreatorPropertiesFormat: Format[RandomDataCreatorProperties] = Json.format

  def sourceDialog: DatasetDialog = DatasetDialog("DataCreator")
    .addSection(
      "PROPERTIES",
      ColumnsLayout(gap = Some("1rem"), height = Some("100%"))
        .addColumn(
          ScrollBox().addElement(
            StackLayout(height = Some("100%"))
              .addElement(
                StackItem(grow = Some(1)).addElement(
                  FieldPicker(height = Some("100%"))
                    .addField(
                      TextArea("Description", 2, placeholder = "Data description..."),
                      "description",
                      true
                    )
                    .addField(TextBox("Number of Rows"), "numRows", true)
                    .addField(TextArea("Json Config", 25), "dataConfig", true)
                )
              )
          ),
          "3fr"
        )
        .addColumn(SchemaTable("").bindProperty("schema"), "5fr")
    )

  def targetDialog: DatasetDialog = DatasetDialog("DataCreator")

  override def validate(component: Component)(implicit context: WorkflowContext): List[Diagnostic] = {
    import scala.collection.mutable.ListBuffer
    val diagnostics = ListBuffer[Diagnostic]()
    diagnostics ++= super.validate(component)

    if (!(component.properties.numRows.exists(_.toInt >= 0))) {
      diagnostics += Diagnostic("properties.numRows", "numRows must be an integer", SeverityLevel.Error)
    }
    if (component.properties.dataConfig.isEmpty) {
      diagnostics += Diagnostic("properties.dataConfig", "dataConfig must be a JSON Config", SeverityLevel.Error)
    } else {
      try {
        import org.apache.spark.sql.prophecy.util.JsonUtils.jsonStrToMap
        val jsonExtractedDataConfig = jsonStrToMap(component.properties.dataConfig.get)
      } catch {
        case e: Exception =>
          diagnostics += Diagnostic("properties.dataConfig", "dataConfig must be a valid JSON", SeverityLevel.Error)
      }
    }
    if (component.properties.schema.isEmpty) {
      // diagnostics += Diagnostic("properties.schema", "Schema cannot be empty [Properties]", SeverityLevel.Error)
    }

    diagnostics.toList
  }

  def onChange(oldState: Component, newState: Component)(implicit context: WorkflowContext): Component = newState

  class RandomDataCreatorFormatCode(props: RandomDataCreatorProperties) extends ComponentCode {

    def sourceApply(spark: SparkSession): DataFrame = {
      // Random Data Creator Code.
      import org.apache.spark.sql.prophecy.util.DataUtils.addRandomColumns
      import org.apache.spark.sql.prophecy.util.JsonUtils.jsonStrToMap
      val df =
        spark.range(props.numRows.get.toInt).repartition(10).select(monotonically_increasing_id().as("initial_id"))
      val dataConfig = jsonStrToMap(props.dataConfig.get)
      val randomColumnsList = addRandomColumns(dataConfig)
      val df_final = df.select((col("initial_id") +: randomColumnsList): _*)
      df_final
    }

    def targetApply(spark: SparkSession, in: DataFrame): Unit = {}

  }

  override def deserializeProperty(props: String): PropertiesType = Json.parse(props).as[PropertiesType]

  override def serializeProperty(props: PropertiesType): String = Json.stringify(Json.toJson(props))
}
