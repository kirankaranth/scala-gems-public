package io.prophecy.scalagems.custom

import io.prophecy.gems.componentSpec.{ColumnsUsage, ComponentSpec}
import io.prophecy.gems.dataTypes._
import io.prophecy.gems.diagnostics._
import io.prophecy.gems.uiSpec._
import io.prophecy.gems.{ComponentCode, ComponentProperties, Property, WorkflowContext, sanitizedColumn}
import org.apache.spark.sql.{DataFrame, SparkSession}
import play.api.libs.json.{Format, Json}

class Hashing extends ComponentSpec {

  val name: String = "Hashing"
  val category: String = "Custom"
  type PropertiesType = HashingProperties

  override def optimizeCode: Boolean = true

  // todo: columnSelector property can be hidden from user
  // and move column select to the ports
  case class HashingProperties(
    @Property("Columns selector") columnsSelector: List[String] = Nil,
    @Property("Column expressions", "List of all the column expressions")
    expressions: List[SColumn] = Nil,
    @Property("Hashing Type", "Type of the Hashing technique")
    hashType: String = "sha1",
    @Property("Hash Column name", "Hash Column name")
    hashColumnName: String = ""
  ) extends ComponentProperties

  implicit val hashingPropertiesFormat: Format[HashingProperties] = Json.format

  def dialog: Dialog = {

    val hashColumnsTable = BasicTable(
      "HashColumnsTable",
      columns = List(
        Column(
          "Hashing Columns/Expressions",
          "expression",
          Some(
            ExpressionBox(ignoreTitle = true)
              .bindPlaceholders()
              .bindLanguage("${record.format}")
              .withSchemaSuggestions()
          )
        )
      )
    ).bindProperty("expressions")

    Dialog("Hashing")
      .addElement(
        ColumnsLayout(gap = Some("1rem"), height = Some("100%"))
          .addColumn(
            PortSchemaTabs(
              allowInportRename = true,
              selectedFieldsProperty = Some("columnsSelector"),
              singleColumnClickCallback = Some { (portId: String, column: String, state: Any) ⇒
                val st = state.asInstanceOf[Component]
                st.copy(properties =
                  st.properties.copy(
                    expressions = st.properties.expressions ::: List(
                      SColumn(s"""col("${sanitizedColumn(column)}")""")
                    )
                  )
                )
              },
              allColumnsSelectionCallback = Some { (portId: String, state: Any) ⇒
                val st = state.asInstanceOf[Component]
                val columnsInSchema = getColumnsInSchema(portId, st, SchemaFields.TopLevel)
                val updatedExpressions = st.properties.expressions ::: columnsInSchema.map { x ⇒
                  SColumn(s"""col("${sanitizedColumn(x)}")""")
                }
                st.copy(properties = st.properties.copy(expressions = updatedExpressions))
              }
            ).importSchema(),
            "2fr"
          )
          .addColumn(
            StackLayout(height = Some("100%"))
              .addElement(
                ColumnsLayout(gap = Some("1rem"))
                  .addColumn(
                    TextBox("Output column name")
                      .bindPlaceholder("hashed_value")
                      .bindProperty("hashColumnName")
                  )
                  .addColumn(
                    SelectBox("Hashing Type")
                      .addOption("murmur3", "murmur3")
                      .addOption("md5", "md5")
                      .addOption("sha1", "sha1")
                      .addOption("sha2", "sha2")
                      .addOption("sha224", "sha224")
                      .addOption("sha256", "sha256")
                      .addOption("sha384", "sha384")
                      .addOption("sha512", "sha512")
                      .bindProperty("hashType")
                  )
              )
              .addElement(
                StackItem(
                  List(hashColumnsTable),
                  grow = Some(1)
                )
              ),
            "5fr"
          )
      )
  }

  def validate(component: Component)(implicit context: WorkflowContext): List[Diagnostic] = {
    import scala.collection.mutable.ListBuffer
    val diagnostics = ListBuffer[Diagnostic]()
    if (component.properties.expressions.isEmpty)
      diagnostics += Diagnostic(
        "properties.expressions",
        "At least one column for hashing has to be specified",
        SeverityLevel.Error
      )
    else {
      component.properties.expressions.zipWithIndex.foreach {
        case (expr, idx) ⇒
          diagnostics ++= validateSColumn(
            expr,
            s"expressions[$idx]",
            component,
            Some(ColumnsUsage.WithoutInputAlias)
          )
      }
    }
    diagnostics.toList
    if (component.properties.hashType.trim.isBlank) {
      diagnostics += Diagnostic(
        "properties.hashType",
        "Hashing Type field cannot be empty",
        SeverityLevel.Error
      )
    }
    if (component.properties.hashColumnName.trim.isBlank) {
      diagnostics += Diagnostic(
        "properties.hashColumnName",
        "Hashing Columns/Expressions field cannot be empty",
        SeverityLevel.Error
      )
    }
    diagnostics.toList
  }

  def onChange(oldState: Component, newState: Component)(implicit
    context: WorkflowContext
  ): Component = {
    val newProps = newState.properties

    val usedCols = getColumnsToHighlight(newProps.expressions, newState)
    newState.copy(properties =
      newProps.copy(
        columnsSelector = usedCols,
        expressions = newProps.expressions
      )
    )
  }

  override def deserializeProperty(props: String): PropertiesType = Json.parse(props).as[PropertiesType]

  override def serializeProperty(props: PropertiesType): String = Json.stringify(Json.toJson(props))

  class HashingCode(props: PropertiesType)(implicit context: WorkflowContext) extends ComponentCode {

    def apply(spark: SparkSession, in: DataFrame): DataFrame = {
      props.hashType match {
        case "md5" =>
          import org.apache.spark.sql.functions.{concat, md5}
          in.withColumn(props.hashColumnName, md5(concat(props.expressions.map(_.column): _*)))
        case "sha1" =>
          import org.apache.spark.sql.functions.{concat, sha1}
          in.withColumn(props.hashColumnName, sha1(concat(props.expressions.map(_.column): _*)))
        case "sha2" =>
          import org.apache.spark.sql.functions.{concat, sha2}
          in.withColumn(props.hashColumnName, sha2(concat(props.expressions.map(_.column): _*), 0))
        case "sha224" =>
          import org.apache.spark.sql.functions.{concat, sha2}
          in.withColumn(props.hashColumnName, sha2(concat(props.expressions.map(_.column): _*), 224))
        case "sha256" =>
          import org.apache.spark.sql.functions.{concat, sha2}
          in.withColumn(props.hashColumnName, sha2(concat(props.expressions.map(_.column): _*), 256))
        case "sha384" =>
          import org.apache.spark.sql.functions.{concat, sha2}
          in.withColumn(props.hashColumnName, sha2(concat(props.expressions.map(_.column): _*), 384))
        case "sha512" =>
          import org.apache.spark.sql.functions.{concat, sha2}
          in.withColumn(props.hashColumnName, sha2(concat(props.expressions.map(_.column): _*), 512))
        case "murmur3" =>
          import org.apache.spark.sql.functions.{concat, hash}
          in.withColumn(props.hashColumnName, hash(concat(props.expressions.map(_.column): _*)))
      }
    }
  }
}
