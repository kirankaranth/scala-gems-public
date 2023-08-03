package io.prophecy.scalagems.transform

import io.prophecy.gems._
import io.prophecy.gems.componentSpec.{ColumnsUsage, ComponentSpec}
import io.prophecy.gems.copilot.{ProjectionExpression, SelectStmt}
import io.prophecy.gems.dataTypes._
import io.prophecy.gems.diagnostics._
import io.prophecy.gems.uiSpec._
import org.apache.spark.sql.{DataFrame, SparkSession}
import play.api.libs.json.{Json, OFormat}

class Reformat extends ComponentSpec {

  val name: String = "Reformat"
  val category: String = "Transform"
  val gemDescription: String = "Edits column names or values using expressions."
  val docUrl: String = "https://docs.prophecy.io/low-code-spark/gems/transform/reformat/"

  type PropertiesType = ReformatProperties

  override def optimizeCode: Boolean = true

  // todo: columnSelector property can be hidden from user
  // and move column select to the ports
  case class ReformatProperties(
    @Property("Columns selector")
    columnsSelector: List[String] = Nil,
    @Property("Column expressions", "List of all the column expressions")
    expressions: List[SColumnExpression] = Nil
  ) extends ComponentProperties

  implicit val reformatPropertiesFormat: OFormat[ReformatProperties] = Json.format[ReformatProperties]

  def dialog: Dialog = Dialog("Reformat")
    .addElement(
      ColumnsLayout(gap = Some("1rem"), height = Some("100%"))
        .addColumn(
          PortSchemaTabs(
            allowInportRename = true,
            selectedFieldsProperty = Some("columnsSelector"),
            singleColumnClickCallback = Some { (portId: String, column: String, state: Any) ⇒
              val st = state.asInstanceOf[Component]
              val existingTargetNames = st.properties.expressions.map(_.target)
              val targetCol =
                getTargetTokens(column, existingTargetNames.map(_.split('.').toList), shortest = true).mkString(".")
              st.copy(properties =
                st.properties.copy(
                  expressions = st.properties.expressions ::: List(
                    SColumnExpression(targetCol, s"""col("${sanitizedColumn(column)}")""")
                  )
                )
              )
            },
            allColumnsSelectionCallback = Some { (portId: String, state: Any) ⇒
              val st = state.asInstanceOf[Component]
              val columnsInSchema = getColumnsInSchema(portId, st, SchemaFields.TopLevel)
              val updatedExpressions = st.properties.expressions ::: columnsInSchema.map { x ⇒
                SColumnExpression(x, s"""col("${sanitizedColumn(x)}")""")
              }
              st.copy(properties = st.properties.copy(expressions = updatedExpressions))
            }
          ).importSchema(),
          "2fr"
        )
        .addColumn(
          ExpTable("Reformat Expression")
            .withRowId()
            .bindProperty("expressions"),
          "5fr"
        )
    )

  def validate(component: Component)(implicit context: WorkflowContext): Diagnostics = {
    val diagnostics = validateExpTable(
      component.properties.expressions,
      "expressions",
      component,
      testColumnPresence = Some(ColumnsUsage.WithoutInputAlias)
    )
    diagnostics.toList
  }

  def onChange(oldState: Component, newState: Component)(implicit
    context: WorkflowContext
  ): Component = {
    val newProps = newState.properties

    val usedColExps = getColumnsToHighlight(newProps.expressions, newState)

    // todo: column selector internal format must be hidden from user.
    // move following logic in ComponentSpec trait
    newState.copy(properties =
      newProps.copy(
        columnsSelector = usedColExps,
        expressions = newProps.expressions.map(_.withRowId)
      )
    )
  }

  override def getCPStmt(component: Component): Option[SelectStmt] = {
    val props = component.properties
    Some(
      SelectStmt(
        component.component,
        props.expressions.map(x => ProjectionExpression(Some(x.target), x.expression.expression)),
        Nil,
        Nil,
        Nil
      )
    )
  }

  override def userReadme: String =
    """
      |## Reformat
      |---
      |
      |Reformat is often use to edit one or more columns' values, often by simple expressions and functions. Often this involves expressions such as `substring` to extract parts of incoming string, `case when` statement to add columns summarizing conditions of incoming data. This is also used to rename columns and select a few columns.
      |
      |> **Note:** Any columns not selected explicitly are removed, use `Select All` in the left bar to add all columns by default.
      |
      |When there are no columns selected, all columns are passed through to the output. However, once you start selecting some columns, only the selected columns are output.
      |
      |> **Note:** _Implementation_: Reformat converts to a SQL `Select` or in relational terms into a `projection`. However, when changing (add, edit, delete) one or two columns, one can use [SchemaTransformer](https://docs.prophecy.io/spark/standard_components/transforms/reformat/) that uses underlying `withColumn` construct. However, Reformat is best for performance. For more details refer to [Spark APIs](https://spark.apache.org/docs/latest/api/scala/org/apache/spark/sql/Dataset.html)
      |
      |
      |
      |## Example
      |---
      |
      |Reformat gives you a spreadsheet like interface where you can write expressions based on incoming columns to produce new columns.
      |
      |___Expression Builder___
      |
      |There is an `expression builder` that enables one to quickly write expressions. Expression builder will suggest
      |
      |- Incoming columns
      |- Inbuilt operators
      |- In-built functions
      |- User defined functions
      |
      |
      |## Code
      |---
      |
      |This component is a simple select statement, however the expressions can become very large and we see tables of 1000+ columns frequently with this component being hundreds of lines long.
      |
      |
      |```scala
      |object PrepareComponent {
      |
      |    def apply(spark: SparkSession, in: DataFrame): Reformat = {
      |        import spark.implicits._
      |
      |        val out = in.select(
      |          datediff(current_date(), col("account_open_date")).as("account_length_days"),
      |          col("order_id"),
      |          col("customer_id"),
      |          col("amount"),
      |          col("first_name"),
      |          col("last_name"),
      |          col("phone"),
      |          col("email"),
      |          col("country_code"),
      |          col("account_flags")
      |        )
      |
      |        out
      |    }
      |}
      |```
      |""".stripMargin

  class ReformatCode(props: PropertiesType)(implicit context: WorkflowContext) extends ComponentCode {

    def apply(spark: SparkSession, in: DataFrame): DataFrame = {
      val out = props.expressions match {
        case Nil ⇒ in
        case _ ⇒ in.select(props.expressions.columns: _*)
      }
      out
    }

  }

  override def deserializeProperty(props: String): ReformatProperties =
    Json.parse(props).as[ReformatProperties]

  override def serializeProperty(props: ReformatProperties): String =
    Json.stringify(Json.toJson(props))
}
