package io.prophecy.scalagems.transform

import io.prophecy.gems._
import io.prophecy.gems.uiSpec._
import io.prophecy.gems.componentSpec._
import io.prophecy.gems.dataTypes._
import io.prophecy.gems.diagnostics._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import play.api.libs.json.{Format, Json}

class SanitizeRules extends ComponentSpec {

  val name: String = "SanitizeRules"
  val category: String = "Transform"

  type PropertiesType = SanitizeRulesComponentProperties

  override def optimizeCode: Boolean = true

  // todo: columnSelector property can be hidden from user
  // and move column select to the ports

  @Property("Order Expression Helper")
  case class RulesCheckboxes(
    @Property("Target Column") targetColumn: String = "default_value",
    @Property("Full expression") expression: SColumn,
    @Property("Upper Case") upper_case_rule: Boolean = false,
    @Property("Lower Case") lower_case_rule: Boolean = false,
    @Property("Trim") trim_rule: Boolean = false
  )

  case class SanitizeRulesComponentProperties(
    @Property("Columns selector")
    columnsSelector: List[String] = Nil,
    @Property("Rules to apply", "List of rules to apply to columns")
    columnRules: List[RulesCheckboxes] = Nil
  ) extends ComponentProperties

  implicit val rulesCheckboxesFormat: Format[RulesCheckboxes] = Json.format
  implicit val sanitizeRulesComponentPropertiesFormat: Format[SanitizeRulesComponentProperties] = Json.format

  def validate(component: Component)(implicit context: WorkflowContext): List[Diagnostic] = {
    import scala.collection.mutable.ListBuffer

    val diagnostics = ListBuffer[Diagnostic]()

    diagnostics.toList
  }

  override def onChange(oldState: Component, newState: Component)(implicit context: WorkflowContext): Component =
    newState

  def dialog: Dialog = {

    val columns = List(
      Column(
        "Output Column",
        "targetColumn",
        Some(TextBox("", ignoreTitle = true))
      ),
      Column(
        "Input Column",
        "expression.expression",
        Some(
          ExpressionBox(ignoreTitle = true)
        )
      ),
      Column(
        "Upper Case",
        "record.upper_case_rule",
        Some(Checkbox("").bindProperty("record.upper_case_rule"))
      ),
      Column(
        "Lower Case",
        "record.lower_case_rule",
        Some(Checkbox("").bindProperty("record.lower_case_rule"))
      ),
      Column(
        "Trim",
        "record.trim_rule",
        Some(Checkbox("").bindProperty("record.trim_rule"))
      )
    )

    val rulesTable =
      BasicTable(
        "Test",
        columns = columns,
        targetColumnKey = Some("targetColumn")
      ).bindProperty("columnRules")

    Dialog("Simple Rules Component")
      .addElement(
        ColumnsLayout(gap = Some("1rem"), height = Some("100%"))
          .addColumn(
            PortSchemaTabs(
              selectedFieldsProperty = Some("columnsSelector"),
              singleColumnClickCallback = Some { (portId: String, column: String, state: Any) ⇒
                val st = state.asInstanceOf[Component]
                st.copy(properties =
                  st.properties.copy(
                    columnsSelector = st.properties.columnsSelector ::: List(s"$portId##$column"),
                    //                    columnRules = st.properties.columnRules ::: RulesCheckboxes(column, SColumn(column)) :: Nil
                    st.properties.columnRules ::: RulesCheckboxes(column, SColumn(s"""col("$column")""")) :: Nil
                  )
                )
              }
            ).importSchema(),
            "2fr"
          )
          .addColumn(
            rulesTable,
            "5fr"
          )
      )
  }

  override def deserializeProperty(props: String): PropertiesType = Json.parse(props).as[PropertiesType]

  override def serializeProperty(props: PropertiesType): String = Json.stringify(Json.toJson(props))

  class SanitizeRulesCode(props: PropertiesType)(implicit context: WorkflowContext) extends ComponentCode {

    def apply(spark: SparkSession, in0: DataFrame): DataFrame = {

      var df: DataFrame = in0
      props.columnRules.foreach { rulesCheckbox =>
        var col_expression = rulesCheckbox.expression.column
        if (rulesCheckbox.upper_case_rule) {
          col_expression = upper(col_expression)
        }
        if (rulesCheckbox.lower_case_rule) {
          col_expression = lower(col_expression)
        }
        if (rulesCheckbox.trim_rule) {
          col_expression = trim(col_expression)
        }
        df = df.withColumn(rulesCheckbox.targetColumn, col_expression)
      }
      df
    }
  }
}
