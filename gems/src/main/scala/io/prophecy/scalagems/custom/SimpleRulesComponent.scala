package io.prophecy.scalagems.custom

import io.prophecy.gems._
import io.prophecy.gems.componentSpec._
import io.prophecy.gems.dataTypes._
import io.prophecy.gems.diagnostics._
import io.prophecy.gems.uiSpec._
import org.apache.spark.sql.functions._
import play.api.libs.json.{Format, Json}

class SimpleRulesComponent extends ComponentSpec {

  val name: String = "SimpleRulesComponent"
  val category: String = "Custom"
  import org.apache.spark.sql._
  import org.apache.spark.sql.types._

  val struct =
    StructType(
      StructField("a", IntegerType, true) ::
        StructField("b", LongType, false) ::
        StructField("c", BooleanType, false) :: Nil
    )
  //  val rulesSchema = StructType(Seq(StructField("uppercase_rule", StringType)))

  type PropertiesType = SimpleRulesComponentProperties

  override def optimizeCode: Boolean = true

  // todo: columnSelector property can be hidden from user
  // and move column select to the ports
  import org.apache.spark.sql.types.{StructField, StructType}

  @Property("Order Expression Helper")
  case class RulesCheckboxes(
    @Property("Target Column") targetColumn: String = "default_value",
    @Property("Full expression") expression: SColumn,
    @Property("Upper Case") upper_case_rule: Boolean = false,
    @Property("Validate Not Null") validate_not_null_rule: Boolean = false
  )

  case class SimpleRulesComponentProperties(
    @Property("Columns selector")
    columnsSelector: List[String] = Nil,
    @Property("Rules to apply", "List of rules to apply to columns")
    columnRules: List[RulesCheckboxes] = Nil
  ) extends ComponentProperties

  implicit val rulesCheckboxesFormat: Format[RulesCheckboxes] = Json.format
  implicit val simpleRulesComponentPropertiesFormat: Format[SimpleRulesComponentProperties] = Json.format
  //  override def validate(component: CompareColumns.Component)(implicit context: WorkflowContext): Diagnostics = Nil

  def validate(component: Component)(implicit context: WorkflowContext): List[Diagnostic] = {
    import scala.collection.mutable.ListBuffer

    val diagnostics = ListBuffer[Diagnostic]()

    diagnostics.toList
  }

  override def onChange(oldState: Component, newState: Component)(implicit
    context: WorkflowContext
  ): Component = newState

  //    def onChange(oldState: Component, newState: Component): Component = newState

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
        Some(Checkbox("r1").bindProperty("record.upper_case_rule")),
        width = "15%"
      ),
      Column(
        "Validate Not Null",
        "record.validate_not_null_rule",
        Some(Checkbox("r2").bindProperty("record.validate_not_null_rule")),
        width = "15%"
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
            ).importSchema()
          )
          .addColumn(
            rulesTable,
            "2fr"
          )
      )
  }

  override def deserializeProperty(props: String): PropertiesType = Json.parse(props).as[PropertiesType]

  override def serializeProperty(props: PropertiesType): String = Json.stringify(Json.toJson(props))

  class RulesComponentCode(props: PropertiesType)(implicit context: WorkflowContext) extends ComponentCode {

    def apply(spark: SparkSession, in0: DataFrame): (DataFrame, DataFrame, DataFrame, DataFrame) = {

      val emptyInvalid = in0.filter(lit(false))
      val (a, invalidRows) = props.columnRules.foldLeft((in0, emptyInvalid)) {
        case (
              (original, invalid_so_far),
              rulesCheckbox: RulesCheckboxes
            ) ⇒
          if (rulesCheckbox.validate_not_null_rule) {
            (
              original,
              invalid_so_far.union(
                original.filter(not(isnull(rulesCheckbox.expression.column)))
              )
            )
          } else {
            (original, invalid_so_far)
          }
      }

      val validatedOutput = props.columnRules.foldLeft(in0) {
        case (df, rulesCheckbox: RulesCheckboxes) ⇒
          if (rulesCheckbox.validate_not_null_rule) {
            df.filter(not(isnull(rulesCheckbox.expression.column)))
          } else {
            df
          }
      }

      val sanitizedOutput = props.columnRules.foldLeft(validatedOutput) {
        case (df, rulesCheckbox: RulesCheckboxes) ⇒
          if (rulesCheckbox.upper_case_rule) {
            val out = upper(rulesCheckbox.expression.column)
            df.withColumn(rulesCheckbox.targetColumn, out)
          } else {
            df
          }
      }
      (sanitizedOutput, validatedOutput, invalidRows, emptyInvalid)
    }
  }
}
