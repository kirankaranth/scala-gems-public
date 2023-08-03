package io.prophecy.scalagems.join

import io.prophecy.gems._
import io.prophecy.gems.uiSpec._
import io.prophecy.gems.componentSpec._
import io.prophecy.gems.dataTypes._
import io.prophecy.gems.diagnostics._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import play.api.libs.json.{Format, Json}

class ValidateRules extends ComponentSpec {

  val name: String = "ValidateRules"
  val category: String = "Join/Split"

  type PropertiesType = ValidateRulesProperties

  override def optimizeCode: Boolean = true

  @Property("Order Expression Helper")
  case class RulesCheckboxes(
    @Property("Full expression") expression: SColumn,
    @Property("Validate Not Null") validate_not_null_rule: Boolean = false,
    @Property("Validate Not Blank") validate_not_blank_rule: Boolean = false
  )

  case class ValidateRulesProperties(
    @Property("Columns selector")
    columnsSelector: List[String] = Nil,
    @Property("Rules to apply", "List of rules to apply to columns")
    columnRules: List[RulesCheckboxes] = Nil
  ) extends ComponentProperties
  //  override def validate(component: CompareColumns.Component)(implicit context: WorkflowContext): Diagnostics = Nil

  implicit val rulesCheckboxesFormat: Format[RulesCheckboxes] = Json.format
  implicit val validateRulesPropertiesFormat: Format[ValidateRulesProperties] = Json.format

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
        "Input Column",
        "expression.expression",
        Some(
          ExpressionBox(ignoreTitle = true)
        )
      ),
      Column(
        "Validate Not Null",
        "record.validate_not_null_rule",
        Some(Checkbox("").bindProperty("record.validate_not_null_rule"))
      ),
      Column(
        "Validate Not Blank",
        "record.validate_not_blank_rule",
        Some(Checkbox("").bindProperty("record.validate_not_blank_rule"))
      )
    )

    val rulesTable =
      BasicTable(
        "Test",
        columns = columns
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
                    st.properties.columnRules ::: RulesCheckboxes(SColumn(s"""col("$column")""")) :: Nil
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
  class ValidateRulesCode(props: PropertiesType)(implicit context: WorkflowContext) extends ComponentCode {

    def apply(spark: SparkSession, in0: DataFrame): (DataFrame, DataFrame, DataFrame) = {
      import spark.implicits._

      val validOutput = props.columnRules.foldLeft(in0) {
        case (df, rulesCheckbox: RulesCheckboxes) ⇒
          val c = rulesCheckbox.expression.column
          val isValid = lit(true)
          val withNotNull = if (rulesCheckbox.validate_not_null_rule) not(isnull(c)).and(isValid) else isValid
          val withNotBlank =
            if (rulesCheckbox.validate_not_blank_rule) c.notEqual(lit("")).and(withNotNull) else withNotNull
          df.filter(withNotBlank)
      }

      val invalidOutput = in0.exceptAll(validOutput)

      val emptyDF2 = Seq(("base_rule", true, "column", 0)).toDF("validation_rule", "valid", "column", "count")
      val validationReport = props.columnRules.foldLeft(emptyDF2) {
        case (df, rulesCheckbox: RulesCheckboxes) ⇒
          val c = rulesCheckbox.expression.column
          val withNull = in0.groupBy(lit("null_values").as("validation_rule"), not(isnull(c)).as("valid")).count()
          val withBlank =
            in0.groupBy(lit("blank_values").as("validation_rule"), c.notEqual(lit("")).as("valid")).count()
          df.union(withNull.union(withBlank).withColumn("column", lit(c.toString())))
      }

      (validOutput, invalidOutput, validationReport)
    }
  }
}
