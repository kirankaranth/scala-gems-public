package io.prophecy.scalagems.transform

import io.prophecy.gems._
import io.prophecy.gems.componentSpec._
import io.prophecy.gems.dataTypes._
import io.prophecy.gems.diagnostics._
import io.prophecy.gems.uiSpec._
import org.apache.spark.sql.{DataFrame, SparkSession}
import play.api.libs.json.{Format, Json}

class OrderBy extends ComponentSpec {

  val name: String = "OrderBy"
  val category: String = "Transform"
  type PropertiesType = OrderByProperties
  override def optimizeCode: Boolean = true

    case class OrderByProperties(
    @Property("Columns selector")
    columnsSelector: List[String] = Nil,
    @Property("Order By Rules", "List of conditions to order dataframes on")
    orders: List[OrderByRule] = Nil
  ) extends ComponentProperties

  @Property("Order Expression Helper")
  case class OrderByRule(
    @Property("Full expression") expression: SColumn,
    @Property("Sort") sortType: String
  )
  
  implicit val orderByRuleFormat: Format[OrderByRule] = Json.format
  implicit val orderByPropertiesFormat: Format[OrderByProperties] = Json.format

  def dialog: Dialog = {

    val selectBox = SelectBox("")
      .addOption("Ascending", "asc")
      .addOption("Descending", "desc")

    val columns = List(
      io.prophecy.gems.uiSpec.Column(
        "Order Columns",
        "expression.expression",
        Some(
          ExpressionBox(ignoreTitle = true)
            .bindPlaceholders()
            .bindLanguage("${record.expression.format}")
            .withSchemaSuggestions()
        )
      ),
      io.prophecy.gems.uiSpec.Column("Sort", "sortType", Some(selectBox), width = "25%")
    )

    val orderByTable = BasicTable("OrderByTable", columns = columns)
      .bindProperty("orders")

    Dialog("OrderBy")
      .addElement(
        ColumnsLayout(gap = Some("1rem"), height = Some("100%"))
          .addColumn(
            PortSchemaTabs(
              selectedFieldsProperty = Some("columnsSelector"),
              singleColumnClickCallback = Some { (portId: String, column: String, state: Any) ⇒
                val st = state.asInstanceOf[Component]
                st.copy(properties =
                  st.properties.copy(
                    orders = st.properties.orders ::: List(
                      OrderByRule(SColumn(s"""col("${sanitizedColumn(column)}")"""), "asc")
                    )
                  )
                )
              },
              allColumnsSelectionCallback = Some { (portId: String, state: Any) ⇒
                val st = state.asInstanceOf[Component]
                val columnsInSchema = getColumnsInSchema(portId, st, SchemaFields.TopLevel)
                val updatedOrderRules = st.properties.orders ::: columnsInSchema.map { x ⇒
                  OrderByRule(SColumn(s"""col("${sanitizedColumn(x)}")"""), "asc")
                }
                st.copy(properties = st.properties.copy(orders = updatedOrderRules))
              }
            ).importSchema(),
            "2fr"
          )
          .addColumn(
            orderByTable,
            "5fr"
          )
      )
  }
  def validate(component: Component)(implicit context: WorkflowContext): List[Diagnostic] = {
    import scala.collection.mutable.ListBuffer
    val diagnostics = ListBuffer[Diagnostic]()

    if (component.properties.orders.isEmpty)
      diagnostics += Diagnostic("properties.orders", "At least one order rule has to be specified", SeverityLevel.Error)
    else {
      component.properties.orders.zipWithIndex.foreach {
        case (expr, idx) ⇒
          diagnostics ++= validateSColumn(
            expr.expression,
            s"orders[$idx].expression",
            component,
            Some(ColumnsUsage.WithoutInputAlias)
          )
      }
    }
    diagnostics.toList
  }

  def onChange(oldState: Component, newState: Component)(implicit context: WorkflowContext): Component = {
    val newProps = newState.properties

    val usedCols = getColumnsToHighlight(newProps.orders.map(_.expression), newState)
    var orders = newProps.orders
    orders = orders.map(x ⇒ if (x.sortType.nonEmpty) x else OrderByRule(SColumn(x.expression.expression), "asc"))
    newState.copy(properties =
      newProps.copy(
        columnsSelector = usedCols,
        orders = orders
      )
    )
  }

  override def deserializeProperty(props: String): PropertiesType = Json.parse(props).as[PropertiesType]

  override def serializeProperty(props: PropertiesType): String = Json.stringify(Json.toJson(props))

  class OrderByCode(props: PropertiesType) extends ComponentCode {
    def apply(spark: SparkSession, in: DataFrame): DataFrame = {

      val orderRules = props.orders.map(x ⇒
        x.sortType match {
          case "asc" ⇒ x.expression.column.asc
          case _ ⇒ x.expression.column.desc
        }
      )

      val out = in.orderBy(orderRules: _*)
      out
    }
  }
}
