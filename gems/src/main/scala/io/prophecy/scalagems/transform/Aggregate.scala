package io.prophecy.scalagems.transform

import io.prophecy.gems._
import io.prophecy.gems.componentSpec._
import io.prophecy.gems.dataTypes._
import io.prophecy.gems.diagnostics._
import io.prophecy.gems.uiSpec._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import play.api.libs.json.{Format, Json}

class Aggregate extends ComponentSpec {

  val name: String = "Aggregate"
  val category: String = "Transform"
  val gemDescription: String = "Performs a Group-by and Aggregate Operation"
  val docUrl: String = "https://docs.prophecy.io/low-code-spark/gems/transform/aggregate/"
  type PropertiesType = AggregateProperties

  override def optimizeCode: Boolean = true

  case class AggregateProperties(
    @Property("Last open tab")
    activeTab: String = "aggregate",
    @Property("Columns selector")
    columnsSelector: List[String] = Nil,
    @Property("Group by expressions", "List of all the group by expressions")
    groupBy: List[SColumnExpression] = Nil,
    @Property("Aggregate expressions", "List of all the aggregate expressions")
    aggregate: List[SColumnExpression] = Nil,
    @Property("Do pivot", "Whether pivot should be performed")
    doPivot: Boolean = false,
    // todo https://github.com/SimpleDataLabsInc/prophecy/issues/5512
    @Property("Pivot column", "Column to perform the pivot on")
    pivotColumn: Option[SColumn] = None,
    @Property("Aggregate expressions", "Column values to pivot on")
    pivotValues: List[StringColName] = Nil,
    @Property("flag to allow/disallow selection", "user will see 'clicking-ux' if this flag is true")
    allowSelection: Option[Boolean] = Some(true),
    @Property("Flag to propagate all columns", "Flag to propagate all columns")
    allIns: Option[Boolean] = Some(false)
  ) extends ComponentProperties

  @Property("String Wrapper")
  case class StringColName(
    @Property("Sort") colName: String
  )

  implicit val stringColNameFormat: Format[StringColName] = Json.format
  implicit val aggregatePropertiesFormat: Format[AggregateProperties] = Json.format

  def dialog: Dialog = {
    import scala.collection.mutable
    Dialog("Aggregate")
      .addElement(
        ColumnsLayout(gap = Some("1rem"), height = Some("100%"))
          .addColumn(
            PortSchemaTabs(
              selectedFieldsProperty = Some("columnsSelector"),
              singleColumnClickCallback = Some { (portId: String, column: String, state: Any) ⇒
                val st = state.asInstanceOf[Component]
                st.copy(properties = st.properties.activeTab match {
                  case "groupBy" ⇒
                    st.properties.copy(
                      columnsSelector = st.properties.columnsSelector ::: List(s"$portId##$column"),
                      groupBy = st.properties.groupBy ::: List(SColumnExpression(sanitizedColumn(column))),
                      allowSelection = Some(true)
                    )
                  case "aggregate" ⇒
                    st.properties.copy(
                      columnsSelector = st.properties.columnsSelector ::: List(s"$portId##$column"),
                      aggregate = st.properties.aggregate ::: List(
                        SColumnExpression(column, s"""first(col("${sanitizedColumn(column)}"))""")
                      ),
                      allowSelection = Some(true)
                    )
                  case _ ⇒
                    st.properties.copy(allowSelection = Some(false))
                })
              },
              allColumnsSelectionCallback = Some { (portId: String, state: Any) ⇒
                val st = state.asInstanceOf[Component]
                val columnsInSchema = getColumnsInSchema(portId, st, SchemaFields.TopLevel)
                st.properties.activeTab match {
                  case "groupBy" ⇒
                    val updatedGroupbyRules = st.properties.groupBy ::: columnsInSchema.map { x ⇒
                      SColumnExpression(x, s"""col("${sanitizedColumn(x)}")""")
                    }
                    st.copy(properties = st.properties.copy(groupBy = updatedGroupbyRules))
                  case "aggregate" ⇒
                    val updatedAggregate = st.properties.aggregate ::: columnsInSchema.map { x ⇒
                      SColumnExpression(x, s"""first(col("${sanitizedColumn(x)}"))""")
                    }
                    st.copy(properties = st.properties.copy(aggregate = updatedAggregate))
                  case _ ⇒ st
                }
              }
            )
              .allowColumnClickBasedOn("allowSelection")
              .importSchema(),
            "2fr"
          )
          .addColumn(
            Tabs()
              .bindProperty("activeTab")
              .addTabPane(
                TabPane("Aggregate", "aggregate").addElement(
                  StackLayout(height = Some("100%"))
                    .addElement(
                      ExpTable("Aggregate Expressions")
                        .bindProperty("aggregate")
                    )
                    .addElement(
                      ColumnsLayout().addColumn(
                        Checkbox("Propagate all input columns").bindProperty(
                          "allIns"
                        )
                      )
                    )
                )
              )
              .addTabPane(
                TabPane("Group By", "groupBy")
                  .addElement(
                    ExpTable("Group By Columns")
                      .bindProperty("groupBy")
                  )
              )
              .addTabPane(
                TabPane("Pivot", "pivot")
                  .addElement(
                    Checkbox("Do pivot")
                      .bindProperty("doPivot")
                  )
                  .addElement(
                    Condition()
                      .ifEqual(PropExpr("component.properties.doPivot"), BooleanExpr(true))
                      .then(
                        StackLayout(gap = Some("1rem"))
                          .addElement(
                            ExpressionBox("Pivot Column")
                              .makeFieldOptional()
                              .withSchemaSuggestions()
                              .bindPlaceholders(
                                mutable
                                  .Map(
                                    "scala" → """col("col_name")""",
                                    "python" → """col("col_name")""",
                                    "sql" → """col_name"""
                                  )
                              )
                              .bindProperty("pivotColumn")
                          )
                          .addElement(
                            BasicTable(
                              "Unique Values",
                              height = Some("400px"),
                              columns = List(
                                Column(
                                  "Unique Values",
                                  "colName",
                                  Some(
                                    TextBox("").bindPlaceholder("Enter value present in pivot column")
                                  )
                                )
                              )
                            )
                              .bindProperty("pivotValues")
                          )
                      )
                  )
              ),
            "5fr"
          )
      )
  }
  def validate(component: Component)(implicit context: WorkflowContext): List[Diagnostic] = {
    import scala.collection.mutable.ListBuffer
    val diagnostics = ListBuffer[Diagnostic]()

    if (component.properties.aggregate.isEmpty) {
      diagnostics += Diagnostic(
        s"properties.aggregate",
        s"At least one aggregate expression is required in Aggregate.",
        SeverityLevel.Error
      )
    } else {
      // validate aggregate tab
      diagnostics ++= validateExpTable(component.properties.aggregate, "aggregate", component)
        .map(_.appendMessage("[Aggregate]"))

      // validate groupby tab
      diagnostics ++= validateExpTable(component.properties.groupBy, "groupBy", component)
        .map(_.appendMessage("[GroupBy]"))

      // validate pivot tab
      if (component.properties.doPivot) {
        if (component.properties.groupBy.isEmpty) {
          diagnostics += Diagnostic(
            s"properties.pivotColumn.expression",
            "Pivot operation is only supported with groupBy operation. Please fill groupBy columns.",
            SeverityLevel.Error
          )
        } else if (component.properties.pivotColumn.isDefined) {
          diagnostics ++= validateSColumn(
            component.properties.pivotColumn.get,
            "pivotColumn",
            component,
            testColumnPresence = Some(ColumnsUsage.WithoutInputAlias)
          ).map(_.appendMessage("[Pivot]"))

          if (!(component.properties.pivotValues.isEmpty)) {
            component.properties.pivotValues.zipWithIndex.foreach {
              case (pivotVal, idx) ⇒
                if (pivotVal.colName.trim.isEmpty) {
                  diagnostics += Diagnostic(
                    s"properties.pivotValues[$idx].colName",
                    "Row cannot be empty. [Pivot]",
                    SeverityLevel.Error
                  )
                }
            }

          }
        } else
          diagnostics += Diagnostic(
            s"properties.pivotColumn.expression",
            "Pivot column cannot be empty. [Pivot]",
            SeverityLevel.Error
          )
      }
    }
    diagnostics.toList
  }

  def onChange(oldState: Component, newState: Component)(implicit context: WorkflowContext): Component = {
    val newProps = newState.properties
    val used = getColumnsToHighlight(newProps.groupBy ++ newProps.aggregate, newState)
    val updatedPivotCol = if (!newProps.doPivot) None else newProps.pivotColumn
    newState.copy(properties =
      newProps.copy(
        columnsSelector = used,
        activeTab = newProps.activeTab,
        pivotColumn = updatedPivotCol,
        allowSelection = Some(newProps.activeTab != "pivot")
      )
    )
  }

  class AggregateCode(props: PropertiesType)(implicit context: WorkflowContext) extends ComponentCode {

    def apply(spark: SparkSession, in: DataFrame): DataFrame = {

      val propagate_cols = props.allIns match {
        case None => props.aggregate.tail.map(x ⇒ x.expression.column.as(x.target))
        case Some(value) =>
          if (value) {
            val agg_cols = props.aggregate.map(x ⇒ x.target)
            val groupBy_cols = if (props.doPivot) {
              props.pivotColumn.get.column.toString() :: props.groupBy.map(x ⇒ x.target)
            } else {
              props.groupBy.map(x ⇒ x.target)
            }
            props.aggregate.tail.map(x ⇒ x.expression.column.as(x.target)) ++ in.columns.toList
              .diff(agg_cols ++ groupBy_cols)
              .map(x ⇒ first(col(x)).as(x))
          } else {
            props.aggregate.tail.map(x ⇒ x.expression.column.as(x.target))
          }
      }
      val out = props.groupBy match {
        case Nil ⇒
          in.agg(
            props.aggregate.head.expression.column.as(props.aggregate.head.target),
            propagate_cols: _*
          )
        case _ ⇒
          val grouped = in.groupBy(props.groupBy.map(x ⇒ x.expression.column.as(x.target)): _*)
          val pivoted = if (props.doPivot) {
            (props.pivotValues.map(_.colName), props.pivotColumn.get.column) match {
              case (Nil, column) ⇒ grouped.pivot(column)
              case (nonNil, column) ⇒ grouped.pivot(column, nonNil)
              case _ ⇒ grouped
            }
          } else {
            grouped
          }
          pivoted.agg(
            props.aggregate.head.expression.column.as(props.aggregate.head.target),
            propagate_cols: _*
          )
      }
      out
    }
  }

  override def deserializeProperty(props: String): PropertiesType = Json.parse(props).as[PropertiesType]

  override def serializeProperty(props: PropertiesType): String = Json.stringify(Json.toJson(props))
}
