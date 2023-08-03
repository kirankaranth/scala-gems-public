package io.prophecy.scalagems.transform

import io.prophecy.gems._
import io.prophecy.gems.uiSpec._
import io.prophecy.gems.componentSpec._
import io.prophecy.gems.dataTypes._
import io.prophecy.gems.diagnostics._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import play.api.libs.json.{Format, Json}

class Deduplicate extends ComponentSpec {

  val name: String = "Deduplicate"
  val category: String = "Transform"
  val gemDescription: String = "Removes rows with duplicate values of specified columns."
  val docUrl: String = "https://docs.prophecy.io/low-code-spark/gems/transform/deduplicate"

  type PropertiesType = DeduplicateProperties
  override def optimizeCode: Boolean = true

  case class DeduplicateProperties(
    @Property("Columns selector")
    columnsSelector: List[String] = Nil,
    @Property("Deduplication Type", "")
    dedupType: String = "any",
    @Property("Deduplicate", "Column to deduplicate by")
    dedupColumns: List[StringColName] = Nil,
    @Property("useOrderBy", "Use custom order by")
    useOrderBy: Option[Boolean] = Some(false),
    @Property("Order By Rules", "List of conditions to order dataframes on")
    orders: Option[List[OrderByRule]] = Some(Nil)
  ) extends ComponentProperties

  @Property("String Column")
  case class StringColName(
    @Property("Column name")
    colName: String
  )

  @Property("Order Expression Helper")
  case class OrderByRule(
    @Property("Full expression") expression: SColumn,
    @Property("Sort") sortType: String
  )

  implicit val stringColNameFormat: Format[StringColName] = Json.format
  implicit val orderByRuleFormat: Format[OrderByRule] = Json.format
  implicit val deduplicatePropertiesFormat: Format[DeduplicateProperties] = Json.format

  def dialog: Dialog = {

    val orderBySelectBox = SelectBox("")
      .addOption("Ascending", "asc")
      .addOption("Descending", "desc")

    val columns = List(
      Column(
        "Order Columns",
        "expression.expression",
        Some(
          ExpressionBox(ignoreTitle = true)
            .bindPlaceholders()
            .bindLanguage("${record.expression.format}")
            .withSchemaSuggestions()
        )
      ),
      Column("Sort", "sortType", Some(orderBySelectBox), width = "25%")
    )

    val orderByTable = BasicTable("OrderByTable", height = Some("300px"), columns = columns)
      .bindProperty("orders")

    val selectBox = SelectBox("Row to keep")
      .addOption("Distinct Rows", "distinct")
      .addOption("Unique Only", "unique_only")
      .addOption("Last", "last")
      .addOption("First", "first")
      .addOption("Any", "any")
      .bindProperty("dedupType")

    val testTable =
      BasicTable(
        "Test",
        height = Some("300px"),
        columns = List(
          io.prophecy.gems.uiSpec.Column(
            "Deduplicate Columns",
            "colName",
            Some(
              TextBox("", ignoreTitle = true)
                .disabled()
            )
          )
        )
      ).bindProperty("dedupColumns")

    Dialog("Deduplicate")
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
                    dedupColumns = st.properties.dedupColumns ::: StringColName(column) :: Nil
                  )
                )
              },
              allColumnsSelectionCallback = Some { (portId: String, state: Any) ⇒
                val st = state.asInstanceOf[Component]
                val columnsInSchema = getColumnsInSchema(portId, st, SchemaFields.TopLevel)
                val updatedDedupColumns = st.properties.dedupColumns ::: columnsInSchema.map(x ⇒ StringColName(x))
                st.copy(properties = st.properties.copy(dedupColumns = updatedDedupColumns))
              }
            ).importSchema()
          )
          .addColumn(
            StackLayout(height = Some("100%"))
              .addElement(selectBox)
              .addElement(
                Condition()
                  .ifNotEqual(PropExpr("component.properties.dedupType"), StringExpr("distinct"))
                  .then(testTable)
              )
              .addElement(
                Condition()
                  .ifEqual(PropExpr("component.properties.dedupType"), StringExpr("first"))
                  .then(
                    StackLayout()
                      .addElement(Checkbox("Use Custom Order By").bindProperty("useOrderBy"))
                      .addElement(
                        Condition()
                          .ifEqual(PropExpr("component.properties.useOrderBy"), BooleanExpr(true))
                          .then(orderByTable)
                      )
                  )
                  .otherwise(
                    Condition()
                      .ifEqual(PropExpr("component.properties.dedupType"), StringExpr("last"))
                      .then(
                        StackLayout(height = Some("100%"))
                          .addElement(Checkbox("Use Custom Order By").bindProperty("useOrderBy"))
                          .addElement(
                            Condition()
                              .ifEqual(PropExpr("component.properties.useOrderBy"), BooleanExpr(true))
                              .then(orderByTable)
                          )
                      )
                  )
              ),
            "2fr"
          )
      )
  }

  def validate(component: Component)(implicit context: WorkflowContext): List[Diagnostic] = {
    import scala.collection.mutable.ListBuffer
    val diagnostics = ListBuffer[Diagnostic]()
    if (component.properties.dedupType != "distinct") {
      if (component.properties.dedupColumns.isEmpty)
        diagnostics += Diagnostic(
          "properties.dedupColumns",
          "At least one rule has to be specified",
          SeverityLevel.Error
        )
      else {
        component.properties.dedupColumns.zipWithIndex.foreach {
          case (col, idx) ⇒
            if (col.colName.contains("."))
              diagnostics += Diagnostic(
                s"properties.dedupColumns[$idx].colName",
                "Deduplication can not be done for nested columns. Please flatten your schema before deduplicating using a nested column.",
                SeverityLevel.Error
              )
        }
      }

      if (component.properties.useOrderBy.getOrElse(false)) {
        component.properties.orders match {
          case None | Some(Nil) =>
            diagnostics += Diagnostic(
              "properties.orders",
              "At least one order rule has to be specified",
              SeverityLevel.Error
            )
          case Some(orders) =>
            orders.zipWithIndex.foreach {
              case (expr, idx) ⇒
                diagnostics ++= validateSColumn(
                  expr.expression,
                  s"orders[$idx].expression",
                  component,
                  Some(ColumnsUsage.WithoutInputAlias)
                )
            }
        }
      }
    }
    diagnostics.toList
  }

  def onChange(oldState: Component, newState: Component)(implicit context: WorkflowContext): Component = {
    val newProps = newState.properties
    val usedCols = getColumnsToHighlight(newProps.dedupColumns.map(_.colName), newState)
    var useOrderByFlag = newProps.useOrderBy
    if (newProps.dedupType == "any" || newProps.dedupType == "unique_only") {
      useOrderByFlag = Some(false)
    }

    newState.copy(properties =
      newProps.copy(
        columnsSelector = usedCols,
        useOrderBy = useOrderByFlag
      )
    )
  }

  override def deserializeProperty(props: String): PropertiesType = Json.parse(props).as[PropertiesType]

  override def serializeProperty(props: PropertiesType): String = Json.stringify(Json.toJson(props))

  class DeduplicateCode(props: PropertiesType) extends ComponentCode {

    /**
      * Method for Deduplicate operation when rows to be kept in each group of rows to be either first, Last or
      * unique-only. It does first groupBy on all passed groupByColumns and then depending on typeToKeep value it
      * does further operations.
      *
      * For both first and last option, it adds new temporary row_number column which returns the row number within a
      * group of rows grouped by groupByColumns. Then to find first records it simply filters out all rows with
      * row_number as 1. To find last records within each group it also computes the count value for each group
      * and filters out all the records where row_number is same as group count
      *
      * For unique-only case it adds new temporary count column which returns the count of rows within a window
      * partition. Then it filters the resultant dataframe with count value 1.
      *
      * @param typeToKeep     option to find kind of rows. Possible values are first, last and unique-only
      * @param groupByColumns columns to be used to group input records.
      * @return DataFrame with first or last or unique-only records in each grouping of input records.
      */
    def apply(spark: SparkSession, in: DataFrame): DataFrame = {
      val df = if (props.dedupType == "distinct") {
        in.distinct()
      } else {
        import org.apache.spark.sql.expressions.Window

        val typeToKeep = props.dedupType
        val groupByColumns = props.dedupColumns.map(_.colName)
        val orderRules = if (props.useOrderBy.contains(true)) {
          props.orders.get.map(x ⇒
            x.sortType match {
              case "asc" ⇒ x.expression.column.asc
              case _ ⇒ x.expression.column.desc
            }
          )
        } else {
          List(lit(1))
        }

        val window = Window
          .partitionBy(groupByColumns.head, groupByColumns.tail: _*)
          .orderBy(orderRules: _*)
        val windowForCount = Window
          .partitionBy(groupByColumns.head, groupByColumns.tail: _*)

        typeToKeep match {
          case "any" ⇒
            val columns =
              if (groupByColumns.isEmpty) in.columns.toList else groupByColumns
            in.dropDuplicates(columns)
          case "first" ⇒
            val dataFrameWithRowNumber =
              in.withColumn("row_number", row_number().over(window))
            dataFrameWithRowNumber
              .filter(col("row_number") === lit(1))
              .drop("row_number")
          case "last" ⇒
            val dataFrameWithRowNumber = in
              .withColumn("row_number", row_number().over(window))
              .withColumn("count", count("*").over(windowForCount))
            dataFrameWithRowNumber
              .filter(col("row_number") === col("count"))
              .drop("row_number")
              .drop("count")

          case "unique_only" ⇒
            val dataFrameWithCount =
              in.withColumn("count", count("*").over(windowForCount))
            dataFrameWithCount.filter(col("count") === lit(1)).drop("count")
        }
      }
      df
    }
  }
}
