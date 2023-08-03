package io.prophecy.scalagems.transform

import io.prophecy.gems._
import io.prophecy.gems.componentSpec._
import io.prophecy.gems.dataTypes._
import io.prophecy.gems.diagnostics._
import io.prophecy.gems.uiSpec._
import org.apache.spark.sql.{DataFrame, SparkSession}
import play.api.libs.json.{Format, Json}

class WindowFunction extends ComponentSpec {

  val name: String = "WindowFunction"
  val category: String = "Transform"
  val gemDescription: String = "Define a WindowSpec and apply Window functions on a DataFrame."
  val docUrl: String = "https://docs.prophecy.io/low-code-spark/gems/transform/window-function"
  type PropertiesType = WindowFunctionProperties
  override def optimizeCode: Boolean = true

  case class WindowFunctionProperties(
    @Property("Last open tab")
    activeTab: String = "windowPartition",
    @Property("Columns selector")
    columnsSelector: List[String] = Nil,
    @Property("foo")
    partitionColumns: List[SColumn] = Nil,
    @Property("foo")
    orderColumns: List[OrderByRule] = Nil,
    @Property("foo")
    expressionColumns: List[SColumnExpression] = Nil,
    @Property("")
    specifyFrame: Boolean = false,
    @Property("")
    frameType: String = "row",
    @Property("")
    frameStart: Option[String] = None,
    @Property("")
    frameEnd: Option[String] = None,
    @Property("")
    userSpecifiedStart: Option[String] = None,
    @Property("")
    userSpecifiedEnd: Option[String] = None
  ) extends ComponentProperties

  @Property("Order Expression Helper")
  case class OrderByRule(
    @Property("Full expression") expression: SColumn,
    @Property("Sort") sortType: String
  )

  implicit val orderByRuleFormat: Format[OrderByRule] = Json.format
  implicit val formatWindowFunctionProperties: Format[WindowFunctionProperties] = Json.format

  def dialog: Dialog = {
    val selectBox = SelectBox("")
      .addOption("Ascending", "asc")
      .addOption("Descending", "desc")

    val columns = List(
      Column(
        "Order Columns",
        "expression.expression",
        Some(
          ExpressionBox(ignoreTitle = true)
            .bindLanguage("${record.expression.format}")
            .withSchemaSuggestions()
            .bindPlaceholders()
        )
      ),
      Column("Sort", "sortType", Some(selectBox), width = "25%")
    )

    val orderTable = BasicTable("Test", columns = columns)
      .bindProperty("orderColumns")

    val partitiontable =
      BasicTable(
        "Test",
        columns = List(
          Column(
            "Partition Column",
            "expression",
            Some(
              ExpressionBox(ignoreTitle = true)
                .bindLanguage("${record.format}")
                .withSchemaSuggestions()
                .bindPlaceholders()
            )
          )
        )
      ).bindProperty("partitionColumns")

    val portSchemaTabs = PortSchemaTabs(
      allowInportRename = true,
      selectedFieldsProperty = Some("columnsSelector"),
      singleColumnClickCallback = Some { (portId: String, column: String, state: Any) ⇒
        val st = state.asInstanceOf[Component]
        st.copy(properties = st.properties.activeTab match {
          case "windowPartition" ⇒
            st.properties.copy(
              columnsSelector = st.properties.columnsSelector ::: List(s"$portId##$column"),
              partitionColumns =
                st.properties.partitionColumns ::: List(SColumn(s"""col("${sanitizedColumn(column)}")"""))
            )
          case "windowOrder" ⇒
            st.properties.copy(
              columnsSelector = st.properties.columnsSelector ::: List(s"$portId##$column"),
              orderColumns = st.properties.orderColumns ::: List(
                OrderByRule(SColumn(s"""col("${sanitizedColumn(column)}")"""), "asc")
              )
            )
          case "windowUse" ⇒
            st.properties.copy(
              columnsSelector = st.properties.columnsSelector ::: List(s"$portId##$column"),
              expressionColumns = st.properties.expressionColumns ::: List(
                SColumnExpression(column, "row_number()")
              )
            )
        })
      },
      allColumnsSelectionCallback = Some { (portId: String, state: Any) ⇒
        val st = state.asInstanceOf[Component]
        val columnsInSchema = getColumnsInSchema(portId, st, SchemaFields.TopLevel)
        st.properties.activeTab match {
          case "windowPartition" ⇒
            val updatedPartitionColumns = st.properties.partitionColumns ::: columnsInSchema.map { x ⇒
              SColumn(s"""col("${sanitizedColumn(x)}")""")
            }
            st.copy(properties = st.properties.copy(partitionColumns = updatedPartitionColumns))
          case "windowOrder" ⇒
            val updatedOrderColumns = st.properties.orderColumns ::: columnsInSchema.map { x ⇒
              OrderByRule(SColumn(s"""col("${sanitizedColumn(x)}")"""), "asc")
            }
            st.copy(properties = st.properties.copy(orderColumns = updatedOrderColumns))
          case "windowUse" ⇒
            val updatedExpressionColumns = st.properties.expressionColumns ::: columnsInSchema.map { x ⇒
              SColumnExpression(x, "row_number()")
            }
            st.copy(properties = st.properties.copy(expressionColumns = updatedExpressionColumns))
        }
      }
    ).importSchema()
    val rowStartColumn = StackLayout(height = Some("100%"))
      .addElement(
        SelectBox("Start")
          .addOption("Unbounded Preceding", "unboundedPreceding")
          .addOption("Current Row", "currentRow")
          .addOption("Row Number", "userPreceding")
          .bindProperty("frameStart")
      )
      .addElement(
        Condition()
          .ifEqual(PropExpr("component.properties.frameStart"), StringExpr("userPreceding"))
          .then(TextBox("Number of rows").bindPlaceholder("-1").bindProperty("userSpecifiedStart"))
      )
    val rowEndColumn = StackLayout(height = Some("100%"))
      .addElement(
        SelectBox("End")
          .addOption("Unbounded Following", "unboundedFollowing")
          .addOption("Current Row", "currentRow")
          .addOption("Row Number", "userFollowing")
          .bindProperty("frameEnd")
      )
      .addElement(
        Condition()
          .ifEqual(PropExpr("component.properties.frameEnd"), StringExpr("userFollowing"))
          .then(TextBox("Number of rows").bindPlaceholder("1").bindProperty("userSpecifiedEnd"))
      )
    val rangeStartColumn = StackLayout(height = Some("100%"))
      .addElement(
        SelectBox("Start")
          .addOption("Unbounded Preceding", "unboundedPreceding")
          .addOption("Current Row", "currentRow")
          .addOption("Range Value", "userPreceding")
          .bindProperty("frameStart")
      )
      .addElement(
        Condition()
          .ifEqual(PropExpr("component.properties.frameStart"), StringExpr("userPreceding"))
          .then(TextBox("Value Preceding").bindPlaceholder("-1000").bindProperty("userSpecifiedStart"))
      )
    val rangeEndColumn = StackLayout(height = Some("100%"))
      .addElement(
        SelectBox("End")
          .addOption("Unbounded Following", "unboundedFollowing")
          .addOption("Current Row", "currentRow")
          .addOption("Range Value", "userFollowing")
          .bindProperty("frameEnd")
      )
      .addElement(
        Condition()
          .ifEqual(PropExpr("component.properties.frameEnd"), StringExpr("userFollowing"))
          .then(TextBox("Value Following").bindPlaceholder("1000").bindProperty("userSpecifiedEnd"))
      )
    val windowTabs = Tabs()
      .bindProperty("activeTab")
      .addTabPane(
        TabPane("PartitionBy", "windowPartition")
          .addElement(partitiontable)
      )
      .addTabPane(
        TabPane("OrderBy", "windowOrder")
          .addElement(orderTable)
      )
      .addTabPane(
        TabPane("Frame", "windowFrame")
          .addElement(
            StackLayout()
              .addElement(Checkbox("Specify Frame").bindProperty("specifyFrame"))
              .addElement(NativeText("Unselecting above uses default frame specifications."))
              .addElement(
                Condition()
                  .ifEqual(PropExpr("component.properties.specifyFrame"), BooleanExpr(true))
                  .then(
                    StackLayout(height = Some("100%"))
                      .addElement(
                        RadioGroup("Frame Type")
                          .addOption("Row Frame", "row")
                          .addOption("Range Frame", "range")
                          .bindProperty("frameType")
                      )
                      .addElement(TitleElement("Frame Boundaries"))
                      .addElement(
                        Condition()
                          .ifEqual(PropExpr("component.properties.frameType"), StringExpr("row"))
                          .then(ColumnsLayout(gap = Some("1rem")).addColumn(rowStartColumn).addColumn(rowEndColumn))
                          .otherwise(
                            ColumnsLayout(gap = Some("1rem")).addColumn(rangeStartColumn).addColumn(rangeEndColumn)
                          )
                      )
                  )
              )
          )
      )
      .addTabPane(
        TabPane("Window Use", "windowUse")
          .addElement(
            StackLayout(height = Some("100%"))
              .addElement(
                NativeText(
                  "The below expressions are already computed over the defined window spec." +
                    "User is not required to provide .over(window) in the expressions below."
                )
              )
              .addElement(
                ExpTable(
                  "Expressions",
                  targetColumn =
                    Column("Target Column", "target", Some(TextBox("", ignoreTitle = true)), width = "30%"),
                  expressionColumn = Column(
                    "Source Expression",
                    "expression.expression",
                    Some(
                      ExpressionBox(ignoreTitle = true)
                        .bindLanguage("${record.expression.format}")
                        .bindScalaPlaceholder("row_number()")
                        .bindSQLPlaceholder("row_number()")
                        .bindPythonPlaceholder("row_number()")
                        .withSchemaSuggestions()
                    )
                  )
                ).bindProperty("expressionColumns")
              )
          )
      )
    Dialog("WindowFunction")
      .addElement(
        ColumnsLayout(gap = Some("1rem"), height = Some("100%"))
          .addColumn(portSchemaTabs, "2fr")
          .addColumn(windowTabs, "5fr")
      )
  }

  def validate(component: Component)(implicit context: WorkflowContext): List[Diagnostic] = {

    import scala.collection.mutable.ListBuffer
    val diagnostics = ListBuffer[Diagnostic]()

    // validate expressionColumns
    if (component.properties.expressionColumns.isEmpty) {
      diagnostics += Diagnostic(
        s"properties.expressionColumns",
        "At least one expression in required. [Window Use]",
        SeverityLevel.Error
      )
    } else {
      diagnostics ++= validateExpTable(component.properties.expressionColumns, "expressionColumns", component)
        .map(_.appendMessage("[Window Use]"))
    }
    // validate orderColumns
    component.properties.orderColumns.zipWithIndex.foreach {
      case (expr, idx) ⇒
        diagnostics ++= validateSColumn(
          expr.expression,
          s"orderColumns[$idx].expression",
          component
        )
          .map(_.appendMessage("[OrderBy]"))
    }
    // validate partitionColumns
    component.properties.partitionColumns.zipWithIndex.foreach {
      case (expr, idx) ⇒
        diagnostics ++= validateSColumn(expr, s"partitionColumns[$idx]", component).map(
          _.appendMessage("[PartitionBy]")
        )
    }
    // validate frames
    component.properties.specifyFrame match {
      case true ⇒
        if (component.properties.frameType == "range" && component.properties.orderColumns.isEmpty) {
          diagnostics += Diagnostic(
            "properties.userSpecifiedStart",
            "A range window frame cannot be used in an unordered window specification. [Frame]",
            SeverityLevel.Error
          )
        } else {
          if (component.properties.frameStart.isEmpty) {
            diagnostics += Diagnostic(
              "properties.frameStart",
              "Frame start boundary has to be specified [Frame]",
              SeverityLevel.Error
            )
          } else if (component.properties.frameStart.get == "userPreceding") {
            getLongOption(component.properties.userSpecifiedStart.getOrElse("").trim) match {
              case None ⇒
                diagnostics += Diagnostic(
                  "properties.userSpecifiedStart",
                  "Frame start boundary has to be a Long [Frame]",
                  SeverityLevel.Error
                )
              case Some(value) ⇒
            }
          }

          if (component.properties.frameEnd.isEmpty) {
            diagnostics += Diagnostic(
              "properties.frameEnd",
              "Frame end boundary has to be specified [Frame]",
              SeverityLevel.Error
            )
          } else if (component.properties.frameEnd.get == "userFollowing") {
            getLongOption(component.properties.userSpecifiedEnd.getOrElse("").trim) match {
              case None ⇒
                diagnostics += Diagnostic(
                  "properties.userSpecifiedEnd",
                  "Frame end boundary has to be a Long [Frame]",
                  SeverityLevel.Error
                )
              case Some(value) ⇒
            }
          }
          if (component.properties.frameStart.isDefined && component.properties.frameEnd.isDefined) {
            import org.apache.spark.sql.expressions.Window
            val a: Option[Long] = component.properties.frameStart.getOrElse("") match {
              case "currentRow" ⇒ Some(0)
              case "unboundedPreceding" ⇒ Some(Window.unboundedPreceding)
              case "userPreceding" ⇒
                if (component.properties.userSpecifiedStart.isDefined)
                  getLongOption(component.properties.userSpecifiedStart.getOrElse("").trim)
                else None
              case _ ⇒ Some(0)
            }
            val b: Option[Long] = component.properties.frameEnd.get match {
              case "currentRow" ⇒ Some(0)
              case "unboundedFollowing" ⇒ Some(Window.unboundedFollowing)
              case "userFollowing" ⇒
                if (component.properties.userSpecifiedEnd.isDefined)
                  getLongOption(component.properties.userSpecifiedEnd.getOrElse("").trim)
                else None
              case _ ⇒ Some(0)
            }
            if (a.isDefined && b.isDefined && a.get > b.get)
              diagnostics += Diagnostic(
                "properties.userSpecifiedStart",
                "Frame start boundary can not lie after the frame end boundary [Frame]",
                SeverityLevel.Error
              )
          }

        }
      case false ⇒
    }

    diagnostics.toList
  }

  def onChange(oldState: Component, newState: Component)(implicit context: WorkflowContext): Component = {
    val newProps = newState.properties

    val usedCols = getColumnsToHighlight(
      newProps.partitionColumns ++ newProps.orderColumns.map(_.expression) ++ newProps.expressionColumns,
      newState
    )

    var updatedprops = newProps
    if (newProps.frameType != oldState.properties.frameType) {
      updatedprops = updatedprops.copy(frameStart = None, frameEnd = None)
    }
    if (newProps.frameStart != oldState.properties.frameStart) {
      updatedprops = updatedprops.copy(userSpecifiedStart = None)
    }
    if (newProps.frameEnd != oldState.properties.frameEnd) {
      updatedprops = updatedprops.copy(userSpecifiedEnd = None)
    }

    newState.copy(properties = updatedprops.copy(columnsSelector = usedCols, activeTab = newProps.activeTab))
  }

  override def deserializeProperty(props: String): PropertiesType = Json.parse(props).as[PropertiesType]

  override def serializeProperty(props: PropertiesType): String = Json.stringify(Json.toJson(props))
  class WindowFunctionCode(props: PropertiesType) extends ComponentCode {

    def apply(spark: SparkSession, in: DataFrame): DataFrame = {
      import org.apache.spark.sql.expressions.{Window, WindowSpec}

      val orderRules = props.orderColumns.map(x ⇒
        x.sortType match {
          case "asc" ⇒ x.expression.column.asc
          case _ ⇒ x.expression.column.desc
        }
      )

      val (fStart, fEnd) = {
        val fSt = props.frameStart.get match {
          case "currentRow" ⇒ Window.currentRow
          case "unboundedPreceding" ⇒ Window.unboundedPreceding
          case _ ⇒ props.userSpecifiedStart.get.toLong
        }
        val fEn = props.frameEnd.get match {
          case "currentRow" ⇒ Window.currentRow
          case "unboundedFollowing" ⇒ Window.unboundedFollowing
          case _ ⇒ props.userSpecifiedEnd.get.toLong
        }
        (fSt, fEn)
      }

      // Partition columns: If partitionColumn not provided, it will apply operation on all records.
      // Order columns: It is required for window functions which depend on the order of rows like
      // LAG / LEAD or FIRST / LAST.

      val partSpec: WindowSpec = props.partitionColumns match {
        case Nil ⇒ Window.partitionBy()
        case _ ⇒ Window.partitionBy(props.partitionColumns.map(_.column): _*)
      }

      val orderSpec: WindowSpec = props.orderColumns match {
        case Nil ⇒ partSpec
        case _ ⇒ partSpec.orderBy(orderRules: _*)
      }

      val windowWithFrame: WindowSpec = if (props.specifyFrame) {
        props.frameType match {
          case "range" ⇒ orderSpec.rangeBetween(fStart, fEnd)
          case "row" ⇒ orderSpec.rowsBetween(fStart, fEnd)
          case _ ⇒ orderSpec
        }
      } else {
        orderSpec
      }

      val res = props.expressionColumns.foldLeft(in) {
        case (df, colExp) ⇒
          df.withColumn(colExp.target, colExp.unaliasedColumn.over(windowWithFrame))
      }
      res
    }
  }
}
