package io.prophecy.scalagems.join

import io.prophecy.gems._
import io.prophecy.gems.componentSpec._
import io.prophecy.gems.dataTypes._
import io.prophecy.gems.diagnostics._
import io.prophecy.gems.uiSpec._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import play.api.libs.json.{Format, Json}

class Join extends ComponentSpec {

  val name: String = "Join"
  val category: String = "Join/Split"
  val gemDescription: String = "Joins two or more dataframes"
  val docUrl: String = "https://docs.prophecy.io/low-code-spark/gems/join-split/join/"
  type PropertiesType = JoinProperties
  override def optimizeCode: Boolean = true

  case class JoinProperties(
    @Property("Last open tab")
    activeTab: String = "conditions",
    @Property("Columns selector")
    columnsSelector: List[String] = Nil,
    @Property("Join Conditions", "List of conditions to join dataframes on")
    conditions: List[JoinCondition] = List(JoinCondition("in1", SColumn(""), "inner")),
    @Property("Expressions", "List of all the expressions")
    expressions: List[SColumnExpression] = Nil,
    @Property("First Port's alias")
    headAlias: String = "in0",
    @Property("Where clause", "Conditions to filter the data before moving downstream")
    whereClause: Option[SColumn] = None,
    @Property("Hints", "List of hints")
    hints: Option[List[Hint]] = Some(List(Hint("in0", "in0", "none", false), Hint("in1", "in1", "none", false)))
  ) extends ComponentProperties

  @Property("Join Condition Helper")
  case class JoinCondition(
    @Property("Alias column")
    alias: String,
    @Property("Full expression")
    expression: SColumn,
    @Property("Description")
    joinType: String
  )

  @Property("Hint")
  case class Hint(
    @Property("Id")
    id: String,
    @Property("Input port")
    alias: String,
    @Property("Hint")
    hintType: String,
    @Property("Propagate Columns")
    propagateColumns: Boolean
  )

  implicit val hintFormat: Format[Hint] = Json.format
  implicit val joinConditionFormat: Format[JoinCondition] = Json.format
  implicit val joinPropertiesFormat: Format[JoinProperties] = Json.format

  def dialog: Dialog = {
    val selectBox = SelectBox("")
      .addOption("Inner", "inner")
      .addOption("Outer", "outer")
      .addOption("Left Outer", "left_outer")
      .addOption("Right Outer", "right_outer")
      .addOption("Left Semi", "left_semi")
      .addOption("Left Anti", "left_anti")

    val hintsSelectBox = SelectBox("")
      .addOption("None", "none")
      .addOption("Broadcast", "broadcast")
      .addOption("Merge", "merge")
      .addOption("Shuffle Hash", "shuffle_hash")
      .addOption("Shuffle Replicate NL", "shuffle_replicate_nl")

    val hintsTable = BasicTable(
      "Test",
      columns = List(
        Column("Input Alias", "alias", width = "25%"),
        Column("Hint Type", "hintType", Some(hintsSelectBox), width = "80%"),
        Column(
          "Propagate All Columns",
          "propagateColumns",
          Some(Checkbox("").bindProperty("record.propagateColumns")),
          width = "80%"
        )
      ),
      targetColumnKey = Some("alias"),
      delete = false,
      appendNewRow = false
    ).bindProperty("hints")

    val testTable = BasicTable(
      "Test",
      columns = List(
        Column("Input Alias", "alias", width = "25%"),
        Column(
          "Join Condition",
          "expression.expression",
          Some(
            ExpressionBox(ignoreTitle = true)
              .bindLanguage("${record.expression.format}")
              .withSchemaSuggestions()
              .bindScalaPlaceholder("""col("in0.source_column") === col("in1.other_column")""")
              .bindPythonPlaceholder("""col("in0.source_column") == col("in1.other_column")""")
              .bindSQLPlaceholder("""in0.source_column = in1.other_column""")
          )
        ),
        Column("Type", "joinType", Some(selectBox), width = "20%")
      ),
      targetColumnKey = Some("alias"),
      delete = false,
      appendNewRow = false
    ).bindProperty("conditions")

    Dialog("Join")
      .addElement(
        ColumnsLayout(gap = Some("1rem"), height = Some("100%"))
          .addColumn(
            PortSchemaTabs(
              editableInput = Some(true),
              minNumberOfPorts = 2,
              selectedFieldsProperty = Some("columnsSelector"),
              singleColumnClickCallback = Some { (portId: String, column: String, state: Any) ⇒
                val st = state.asInstanceOf[Component]
                st.copy(properties = st.properties.activeTab match {
                  case "expressions" ⇒
                    val portSlug: String = st.ports.inputs.filter(_.id == portId).map(_.slug).head
                    st.properties.copy(
                      columnsSelector = st.properties.columnsSelector ::: List(s"$portId##$column"),
                      expressions = st.properties.expressions ::: List(
                        SColumnExpression(column, s"""col("$portSlug.${sanitizedColumn(column)}")""")
                      )
                    )
                  case _ ⇒ st.properties
                })
              },
              allColumnsSelectionCallback = Some { (portId: String, state: Any) ⇒
                val st = state.asInstanceOf[Component]
                val portSlug: String = st.ports.inputs.filter(_.id == portId).map(_.slug).head

                val columnsInSchema = getColumnsInSchema(portId, st, SchemaFields.TopLevel)
                st.properties.activeTab match {
                  case "expressions" ⇒
                    val updatedExpressions = st.properties.expressions ::: columnsInSchema.map { x ⇒
                      SColumnExpression(x, s"""col("$portSlug.${sanitizedColumn(x)}")""")
                    }
                    st.copy(properties = st.properties.copy(expressions = updatedExpressions))
                  case _ ⇒ st
                }
              }
            ).importSchema()
          )
          .addColumn(
            Tabs()
              .bindProperty("activeTab")
              .addTabPane(
                TabPane("Conditions", "conditions")
                  .addElement(
                    StackLayout(height = Some("100%"))
                      .addElement(testTable)
                      .addElement(TitleElement("Where Clause"))
                      .addElement(
                        Editor(height = Some("20%"))
                          .makeFieldOptional()
                          .withSchemaSuggestions()
                          .bindProperty("whereClause")
                      )
                  )
              )
              .addTabPane(
                TabPane("Expressions", "expressions")
                  .addElement(
                    ExpTable("Expressions")
                      .bindProperty("expressions")
                  )
              )
              .addTabPane(
                TabPane("Advanced", "advanced")
                  .addElement(
                    hintsTable
                  )
              ),
            "2fr"
          )
      )
  }

  def validate(component: Component)(implicit context: WorkflowContext): List[Diagnostic] = {
    import scala.collection.mutable.ListBuffer

    val diagnostics = ListBuffer[Diagnostic]()
    if (component.ports.inputs.size < 2) {
      // todo
      diagnostics += Diagnostic(
        "ports.inputs",
        "Input ports can't be less than two in Join transformation",
        SeverityLevel.Error
      )
    } else {
      if (component.properties.conditions.nonEmpty) {
        component.properties.conditions.zipWithIndex.foreach {
          case (expr, idx) ⇒
            diagnostics ++= validateSColumn(
              expr.expression,
              s"conditions[$idx].expression",
              component,
              Some(ColumnsUsage.WithAndWithoutInputAlias)
            ).map(_.appendMessage("[Conditions]"))
        }

      }

      if (component.properties.whereClause.isDefined && component.properties.whereClause.get.expression.isEmpty)
        diagnostics += Diagnostic(
          "properties.whereClause.expression",
          s"""Unsupported expression ${component.properties.whereClause
            .map(_.expression)
            .getOrElse("")} [Conditions]""",
          SeverityLevel.Error
        )

      diagnostics ++= validateExpTable(
        component.properties.expressions,
        "expressions",
        component,
        Some(ColumnsUsage.WithAndWithoutInputAlias)
      ).map(_.appendMessage("[Expressions]"))
    }
    diagnostics.toList
  }

  def onChange(oldState: Component, newState: Component)(implicit context: WorkflowContext): Component = {
    import scala.collection.mutable.ListBuffer
    val oldConditions = oldState.properties.conditions
    var conditionsAfterChangeApplication = oldConditions

    val slugChanged: List[(NodePort, Int)] = oldState.ports.inputs.zipWithIndex.filter {
      case (oldPort, _) ⇒
        newState.ports.inputs.exists(newPort ⇒ (oldPort.id == newPort.id) && (oldPort.slug != newPort.slug))
    }

    slugChanged.foreach {
      case (port, idx) ⇒
        if (idx - 1 >= 0) {
          val newAlias = newState.ports.inputs.filter(newPort ⇒ newPort.id == port.id).head.slug
          conditionsAfterChangeApplication = conditionsAfterChangeApplication.updated(
            idx - 1,
            conditionsAfterChangeApplication(idx - 1).copy(alias = newAlias)
          )
        }
    }

    val deletedPorts: List[(NodePort, Int)] = oldState.ports.inputs.zipWithIndex.filterNot {
      case (oldPort, _) ⇒ newState.ports.inputs.map(newPort ⇒ newPort.id) contains oldPort.id
    }
    val conditionIndicesToDelete =
      deletedPorts.map(x ⇒ if ((x._2 - 1) >= 0) x._2 - 1 else 0).distinct.sorted(Ordering[Int].reverse)

    conditionIndicesToDelete.foreach(idx ⇒
      conditionsAfterChangeApplication = conditionsAfterChangeApplication.patch(idx, Nil, 1)
    )

    val addedPorts: List[NodePort] =
      newState.ports.inputs.filterNot(newPort ⇒ oldState.ports.inputs.map(x ⇒ x.id) contains newPort.id)

    val conditionsToAdd = ListBuffer[JoinCondition]()
    var lastSlug = newState.ports.inputs.takeRight(2).head.slug
    addedPorts.foreach { addedPort ⇒
      conditionsToAdd += JoinCondition(
        addedPort.slug,
        SColumn(s"""col("${addedPort.slug}.id") === col("$lastSlug.id")"""),
        "inner"
      )
      lastSlug = addedPort.slug
    }

    conditionsAfterChangeApplication = conditionsAfterChangeApplication ++ conditionsToAdd

    var hintsAfterChangeApplication = List[Hint]()

    for (newport ← newState.ports.inputs) {
      var flag = true
      for (hint ← newState.properties.hints.get) {
        if (newport.id == hint.id) {
          hintsAfterChangeApplication =
            hintsAfterChangeApplication :+ Hint(newport.id, newport.slug, hint.hintType, hint.propagateColumns)
          flag = false
        }
      }
      if (flag) {
        hintsAfterChangeApplication = hintsAfterChangeApplication :+ Hint(newport.id, newport.slug, "none", false)
      }
    }

    val newProps = newState.properties

    val usedCols =
      getColumnsToHighlight(
        newProps.conditions.map(_.expression),
        newState,
        aliasUse = ColumnsUsage.WithAndWithoutInputAlias
      ) ++ getColumnsToHighlight(
        newProps.expressions,
        newState,
        ColumnsUsage.WithAndWithoutInputAlias
      )

    val finalConditions = conditionsAfterChangeApplication match {
      case `oldConditions` ⇒ newState.properties.conditions
      case _ ⇒ conditionsAfterChangeApplication
    }

    val updatedWhereClause = newProps.whereClause.map(_.expression.trim) match {
      case None | Some("") ⇒ None
      case Some(_) ⇒ newProps.whereClause
    }
    newState.copy(properties =
      newProps.copy(
        columnsSelector = usedCols,
        expressions = newProps.expressions,
        conditions = finalConditions,
        activeTab = newProps.activeTab,
        headAlias = newState.ports.inputs.head.slug,
        whereClause = updatedWhereClause,
        hints = Some(hintsAfterChangeApplication)
      )
    )
  }

  override def deserializeProperty(props: String): PropertiesType = Json.parse(props).as[PropertiesType]

  override def serializeProperty(props: PropertiesType): String = Json.stringify(Json.toJson(props))

  class JoinCode(props: PropertiesType)(implicit context: WorkflowContext) extends ComponentCode {

    def apply(spark: SparkSession, in0: DataFrame, in1: DataFrame, inDFs: DataFrame*): DataFrame = {
      val _inputs = in1 +: inDFs
      var res = props.hints match {
        case None ⇒ in0.as(props.headAlias)
        case Some(value) ⇒
          value.head.hintType match {
            case "none" ⇒ in0.as(props.headAlias)
            case _ ⇒ in0.as(props.headAlias).hint(value.head.hintType)
          }
      }

      val propagate_cols = props.hints match {
        case None ⇒ props.expressions.columns
        case Some(value) ⇒
          props.expressions.columns ++ value.filter(x ⇒ x.propagateColumns).map(x ⇒ col(x.alias + ".*"))
      }

      props.hints match {
        case None ⇒
          val inputConditionPair = _inputs.zip(props.conditions)
          inputConditionPair.foreach {
            case (inPort, _condition) ⇒
              val nextDF = inPort.as(_condition.alias)
              res = res.join(nextDF, _condition.expression.column, _condition.joinType)
          }
        case Some(value) ⇒
          val inputConditionPair = _inputs.zip(props.conditions).zip(value.tail)
          inputConditionPair.foreach {
            case ((inPort, _condition), _hint) ⇒
              val nextDF = _hint.hintType match {
                case "none" ⇒ inPort.as(_condition.alias)
                case _ ⇒ inPort.as(_condition.alias).hint(_hint.hintType)
              }
              res = res.join(nextDF, _condition.expression.column, _condition.joinType)
          }
      }

      val resFiltered = props.whereClause match {
        case None ⇒ res
        case Some(filterCondition) ⇒ res.where(filterCondition.column)
      }

      val out = props.expressions match {
        case Nil ⇒ resFiltered
        case _ ⇒ resFiltered.select(propagate_cols: _*)
      }

      out
    }
  }
}
