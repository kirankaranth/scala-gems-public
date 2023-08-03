package io.prophecy.scalagems.transform

import io.prophecy.gems._
import io.prophecy.gems.uiSpec._
import io.prophecy.gems.componentSpec._
import io.prophecy.gems.dataTypes._
import io.prophecy.gems.diagnostics._

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import play.api.libs.json.{Format, Json}

class FlattenSchema extends ComponentSpec {

  val name: String = "FlattenSchema"
  val category: String = "Transform"
  val gemDescription: String = "Flatten Complex nested datatypes into rows and columns"
  val docUrl: String = "https://docs.prophecy.io/low-code-spark/gems/transform/flatten-schema/"
  type PropertiesType = FlattenSchemaProperties
  override def optimizeCode: Boolean = true

  case class FlattenSchemaProperties(
    @Property("Columns selector")
    columnsSelector: List[String] = Nil,
    @Property("foo", "")
    delimiter: String = "-",
    @Property("Flatten Columns", "List of columns to flatten")
    fsExpressions: List[FlattenSchemaExpression] = Nil,
    @Property("Columns to be exploded", "Set of column names that need to be exploded (ie, are of arrayType)")
    explodeColumns: List[String] = Nil,
    @Property("", "")
    explodedColsNewName: List[ExplodeColInfo] = Nil
  ) extends ComponentProperties

  @Property("Flatten Schema Helper")
  case class ExplodeColInfo(
    @Property("Expression", "Column that has been selected for flattening")
    originalColToExplode: String,
    @Property("Expression", "updated column name that should be used while writing the code")
    updatedColToExplode: String,
    @Property("Expression", "New column's name to refer to if this column gets exploded into a new column")
    colNameAfterExplode: String
  )

  @Property("Flatten Schema Helper")
  case class FlattenSchemaExpression(
    @Property("Target column", "stores the target column name to be used (gets updated with the delimiter)")
    target: String,
    @Property("Expression")
    expression: SColumn,
    @Property(
      "Expression flattened",
      "this replaces column names of parent columns to their new names after explode (if the corresponding parent columns have been exploded)"
    )
    flattenedExpression: String,
    @Property("Exploded")
    exploded: String,
    @Property(
      "Target tokens",
      "list of column names at every level in case of a nested colum name. Useful for coming up with the target column name when delimiter changes"
    )
    targetTokens: List[String]
  )

  implicit val explodeColInfoFormat: Format[ExplodeColInfo] = Json.format
  implicit val flattenSchemaExpressionFormat: Format[FlattenSchemaExpression] = Json.format
  implicit val flattenSchemaProperties: Format[FlattenSchemaProperties] = Json.format

  def dialog: Dialog = Dialog("FlattenSchema")
    .addElement(
      ColumnsLayout(gap = Some("1rem"), height = Some("100%"))
        .addColumn(
          PortSchemaTabs(
            selectedFieldsProperty = Some("columnsSelector"),
            singleColumnClickCallback = Some { (portId: String, column: String, state: Any) ⇒
              val st = state.asInstanceOf[Component]
              val tokensOfUsedTargetNames = st.properties.fsExpressions.map(_.targetTokens)
              val targetColTokens = getTargetTokens(column, tokensOfUsedTargetNames)
              val colToExplode = getColumnsToExplode(
                Nil,
                column.split('.').toList,
                schemaOption = st.ports.inputs.map(_.schema).head
              )
              val exploded = if (colToExplode.nonEmpty) "\u2713" else ""
              st.copy(properties =
                st.properties.copy(
                  columnsSelector = getColumnsToHighlight(st.properties.fsExpressions.map(_.expression), st),
                  fsExpressions = st.properties.fsExpressions ::: List(
                    FlattenSchemaExpression(
                      targetColTokens.mkString(st.properties.delimiter),
                      SColumn(s"""col("${sanitizedColumn(column)}")"""),
                      column,
                      exploded,
                      targetColTokens
                    )
                  )
                )
              )
            },
            allColumnsSelectionCallback = Some { (portId: String, state: Any) ⇒
              val st = state.asInstanceOf[Component]
              val columnsInSchema = getColumnsInSchema(portId, st, SchemaFields.LeafLevel)
              val resultForEachCol = columnsInSchema.map { column ⇒
                val tokensOfUsedTargetNames = st.properties.fsExpressions.map(_.targetTokens)
                val targetColTokens = getTargetTokens(column, tokensOfUsedTargetNames)
                val colsToExplode = getColumnsToExplode(
                  Nil,
                  column.split('.').toList,
                  schemaOption = st.ports.inputs.map(_.schema).head
                )
                val exploded = if (colsToExplode.nonEmpty) "\u2713" else ""
                (
                  FlattenSchemaExpression(
                    targetColTokens.mkString(st.properties.delimiter),
                    SColumn(s"""col("${sanitizedColumn(column)}")"""),
                    column,
                    exploded,
                    targetColTokens
                  ),
                  colsToExplode
                )
              }
              val (updatedExpressions, explodeTheseCols) = (resultForEachCol.map(_._1), resultForEachCol.flatMap(_._2))
              st.copy(properties =
                st.properties.copy(
                  fsExpressions = st.properties.fsExpressions ::: updatedExpressions
                )
              )
            }
          ).importSchema(),
          "2fr"
        )
        .addColumn(
          StackLayout(height = Some("100%"))
            .addElement(
              ColumnsLayout()
                .addColumn(
                  SelectBox("Columns Delimiter")
                    .addOption("Dash (-)", "-")
                    .addOption("Underscore (_)", "_")
                    .bindProperty("delimiter")
                )
            )
            .addElement(
              BasicTable(
                "Test",
                columns = List(
                  Column("Target Column", "target", Some(TextBox("", ignoreTitle = true)), width = "30%"),
                  Column(
                    "Expression",
                    "expression.expression",
                    Some(
                      ExpressionBox(ignoreTitle = true)
                        .disabled()
                        .bindPlaceholders()
                        .bindLanguage("${record.expression.format}")
                        .withSchemaSuggestions()
                    )
                  ),
                  Column("Exploded", "exploded", width = "15%")
                ),
                appendNewRow = false
              ).bindProperty("fsExpressions")
            ),
          "5fr"
        )
    )

  def validate(component: Component)(implicit context: WorkflowContext): List[Diagnostic] = {
    import scala.collection.mutable.ListBuffer
    val diagnostics = ListBuffer[Diagnostic]()

    if (component.properties.fsExpressions.isEmpty)
      diagnostics += Diagnostic(
        "properties.fsExpressions",
        "At least one expression has to be specified",
        SeverityLevel.Error
      )
    else
      component.properties.fsExpressions.zipWithIndex.foreach {
        case (expr, idx) ⇒
          if (expr.target.trim.isEmpty)
            diagnostics += Diagnostic(
              s"properties.fsExpressions[$idx].target",
              s"Target cannot be empty.",
              SeverityLevel.Error
            )
      }
    diagnostics.toList
  }

  def onChange(oldState: Component, newState: Component)(implicit context: WorkflowContext): Component = {
    import scala.collection.mutable

    val newProps = newState.properties
    val usedCols = getColumnsToHighlight(newProps.fsExpressions.map(_.expression), newState)

    // looks up the map to see if any of the parent level columns of the current input column (ipCol) are exploded.
    // If yes, a match would be found for a prefix of the current input column, and that match should be used instead of
    // the current prefix whe the current ipCol would be exploded.
    def getCorrectColNameForExploding(
      ipCol: String,
      colNameTransformationsDuringExplode: mutable.LinkedHashMap[String, (String, String)]
    ): String = {
      val columnNameCleanOfBackticks = getColumnNameWithoutBackticks(ipCol)
      val lastIndex = columnNameCleanOfBackticks.lastIndexOf('.')
      val dotNotFound = lastIndex == -1
      if (dotNotFound)
        columnNameCleanOfBackticks
      else {
        // split the column by the last dot to get parent columns and last column
        val parentColumn = columnNameCleanOfBackticks.substring(0, lastIndex)
        val lastColumn = columnNameCleanOfBackticks.substring(lastIndex)
        colNameTransformationsDuringExplode
          .get(parentColumn)
          .map(_._2)
          .getOrElse(
            getCorrectColNameForExploding(parentColumn, colNameTransformationsDuringExplode)
          ) + s"$lastColumn"
      }
    }

    // Sort the columns to be exploded by length (the idea being that the shortest column to be exploded
    // would be somewhere at the root level. another way to think is "The more nested the column, the more columns in
    // the column name", which again implies that the lesser nested column would be at the top level and
    // would need to be exploded first)
    val emptyMap = mutable.LinkedHashMap[String, (String, String)]()
    //    val columnsInSchema = getColumnsInSchema(newState.ports.inputs.head.id, newState, SchemaFields.LeafLevel)
    val colsFromFSExpressions = newProps.fsExpressions.map { fsExp =>
      val a = fsExp.expression
      getColumnNameWithoutBackticks(a.expression.replace("col(\"", "").replace("\")", ""))
    }
    // col("col1.col2.a")
    val resultForEachCol = colsFromFSExpressions.map(column ⇒ {
      try {
        getColumnsToExplode(
          Nil,
          column.split('.').toList,
          schemaOption = newState.ports.inputs.map(_.schema).head
        )
      } catch {
        case _: NoSuchElementException => List.empty
      }
    })
    val explodeColumnsList = resultForEachCol.flatten.distinct
    val columnNameTransformationMap = explodeColumnsList.sortBy(_.split('.').length).foldLeft(emptyMap) {
      case (someMap, actualColumnName) ⇒
        val newColNameAfterExplode = actualColumnName.replace(".", newProps.delimiter)
        // If this is a nested column with parent column exploded, then we'll have to replace the parent column with
        // its (parent column's) new name it got after explode.
        val correctColNameForExploding = getCorrectColNameForExploding(actualColumnName, someMap)
        someMap += (actualColumnName → (correctColNameForExploding, newColNameAfterExplode))
    }

    val updatedExpressions: List[FlattenSchemaExpression] = if (newProps.delimiter != oldState.properties.delimiter) {
      newProps.fsExpressions.map(expr ⇒
        expr.copy(
          target = expr.targetTokens.mkString(newProps.delimiter),
          flattenedExpression =
            getCorrectColNameForExploding(expr.expression.unaliasedColumn.toString(), columnNameTransformationMap)
        )
      )
    } else
      newProps.fsExpressions.map(expr ⇒
        expr.copy(
          flattenedExpression =
            getCorrectColNameForExploding(expr.expression.unaliasedColumn.toString(), columnNameTransformationMap)
        )
      )

    newState.copy(properties =
      newProps.copy(
        columnsSelector = usedCols,
        fsExpressions = updatedExpressions,
        explodeColumns = explodeColumnsList,
        explodedColsNewName = columnNameTransformationMap.map(x ⇒ ExplodeColInfo(x._1, x._2._1, x._2._2)).toList
      )
    )
  }

  override def deserializeProperty(props: String): PropertiesType = Json.parse(props).as[PropertiesType]

  override def serializeProperty(props: PropertiesType): String = Json.stringify(Json.toJson(props))
  class FlattenSchemaCode(props: PropertiesType) extends ComponentCode {

    def apply(spark: SparkSession, in: DataFrame): DataFrame = {
      val flattened = props.explodedColsNewName.foldLeft(in) {
        case (df, ExplodeColInfo(originalColToExplode, updatedColToExplode, colNameAfterExplode)) ⇒
          df.withColumn(colNameAfterExplode, explode_outer(col(updatedColToExplode)))
      }
      val flattenedColumns = flattened.columns
      val selectCols = props.fsExpressions.map(_exp ⇒
        if (flattenedColumns.contains(_exp.target)) col(_exp.target) else col(_exp.flattenedExpression).as(_exp.target)
      )
      val out = flattened.select(selectCols: _*)
      out
    }

  }

}
