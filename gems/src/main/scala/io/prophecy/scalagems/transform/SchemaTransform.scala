package io.prophecy.scalagems.transform

import io.prophecy.gems._
import io.prophecy.gems.componentSpec._
import io.prophecy.gems.dataTypes._
import io.prophecy.gems.diagnostics._
import io.prophecy.gems.uiSpec._
import org.apache.spark.sql.{DataFrame, SparkSession}
import play.api.libs.json._

class SchemaTransform extends ComponentSpec {

  val name: String = "SchemaTransform"
  val category: String = "Transform"
  val gemDescription: String =
    "Adds, edits, renames, or drops columns."
  val docUrl: String = "https://docs.prophecy.io/low-code-spark/gems/transform/schema-transform"
  type PropertiesType = SchemaTransformProperties
  override def optimizeCode: Boolean = true

  sealed trait Transformation

  @Property("Add or Replace Column", "Adds or replaces a single column with the value of the given expression")
  case class AddReplaceColumn(
    @Property("Source Column") sourceColumn: SString = SString(""),
    @Property("Value Expression") expression: SColumn = SColumn("")
  ) extends Transformation

  @Property("Rename an existing Column", "Renames a single column with the new name")
  case class RenameColumn(
    @Property("Source Column Name") sourceColumn: SString = SString(""),
    @Property("Target Column Name") targetColumn: SString = SString("")
  ) extends Transformation

  @Property("Drop Columns", "Drop an existing column from the incoming DataFrame")
  case class DropColumn(
    @Property("List of Columns to Drop") sourceColumn: SString = SString("")
  ) extends Transformation

  case class SchemaTransformProperties(
    @Property("Columns selector")
    columnsSelector: List[String] = Nil,
    @Property("Transformations to Apply", "Each transformation renames, deletes, adds or redefines one column")
    transformations: List[Transformation] = List()
  ) extends ComponentProperties

  implicit val transformationPropertiesFormat: Format[Transformation] = new Format[Transformation] {
    override def reads(json: JsValue): JsResult[Transformation] = {
      for {
        kind <- (json \ "kind").validate[String]
        transformationJsValue <- (json \ kind).validate[JsObject]
        transformation = if (kind == "AddReplaceColumn") {
          transformationJsValue.as[AddReplaceColumn]
        } else if (kind == "DropColumn") {
          transformationJsValue.as[DropColumn]
        } else if (kind == "RenameColumn") {
          transformationJsValue.as[RenameColumn]
        } else throw new Exception("unexpected type")
      } yield transformation
    }

    override def writes(o: Transformation): JsValue = {
      val (typeName, transformationJsValue) = o match {
        case x: AddReplaceColumn => ("AddReplaceColumn", Json.toJson(x))
        case x: RenameColumn => ("RenameColumn", Json.toJson(x)) 
        case x: DropColumn => ("DropColumn", Json.toJson(x))
      }
      JsObject(Map(
        "kind" -> JsString(typeName),
        typeName -> transformationJsValue
      ))
    }
  }
  implicit val addReplaceColumnFormat: Format[AddReplaceColumn] = Json.format[AddReplaceColumn]
  implicit val renameColumnFormat: Format[RenameColumn] = Json.format[RenameColumn]
  implicit val dropColumnFormat: Format[DropColumn] = Json.format[DropColumn]
  implicit val schemaTransformPropertiesFormat: Format[SchemaTransformProperties] = Json.format[SchemaTransformProperties]

  def dialog: Dialog = {

    val selectBox = SelectBox("Operation")
      .addOption("Add/Replace Column", "AddReplaceColumn")
      .addOption("Drop Column", "DropColumn")
      .addOption("Rename Column", "RenameColumn")
      .bindProperty("record.kind")

    Dialog("Schema Transform")
      .addElement(
        ColumnsLayout(height = Some("100%"))
          .addColumn(
            PortSchemaTabs(selectedFieldsProperty = Some("columnsSelector")).importSchema(),
            "2fr"
          )
          .addColumn(VerticalDivider(), width = "content")
          .addColumn(
            StackLayout(gap = Some("1rem"), height = Some("100bh"))
              .addElement(TitleElement("Transformations"))
              .addElement(
                OrderedList("Transformations")
                  .bindProperty("transformations")
                  .setEmptyContainerText("Add Transformations")
                  .addElement(
                    Condition()
                      .ifEqual((PropExpr("record.kind")), (StringExpr("AddReplaceColumn")))
                      .then(
                        ColumnsLayout(Some("1rem"), alignY = Some("end"))
                          .addColumn(
                            ColumnsLayout(Some("1rem"))
                              .addColumn(selectBox, "0.47fr")
                              .addColumn(
                                ExpressionBox("New Column")
                                  .bindProperty("record.AddReplaceColumn.sourceColumn")
                                  .bindPlaceholder("New column's name")
                                  .withFrontEndLanguage,
                                "0.53fr"
                              )
                              .addColumn(
                                ExpressionBox("Expression")
                                  .bindLanguage("${record.AddReplaceColumn.expression.format}")
                                  .bindPlaceholders()
                                  .withSchemaSuggestions()
                                  .bindProperty("record.AddReplaceColumn.expression.expression"),
                                "2fr",
                                overflow = Some("visible")
                              ),
                            "1fr",
                            overflow = Some("visible")
                          )
                          .addColumn(ListItemDelete("delete"), width = "content")
                      )
                  )
                  .addElement(
                    Condition()
                      .ifEqual(PropExpr("record.kind"), StringExpr("DropColumn"))
                      .then(
                        ColumnsLayout(Some("1rem"), alignY = Some("end"))
                          .addColumn(
                            ColumnsLayout(Some("1rem"))
                              .addColumn(selectBox, "0.47fr")
                              .addColumn(
                                ExpressionBox("Column to drop")
                                  .bindProperty("record.DropColumn.sourceColumn")
                                  .bindPlaceholder("column_to_drop")
                                  .withFrontEndLanguage,
                                "0.53fr"
                              )
                          )
                          .addColumn(ListItemDelete("delete"), width = "content")
                      )
                  )
                  .addElement(
                    Condition()
                      .ifEqual(PropExpr("record.kind"), StringExpr("RenameColumn"))
                      .then(
                        ColumnsLayout(Some("1rem"), alignY = Some("end"))
                          .addColumn(
                            ColumnsLayout(Some("1rem"))
                              .addColumn(selectBox, "0.47fr")
                              .addColumn(
                                SchemaColumnsDropdown("Old Column Name")
                                  .withSearchEnabled()
                                  .bindSchema("component.ports.inputs[0].schema")
                                  .bindProperty("record.RenameColumn.sourceColumn")
                                  .showErrorsFor("record.RenameColumn.sourceColumn"),
                                "0.53fr"
                              )
                              .addColumn(
                                ExpressionBox("New Column Name")
                                  .bindPlaceholder("New column name")
                                  .bindProperty("record.RenameColumn.targetColumn")
                                  .withFrontEndLanguage,
                                "2fr"
                              )
                          )
                          .addColumn(ListItemDelete("delete"), width = "content")
                      )
                  )
              )
              .addElement(
                SimpleButtonLayout(
                  "Add Transformation",
                  onClick = Some { (state: Any) ⇒
                    val st = state.asInstanceOf[Component]
                    st.copy(properties =
                      st.properties.copy(
                        transformations = st.properties.transformations ::: AddReplaceColumn(
                          SString(""),
                          SColumn("")
                        ) :: Nil
                      )
                    )
                  }
                )
              ),
            "5fr"
          )
      )
  }

  def validate(component: Component)(implicit context: WorkflowContext): List[Diagnostic] = {
    import scala.collection.mutable.ListBuffer

    val diagnostics = ListBuffer[Diagnostic]()

    val transforms = component.properties.transformations
    transforms.zipWithIndex.map {
      case (tf: AddReplaceColumn, idx: Int) ⇒
        // validate source column
        val diag = tf.sourceColumn.diagnostics
        diagnostics ++= diag
        if (diag.isEmpty && tf.sourceColumn.isEmpty) {
          diagnostics += Diagnostic(
            s"properties.transformations[$idx].sourceColumn",
            "Target can't be empty.",
            SeverityLevel.Error
          )
        } else if (tf.expression.expression.trim.isEmpty)
          diagnostics += Diagnostic(
            s"properties.transformations[$idx].expression.expression",
            "Expression can't be empty.",
            SeverityLevel.Error
          )
        else {
          tf.expression.columnOption match {
            case None ⇒
              diagnostics += Diagnostic(
                s"properties.transformations[$idx].expression.expression",
                s"${tf.expression.expression} is not a valid  ${getExprLanguage(tf.expression.format)} expression",
                SeverityLevel.Error
              )
            case Some(expression) ⇒ Unit
          }
        }

      case (tf: RenameColumn, idx: Int) ⇒
        val (sDiag, sColumn) = (tf.sourceColumn.diagnostics, tf.sourceColumn.value)
        val (tDiag, tColumn) = (tf.targetColumn.diagnostics, tf.targetColumn.value)
        diagnostics ++= sDiag ++ tDiag
        if (tf.sourceColumn.isEmpty || tf.targetColumn.isEmpty) {
          diagnostics += Diagnostic(
            s"properties.transformations[$idx]",
            "Source and target columns can't be empty in Rename",
            SeverityLevel.Error
          )
        } else {
          if (sColumn.get == tColumn.get)
            diagnostics += Diagnostic(
              s"properties.transformations[$idx]",
              "Source and target columns are the same in Rename",
              SeverityLevel.Warning
            )
        }

      case (tf: DropColumn, idx: Int) ⇒
        val (diag, sColumn) = (tf.sourceColumn.diagnostics, tf.sourceColumn.value)
        diagnostics ++= diag
        if (diag.isEmpty && sColumn.isEmpty) {
          diagnostics += Diagnostic(
            s"properties.transformations[$idx]",
            "Drop column can't be empty.",
            SeverityLevel.Error
          )
        }
    }
    diagnostics.toList
  }

  def onChange(oldState: Component, newState: Component)(implicit context: WorkflowContext): Component = {
    val newProps = newState.properties

    val usedColumnNames = newProps.transformations.collect {
      case AddReplaceColumn(_, expression: SColumn) ⇒ expression
      case RenameColumn(sourceColumn, _) if !sourceColumn.isEmpty ⇒
        val col = sourceColumn.value
        SColumn(s"""col("${col.get}")""", Some(List(col.get)))
      case DropColumn(sourceColumn) if !sourceColumn.isEmpty ⇒
        val col = sourceColumn.value
        SColumn(s"""col("${col.get}")""", Some(List(col.get)))

    }

    val usedCols = getColumnsToHighlight(usedColumnNames, newState)

    newState.copy(properties =
      newProps.copy(
        columnsSelector = usedCols,
        transformations = newProps.transformations
      )
    )
  }

  override def deserializeProperty(props: String): PropertiesType = Json.parse(props).as[PropertiesType]

  override def serializeProperty(props: PropertiesType): String = Json.stringify(Json.toJson(props))

  class SchemaTransformCode(props: PropertiesType) extends ComponentCode {

    def apply(spark: SparkSession, in: DataFrame): DataFrame = {
      val out = props.transformations.foldLeft(in) {
        case (df, AddReplaceColumn(sourceColumn: SString, expression: SColumn)) ⇒
          df.withColumn(sourceColumn, expression.column)

        case (df, RenameColumn(sourceColumn: SString, targetColumn: SString)) ⇒
          df.withColumnRenamed(sourceColumn, targetColumn)

        case (df, DropColumn(sourceColumn: SString)) ⇒
          df.drop(sourceColumn)
      }
      out
    }
  }
}
