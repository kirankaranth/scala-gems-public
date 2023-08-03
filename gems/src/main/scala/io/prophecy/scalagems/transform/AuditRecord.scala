package io.prophecy.scalagems.transform

import io.prophecy.gems._
import io.prophecy.gems.componentSpec._
import io.prophecy.gems.dataTypes._
import io.prophecy.gems.diagnostics._
import io.prophecy.gems.uiSpec._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import play.api.libs.json.{Format, Json}

class AuditRecord extends ComponentSpec {

  val name: String = "AuditRecord"
  val category: String = "Transform"
  val gemDescription: String = "Used for Auditing Record"

  type PropertiesType = AuditRecordProperties

  override def optimizeCode: Boolean = true

  case class AuditRecordProperties(
                                    @Property("Columns selector")
                                    columnsSelector: List[String] = Nil,
                                    @Property("Workflow Name")
                                    workflowName: String = "${WORKFLOW_NAME}",
                                    @Property("Audit Dataset Type")
                                    auditDatasetType: String = "source"
                                  ) extends ComponentProperties

  //  override def validate(component: CompareColumns.Component)(implicit context: WorkflowContext): Diagnostics = Nil

  implicit val auditRecordFormat: Format[AuditRecordProperties] = Json.format
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

    Dialog("Simple Rules Component")
      .addElement(
        ColumnsLayout(gap = Some("1rem"), height = Some("100%"))
          .addColumn(
            PortSchemaTabs(
              selectedFieldsProperty = Some("columnsSelector")
            ).importSchema()
          )
          .addColumn(
            StackLayout(height = Some("100%"))
              .addElement(
                TextBox("Workflow Name")
                  .bindProperty("workflowName")
              )
              .addElement(
                SelectBox("Audit Dataset Type")
                  .addOption("Source", "source")
                  .addOption("Target", "target")
                  .bindProperty("auditDatasetType")
              )
          )
      )
  }

  override def deserializeProperty(props: String): PropertiesType = Json.parse(props).as[PropertiesType]

  override def serializeProperty(props: PropertiesType): String = Json.stringify(Json.toJson(props))

  class AuditRecordCode(props: PropertiesType)(implicit context: WorkflowContext) extends ComponentCode {

    def apply(spark: SparkSession, in0: DataFrame): DataFrame = {

      import spark.implicits._

      val num_rows = in0.count().toInt
      val auditType = props.auditDatasetType
      val workflowName = props.workflowName
      val out0 = Seq(num_rows)
        .toDF("num_rows")
        .withColumn("dataset_type", lit(auditType))
        .withColumn("workflow", lit(workflowName))
        .withColumn("timestamp", current_timestamp())
      out0
    }
  }
}
