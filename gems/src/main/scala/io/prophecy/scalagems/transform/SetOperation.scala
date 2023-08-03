package io.prophecy.scalagems.transform

import io.prophecy.gems._
import io.prophecy.gems.componentSpec._
import io.prophecy.gems.dataTypes._
import io.prophecy.gems.diagnostics._
import io.prophecy.gems.uiSpec._
import org.apache.spark.sql.{DataFrame, SparkSession}
import play.api.libs.json.{Format, Json}

class SetOperation extends ComponentSpec {

  val name: String = "SetOperation"
  val category: String = "Transform"
  val gemDescription: String =
    "Allows you to perform Unions and Intersections of records from DataFrames with similar schemas and different data."
  val docUrl: String = "https://docs.prophecy.io/low-code-spark/gems/transform/set-operation"
  type PropertiesType = SetOperationProperties
  override def optimizeCode: Boolean = true
  override def allInputsRequired: Boolean = false

  case class SetOperationProperties(
    @Property("Operation Type", "")
    operationType: String = "unionAll",
    @Property("Allow Missing Columns", "")
    allowMissingColumns: Option[Boolean] = Some(false)
  ) extends ComponentProperties

  implicit val setOperationPropertiesFormat: Format[SetOperationProperties] = Json.format

  def dialog: Dialog = {
    val radioGroup = RadioGroup("Operation Type")
      .addOption(
        "Union",
        "unionAll",
        Some("UnionAll"),
        Some("Returns a dataset containing rows in any one of the input Datasets, while preserving duplicates.")
      )
      .addOption(
        "Intersect All",
        "intersectAll",
        Some("IntersectAll"),
        Some("Returns a dataset containing rows in all of the input Datasets, while preserving duplicates.")
      )
      .addOption(
        "Except All",
        "exceptAll",
        Some("ExceptAll"),
        Some("Returns a dataset containing rows in the first Dataset, but not in the other datasets, while preserving duplicates.")
      )
      .addOption(
        "Union By Name",
        "unionByName",
        Some("UnionAll"),
        Some("Returns a dataset containing rows in any one of the input Datasets, while preserving duplicates merging them by the column names instead of merging them by position")
      )
      .setOptionType("button")
      .setVariant("large")
      .setButtonStyle("solid")
      .bindProperty("operationType")

    Dialog("SetOperation")
      .addElement(
        ColumnsLayout(gap = Some("1rem"), height = Some("100%"))
          .addColumn(
            PortSchemaTabs(editableInput = Some(true), minNumberOfPorts = 2).importSchema()
          )
          .addColumn(
            StackLayout()
              .addElement(
                radioGroup
              )
              .addElement(
                Checkbox("Allow missing columns").bindProperty("allowMissingColumns")
              ),
            "2fr"
          )
      )
  }

  def validate(component: Component)(implicit context: WorkflowContext): List[Diagnostic] = {
    import org.apache.spark.sql.types.{DataType, NullType}

    import scala.collection.{Set, mutable}

    def mismatchedNumberOfColumnsError(
      anomalous: mutable.LinkedHashMap[String, DataType],
      baseline: mutable.LinkedHashMap[String, DataType],
      anomalousPortSlug: String
    ): String = {
      val missingInAnomalous = baseline -- anomalous.keySet
      val m1 =
        if (missingInAnomalous.nonEmpty)
          s"""Columns ${missingInAnomalous.keys.mkString(", ")} not found in $anomalousPortSlug. """
      val extraInAnomalous = anomalous -- (baseline.keySet)
      val m2 =
        if (extraInAnomalous.nonEmpty)
          s"""Additional columns ${extraInAnomalous.keys.mkString(", ")} found in $anomalousPortSlug."""
      s"Number of columns should be the same. $m1$m2"
    }

    def mismatchedColNameOrDatatypeError(
      anomalous: mutable.LinkedHashMap[String, DataType],
      baseline: mutable.LinkedHashMap[String, DataType],
      anomalousPortSlug: String
    ): String = {
      val diffs = (anomalous, baseline).zipped.collect {
        case (a, b) if a._1 != b._1 || a._2 != b._2 ⇒ s"${a._1}(${a._2})/${b._1}(${b._2})"
      }
      s"""Mismatch in columns of "$anomalousPortSlug" port. ${diffs.mkString(". ")}"""
    }

    def getAnomalousSchema(
      schemasGrouped: Map[List[(String, DataType)], List[InputPortSchema]]
    ): (String, mutable.LinkedHashMap[String, DataType], mutable.LinkedHashMap[String, DataType]) = {
      val (firstGroupRep, secondGroupRep, secondGroupNumItems) =
        (schemasGrouped.head._2.head, schemasGrouped.last._2.head, schemasGrouped.last._2.length)
      if (secondGroupNumItems == 1) (secondGroupRep.slug, secondGroupRep.schemaSummary, firstGroupRep.schemaSummary)
      else (firstGroupRep.slug, firstGroupRep.schemaSummary, secondGroupRep.schemaSummary)
    }

    def supersetForInputSchema(schemaList: List[InputPortSchema]): List[InputPortSchema] = {
      component.properties.operationType match {
        case "unionByName" =>
          val allColumns = schemaList.flatMap(inputPortSchema => inputPortSchema.schemaSummary.keySet)
          component.properties.allowMissingColumns.getOrElse() match {
            case true =>
              // For any column in allColumns which isn't available in any of the Dataframes, add the column as a NullType() column
              val newSchemaList = schemaList.map { inputPortSchema =>
                allColumns.foreach(column => {
                  if (!(inputPortSchema.schemaSummary.keySet.contains(column))) {
                    inputPortSchema.schemaSummary(column) = NullType
                  }
                })
                val finalInPortSchema = InputPortSchema(
                  inputPortSchema.slug,
                  mutable.LinkedHashMap(inputPortSchema.schemaSummary.toList.sortWith(_._1 > _._1): _*)
                )
                finalInPortSchema
              }
              def typeCast(first: DataType, second: DataType): (Boolean, Option[DataType]) = first match {
                case second   => (true, Some(first))
                case NullType => (true, Some(second))
                case _ =>
                  second match {
                    case NullType => (true, Some(first))
                    case _        => (false, None)
                  }
              }
              val resolvedColumnMap = mutable.Map[String, DataType]()
              var toggleEnd = false
              val finalSchemaList = newSchemaList.map { newInPortSchema =>
                newInPortSchema.schemaSummary.keySet.foreach { colm =>
                  val typeResolutionResult =
                    typeCast(resolvedColumnMap.getOrElse(colm, NullType), newInPortSchema.schemaSummary(colm))
                  if (typeResolutionResult._1) resolvedColumnMap(colm) = typeResolutionResult._2.getOrElse(NullType)
                  else toggleEnd = true
                }
                if (!toggleEnd) {
                  newInPortSchema.schemaSummary.keySet.foreach(colm =>
                    newInPortSchema.schemaSummary(colm) = resolvedColumnMap(colm)
                  )
                }
                newInPortSchema
              }
              if (toggleEnd) schemaList
              else finalSchemaList
            case _ => schemaList
          }
        case _ => schemaList
      }
    }

    def getDiagnostic(errorMessage: String): List[Diagnostic] =
      Diagnostic("ports.inputs", errorMessage, SeverityLevel.Error) :: Nil

    if (component.ports.inputs.length < 2) getDiagnostic("Input ports can't be less than two in SetOperation")
    else {
      // Each array in the list represents schema of one input port. Contains the fieldname and field datatype of that schema.
      val schemaNamesAndDataTypes: List[InputPortSchema] = supersetForInputSchema(
        component.ports.inputs.map(port ⇒
          port.schema
            .map { structType ⇒
              val testMap = mutable.LinkedHashMap[String, DataType]()
              structType.fields.map(field ⇒ (field.name, field.dataType)).foreach(x ⇒ testMap += (x._1 → x._2))
              InputPortSchema(port.slug, testMap)
            }
            .getOrElse(InputPortSchema("", mutable.LinkedHashMap()))
        )
      )

      // if we group by keyset (Set), then it groups schemas with shuffled columns in same group.
      // Converting the keyset to a list ensures the positions of the keys becomes static
      // and case of shuffled columns does not break
      val schemasGrouped: Map[List[(String, DataType)], List[InputPortSchema]] =
        schemaNamesAndDataTypes.groupBy(_.schemaSummary.toList)
      // CASE: perfect scenario. number of columns match and their data types also match
      if (schemasGrouped.keys.map(x => x.toSet).toSet.size == 1) Nil
      // CASE if two groups are created, the reason might have either been
      // 1. diff num of columns or 2. same num of columns but diff colnames or datatypes
      // We'll give a clean diagnostics if we have only one port in such an error state. Generic otherwise.
      else if (schemasGrouped.size == 2 && schemasGrouped.values.map(_.length).exists(_ == 1)) {
        val (anomalousPortSlug, anomalous, baseline) = getAnomalousSchema(schemasGrouped)
        if (schemasGrouped.keys.map(_.size).toSet.size == 2) // different number of columns
          {
            getDiagnostic(mismatchedNumberOfColumnsError(anomalous, baseline, anomalousPortSlug))
          } else // same number of columns but diff colname and datatype
          getDiagnostic(mismatchedColNameOrDatatypeError(anomalous, baseline, anomalousPortSlug))
      } else
        getDiagnostic("Mismatch in columns found. All inputs must have the same columns.")
    }
  }

  def onChange(oldState: Component, newState: Component)(implicit context: WorkflowContext): Component = newState

  override def deserializeProperty(props: String): SetOperationProperties = Json.parse(props).as[PropertiesType]

  override def serializeProperty(props: SetOperationProperties): String = Json.stringify(Json.toJson(props))

  class SetOperationCode(props: PropertiesType) extends ComponentCode {

    def apply(spark: SparkSession, in0: DataFrame, in1: DataFrame, inDFs: DataFrame*): DataFrame = {
      val _inputs = in0 +: in1 +: inDFs
      props.operationType match {
        case "intersectAll" ⇒
          _inputs.flatMap(Option(_)).reduce(_.intersectAll(_))
        case "exceptAll" ⇒
          _inputs.flatMap(Option(_)).reduce(_.exceptAll(_))
        case "unionAll" ⇒
          _inputs.flatMap(Option(_)).reduce(_.unionAll(_))
        case "unionByName" ⇒
          if (props.allowMissingColumns.getOrElse(false)) {
            _inputs.flatMap(Option(_)).reduce(_.unionByName(_, allowMissingColumns = true))
          } else {
            _inputs.flatMap(Option(_)).reduce(_.unionByName(_))
          }
      }
    }
  }
}
