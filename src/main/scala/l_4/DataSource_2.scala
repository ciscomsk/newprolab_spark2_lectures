package l_4

object DataSource_2 {
  /** ColumnPruning/PredicatePushdown - оптимизации каталиста. */

  /**
   * Column Pruning.
   * Данный механизм позволяет избежать вычитывания ненужных колонок при использовании оператора Project в плане запроса.
   *
   * DataSource v1 - поддерживает только плоские колонки (колонки структуры будут возвращаться целиком).
   */
  /** package org.apache.spark.sql.sources */
  /*
    /**
     * A BaseRelation that can eliminate unneeded columns before producing an RDD
     * containing all of its tuples as Row objects.
     *
     * @since 1.3.0
     */
    @Stable
    trait PrunedScan {
      def buildScan(requiredColumns: Array[String]): RDD[Row]
    }

    requiredColumns - список колонок, указанных в первом физическом операторе Project.
   */

  /**
   * Predicate Pushdown.
   * Данный механизм позволяет выполнить фильтры, указанные в операторе Filter плана запроса, на уровне чтения данных из источника.
   */
  /** package org.apache.spark.sql.sources */
  /*
    /**
     * A BaseRelation that can eliminate unneeded columns and filter using selected
     * predicates before producing an RDD containing all matching tuples as Row objects.
     *
     * The actual filter should be the conjunction of all `filters`,
     * i.e. they should be "and" together.
     *
     * The pushed down filters are currently purely an optimization as they will all be evaluated
     * again.  This means it is safe to use them with methods that produce false positives such
     * as filtering partitions based on a bloom filter.
     *
     * @since 1.3.0
     */
    @Stable
    trait PrunedFilteredScan {
      def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row]
    }

    requiredColumns - список колонок, указанных в первом физическом операторе Project.
    filters - список фильтров, указанных в первом физическом операторе Filter.
   */

  /** Поддерживается Predicate Pushdown фильтров перечисленных ниже. */

  /**
   * Список фильтров, которые не поддерживаются источником, необходимо вернуть в реализации
   * функции unhandledFilters - abstract class BaseRelation.
   */
  /** package org.apache.spark.sql.sources */
  /*
    /**
       * Returns the list of [[Filter]]s that this datasource may not be able to handle.
       * These returned [[Filter]]s will be evaluated by Spark SQL after data is output by a scan.
       * By default, this function will return all filters, as it is always safe to
       * double evaluate a [[Filter]]. However, specific implementations can override this function to
       * avoid double filtering when they are capable of processing a filter internally.
       *
       * @since 1.6.0
       */
      def unhandledFilters(filters: Array[Filter]): Array[Filter] = filters
    }

   !!! По умолчанию - отсекает все фильтры на уровне источника.
   */

  /** package org.apache.spark.sql.sources */
  /*
    /**
     * A filter that evaluates to `true` iff the column evaluates to a value
     * equal to `value`.
     *
     * @param attribute of the column to be evaluated; `dots` are used as separators
     *                  for nested columns. If any part of the names contains `dots`,
     *                  it is quoted to avoid confusion.
     * @since 1.3.0
     */
    @Stable
    case class EqualTo(attribute: String, value: Any) extends Filter {
      override def references: Array[String] = Array(attribute) ++ findReferences(value)
    }

    /**
     * Performs equality comparison, similar to [[EqualTo]]. However, this differs from [[EqualTo]]
     * in that it returns `true` (rather than NULL) if both inputs are NULL, and `false`
     * (rather than NULL) if one of the input is NULL and the other is not NULL.
     *
     * @param attribute of the column to be evaluated; `dots` are used as separators
     *                  for nested columns. If any part of the names contains `dots`,
     *                  it is quoted to avoid confusion.
     * @since 1.5.0
     */
    @Stable
    case class EqualNullSafe(attribute: String, value: Any) extends Filter {
      override def references: Array[String] = Array(attribute) ++ findReferences(value)
    }

    /**
     * A filter that evaluates to `true` iff the attribute evaluates to a value
     * greater than `value`.
     *
     * @param attribute of the column to be evaluated; `dots` are used as separators
     *                  for nested columns. If any part of the names contains `dots`,
     *                  it is quoted to avoid confusion.
     * @since 1.3.0
     */
    @Stable
    case class GreaterThan(attribute: String, value: Any) extends Filter {
      override def references: Array[String] = Array(attribute) ++ findReferences(value)
    }

    /**
     * A filter that evaluates to `true` iff the attribute evaluates to a value
     * greater than or equal to `value`.
     *
     * @param attribute of the column to be evaluated; `dots` are used as separators
     *                  for nested columns. If any part of the names contains `dots`,
     *                  it is quoted to avoid confusion.
     * @since 1.3.0
     */
    @Stable
    case class GreaterThanOrEqual(attribute: String, value: Any) extends Filter {
      override def references: Array[String] = Array(attribute) ++ findReferences(value)
    }

    /**
     * A filter that evaluates to `true` iff the attribute evaluates to a value
     * less than `value`.
     *
     * @param attribute of the column to be evaluated; `dots` are used as separators
     *                  for nested columns. If any part of the names contains `dots`,
     *                  it is quoted to avoid confusion.
     * @since 1.3.0
     */
    @Stable
    case class LessThan(attribute: String, value: Any) extends Filter {
      override def references: Array[String] = Array(attribute) ++ findReferences(value)
    }

    /**
     * A filter that evaluates to `true` iff the attribute evaluates to a value
     * less than or equal to `value`.
     *
     * @param attribute of the column to be evaluated; `dots` are used as separators
     *                  for nested columns. If any part of the names contains `dots`,
     *                  it is quoted to avoid confusion.
     * @since 1.3.0
     */
    @Stable
    case class LessThanOrEqual(attribute: String, value: Any) extends Filter {
      override def references: Array[String] = Array(attribute) ++ findReferences(value)
    }

    /**
     * A filter that evaluates to `true` iff the attribute evaluates to one of the values in the array.
     *
     * @param attribute of the column to be evaluated; `dots` are used as separators
     *                  for nested columns. If any part of the names contains `dots`,
     *                  it is quoted to avoid confusion.
     * @since 1.3.0
     */
    @Stable
    case class In(attribute: String, values: Array[Any]) extends Filter {
      override def hashCode(): Int = {
        var h = attribute.hashCode
        values.foreach { v =>
          h *= 41
          h += (if (v != null) v.hashCode() else 0)
        }
        h
      }
      override def equals(o: Any): Boolean = o match {
        case In(a, vs) =>
          a == attribute && vs.length == values.length && vs.zip(values).forall(x => x._1 == x._2)
        case _ => false
      }
      private def formatValue(v: Any): String = v match {
        case null => "null"
        case ar: Seq[Any] => ar.map(formatValue).mkString("[", ", ", "]")
        case _ => v.toString
      }
      override def toString: String = {
        // Sort elements for deterministic behaviours
        s"In($attribute, [${values.map(formatValue).sorted.mkString(",")}])"
      }

      override def references: Array[String] = Array(attribute) ++ values.flatMap(findReferences)
    }

    /**
     * A filter that evaluates to `true` iff the attribute evaluates to null.
     *
     * @param attribute of the column to be evaluated; `dots` are used as separators
     *                  for nested columns. If any part of the names contains `dots`,
     *                  it is quoted to avoid confusion.
     * @since 1.3.0
     */
    @Stable
    case class IsNull(attribute: String) extends Filter {
      override def references: Array[String] = Array(attribute)
    }

    /**
     * A filter that evaluates to `true` iff the attribute evaluates to a non-null value.
     *
     * @param attribute of the column to be evaluated; `dots` are used as separators
     *                  for nested columns. If any part of the names contains `dots`,
     *                  it is quoted to avoid confusion.
     * @since 1.3.0
     */
    @Stable
    case class IsNotNull(attribute: String) extends Filter {
      override def references: Array[String] = Array(attribute)
    }

    /**
     * A filter that evaluates to `true` iff both `left` or `right` evaluate to `true`.
     *
     * @since 1.3.0
     */
    @Stable
    case class And(left: Filter, right: Filter) extends Filter {
      override def references: Array[String] = left.references ++ right.references
    }

    /**
     * A filter that evaluates to `true` iff at least one of `left` or `right` evaluates to `true`.
     *
     * @since 1.3.0
     */
    @Stable
    case class Or(left: Filter, right: Filter) extends Filter {
      override def references: Array[String] = left.references ++ right.references
    }

    /**
     * A filter that evaluates to `true` iff `child` is evaluated to `false`.
     *
     * @since 1.3.0
     */
    @Stable
    case class Not(child: Filter) extends Filter {
      override def references: Array[String] = child.references
    }

    /**
     * A filter that evaluates to `true` iff the attribute evaluates to
     * a string that starts with `value`.
     *
     * @param attribute of the column to be evaluated; `dots` are used as separators
     *                  for nested columns. If any part of the names contains `dots`,
     *                  it is quoted to avoid confusion.
     * @since 1.3.1
     */
    @Stable
    case class StringStartsWith(attribute: String, value: String) extends Filter {
      override def references: Array[String] = Array(attribute)
    }

    /**
     * A filter that evaluates to `true` iff the attribute evaluates to
     * a string that ends with `value`.
     *
     * @param attribute of the column to be evaluated; `dots` are used as separators
     *                  for nested columns. If any part of the names contains `dots`,
     *                  it is quoted to avoid confusion.
     * @since 1.3.1
     */
    @Stable
    case class StringEndsWith(attribute: String, value: String) extends Filter {
      override def references: Array[String] = Array(attribute)
    }

    /**
     * A filter that evaluates to `true` iff the attribute evaluates to
     * a string that contains the string `value`.
     *
     * @param attribute of the column to be evaluated; `dots` are used as separators
     *                  for nested columns. If any part of the names contains `dots`,
     *                  it is quoted to avoid confusion.
     * @since 1.3.1
     */
    @Stable
    case class StringContains(attribute: String, value: String) extends Filter {
      override def references: Array[String] = Array(attribute)
    }
   */
}
