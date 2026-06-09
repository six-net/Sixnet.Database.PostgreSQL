using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Sixnet.Development.Data.Field.Formatting;
using Sixnet.Development.Queryable;
using Sixnet.Exceptions;

namespace Sixnet.Database.PostgreSQL
{
    /// <summary>
    /// Default field formatter for postgresql
    /// </summary>
    public class PostgreSqlDefaultFieldFormatter : ISixnetFieldFormatter
    {
        static List<StringComparison> StringIgnoreCaseValues = new List<StringComparison>()
        {
            StringComparison.OrdinalIgnoreCase,
            StringComparison.InvariantCultureIgnoreCase,
            StringComparison.CurrentCultureIgnoreCase
        };
        public string Format(SixnetFormatFieldContext context)
        {
            var formatOption = context.FormatSetting;
            var formatedFieldName = context.FieldName;
            var parameterString = formatOption.Parameter?.ToString();
            formatedFieldName = formatOption.Name switch
            {
                SixnetFieldFormatterNames.TO_STRING => $"CAST({formatedFieldName} AS TEXT)",
                SixnetFieldFormatterNames.DISTINCT => $"DISTINCT {formatedFieldName}",
                SixnetFieldFormatterNames.IS_NULL => $"{formatedFieldName} IS NULL",
                SixnetFieldFormatterNames.NOT_NULL => $"{formatedFieldName} IS NOT NULL",
                SixnetFieldFormatterNames.CHARLENGTH => $"CHAR_LENGTH({formatedFieldName})",
                SixnetFieldFormatterNames.COUNT => $"COUNT({formatedFieldName})",
                SixnetFieldFormatterNames.SUM => $"SUM({formatedFieldName})",
                SixnetFieldFormatterNames.MAX => $"MAX({formatedFieldName})",
                SixnetFieldFormatterNames.MIN => $"MIN({formatedFieldName})",
                SixnetFieldFormatterNames.AVG => $"AVG({formatedFieldName})",
                SixnetFieldFormatterNames.JSON_VALUE => $"({formatedFieldName}->>{formatOption.Parameter})",
                SixnetFieldFormatterNames.JSON_OBJECT => $"({formatedFieldName}->{formatOption.Parameter})",
                SixnetFieldFormatterNames.AND => $"({formatedFieldName}&{formatOption.Parameter})",
                SixnetFieldFormatterNames.OR => $"({formatedFieldName}|{formatOption.Parameter})",
                SixnetFieldFormatterNames.XOR => $"({formatedFieldName}#{formatOption.Parameter})",
                SixnetFieldFormatterNames.NOT => $"(~{formatedFieldName})",
                SixnetFieldFormatterNames.ADD => $"({formatedFieldName}+{formatOption.Parameter})",
                SixnetFieldFormatterNames.SUBTRACT => $"({formatedFieldName}-{formatOption.Parameter})",
                SixnetFieldFormatterNames.MULTIPLY => $"({formatedFieldName}*{formatOption.Parameter})",
                SixnetFieldFormatterNames.DIVIDE => $"({formatedFieldName}/{formatOption.Parameter})",
                SixnetFieldFormatterNames.MODULO => $"({formatedFieldName}%{formatOption.Parameter})",
                SixnetFieldFormatterNames.LEFT_SHIFT => $"({formatedFieldName}<<{formatOption.Parameter})",
                SixnetFieldFormatterNames.RIGHT_SHIFT => $"({formatedFieldName}>>{formatOption.Parameter})",
                SixnetFieldFormatterNames.TRIM => StringTrim(formatedFieldName, formatOption.Parameter),
                SixnetFieldFormatterNames.TRIM_START => StringTrimStart(formatedFieldName, formatOption.Parameter),
                SixnetFieldFormatterNames.TRIM_END => StringTrimEnd(formatedFieldName, formatOption.Parameter),
                SixnetFieldFormatterNames.STRING_CONCAT => $"({formatedFieldName}||{formatOption.Parameter})",
                SixnetFieldFormatterNames.DATE_TIME_DATE => $"CAST({formatedFieldName} AS DATE)",
                SixnetFieldFormatterNames.DATE_TIME_YEAR => $"EXTRACT(YEAR FROM {formatedFieldName})::INT",
                SixnetFieldFormatterNames.DATE_TIME_MONTH => $"EXTRACT(MONTH FROM {formatedFieldName})::INT",
                SixnetFieldFormatterNames.DATE_TIME_DAY => $"EXTRACT(DAY FROM {formatedFieldName})::INT",
                SixnetFieldFormatterNames.DATE_TIME_DAY_OF_YEAR => $"EXTRACT(DOY FROM {formatedFieldName})::INT",
                SixnetFieldFormatterNames.DATE_TIME_DAY_OF_WEEK => $"EXTRACT(DOW FROM {formatedFieldName})::INT",
                SixnetFieldFormatterNames.DATE_TIME_HOUR => $"EXTRACT(HOUR FROM {formatedFieldName})::INT",
                SixnetFieldFormatterNames.DATE_TIME_MINUTE => $"EXTRACT(MINUTE FROM {formatedFieldName})::INT",
                SixnetFieldFormatterNames.DATE_TIME_SECOND => $"EXTRACT(SECOND FROM {formatedFieldName})::INT",
                SixnetFieldFormatterNames.DATE_TIME_MILLISECOND => $"EXTRACT(MILLISECOND FROM {formatedFieldName})::INT",
                SixnetFieldFormatterNames.DATE_TIME_TIME_OF_DAY => $"CAST({formatedFieldName} AS TIME)",
                SixnetFieldFormatterNames.DATE_TIME_UTC => $"({formatedFieldName} AT TIME ZONE 'UTC')",
                SixnetFieldFormatterNames.DATE_TIME_FORMAT_STRING => $"FORMAT({formatedFieldName}, {parameterString})",
                SixnetFieldFormatterNames.DATE_TIME_STRING => $"CONVERT(VARCHAR(20), {parameterString}, 120)",
                SixnetFieldFormatterNames.DATE_TIME_WITH_MILLISECOND_STRING => $"CONVERT(VARCHAR(25), {parameterString}, 121)",
                SixnetFieldFormatterNames.DATE_STRING => $"CONVERT(VARCHAR(15), {parameterString}, 23)",
                SixnetFieldFormatterNames.US_DATE_STRING => $"CONVERT(VARCHAR(15), {parameterString}, 101)",
                SixnetFieldFormatterNames.JAPAN_DATE_STRING => $"CONVERT(VARCHAR(15), {parameterString}, 111)",
                SixnetFieldFormatterNames.TIME_SPAN_DAYS => $"DATEDIFF(DAY, {formatedFieldName?.Trim('(').Replace("-", ",")}",
                SixnetFieldFormatterNames.TIME_SPAN_TOTAL_DAYS => $"(DATEDIFF_BIG(SECOND, {formatedFieldName?.Trim('(').Replace("-", ",")} / 86400.0)",
                SixnetFieldFormatterNames.TIME_SPAN_HOURS => $"DATEDIFF(HOUR, {formatedFieldName?.Trim('(').Replace("-", ",")}",
                SixnetFieldFormatterNames.TIME_SPAN_TOTAL_HOURS => $"(DATEDIFF_BIG(SECOND, {formatedFieldName?.Trim('(').Replace("-", ",")} / 3600.0)",
                SixnetFieldFormatterNames.TIME_SPAN_MINUTES => $"DATEDIFF(MINUTE, {formatedFieldName?.Trim('(').Replace("-", ",")}",
                SixnetFieldFormatterNames.TIME_SPAN_TOTAL_MINUTES => $"(DATEDIFF_BIG(SECOND, {formatedFieldName?.Trim('(').Replace("-", ",")} / 60.0)",
                SixnetFieldFormatterNames.TIME_SPAN_SECONDS => $"DATEDIFF(SECOND, {formatedFieldName?.Trim('(').Replace("-", ",")}",
                SixnetFieldFormatterNames.TIME_SPAN_TOTAL_SECONDS => $"(DATEDIFF(MILLISECOND, {formatedFieldName?.Trim('(').Replace("-", ",")} / 1000.0)",
                SixnetFieldFormatterNames.TIME_SPAN_MILLISECONDS => $"DATEDIFF(MILLISECOND, {formatedFieldName?.Trim('(').Replace("-", ",")}",
                SixnetFieldFormatterNames.TIME_SPAN_TOTAL_MILLISECONDS => $"(DATEDIFF(MICROSECOND, {formatedFieldName?.Trim('(').Replace("-", ",")} / 1000.0)",
                SixnetFieldFormatterNames.TO_LOWER => $"LOWER({formatedFieldName})",
                SixnetFieldFormatterNames.TO_UPPER => $"UPPER({formatedFieldName})",
                SixnetFieldFormatterNames.SUB_STRING => Substring(formatedFieldName, formatOption.Parameter),
                SixnetFieldFormatterNames.STRING_REPLACE => ReplaceString(formatedFieldName, formatOption.Parameter),
                SixnetFieldFormatterNames.DATE_TIME_ADD_YEAR => $"({formatedFieldName} + MAKE_INTERVAL(years => {parameterString}))",
                SixnetFieldFormatterNames.DATE_TIME_ADD_MONTH => $"({formatedFieldName} + MAKE_INTERVAL(months => {parameterString}))",
                SixnetFieldFormatterNames.DATE_TIME_ADD_DAY => $"({formatedFieldName} + MAKE_INTERVAL(days => {parameterString}))",
                SixnetFieldFormatterNames.DATE_TIME_ADD_HOUR => $"({formatedFieldName} + MAKE_INTERVAL(hours => {parameterString}))",
                SixnetFieldFormatterNames.DATE_TIME_ADD_MINUTE => $"({formatedFieldName} + MAKE_INTERVAL(minutes => {parameterString}))",
                SixnetFieldFormatterNames.DATE_TIME_ADD_SECOND => $"({formatedFieldName} + MAKE_INTERVAL(seconds => {parameterString}))",
                SixnetFieldFormatterNames.DATE_TIME_ADD_MILLISECOND => $"({formatedFieldName} + MAKE_INTERVAL(milliseconds => {parameterString}))",
                SixnetFieldFormatterNames.CONVERT_TO_INT => $"CAST({formatedFieldName} AS INTEGER)",
                SixnetFieldFormatterNames.CONVERT_TO_BOOLEAN => $"CAST({formatedFieldName} AS BOOLEAN)",
                SixnetFieldFormatterNames.CONVERT_TO_BYTE => $"CAST({formatedFieldName} AS SMALLINT)",
                SixnetFieldFormatterNames.CONVERT_TO_CHAR => $"CAST({formatedFieldName} AS CHAR(4000))",
                SixnetFieldFormatterNames.CONVERT_TO_DATE_TIME => $"CAST({formatedFieldName} AS TIMESTAMP WITHOUT TIME ZONE)",
                SixnetFieldFormatterNames.CONVERT_TO_DECIMAL => $"CAST({formatedFieldName} AS NUMERIC)",
                SixnetFieldFormatterNames.CONVERT_TO_DOUBLE => $"CAST({formatedFieldName} AS DOUBLE PRECISION)",
                SixnetFieldFormatterNames.CONVERT_TO_INT_16 => $"CAST({formatedFieldName} AS SMALLINT)",
                SixnetFieldFormatterNames.CONVERT_TO_INT_64 => $"CAST({formatedFieldName} AS BIGINT)",
                SixnetFieldFormatterNames.CONVERT_TO_SBYTE => $"CAST({formatedFieldName} AS SMALLINT)",
                SixnetFieldFormatterNames.CONVERT_TO_SINGLE => $"CAST({formatedFieldName} AS REAL)",
                SixnetFieldFormatterNames.CONVERT_TO_UINT_16 => $"CAST({formatedFieldName} AS INTEGER)",
                SixnetFieldFormatterNames.CONVERT_TO_UINT_32 => $"CAST({formatedFieldName} AS INTEGER)",
                SixnetFieldFormatterNames.CONVERT_TO_UINT_64 => $"CAST({formatedFieldName} AS BIGINT)",
                SixnetFieldFormatterNames.MATH_ROUND => $"ROUND({formatedFieldName}, parameterString)",
                SixnetFieldFormatterNames.MATH_ABS => $"ABS({formatedFieldName})",
                SixnetFieldFormatterNames.MATH_CEILING => $"CEIL({formatedFieldName})",
                SixnetFieldFormatterNames.MATH_FLOOR => $"FLOOR({formatedFieldName})",
                SixnetFieldFormatterNames.MATH_TRUNCATE => $"TRUNC({formatedFieldName})",
                SixnetFieldFormatterNames.MATH_SIGN => $"SIGN({formatedFieldName})",
                SixnetFieldFormatterNames.MATH_POW => $"POWER({formatedFieldName}, {parameterString})",
                SixnetFieldFormatterNames.MATH_SQRT => $"SQRT({formatedFieldName})",
                SixnetFieldFormatterNames.MATH_EXP => $"EXP({formatedFieldName})",
                SixnetFieldFormatterNames.MATH_LOG => $"LOG({parameterString}, {formatedFieldName})",
                SixnetFieldFormatterNames.MATH_COS => $"COS({formatedFieldName})",
                SixnetFieldFormatterNames.MATH_SIN => $"SIN({formatedFieldName})",
                SixnetFieldFormatterNames.MATH_TAN => $"TAN({formatedFieldName})",
                SixnetFieldFormatterNames.MATH_ACOS => $"ACOS({formatedFieldName})",
                SixnetFieldFormatterNames.MATH_ASIN => $"ASIN({formatedFieldName})",
                SixnetFieldFormatterNames.MATH_ATAN => $"ATAN({formatedFieldName})",
                SixnetFieldFormatterNames.MATH_ATAN2 => $"ATN2({formatedFieldName}, {parameterString})",
                SixnetFieldFormatterNames.STRING_PAD_LEFT => StringPadLeft(formatedFieldName, formatOption.Parameter),
                SixnetFieldFormatterNames.STRING_PAD_RIGHT => StringPadRight(formatedFieldName, formatOption.Parameter),
                SixnetFieldFormatterNames.STRING_INDEX_OF => StringIndexOf(formatedFieldName, formatOption.Parameter),
                SixnetFieldFormatterNames.STRING_INDEX_OF_ANY => StringIndexOfAny(formatedFieldName, formatOption.Parameter),
                SixnetFieldFormatterNames.STRING_LAST_INDEX_OF => StringLastIndexOf(formatedFieldName, formatOption.Parameter),
                SixnetFieldFormatterNames.STRING_LAST_INDEX_OF_ANY => StringLastIndexOfAny(formatedFieldName, formatOption.Parameter),
                _ => throw new SixnetException($"{context.Server.DatabaseType} does not support field formatter: {formatOption.Name}"),
            };
            return formatedFieldName;
        }

        #region String

        string StringIndexOf(string formatedFieldName, object parameter)
        {
            if (parameter is Tuple<dynamic, dynamic, dynamic> tupeThreeParameter)
            {
                var charValue = tupeThreeParameter.Item1;
                var startIndex = tupeThreeParameter.Item2;
                var count = tupeThreeParameter.Item3;
                return $"(POSITION('{charValue}' IN SUBSTRING({formatedFieldName},{startIndex + 1}, {count})) + {startIndex - 1})";
            }
            else if (parameter is Tuple<dynamic, dynamic> tupeTwoParameter)
            {
                var charValue = tupeTwoParameter.Item1;
                var startIndex = tupeTwoParameter.Item2;
                var count = $"CHAR_LENGTH({formatedFieldName})";
                return $"(POSITION('{charValue}' IN SUBSTRING({formatedFieldName},{startIndex + 1}, {count})) + {startIndex - 1})";
            }
            else
            {
                return $"(POSITION('{parameter}' IN {formatedFieldName}) -1)";
            }
        }

        string StringIndexOfAny(string formatedFieldName, dynamic parameter)
        {
            if (parameter is Tuple<dynamic, dynamic, dynamic> tupeThreeParameter)
            {
                var charValue = new string(tupeThreeParameter.Item1);
                var valuesScripts = $"VALUES {string.Join(',', charValue.Select(c => $"('{c}')"))}";
                var startIndex = tupeThreeParameter.Item2;
                var count = tupeThreeParameter.Item3;
                return $"(SELECT MIN(SIXNET_POS) - 1 FROM (SELECT POSITION(SCH IN (POSITION('{charValue}' IN SUBSTRING({formatedFieldName},{startIndex + 1}, {count})) + {startIndex - 1})) AS SIXNET_POS FROM ({valuesScripts}) AS t(SCH) WHERE SIXNET_POS > 0) SIXNET_INDEX_SUB)";
            }
            else if (parameter is Tuple<dynamic, dynamic> tupeTwoParameter)
            {
                var charValue = new string(tupeTwoParameter.Item1);
                var valuesScripts = $"VALUES {string.Join(',', charValue.Select(c => $"('{c}')"))}";
                var startIndex = tupeTwoParameter.Item2;
                var count = $"LEN({formatedFieldName})";
                return $"(SELECT MIN(SIXNET_POS) - 1 FROM (SELECT POSITION(SCH IN (POSITION('{charValue}' IN SUBSTRING({formatedFieldName},{startIndex + 1}, {count})) + {startIndex - 1})) AS SIXNET_POS FROM ({valuesScripts}) AS t(SCH) WHERE SIXNET_POS > 0) SIXNET_INDEX_SUB)";
            }
            else
            {
                var charValue = new string(parameter);
                var valuesScripts = $"VALUES {string.Join(',', charValue.Select(c => $"('{c}')"))}";
                return $"(SELECT MIN(SIXNET_POS) - 1 FROM (SELECT POSITION(SCH IN (POSITION('{charValue}' IN {formatedFieldName}) AS SIXNET_POS FROM ({valuesScripts}) AS t(SCH) WHERE SIXNET_POS > 0) SIXNET_INDEX_SUB)";
            }
        }

        string StringLastIndexOf(string formatedFieldName, object parameter)
        {
            if (parameter is Tuple<dynamic, dynamic, dynamic> tupeThreeParameter)
            {
                var charValue = tupeThreeParameter.Item1;
                var startIndex = tupeThreeParameter.Item2;
                var count = tupeThreeParameter.Item3;
                return $"({startIndex + 1} - POSITION('{charValue}' IN REVERSE(SUBSTRING({formatedFieldName}, {startIndex + 2} - {count}))))";
            }
            else if (parameter is Tuple<dynamic, dynamic> tupeTwoParameter)
            {
                var charValue = tupeTwoParameter.Item1;
                var startIndex = tupeTwoParameter.Item2;
                var count = $"LEN({formatedFieldName})";
                return $"({startIndex + 1} - POSITION('{charValue}' IN REVERSE(SUBSTRING({formatedFieldName}, {startIndex + 2} - {count}))))";
            }
            else
            {
                return $"(LENGTH({formatedFieldName})-POSITION('{parameter}' IN REVERSE({formatedFieldName})))";
            }
        }

        string StringLastIndexOfAny(string formatedFieldName, dynamic parameter)
        {
            if (parameter is Tuple<dynamic, dynamic, dynamic> tupeThreeParameter)
            {
                var charValue = new string(tupeThreeParameter.Item1);
                var valuesScripts = $"VALUES {string.Join(',', charValue.Select(c => $"('{c}')"))}";
                var startIndex = tupeThreeParameter.Item2;
                var count = tupeThreeParameter.Item3;
                return $"(SELECT MAX(SIXNET_POS) - 1 FROM (SELECT POSITION(SCH IN (POSITION('{charValue}' IN SUBSTRING({formatedFieldName},{startIndex + 1}, {count})) + {startIndex - 1})) AS SIXNET_POS FROM ({valuesScripts}) AS t(SCH) WHERE SIXNET_POS > 0) SIXNET_INDEX_SUB)";
            }
            else if (parameter is Tuple<dynamic, dynamic> tupeTwoParameter)
            {
                var charValue = new string(tupeTwoParameter.Item1);
                var valuesScripts = $"VALUES {string.Join(',', charValue.Select(c => $"('{c}')"))}";
                var startIndex = tupeTwoParameter.Item2;
                var count = $"LEN({formatedFieldName})";
                return $"(SELECT MAX(SIXNET_POS) - 1 FROM (SELECT POSITION(SCH IN (POSITION('{charValue}' IN SUBSTRING({formatedFieldName},{startIndex + 1}, {count})) + {startIndex - 1})) AS SIXNET_POS FROM ({valuesScripts}) AS t(SCH) WHERE SIXNET_POS > 0) SIXNET_INDEX_SUB)";
            }
            else
            {
                var charValue = new string(parameter);
                var valuesScripts = $"VALUES {string.Join(',', charValue.Select(c => $"('{c}')"))}";
                return $"(SELECT MAX(SIXNET_POS) - 1 FROM (SELECT POSITION(SCH IN (POSITION('{charValue}' IN {formatedFieldName}) AS SIXNET_POS FROM ({valuesScripts}) AS t(SCH) WHERE SIXNET_POS > 0) SIXNET_INDEX_SUB)";
            }
        }

        string StringPadLeft(string formatedFieldName, object parameter)
        {
            if (parameter is Tuple<dynamic, dynamic> tupeParameter)
            {
                return $"LPAD({formatedFieldName}, {tupeParameter.Item1} , '{tupeParameter.Item2}')";
            }
            SixnetDirectThrower.ThrowAppException(true, $"Error field formatter: {formatedFieldName}");
            return string.Empty;
        }

        string StringPadRight(string formatedFieldName, object parameter)
        {
            if (parameter is Tuple<dynamic, dynamic> tupeParameter)
            {
                return $"RPAD({formatedFieldName}, {tupeParameter.Item1} , '{tupeParameter.Item2}')";
            }
            SixnetDirectThrower.ThrowAppException(true, $"Error field formatter: {formatedFieldName}");
            return string.Empty;
        }

        string Substring(string formatedFieldName, dynamic parameter)
        {
            if (parameter is Tuple<dynamic, dynamic> tupeParameter)
            {
                return $"SUBSTRING({formatedFieldName} FROM ({tupeParameter.Item1 + 1}) FOR {tupeParameter.Item2})";
            }
            else
            {
                return $"SUBSTRING({formatedFieldName} FROM ({parameter + 1}))";
            }
        }

        string ReplaceString(string formatedFieldName, object parameter)
        {
            if (parameter is Tuple<dynamic, dynamic, dynamic> tupeThreeParameter)
            {
                if (StringIgnoreCaseValues.Contains(tupeThreeParameter.Item3))
                {
                    return $"REGEXP_REPLACE({formatedFieldName}, '{tupeThreeParameter.Item1}', '{tupeThreeParameter.Item2}', 'gi')";
                }
                else
                {
                    return $"REPLACE({formatedFieldName},'{tupeThreeParameter.Item1}', '{tupeThreeParameter.Item2}')";
                }
            }
            else if (parameter is Tuple<dynamic, dynamic> tupeTwoParameter)
            {
                return $"REPLACE({formatedFieldName},'{tupeTwoParameter.Item1}', '{tupeTwoParameter.Item2}')";
            }
            SixnetDirectThrower.ThrowAppException(true, $"Error field formatter: {formatedFieldName}");
            return string.Empty;
        }

        string StringTrim(string formatedFieldName, dynamic parameter)
        {
            if (parameter == null)
            {
                return $"TRIM({formatedFieldName})";
            }
            else
            {
                var charValue = new string(parameter);
                return $"TRIM(BOTH '{charValue}' FROM {formatedFieldName})";
            }
        }

        string StringTrimStart(string formatedFieldName, dynamic parameter)
        {
            if (parameter == null)
            {
                return $"LTRIM({formatedFieldName})";
            }
            else
            {
                var charValue = new string(parameter);
                return $"LTRIM({formatedFieldName}, '{charValue}')";
            }
        }

        string StringTrimEnd(string formatedFieldName, dynamic parameter)
        {
            if (parameter == null)
            {
                return $"RTRIM({formatedFieldName})";
            }
            else
            {
                var charValue = new string(parameter);
                return $"RTRIM({formatedFieldName}, '{charValue}')";
            }
        }

        #endregion
    }
}
