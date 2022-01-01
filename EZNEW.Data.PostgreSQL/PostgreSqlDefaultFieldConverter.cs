using System;
using System.Collections.Generic;
using System.Text;
using EZNEW.Data.Conversion;
using EZNEW.Exceptions;

namespace EZNEW.Data.PostgreSQL
{
    /// <summary>
    /// Default field converter for postgresql
    /// </summary>
    public class PostgreSqlDefaultFieldConverter : IFieldConverter
    {
        public FieldConversionResult Convert(FieldConversionContext fieldConversionContext)
        {
            if (string.IsNullOrWhiteSpace(fieldConversionContext?.ConversionName))
            {
                return null;
            }
            string formatedFieldName;
            switch (fieldConversionContext.ConversionName)
            {
                case FieldConversionNames.StringLength:
                    formatedFieldName = string.IsNullOrWhiteSpace(fieldConversionContext.ObjectName)
                                        ? $"CHAR_LENGTH({fieldConversionContext.ObjectName}.{PostgreSqlManager.WrapKeyword(fieldConversionContext.FieldName)})"
                                        : $"CHAR_LENGTH({PostgreSqlManager.WrapKeyword(fieldConversionContext.FieldName)})";
                    break;
                default:
                    throw new EZNEWException($"{PostgreSqlManager.CurrentDatabaseServerType} does not support field conversion: {fieldConversionContext.ConversionName}");
            }
            return new FieldConversionResult()
            {
                NewFieldName = formatedFieldName
            };
        }
    }
}
