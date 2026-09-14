using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;

using Sixnet.Development.Data;
using Sixnet.Development.Data.Command;
using Sixnet.Development.Data.Dapper;
using Sixnet.Development.Data.Database;
using Sixnet.Development.Data.Field;
using Sixnet.Development.Entity;
using Sixnet.Development.Queryable;
using Sixnet.Exceptions;

namespace Sixnet.Database.PostgreSQL
{
    /// <summary>
    /// Defines postgresql resolver
    /// </summary>
    public partial class SixnetPostgreSqlDataCommandResolver : SixnetBaseDataCommandResolver
    {
        #region Constructor

        public SixnetPostgreSqlDataCommandResolver()
        {
            DatabaseType = SixnetDatabaseType.PostgreSQL;
            DefaultFieldFormatter = new SixnetPostgreSqlDefaultFieldFormatter();
            ParameterPrefix = ":";
            KeywordPrefix = "\"";
            KeywordSuffix = "\"";
            RecursiveKeyword = "WITH RECURSIVE";
            SplitWrapParameter = true;
            DisableDefaultPrimaryKeyAsc = true;
            MaxIdentifierLength = 63;
            DbTypeDefaultValues = new Dictionary<DbType, string>()
            {
                { DbType.Byte, "0" },
                { DbType.SByte, "0" },
                { DbType.Int16, "0" },
                { DbType.UInt16, "0" },
                { DbType.Int32, "0" },
                { DbType.UInt32, "0" },
                { DbType.Int64, "0" },
                { DbType.UInt64, "0" },
                { DbType.Single, "0" },
                { DbType.Double, "0" },
                { DbType.Decimal, "0" },
                { DbType.Boolean, "FALSE" },
                { DbType.String, "''" },
                { DbType.StringFixedLength, "''" },
                { DbType.Guid, "gen_random_uuid()" },
                { DbType.DateTime, "CURRENT_TIMESTAMP" },
                { DbType.DateTime2, "CURRENT_TIMESTAMP" },
                { DbType.DateTimeOffset, "CURRENT_TIMESTAMP" },
                { DbType.Time, "CURRENT_TIME" }
            };
        }

        #endregion

        #region Data access

        #region Get query statement

        /// <summary>
        /// Get query statement
        /// </summary>
        /// <param name="context">Command resolve context</param>
        /// <param name="translationResult">Queryable translation result</param>
        /// <param name="location">Queryable location</param>
        /// <returns></returns>
        protected override SixnetQueryDatabaseStatement GenerateQueryStatementCore(SixnetDataCommandResolveContext context, SixnetQueryableTranslationResult translationResult, SixnetQueryableLocation location)
        {
            var queryable = translationResult.GetOriginalQueryable();
            string sqlStatement;
            IEnumerable<ISixnetField> outputFields = null;
            switch (queryable.Info.ExecutionMode)
            {
                case SixnetQueryableExecutionMode.Script:
                    sqlStatement = translationResult.GetCondition();
                    break;
                case SixnetQueryableExecutionMode.Regular:
                default:
                    // table pet name
                    var tablePetName = context.GetTablePetName(queryable, queryable.GetModelType());
                    //sort
                    var sort = translationResult.GetSort();
                    var hasSort = !string.IsNullOrWhiteSpace(sort);
                    //limit
                    var limit = GetLimitString(queryable.Info.SkipCount, queryable.Info.TakeCount, hasSort);
                    var hasLimit = !string.IsNullOrWhiteSpace(limit);
                    //combine
                    var combine = translationResult.GetCombine();
                    var hasCombine = !string.IsNullOrWhiteSpace(combine);
                    //group
                    var group = translationResult.GetGroup();
                    //having
                    var having = translationResult.GetHavingCondition();
                    //pre script output
                    var targetScript = translationResult.GetPreOutputStatement();

                    if (string.IsNullOrWhiteSpace(targetScript))
                    {
                        //target
                        var targetStatement = GetFromTargetStatement(context, queryable, location, tablePetName);
                        outputFields = targetStatement.OutputFields;
                        //condition
                        var condition = translationResult.GetCondition(ConditionStartKeyword);
                        //join
                        var join = translationResult.GetJoin();
                        //target statement
                        targetScript = $"{targetStatement.Script}{join}{condition}{group}{having}";
                    }
                    else
                    {
                        targetScript = $"{targetScript}{group}{having}";
                        outputFields = translationResult.GetPreOutputFields();
                    }

                    // output fields
                    if (outputFields.IsNullOrEmpty() || !queryable.Info.SelectedFields.IsNullOrEmpty())
                    {
                        outputFields = SixnetDataManager.GetQueryableFields(DatabaseType, queryable.GetModelType(), queryable, context.IsRootQueryable(queryable));
                    }
                    var outputFieldString = FormatFieldsString(context, queryable, location, SixnetFieldLocation.Output, outputFields);

                    //statement
                    sqlStatement = $"SELECT{GetDistinctString(queryable)} {outputFieldString} FROM {targetScript}{sort}{limit}";
                    //pre script
                    var preScript = GetPreScript(context, location);
                    switch (queryable.Info.OutputType)
                    {
                        case SixnetQueryableOutputType.Count:
                            sqlStatement = hasCombine
                                ? hasSort
                                    ? $"{preScript}SELECT COUNT(1) FROM ((SELECT {tablePetName}.* FROM ({sqlStatement}){TablePetNameKeyword}{tablePetName}){combine}){TablePetNameKeyword}{tablePetName}"
                                    : $"{preScript}SELECT COUNT(1) FROM (({sqlStatement}){combine}){TablePetNameKeyword}{tablePetName}"
                                : $"{preScript}SELECT COUNT(1) FROM ({sqlStatement}){TablePetNameKeyword}{tablePetName}";
                            break;
                        case SixnetQueryableOutputType.Predicate:
                            sqlStatement = hasCombine
                                ? hasSort
                                    ? $"{preScript}SELECT 1 WHERE EXISTS((SELECT {tablePetName}.* FROM ({sqlStatement}){TablePetNameKeyword}{tablePetName}){combine})"
                                    : $"{preScript}SELECT 1 WHERE EXISTS(({sqlStatement}){combine})"
                                : $"{preScript}SELECT 1 WHERE EXISTS({sqlStatement})";
                            break;
                        case SixnetQueryableOutputType.TempTable:
                            sqlStatement = hasCombine
                            ? hasSort
                                ? $"(SELECT {tablePetName}.* FROM ({sqlStatement}){TablePetNameKeyword}{tablePetName}){combine}"
                                : $"({sqlStatement}){combine}"
                            : $"{sqlStatement}";
                            sqlStatement = $"{preScript}(CREATE TEMP TABLE {queryable.Info.TempTableName} AS SELECT * FROM ({sqlStatement}))";
                            break;
                        default:
                            sqlStatement = hasCombine
                            ? hasSort
                                ? $"{preScript}(SELECT {tablePetName}.* FROM ({sqlStatement}){TablePetNameKeyword}{tablePetName}){combine}"
                                : $"{preScript}({sqlStatement}){combine}"
                            : $"{preScript}{sqlStatement}";
                            break;
                    }
                    break;
            }

            //parameters
            var parameters = context.GetParameters();

            return SixnetQueryDatabaseStatement.Create(DatabaseType, location, sqlStatement, parameters, outputFields);
        }

        #endregion

        #region Get insert statement

        /// <summary>
        /// Get insert statement
        /// </summary>
        /// <param name="context">Command resolve context</param>
        /// <returns></returns>
        protected override List<SixnetExecutionDatabaseStatement> GenerateInsertStatements(SixnetDataCommandResolveContext context)
        {
            var command = context.DataCommandExecutionContext.Command;
            var dataCommandExecutionContext = context.DataCommandExecutionContext;
            var entityType = dataCommandExecutionContext.Command.GetEntityType();
            var fields = SixnetDataManager.GetInsertableFields(DatabaseType, entityType);
            var fieldCount = fields.GetCount();
            var insertFields = new List<string>(fieldCount);
            var insertValues = new List<string>(fieldCount);
            SixnetDataField autoIncrementField = null;
            SixnetDataField splitField = null;
            dynamic splitValue = default;

            foreach (var field in fields)
            {
                if (field.InRole(SixnetFieldRole.Increment))
                {
                    autoIncrementField ??= field;
                    if (!autoIncrementField.InRole(SixnetFieldRole.PrimaryKey) && field.InRole(SixnetFieldRole.PrimaryKey)) // get first primary key field
                    {
                        autoIncrementField = field;
                    }
                    if (!SixnetDataManager.AllowInsertIncrementField(context.DataCommandExecutionContext))
                    {
                        continue;
                    }
                }
                // fields
                insertFields.Add(FormatAndWrapObjectName(field.GetFieldName(DatabaseType), SixnetDatabaseObjectType.Column));
                // values
                var insertValue = command.FieldsAssignment.GetNewValue(field.PropertyName);
                insertValues.Add(FormatInsertValueField(context, command.Queryable, insertValue));
                // split value
                if (field.InRole(SixnetFieldRole.SplitValue))
                {
                    splitValue = insertValue;
                    splitField = field;
                }
            }

            SixnetDirectThrower.ThrowNotSupportIf(autoIncrementField != null && splitField != null, $"Not support auto increment field for split table:{entityType.Name}");

            if (splitField != null)
            {
                dataCommandExecutionContext.SetSplitValues(new List<dynamic>(1) { splitValue });
            }
            var tableNames = dataCommandExecutionContext.GetTableNames();
            SixnetDirectThrower.ThrowInvalidOperationIf(tableNames.IsNullOrEmpty(), $"Get table name failed for {entityType.Name}");

            // incr field
            var incrementFieldScript = string.Empty;
            if (autoIncrementField != null)
            {
                var idOutputParameterName = FormatParameterName(command.Id);
                incrementFieldScript = $" RETURNING {FormatAndWrapObjectName(SixnetDatabaseObjectName.Create(autoIncrementField.GetFieldName(DatabaseType), SixnetDatabaseObjectType.Column))}";
                context.AddOutputParameter(command.Id, autoIncrementField.GetDataType().GetDbType());
            }

            var scriptTemplate = $"INSERT INTO {{0}} ({string.Join(",", insertFields)}) VALUES ({string.Join(",", insertValues)}){incrementFieldScript}";

            var statements = new List<SixnetExecutionDatabaseStatement>();
            foreach (var tableName in tableNames)
            {
                statements.Add(SixnetExecutionDatabaseStatement.Create(DatabaseType, data =>
                {
                    data.Script = string.Format(scriptTemplate, FormatAndWrapObjectName(tableName));
                    data.ScriptType = GetCommandType(command);
                    data.Parameters = context.GetParameters();
                    data.MustAffectData = true;
                }));
            }

            return statements;
        }

        #endregion

        #region Get update statement

        /// <summary>
        /// Get update statement
        /// </summary>
        /// <param name="context">Command resolve context</param>
        /// <returns></returns>
        protected override List<SixnetExecutionDatabaseStatement> GenerateUpdateStatements(SixnetDataCommandResolveContext context)
        {
            var command = context.DataCommandExecutionContext.Command;
            SixnetException.ThrowIf(command?.FieldsAssignment?.NewValues.IsNullOrEmpty() ?? true, "No set update field");

            #region translate

            var translationResult = Translate(context);
            var condition = translationResult?.GetCondition(ConditionStartKeyword);
            var join = translationResult?.GetJoin();
            var preScripts = context.GetPreScripts();

            #endregion

            #region script 

            var dataCommandExecutionContext = context.DataCommandExecutionContext;
            var tablePetName = command.Queryable == null ? context.GetNewTablePetName() : context.GetDefaultTablePetName(command.Queryable);
            var newValues = command.FieldsAssignment.NewValues;
            var updateSetArray = new List<string>();
            foreach (var newValueItem in newValues)
            {
                var newValue = newValueItem.Value;
                var propertyName = newValueItem.Key;
                var updateField = SixnetDataManager.GetField(dataCommandExecutionContext.Server.DatabaseType, command.GetEntityType(), SixnetDataField.Create(propertyName)) as SixnetDataField;
                SixnetDirectThrower.ThrowSixnetExceptionIf(updateField == null, $"Not found field:{propertyName}");
                var fieldFormattedName = FormatAndWrapObjectName(SixnetDatabaseObjectName.Create(updateField.GetFieldName(DatabaseType), SixnetDatabaseObjectType.Column));
                var newValueExpression = FormatUpdateValueField(context, command, newValue);
                updateSetArray.Add($"{fieldFormattedName}={newValueExpression}");
            }
            var entityType = dataCommandExecutionContext.Command.GetEntityType();

            var tableNames = dataCommandExecutionContext.GetTableNames(command);
            SixnetDirectThrower.ThrowInvalidOperationIf(tableNames.IsNullOrEmpty(), $"Get table name failed for {entityType.Name}");

            string scriptTemplate;
            if (preScripts.IsNullOrEmpty() && string.IsNullOrWhiteSpace(join))
            {
                scriptTemplate = $"UPDATE {{0}}{TablePetNameKeyword}{tablePetName} SET {string.Join(",", updateSetArray)}{condition};";
            }
            else
            {
                var queryStatement = GenerateQueryStatementCore(context, translationResult, SixnetQueryableLocation.JoinTarget);
                var updateTablePetName = "UTB";
                var joinItems = FormatWrapJoinPrimaryKeys(context, command.Queryable, command.GetEntityType(), tablePetName, tablePetName, updateTablePetName);
                scriptTemplate = $"{FormatPreScript(context)}UPDATE {{0}}{TablePetNameKeyword}{tablePetName} SET {string.Join(",", updateSetArray)} FROM ({queryStatement.Script}){TablePetNameKeyword}{updateTablePetName}{ConditionStartKeyword}{string.Join(" AND ", joinItems)};";
            }

            // parameters
            var parameters = ConvertParameter(command.ScriptParameters) ?? new SixnetDataCommandParameters();
            parameters.Union(context.GetParameters());

            // statements
            var statements = new List<SixnetExecutionDatabaseStatement>();
            foreach (var tableName in tableNames)
            {
                statements.Add(SixnetExecutionDatabaseStatement.Create(DatabaseType, data =>
                {
                    data.Script = string.Format(scriptTemplate, FormatAndWrapObjectName(tableName));
                    data.ScriptType = GetCommandType(command);
                    data.Parameters = parameters;
                    data.MustAffectData = true;
                    data.HasPreScript = !preScripts.IsNullOrEmpty();
                }));
            }
            return statements;

            #endregion
        }

        #endregion

        #region Get delete statement

        /// <summary>
        /// Get delete statement
        /// </summary>
        /// <param name="context">Command resolve context</param>
        /// <returns></returns>
        protected override List<SixnetExecutionDatabaseStatement> GenerateDeleteStatements(SixnetDataCommandResolveContext context)
        {
            var dataCommandExecutionContext = context.DataCommandExecutionContext;
            var command = dataCommandExecutionContext.Command;

            #region translate

            var translationResult = Translate(context);
            var condition = translationResult?.GetCondition(ConditionStartKeyword);
            var join = translationResult?.GetJoin();
            var preScripts = context.GetPreScripts();

            #endregion

            #region script

            var entityType = dataCommandExecutionContext.Command.GetEntityType();

            var tableNames = dataCommandExecutionContext.GetTableNames(command);
            SixnetDirectThrower.ThrowInvalidOperationIf(tableNames.IsNullOrEmpty(), $"Get table name failed for {entityType.Name}");
            var tablePetName = command.Queryable == null ? context.GetNewTablePetName() : context.GetDefaultTablePetName(command.Queryable);

            string scriptTemplate;
            if (preScripts.IsNullOrEmpty() && string.IsNullOrWhiteSpace(join))
            {
                scriptTemplate = $"DELETE FROM {{0}}{TablePetNameKeyword}{tablePetName}{condition};";
            }
            else
            {
                var queryStatement = GenerateQueryStatementCore(context, translationResult, SixnetQueryableLocation.JoinTarget);
                var deleteTablePetName = "DTB";
                var joinItems = FormatWrapJoinPrimaryKeys(context, command.Queryable, command.GetEntityType(), tablePetName, tablePetName, deleteTablePetName);
                scriptTemplate = $"{FormatPreScript(context)}DELETE FROM {{0}}{TablePetNameKeyword}{tablePetName} USING ({queryStatement.Script}){TablePetNameKeyword}{deleteTablePetName} WHERE {string.Join(" AND ", joinItems)};";
            }

            // parameters
            var parameters = ConvertParameter(command.ScriptParameters) ?? new SixnetDataCommandParameters();
            parameters.Union(context.GetParameters());

            // statements
            var statements = new List<SixnetExecutionDatabaseStatement>();
            foreach (var tableName in tableNames)
            {
                statements.Add(SixnetExecutionDatabaseStatement.Create(DatabaseType, data =>
                {
                    data.Script = string.Format(scriptTemplate, FormatAndWrapObjectName(tableName));
                    data.ScriptType = GetCommandType(command);
                    data.Parameters = parameters;
                    data.MustAffectData = true;
                    data.HasPreScript = !preScripts.IsNullOrEmpty();
                }));
            }
            return statements;

            #endregion
        }

        #endregion

        #endregion

        #region Migration

        #region Get create table statements

        protected override SixnetDatabaseScriptInfo GetCreateTableScripts(SixnetGetCreateTableDefineScriptParameter parameter)
        {
            var tableName = FormatAndWrapObjectName(parameter.Table);
            var columnInfo = parameter.ColumnDefineInfo;
            var fields = columnInfo.ColumnScripts;
            var parimaryKeys = columnInfo.PrimaryKeys;
            var migrationInfo = parameter.MigrationInfo;
            var script = $"CREATE TABLE IF NOT EXISTS {tableName} ({string.Join(",", fields)}{(parimaryKeys.IsNullOrEmpty() ? "" : ", PRIMARY KEY (" + string.Join(",", parimaryKeys) + ")")});";
            return new SixnetDatabaseScriptInfo()
            {
                Scripts = new List<string>() { script }
            };
        }

        #endregion

        #region Get delete all table statements

        protected override SixnetDatabaseScriptInfo GetDeleteAllTableScripts(SixnetDeleteAllTableParameter parameter)
        {
            var schema = parameter.Schema;
            var command = parameter.Command;
            var sql = $@"
SELECT
    'DROP TABLE IF EXISTS '
    || quote_ident(schemaname)
    || '.'
    || quote_ident(tablename)
    || ' CASCADE;'
FROM pg_tables
WHERE schemaname = '{schema}' AND tableowner = current_user;
";
            return new SixnetDatabaseScriptInfo()
            {
                Scripts = command.Connection.DbConnection.Query<string>(sql, transaction: command.Connection.Transaction.DbTransaction)?.ToList() ?? new List<string>(0)
            };
        }

        #endregion

        #region Get rename table statements

        /// <summary>
        /// Get rename table statements
        /// </summary>
        /// <param name="parameter"></param>
        /// <returns></returns>
        protected override SixnetDatabaseScriptInfo GetRenameTableScripts(SixnetRenameTableParameter parameter)
        {
            var oldFormattedTableName = FormatAndWrapObjectName(parameter.CurrentTableName);
            var newFormattedTableName = WrapObjectName(FormatObjectName(parameter.NewTableName));
            var script = $@"
DO $$
BEGIN
    IF to_regclass('{oldFormattedTableName}') IS NOT NULL THEN
        ALTER TABLE {oldFormattedTableName}
        RENAME TO {newFormattedTableName.Name};
    END IF;
END $$;
";
            return new SixnetDatabaseScriptInfo()
            {
                Scripts = new List<string>()
                {
                    script
                }
            };
        }

        #endregion


        #region Get add filed statements

        /// <summary>
        /// Get add field scripts
        /// </summary>
        /// <param name="parameter"></param>
        /// <returns></returns>
        protected override SixnetDatabaseScriptInfo GetAddFieldScripts(SixnetAddFieldParameter parameter)
        {
            var table = parameter.Table;
            var fields = parameter.Fields;
            var command = parameter.Command;
            var formattedTableName = FormatAndWrapObjectName(table);
            var scripts = new List<string>();
            foreach (var field in fields)
            {
                var dataFieldName = FormatObjectName(SixnetDatabaseObjectName.Create(field.GetFieldName(DatabaseType), SixnetDatabaseObjectType.Column));
                scripts.Add($@"
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_attribute
        WHERE attrelid = to_regclass('{formattedTableName}')
          AND attname = '{dataFieldName.Name}'
          AND NOT attisdropped
    ) THEN
        ALTER TABLE {formattedTableName}
        ADD COLUMN {WrapObjectName(dataFieldName).Name} {GetFieldDefinition(field, command.MigrationInfo)};
    END IF;
END $$;
");
            }
            return new SixnetDatabaseScriptInfo()
            {
                Scripts = scripts
            };
        }

        #endregion

        #region Get update field statements 

        /// <summary>
        /// Get update field scripts
        /// </summary>
        /// <param name="parameter"></param>
        /// <returns></returns>
        protected override SixnetDatabaseScriptInfo GetUpdateFieldScripts(SixnetUpdateFieldParameter parameter)
        {
            var scripts = new List<string>();
            var table = parameter.Table;
            var fields = parameter.Fields;
            var command = parameter.Command;
            var formattedTableName = FormatAndWrapObjectName(table);
            foreach (var fieldItem in fields)
            {
                var field = fieldItem.Value;
                var nowFieldName = fieldItem.Key;
                var nowFormatedFieldName = FormatObjectName(SixnetDatabaseObjectName.Create(nowFieldName, SixnetDatabaseObjectType.Column));
                var newFieldName = FormatObjectName(SixnetDatabaseObjectName.Create(field.GetFieldName(DatabaseType), SixnetDatabaseObjectType.Column));

                var defaultValue = GetSqlDefaultValue(field, command.MigrationInfo);
                var alterColumnExpressions = new List<string>()
                {
                    $"ALTER COLUMN {WrapObjectName(nowFormatedFieldName)} TYPE {GetSqlDataType(field, command.MigrationInfo)}",
                    $"ALTER COLUMN {WrapObjectName(nowFormatedFieldName)} SET {GetFieldNullable(field, command.MigrationInfo)}"
                };
                if (!string.IsNullOrWhiteSpace(defaultValue))
                {
                    alterColumnExpressions.Add($"ALTER COLUMN {WrapObjectName(nowFormatedFieldName)} SET DEFAULT {defaultValue}");
                }
                scripts.Add($@"
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_attribute
        WHERE attrelid = to_regclass('{formattedTableName}')
          AND attname = '{nowFormatedFieldName}'
          AND NOT attisdropped
    ) THEN
        ALTER TABLE {formattedTableName} {string.Join(",", alterColumnExpressions)};
    END IF;
END $$;
");

                if (!string.Equals(nowFormatedFieldName.Name, newFieldName.Name, StringComparison.OrdinalIgnoreCase))
                {
                    scripts.Add($@"
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM pg_attribute
        WHERE attrelid = to_regclass('{formattedTableName}')
          AND attname = '{nowFormatedFieldName}'
          AND NOT attisdropped
    ) THEN
        ALTER TABLE {formattedTableName}
        RENAME COLUMN {WrapObjectName(nowFormatedFieldName)}
        TO {WrapObjectName(newFieldName)};
    END IF;
END $$;
");
                }
            }
            return new SixnetDatabaseScriptInfo()
            {
                Scripts = scripts
            };
        }

        #endregion

        #region Get delete filed statements

        protected override SixnetDatabaseScriptInfo GetDeleteFieldScripts(SixnetDeleteFieldParameter parameter)
        {
            var scripts = new List<string>();
            var table = parameter.Table;
            var fields = parameter.Fields;
            var formattedTableName = FormatAndWrapObjectName(table);
            foreach (var field in fields)
            {
                var dataFieldName = FormatObjectName(SixnetDatabaseObjectName.Create(field.GetFieldName(DatabaseType), SixnetDatabaseObjectType.Column));
                scripts.Add($"ALTER TABLE {formattedTableName} DROP COLUMN IF EXISTS {WrapObjectName(dataFieldName).Name};");
            }
            return new SixnetDatabaseScriptInfo()
            {
                Scripts = scripts
            };
        }

        #endregion

        #region Add foreign key


        /// <summary>
        /// Get add foreign key scripts
        /// </summary>
        /// <param name="parameter"></param>
        /// <returns></returns>
        protected override SixnetDatabaseScriptInfo GetAddForeignKeyScripts(SixnetAddForeignKeyParameter parameter)
        {
            var foreignKeyInfo = parameter.ForeignKeyInfo;

            var sourceFieldName = FormatObjectName(foreignKeyInfo.SourceField);
            var formattedSourceTableName = FormatObjectName(foreignKeyInfo.SourceTable);
            var wrapedSourceTableName = FormatAndWrapObjectName(foreignKeyInfo.SourceTable);

            var referenceFieldName = FormatAndWrapObjectName(foreignKeyInfo.ReferenceField);
            var referenceTableName = FormatAndWrapObjectName(foreignKeyInfo.ReferenceTable);

            var constraintName = GetForeignKeyName(formattedSourceTableName, sourceFieldName);
            return new SixnetDatabaseScriptInfo()
            {
                Scripts = new List<string>()
                {
                    $@"
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_constraint
        WHERE conname = '{constraintName}'
          AND conrelid = to_regclass('{wrapedSourceTableName}')
    ) THEN
        ALTER TABLE {wrapedSourceTableName}
        ADD CONSTRAINT {WrapObjectName(constraintName)}
        FOREIGN KEY ({WrapObjectName(sourceFieldName)})
        REFERENCES {referenceTableName} ({referenceFieldName});
    END IF;
END $$;
"
                }
            };
        }

        #endregion

        #region Delete foreign key

        /// <summary>
        /// Get delete foreign key scripts
        /// </summary>
        /// <param name="parameter"></param>
        /// <returns></returns>
        public override SixnetDatabaseScriptInfo GetDeleteForeignKeyScripts(SixnetDeleteForeignKeyParameter parameter)
        {
            var foreignKeyInfo = parameter.ForeignKeyInfo;
            var sourceFieldName = FormatObjectName(foreignKeyInfo.SourceField);
            var formattedSourceTableName = FormatObjectName(foreignKeyInfo.SourceTable);
            var wrapedSourceTableName = FormatAndWrapObjectName(foreignKeyInfo.SourceTable);
            var constraintName = WrapObjectName(GetForeignKeyName(formattedSourceTableName, sourceFieldName));
            return new SixnetDatabaseScriptInfo()
            {
                Scripts = new List<string>()
                {
                    $"ALTER TABLE {wrapedSourceTableName} DROP CONSTRAINT IF EXISTS {constraintName};"
                }
            };
        }

        /// <summary>
        /// Get delete all foreign key scripts
        /// </summary>
        /// <param name="parameter"></param>
        /// <returns></returns>
        protected override SixnetDatabaseScriptInfo GetDeleteAllForeignKeyScripts(SixnetDeleteAllForeignKeyParameter parameter)
        {
            var sql = $@"
SELECT
    'ALTER TABLE '
    || quote_ident(n.nspname)
    || '.'
    || quote_ident(c.relname)
    || ' DROP CONSTRAINT IF EXISTS '
    || quote_ident(con.conname)
    || ';'
FROM pg_constraint con
JOIN pg_class c ON con.conrelid = c.oid
JOIN pg_namespace n ON c.relnamespace = n.oid
WHERE con.contype = 'f' AND n.nspname = '{parameter.Schema}';
";
            var deleteScripts = parameter.Command.Connection.DbConnection.Query<string>(sql, transaction: parameter.Command.Connection.Transaction.DbTransaction);
            return new SixnetDatabaseScriptInfo()
            {
                Scripts = deleteScripts.ToList()
            };
        }

        #endregion

        #region Add index

        /// <summary>
        /// Get add index scripts
        /// </summary>
        /// <param name="parameter"></param>
        /// <returns></returns>
        protected override SixnetDatabaseScriptInfo GetAddIndexScripts(SixnetAddIndexParameter parameter)
        {
            var indexInfo = parameter.IndexInfo;
            var formattedAndWrapedTableName = FormatAndWrapObjectName(indexInfo.Table);
            var indexDefine = GetIndexDefine(indexInfo);
            var indexName = indexDefine.Item1;
            var indexFieldStrings = indexDefine.Item2;

            return new SixnetDatabaseScriptInfo()
            {
                Scripts = new List<string>()
                {
                    $@"
CREATE {(indexInfo.Unique ? "UNIQUE " : "")}
INDEX IF NOT EXISTS {WrapObjectName(indexName)}
ON {formattedAndWrapedTableName}
({string.Join(",", indexFieldStrings)});
"
                }
            };
        }

        #endregion

        #region Delete index

        /// <summary>
        /// Get delete index scripts
        /// </summary>
        /// <param name="parameter"></param>
        /// <returns></returns>
        protected override SixnetDatabaseScriptInfo GetDeleteIndexScripts(SixnetDeleteIndexParameter parameter)
        {
            var indexInfo = parameter.IndexInfo;
            var indexName = GetIndexDefine(indexInfo).Item1;
            var formattedAndWrapedTableName = FormatAndWrapObjectName(indexInfo.Table);

            return new SixnetDatabaseScriptInfo()
            {
                Scripts = new List<string>()
                {
                    $@"DROP INDEX IF EXISTS {WrapObjectName(indexName)};"
                }
            };
        }

        #endregion


        #region Get delete all view statements

        protected override SixnetDatabaseScriptInfo GetDeleteAllViewScripts(SixnetDeleteAllViewParameter parameter)
        {
            var sql = $@"
SELECT
    'DROP VIEW IF EXISTS '
    || quote_ident(schemaname)
    || '.'
    || quote_ident(viewname)
    || ' CASCADE;'
FROM pg_views
WHERE schemaname = '{parameter.Schema}' AND viewowner = current_user;
";
            var deleteScripts = parameter.Command.Connection.DbConnection.Query<string>(sql, transaction: parameter.Command.Connection.Transaction.DbTransaction);
            return new SixnetDatabaseScriptInfo()
            {
                Scripts = deleteScripts?.ToList() ?? new List<string>(0)
            };
        }

        #endregion

        #region Get delete all function statements

        /// <summary>
        /// Get delete all function statements
        /// </summary>
        /// <param name="migrationCommand"></param>
        /// <returns></returns>
        protected override SixnetDatabaseScriptInfo GetDeleteAllFunctionScripts(SixnetDeleteAllFunctionParameter parameter)
        {
            var sql = $@"
SELECT
    'DROP FUNCTION IF EXISTS '
    || quote_ident(n.nspname)
    || '.'
    || quote_ident(p.proname)
    || '('
    || pg_get_function_identity_arguments(p.oid)
    || ') CASCADE;'
FROM pg_proc p
JOIN pg_namespace n ON p.pronamespace = n.oid
WHERE p.prokind = 'f' AND n.nspname = '{parameter.Schema}' AND p.proowner = (
      SELECT r.oid
      FROM pg_roles r
      WHERE r.rolname::text = CURRENT_USER::text
  );
";
            var deleteScripts = parameter.Command.Connection.DbConnection.Query<string>(sql, transaction: parameter.Command.Connection.Transaction.DbTransaction);
            return new SixnetDatabaseScriptInfo()
            {
                Scripts = deleteScripts?.ToList() ?? new List<string>(0)
            };
        }

        #endregion

        #region Get delete all custom type statements

        protected override SixnetDatabaseScriptInfo GetDeleteAllCustomTypeScripts(SixnetDeleteAllCustomerTypeParameter parameter)
        {
            var sql = $@"
SELECT
    'DROP TYPE IF EXISTS '
    || quote_ident(n.nspname)
    || '.'
    || quote_ident(t.typname)
    || ' CASCADE;'
FROM pg_type t
JOIN pg_namespace n ON t.typnamespace = n.oid
WHERE n.nspname = '{parameter.Schema}'
  AND t.typtype IN ('c', 'e', 'r') AND t.typowner = (
      SELECT r.oid
      FROM pg_roles r
      WHERE r.rolname::text = CURRENT_USER::text
  );
";
            var deleteScripts = parameter.Command.Connection.DbConnection.Query<string>(sql, transaction: parameter.Command.Connection.Transaction.DbTransaction);
            return new SixnetDatabaseScriptInfo()
            {
                Scripts = deleteScripts?.ToList() ?? new List<string>(0)
            };
        }

        #endregion

        #region Get delete all procedure statements

        protected override SixnetDatabaseScriptInfo GetDeleteAllProcedureScripts(SixnetDeleteAllProcedureParameter parameter)
        {
            var sql = $@"
SELECT
    'DROP PROCEDURE IF EXISTS '
    || quote_ident(n.nspname)
    || '.'
    || quote_ident(p.proname)
    || '('
    || pg_get_function_identity_arguments(p.oid)
    || ') CASCADE;'
FROM pg_proc p
JOIN pg_namespace n
    ON p.pronamespace = n.oid
WHERE p.prokind = 'p'
  AND n.nspname = '{parameter.Schema}' AND p.proowner = (
      SELECT r.oid
      FROM pg_roles r
      WHERE r.rolname::text = CURRENT_USER::text
  );
";
            var deleteScripts = parameter.Command.Connection.DbConnection.Query<string>(sql, transaction: parameter.Command.Connection.Transaction.DbTransaction);
            return new SixnetDatabaseScriptInfo()
            {
                Scripts = deleteScripts?.ToList() ?? new List<string>(0)
            };
        }

        #endregion

        #endregion

        #region Util

        #region Get limit string

        /// <summary>
        /// Get limit string
        /// </summary>
        /// <param name="offsetNum">Offset num</param>
        /// <param name="takeNum">Take num</param>
        /// <returns></returns>
        protected override string GetLimitString(int offsetNum, int takeNum, bool hasSort)
        {
            if (takeNum < 1)
            {
                return string.Empty;
            }
            if (offsetNum < 0)
            {
                offsetNum = 0;
            }
            return $" LIMIT {takeNum} OFFSET {offsetNum}";

        }

        #endregion

        #region Get field sql data type

        /// <summary>
        /// Get sql data type
        /// </summary>
        /// <param name="field">Field</param>
        /// <returns></returns>
        protected override string GetSqlDataType(SixnetDataField field, SixnetMigrationInfo options)
        {
            SixnetDirectThrower.ThrowArgNullIf(field == null, nameof(field));
            var dbTypeName = "";
            if (!string.IsNullOrWhiteSpace(field.DbType))
            {
                dbTypeName = field.DbType;
            }
            else
            {
                var dbType = field.GetDataType().GetDbType();
                var length = field.Length;
                var precision = field.Precision;
                var notFixedLength = options.NotFixedLength || field.HasDbFeature(SixnetFieldDbFeature.NotFixedLength);
                static int getCharLength(int flength, int defLength) => flength < 1 ? defLength : flength;
                switch (dbType)
                {
                    case DbType.Binary:
                        dbTypeName = "BYTEA";
                        break;
                    case DbType.Boolean:
                        dbTypeName = "BOOLEAN";
                        break;
                    case DbType.Currency:
                        dbTypeName = "MONEY";
                        break;
                    case DbType.Date:
                        dbTypeName = "DATE";
                        break;
                    case DbType.DateTime:
                    case DbType.DateTime2:
                        dbTypeName = "TIMESTAMP WITHOUT TIME ZONE";
                        break;
                    case DbType.DateTimeOffset:
                        dbTypeName = "TIMESTAMP WITH TIME ZONE";
                        break;
                    case DbType.Decimal:
                        dbTypeName = "NUMERIC";
                        break;
                    case DbType.Double:
                        dbTypeName = "DOUBLE PRECISION";
                        break;
                    case DbType.Guid:
                        dbTypeName = "UUID";
                        break;
                    case DbType.Int16:
                    case DbType.SByte:
                    case DbType.Byte:
                        dbTypeName = "SMALLINT";
                        break;
                    case DbType.Int32:
                    case DbType.UInt16:
                        dbTypeName = "INTEGER";
                        break;
                    case DbType.Int64:
                    case DbType.UInt32:
                        dbTypeName = "BIGINT";
                        break;
                    case DbType.UInt64:
                        dbTypeName = "NUMERIC(20,0)";
                        break;
                    case DbType.Single:
                        dbTypeName = "REAL";
                        break;
                    case DbType.String:
                    case DbType.AnsiString:
                        length = getCharLength(length, DefaultCharLength);
                        dbTypeName = length > 800 ? "TEXT" : $"VARCHAR({length})";
                        break;
                    case DbType.StringFixedLength:
                    case DbType.AnsiStringFixedLength:
                        dbTypeName = $"CHAR({getCharLength(length, DefaultCharLength)})";
                        break;
                    case DbType.Time:
                        dbTypeName = "INTERVAL";
                        break;
                    default:
                        throw new NotSupportedException(dbType.ToString());
                }
            }
            return $" {dbTypeName}";
        }

        #endregion

        #region Get field identity

        /// <summary>
        /// Get field identity
        /// </summary>
        /// <param name="field">Field</param>
        /// <param name="options">Options</param>
        /// <returns></returns>
        protected override string GetFieldIdentity(SixnetDataField field, SixnetMigrationInfo options)
        {
            SixnetDirectThrower.ThrowArgNullIf(field == null, nameof(field));
            if (!field.InRole(SixnetFieldRole.Increment))
            {
                return string.Empty;
            }
            var startValue = field.StartValue;
            if (startValue == 0)
            {
                startValue = 1;
            }
            var incrementValue = field.IncrementValue;
            if (incrementValue == 0)
            {
                incrementValue = 1;
            }

            return $" GENERATED BY DEFAULT AS IDENTITY (START WITH {startValue} INCREMENT BY {incrementValue})";
        }

        #endregion

        #endregion
    }
}
