using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dapper;
using Npgsql;
using EZNEW.Development.Entity;
using EZNEW.Development.Query;
using EZNEW.Development.Query.Translation;
using EZNEW.Development.Command;
using EZNEW.Exceptions;
using EZNEW.Data.Configuration;
using EZNEW.Data.Modification;

namespace EZNEW.Data.PostgreSQL
{
    /// <summary>
    /// Imeplements database provider for the PostgreSQL
    /// </summary>
    public class PostgreSqlProvider : IDatabaseProvider
    {
        const DatabaseServerType CurrentDatabaseServerType = PostgreSqlManager.CurrentDatabaseServerType;

        #region Execute

        /// <summary>
        /// Execute command
        /// </summary>
        /// <param name="server">Database server</param>
        /// <param name="executionOptions">Execution options</param>
        /// <param name="commands">Commands</param>
        /// <returns>Return the affected data numbers</returns>
        public int Execute(DatabaseServer server, CommandExecutionOptions executionOptions, IEnumerable<ICommand> commands)
        {
            return ExecuteAsync(server, executionOptions, commands).Result;
        }

        /// <summary>
        /// Execute command
        /// </summary>
        /// <param name="server">Database server</param>
        /// <param name="executionOptions">Execution options</param>
        /// <param name="commands">Commands</param>
        /// <returns>Return the affected data numbers</returns>
        public int Execute(DatabaseServer server, CommandExecutionOptions executionOptions, params ICommand[] commands)
        {
            return ExecuteAsync(server, executionOptions, commands).Result;
        }

        /// <summary>
        /// Execute command
        /// </summary>
        /// <param name="server">Database server</param>
        /// <param name="executionOptions">Execution options</param>
        /// <param name="commands">Commands</param>
        /// <returns>Return the affected data numbers</returns>
        public async Task<int> ExecuteAsync(DatabaseServer server, CommandExecutionOptions executionOptions, params ICommand[] commands)
        {
            IEnumerable<ICommand> cmdCollection = commands;
            return await ExecuteAsync(server, executionOptions, cmdCollection).ConfigureAwait(false);
        }

        /// <summary>
        /// Execute command
        /// </summary>
        /// <param name="server">Database server</param>
        /// <param name="executionOptions">Execution options</param>
        /// <param name="commands">Commands</param>
        /// <returns>Return the affected data numbers</returns>
        public async Task<int> ExecuteAsync(DatabaseServer server, CommandExecutionOptions executionOptions, IEnumerable<ICommand> commands)
        {
            #region group execution commands

            IQueryTranslator translator = PostgreSqlManager.GetQueryTranslator(DataAccessContext.Create(server));
            List<DatabaseExecutionCommand> databaseExecutionCommands = new List<DatabaseExecutionCommand>();
            var batchExecutionConfig = DataManager.GetBatchExecutionConfiguration(server.ServerType) ?? BatchExecutionConfiguration.Default;
            var groupStatementsCount = batchExecutionConfig.GroupStatementsCount;
            groupStatementsCount = groupStatementsCount < 0 ? 1 : groupStatementsCount;
            var groupParameterCount = batchExecutionConfig.GroupParametersCount;
            groupParameterCount = groupParameterCount < 0 ? 1 : groupParameterCount;
            StringBuilder commandTextBuilder = new StringBuilder();
            CommandParameters parameters = null;
            int statementsCount = 0;
            bool forceReturnValue = false;
            int cmdCount = 0;

            DatabaseExecutionCommand GetGroupExecuteCommand()
            {
                var executionCommand = new DatabaseExecutionCommand()
                {
                    CommandText = commandTextBuilder.ToString(),
                    CommandType = CommandType.Text,
                    MustAffectedData = forceReturnValue,
                    Parameters = parameters
                };
                statementsCount = 0;
                translator.ParameterSequence = 0;
                commandTextBuilder.Clear();
                parameters = null;
                forceReturnValue = false;
                return executionCommand;
            }

            foreach (var cmd in commands)
            {
                DatabaseExecutionCommand databaseExecutionCommand = GetDatabaseExecutionCommand(translator, cmd as DefaultCommand);
                if (databaseExecutionCommand == null)
                {
                    continue;
                }

                //Trace log
                PostgreSqlManager.LogExecutionCommand(databaseExecutionCommand);

                cmdCount++;
                if (databaseExecutionCommand.PerformAlone)
                {
                    if (statementsCount > 0)
                    {
                        databaseExecutionCommands.Add(GetGroupExecuteCommand());
                    }
                    databaseExecutionCommands.Add(databaseExecutionCommand);
                    continue;
                }
                commandTextBuilder.AppendLine(databaseExecutionCommand.CommandText);
                parameters = parameters == null ? databaseExecutionCommand.Parameters : parameters.Union(databaseExecutionCommand.Parameters);
                forceReturnValue |= databaseExecutionCommand.MustAffectedData;
                statementsCount++;
                if (translator.ParameterSequence >= groupParameterCount || statementsCount >= groupStatementsCount)
                {
                    databaseExecutionCommands.Add(GetGroupExecuteCommand());
                }
            }
            if (statementsCount > 0)
            {
                databaseExecutionCommands.Add(GetGroupExecuteCommand());
            }

            #endregion

            return await ExecuteDatabaseCommandAsync(server, executionOptions, databaseExecutionCommands, executionOptions?.ExecutionByTransaction ?? cmdCount > 1).ConfigureAwait(false);
        }

        /// <summary>
        /// Execute database command
        /// </summary>
        /// <param name="server">Database server</param>
        /// <param name="executionOptions">Execution options</param>
        /// <param name="databaseExecutionCommands">Database execution commands</param>
        /// <param name="useTransaction">Whether use transaction</param>
        /// <returns>Return affected data number</returns>
        async Task<int> ExecuteDatabaseCommandAsync(DatabaseServer server, CommandExecutionOptions executionOptions, IEnumerable<DatabaseExecutionCommand> databaseExecutionCommands, bool useTransaction)
        {
            int resultValue = 0;
            bool success = true;
            using (var conn = PostgreSqlManager.GetConnection(server))
            {
                IDbTransaction transaction = null;
                if (useTransaction)
                {
                    transaction = PostgreSqlManager.GetExecutionTransaction(conn, executionOptions);
                }
                try
                {
                    foreach (var cmd in databaseExecutionCommands)
                    {
                        var cmdDefinition = new CommandDefinition(cmd.CommandText, PostgreSqlManager.ConvertCmdParameters(cmd.Parameters), transaction: transaction, commandType: cmd.CommandType, cancellationToken: executionOptions?.CancellationToken ?? default);
                        var executeResultValue = await conn.ExecuteAsync(cmdDefinition).ConfigureAwait(false);
                        success = success && (cmd.MustAffectedData ? executeResultValue > 0 : true);
                        resultValue += executeResultValue;
                        if (useTransaction && !success)
                        {
                            break;
                        }
                    }
                    if (!useTransaction)
                    {
                        return resultValue;
                    }
                    if (success)
                    {
                        transaction.Commit();
                    }
                    else
                    {
                        resultValue = 0;
                        transaction.Rollback();
                    }
                    return resultValue;
                }
                catch (Exception ex)
                {
                    resultValue = 0;
                    transaction?.Rollback();
                    throw ex;
                }
            }
        }

        /// <summary>
        /// Get database execution command
        /// </summary>
        /// <param name="queryTranslator">Query translator</param>
        /// <param name="command">Command</param>
        /// <returns>Return a database execution command</returns>
        DatabaseExecutionCommand GetDatabaseExecutionCommand(IQueryTranslator queryTranslator, DefaultCommand command)
        {
            DatabaseExecutionCommand GetTextCommand()
            {
                return new DatabaseExecutionCommand()
                {
                    CommandText = command.Text,
                    Parameters = PostgreSqlManager.ConvertParameter(command.Parameters),
                    CommandType = PostgreSqlManager.GetCommandType(command),
                    MustAffectedData = command.MustAffectedData,
                    HasPreScript = true
                };
            }
            if (command.ExecutionMode == CommandExecutionMode.CommandText)
            {
                return GetTextCommand();
            }
            DatabaseExecutionCommand executionCommand;
            switch (command.OperationType)
            {
                case CommandOperationType.Insert:
                    executionCommand = GetDatabaseInsertionCommand(queryTranslator, command);
                    break;
                case CommandOperationType.Update:
                    executionCommand = GetDatabaseUpdateCommand(queryTranslator, command);
                    break;
                case CommandOperationType.Delete:
                    executionCommand = GetDatabaseDeletionCommand(queryTranslator, command);
                    break;
                default:
                    executionCommand = GetTextCommand();
                    break;
            }
            return executionCommand;
        }

        /// <summary>
        /// Get database insertion execution command
        /// </summary>
        /// <param name="translator">Query translator</param>
        /// <param name="command">Command</param>
        /// <returns>Return a database insertion command</returns>
        DatabaseExecutionCommand GetDatabaseInsertionCommand(IQueryTranslator translator, DefaultCommand command)
        {
            translator.DataAccessContext.SetCommand(command);
            string objectName = translator.DataAccessContext.GetCommandEntityObjectName(command);
            var fields = DataManager.GetEditFields(CurrentDatabaseServerType, command.EntityType);
            var fieldCount = fields.GetCount();
            var insertFormatResult = PostgreSqlManager.FormatInsertionFields(command.EntityType, fieldCount, fields, command.Parameters, translator.ParameterSequence);
            if (insertFormatResult == null)
            {
                return null;
            }
            string cmdText = $"INSERT INTO {PostgreSqlManager.WrapKeyword(objectName)} ({string.Join(",", insertFormatResult.Item1)}) VALUES ({string.Join(",", insertFormatResult.Item2)});";
            CommandParameters parameters = insertFormatResult.Item3;
            translator.ParameterSequence += fieldCount;
            return new DatabaseExecutionCommand()
            {
                CommandText = cmdText,
                CommandType = PostgreSqlManager.GetCommandType(command),
                MustAffectedData = command.MustAffectedData,
                Parameters = parameters
            };
        }

        /// <summary>
        /// Get database update command
        /// </summary>
        /// <param name="translator">Query translator</param>
        /// <param name="command">Command</param>
        /// <returns>Return a database update command</returns>
        DatabaseExecutionCommand GetDatabaseUpdateCommand(IQueryTranslator translator, DefaultCommand command)
        {
            if (command?.Fields.IsNullOrEmpty() ?? true)
            {
                throw new EZNEWException($"No fields are set to update");
            }

            #region query translation

            translator.DataAccessContext.SetCommand(command);
            var tranResult = translator.Translate(command.Query);
            string conditionString = string.Empty;
            if (!string.IsNullOrWhiteSpace(tranResult.ConditionString))
            {
                conditionString += "WHERE " + tranResult.ConditionString;
            }
            string preScript = tranResult.PreScript;
            string joinScript = tranResult.AllowJoin ? tranResult.JoinScript : string.Empty;

            #endregion

            #region script 

            CommandParameters parameters = PostgreSqlManager.ConvertParameter(command.Parameters) ?? new CommandParameters();
            string objectName = translator.DataAccessContext.GetCommandEntityObjectName(command);
            var fields = PostgreSqlManager.GetFields(command.EntityType, command.Fields);
            int parameterSequence = translator.ParameterSequence;
            List<string> updateSetArray = new List<string>();
            foreach (var field in fields)
            {
                var parameterValue = parameters.GetParameterValue(field.PropertyName);
                var parameterName = field.PropertyName;
                string newValueExpression = string.Empty;
                if (parameterValue != null)
                {
                    parameterSequence++;
                    parameterName = PostgreSqlManager.FormatParameterName(parameterName, parameterSequence);
                    parameters.Rename(field.PropertyName, parameterName);
                    if (parameterValue is IModificationValue)
                    {
                        var modifyValue = parameterValue as IModificationValue;
                        parameters.ModifyValue(parameterName, modifyValue.Value);
                        if (parameterValue is CalculationModificationValue)
                        {
                            var calculateModifyValue = parameterValue as CalculationModificationValue;
                            string calChar = PostgreSqlManager.GetSystemCalculationOperator(calculateModifyValue.Operator);
                            newValueExpression = $"{translator.ObjectPetName}.{PostgreSqlManager.WrapKeyword(field.FieldName)}{calChar}{PostgreSqlManager.ParameterPrefix}{parameterName}";
                        }
                    }
                }
                if (string.IsNullOrWhiteSpace(newValueExpression))
                {
                    newValueExpression = $"{PostgreSqlManager.ParameterPrefix}{parameterName}";
                }
                updateSetArray.Add($"{PostgreSqlManager.WrapKeyword(field.FieldName)}={newValueExpression}");
            }
            string cmdText;
            string wrapObjName = PostgreSqlManager.WrapKeyword(objectName);
            if (string.IsNullOrWhiteSpace(joinScript))
            {
                cmdText = $"{preScript}UPDATE {wrapObjName} AS {translator.ObjectPetName} SET {string.Join(",", updateSetArray)} {conditionString};";
            }
            else
            {
                string updateTableShortName = translator.ObjectPetName;
                string updateJoinTableShortName = "UJTB";
                var primaryKeyFormatedResult = FormatWrapJoinPrimaryKeys(command.EntityType, translator.ObjectPetName, updateTableShortName, updateJoinTableShortName);
                cmdText = $"{preScript}UPDATE {wrapObjName} AS {updateTableShortName} SET {string.Join(",", updateSetArray)} FROM (SELECT {string.Join(",", primaryKeyFormatedResult.Item1)} FROM {wrapObjName} AS {translator.ObjectPetName} {joinScript} {conditionString}) AS {updateJoinTableShortName} WHERE {string.Join(" AND ", primaryKeyFormatedResult.Item2)};";
            }
            translator.ParameterSequence = parameterSequence;

            #endregion

            #region parameter

            var queryParameters = PostgreSqlManager.ConvertParameter(tranResult.Parameters);
            parameters.Union(queryParameters);

            #endregion

            return new DatabaseExecutionCommand()
            {
                CommandText = cmdText,
                CommandType = PostgreSqlManager.GetCommandType(command),
                MustAffectedData = command.MustAffectedData,
                Parameters = parameters,
                HasPreScript = !string.IsNullOrWhiteSpace(preScript)
            };
        }

        /// <summary>
        /// Get database deletion command
        /// </summary>
        /// <param name="translator">Query translator</param>
        /// <param name="command">Command</param>
        /// <returns>Return a database deletion command</returns>
        DatabaseExecutionCommand GetDatabaseDeletionCommand(IQueryTranslator translator, DefaultCommand command)
        {
            translator.DataAccessContext.SetCommand(command);

            #region query translation

            var tranResult = translator.Translate(command.Query);
            string conditionString = string.Empty;
            if (!string.IsNullOrWhiteSpace(tranResult.ConditionString))
            {
                conditionString += "WHERE " + tranResult.ConditionString;
            }
            string preScript = tranResult.PreScript;
            string joinScript = tranResult.AllowJoin ? tranResult.JoinScript : string.Empty;

            #endregion

            #region script

            string objectName = translator.DataAccessContext.GetCommandEntityObjectName(command);
            string cmdText;
            string wrapObjectName = PostgreSqlManager.WrapKeyword(objectName);
            if (string.IsNullOrWhiteSpace(joinScript))
            {
                cmdText = $"{preScript}DELETE FROM {wrapObjectName} AS {translator.ObjectPetName} {conditionString};";
            }
            else
            {
                string deleteTableShortName = "DTB";
                string deleteJoinTableShortName = "DJTB";
                var primaryKeyFormatedResult = FormatWrapJoinPrimaryKeys(command.EntityType, translator.ObjectPetName, deleteTableShortName, deleteJoinTableShortName);
                cmdText = $"{preScript}DELETE FROM {wrapObjectName} AS {deleteTableShortName} USING (SELECT {string.Join(",", primaryKeyFormatedResult.Item1)} FROM {wrapObjectName} AS {translator.ObjectPetName} {joinScript} {conditionString}) AS {deleteJoinTableShortName} WHERE {string.Join(" AND ", primaryKeyFormatedResult.Item2)};";
            }

            #endregion

            #region parameter

            CommandParameters parameters = PostgreSqlManager.ConvertParameter(command.Parameters) ?? new CommandParameters();
            var queryParameters = PostgreSqlManager.ConvertParameter(tranResult.Parameters);
            parameters.Union(queryParameters);

            #endregion

            return new DatabaseExecutionCommand()
            {
                CommandText = cmdText,
                CommandType = PostgreSqlManager.GetCommandType(command),
                MustAffectedData = command.MustAffectedData,
                Parameters = parameters,
                HasPreScript = !string.IsNullOrWhiteSpace(preScript)
            };
        }

        Tuple<IEnumerable<string>, IEnumerable<string>> FormatWrapJoinPrimaryKeys(Type entityType, string translatorObjectPetName, string sourceObjectPetName, string targetObjectPetName)
        {
            var primaryKeyFields = DataManager.GetFields(CurrentDatabaseServerType, entityType, EntityManager.GetPrimaryKeys(entityType));
            if (primaryKeyFields.IsNullOrEmpty())
            {
                throw new EZNEWException($"{entityType?.FullName} not set primary key");
            }
            return FormatWrapJoinFields(primaryKeyFields, translatorObjectPetName, sourceObjectPetName, targetObjectPetName);
        }

        Tuple<IEnumerable<string>, IEnumerable<string>> FormatWrapJoinFields(IEnumerable<EntityField> fields, string translatorObjectPetName, string sourceObjectPetName, string targetObjectPetName)
        {
            var joinItems = fields.Select(field =>
            {
                string fieldName = PostgreSqlManager.WrapKeyword(field.FieldName);
                return $"{sourceObjectPetName}.{fieldName} = {targetObjectPetName}.{fieldName}";
            });
            var queryItems = fields.Select(field =>
            {
                string fieldName = PostgreSqlManager.WrapKeyword(field.FieldName);
                return $"{translatorObjectPetName}.{fieldName}";
            });
            return new Tuple<IEnumerable<string>, IEnumerable<string>>(queryItems, joinItems);
        }

        #endregion

        #region Query

        /// <summary>
        /// Query datas
        /// </summary>
        /// <typeparam name="T">Data type</typeparam>
        /// <param name="server">Database server</param>
        /// <param name="command">Command</param>
        /// <returns>Return datas</returns>
        public IEnumerable<T> Query<T>(DatabaseServer server, ICommand command)
        {
            return QueryAsync<T>(server, command).Result;
        }

        /// <summary>
        /// Query datas
        /// </summary>
        /// <typeparam name="T">Data type</typeparam>
        /// <param name="server">Database server</param>
        /// <param name="command">Command</param>
        /// <returns>Return datas</returns>
        public async Task<IEnumerable<T>> QueryAsync<T>(DatabaseServer server, ICommand command)
        {
            if (command.Query == null)
            {
                throw new EZNEWException($"{nameof(ICommand.Query)} is null");
            }

            #region query translation

            IQueryTranslator translator = PostgreSqlManager.GetQueryTranslator(DataAccessContext.Create(server, command));
            var tranResult = translator.Translate(command.Query);
            string joinScript = tranResult.AllowJoin ? tranResult.JoinScript : string.Empty;

            #endregion

            #region script

            string cmdText;
            switch (command.Query.ExecutionMode)
            {
                case QueryExecutionMode.Text:
                    cmdText = tranResult.ConditionString;
                    break;
                case QueryExecutionMode.QueryObject:
                default:
                    int size = command.Query.QuerySize;
                    string objectName = translator.DataAccessContext.GetCommandEntityObjectName(command);
                    string sortString = string.IsNullOrWhiteSpace(tranResult.SortString) ? string.Empty : $"ORDER BY {tranResult.SortString}";
                    var queryFields = PostgreSqlManager.GetQueryFields(command.Query, command.EntityType, true);
                    string outputFormatedField = string.Join(",", PostgreSqlManager.FormatQueryFields(translator.ObjectPetName, queryFields, true));
                    if (string.IsNullOrWhiteSpace(tranResult.CombineScript))
                    {
                        cmdText = $"{tranResult.PreScript}SELECT {outputFormatedField} FROM {PostgreSqlManager.WrapKeyword(objectName)} AS {translator.ObjectPetName} {joinScript} {(string.IsNullOrWhiteSpace(tranResult.ConditionString) ? string.Empty : $"WHERE {tranResult.ConditionString}")} {sortString} {(size > 0 ? $"LIMIT {size} OFFSET 0" : string.Empty)}";
                    }
                    else
                    {
                        string innerFormatedField = string.Join(",", PostgreSqlManager.FormatQueryFields(translator.ObjectPetName, queryFields, false));
                        cmdText = $"{tranResult.PreScript}SELECT {outputFormatedField} FROM (SELECT {innerFormatedField} FROM {PostgreSqlManager.WrapKeyword(objectName)} AS {translator.ObjectPetName} {joinScript} {(string.IsNullOrWhiteSpace(tranResult.ConditionString) ? string.Empty : $"WHERE {tranResult.ConditionString}")} {tranResult.CombineScript}) AS {translator.ObjectPetName} {sortString} {(size > 0 ? $"LIMIT {size} OFFSET 0" : string.Empty)}";
                    }
                    break;
            }

            #endregion

            #region parameter

            var parameters = PostgreSqlManager.ConvertCmdParameters(PostgreSqlManager.ConvertParameter(tranResult.Parameters));

            #endregion

            //Trace log
            PostgreSqlManager.LogScript(cmdText, tranResult.Parameters);

            using (var conn = PostgreSqlManager.GetConnection(server))
            {
                var tran = PostgreSqlManager.GetQueryTransaction(conn, command.Query);
                var cmdDefinition = new CommandDefinition(cmdText, parameters, transaction: tran, commandType: PostgreSqlManager.GetCommandType(command as DefaultCommand), cancellationToken: command.Query?.GetCancellationToken() ?? default);
                return await conn.QueryAsync<T>(cmdDefinition).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Query paging data
        /// </summary>
        /// <typeparam name="T">Data type</typeparam>
        /// <param name="server">Databse server</param>
        /// <param name="command">Command</param>
        /// <returns>Return paging data</returns>
        public IEnumerable<T> QueryPaging<T>(DatabaseServer server, ICommand command)
        {
            return QueryPagingAsync<T>(server, command).Result;
        }

        /// <summary>
        /// Query paging data
        /// </summary>
        /// <typeparam name="T">Data type</typeparam>
        /// <param name="server">Databse server</param>
        /// <param name="command">Command</param>
        /// <returns>Return paging data</returns>
        public async Task<IEnumerable<T>> QueryPagingAsync<T>(DatabaseServer server, ICommand command)
        {
            int beginIndex = 0;
            int pageSize = 1;
            if (command?.Query?.PagingInfo != null)
            {
                beginIndex = command.Query.PagingInfo.Page;
                pageSize = command.Query.PagingInfo.PageSize;
                beginIndex = (beginIndex - 1) * pageSize;
            }
            return await QueryOffsetAsync<T>(server, command, beginIndex, pageSize).ConfigureAwait(false);
        }

        /// <summary>
        /// Query datas offset the specified numbers
        /// </summary>
        /// <typeparam name="T">Data type</typeparam>
        /// <param name="server">Database server</param>
        /// <param name="command">Command</param>
        /// <param name="offsetNum">Offset num</param>
        /// <param name="size">Query size</param>
        /// <returns>Return datas</returns>
        public IEnumerable<T> QueryOffset<T>(DatabaseServer server, ICommand command, int offsetNum = 0, int size = int.MaxValue)
        {
            return QueryOffsetAsync<T>(server, command, offsetNum, size).Result;
        }

        /// <summary>
        /// Query datas offset the specified numbers
        /// </summary>
        /// <typeparam name="T">Data type</typeparam>
        /// <param name="server">Database server</param>
        /// <param name="command">Command</param>
        /// <param name="offsetNum">Offset num</param>
        /// <param name="size">Query size</param>
        /// <returns>Return datas</returns>
        public async Task<IEnumerable<T>> QueryOffsetAsync<T>(DatabaseServer server, ICommand command, int offsetNum = 0, int size = int.MaxValue)
        {
            if (command.Query == null)
            {
                throw new EZNEWException($"{nameof(ICommand.Query)} is null");
            }

            #region query translation

            IQueryTranslator translator = PostgreSqlManager.GetQueryTranslator(DataAccessContext.Create(server, command));
            var tranResult = translator.Translate(command.Query);

            #endregion

            #region script

            string joinScript = tranResult.AllowJoin ? tranResult.JoinScript : string.Empty;
            string cmdText;
            switch (command.Query.ExecutionMode)
            {
                case QueryExecutionMode.Text:
                    cmdText = tranResult.ConditionString;
                    break;
                case QueryExecutionMode.QueryObject:
                default:
                    string limitString = $"LIMIT {size} OFFSET {offsetNum}";
                    string objectName = translator.DataAccessContext.GetCommandEntityObjectName(command);
                    string defaultFieldName = PostgreSqlManager.GetDefaultFieldName(command.EntityType);
                    var queryFields = PostgreSqlManager.GetQueryFields(command.Query, command.EntityType, true);
                    string innerFormatedField = string.Join(",", PostgreSqlManager.FormatQueryFields(translator.ObjectPetName, queryFields, false));
                    string outputFormatedField = string.Join(",", PostgreSqlManager.FormatQueryFields(translator.ObjectPetName, queryFields, true));
                    string queryScript = $"SELECT {innerFormatedField} FROM {PostgreSqlManager.WrapKeyword(objectName)} AS {translator.ObjectPetName} {joinScript} {(string.IsNullOrWhiteSpace(tranResult.ConditionString) ? string.Empty : $"WHERE {tranResult.ConditionString}")} {tranResult.CombineScript}";
                    cmdText = $"{(string.IsNullOrWhiteSpace(tranResult.PreScript) ? $"WITH {PostgreSqlManager.PagingTableName} AS ({queryScript})" : $"{tranResult.PreScript},{PostgreSqlManager.PagingTableName} AS ({queryScript})")}SELECT (SELECT COUNT({PostgreSqlManager.WrapKeyword(defaultFieldName)}) FROM {PostgreSqlManager.PagingTableName}) AS {DataManager.PagingTotalCountFieldName},{outputFormatedField} FROM {PostgreSqlManager.PagingTableName} AS {translator.ObjectPetName} ORDER BY {(string.IsNullOrWhiteSpace(tranResult.SortString) ? $"{translator.ObjectPetName}.{PostgreSqlManager.WrapKeyword(defaultFieldName)} DESC" : $"{tranResult.SortString}")} {limitString}";
                    break;
            }

            #endregion

            #region parameter

            var parameters = PostgreSqlManager.ConvertCmdParameters(PostgreSqlManager.ConvertParameter(tranResult.Parameters));

            #endregion

            //Trace log
            PostgreSqlManager.LogScript(cmdText, tranResult.Parameters);

            using (var conn = PostgreSqlManager.GetConnection(server))
            {
                var tran = PostgreSqlManager.GetQueryTransaction(conn, command.Query);
                var cmdDefinition = new CommandDefinition(cmdText, parameters, transaction: tran, commandType: PostgreSqlManager.GetCommandType(command as DefaultCommand), cancellationToken: command.Query?.GetCancellationToken() ?? default);
                return await conn.QueryAsync<T>(cmdDefinition).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Query whether the data exists or not
        /// </summary>
        /// <param name="server">Database server</param>
        /// <param name="command">Command</param>
        /// <returns>Return whether data has existed</returns>
        public bool Exists(DatabaseServer server, ICommand command)
        {
            return ExistsAsync(server, command).Result;
        }

        /// <summary>
        /// Query whether the data exists or not
        /// </summary>
        /// <param name="server">Database server</param>
        /// <param name="command">Command</param>
        /// <returns>Return whether data has existed</returns>
        public async Task<bool> ExistsAsync(DatabaseServer server, ICommand command)
        {
            #region query translation

            var translator = PostgreSqlManager.GetQueryTranslator(DataAccessContext.Create(server, command));
            command.Query.ClearQueryFields();
            var queryFields = EntityManager.GetPrimaryKeys(command.EntityType).ToArray();
            if (queryFields.IsNullOrEmpty())
            {
                queryFields = EntityManager.GetQueryFields(command.EntityType).ToArray();
            }
            command.Query.AddQueryFields(queryFields);
            var tranResult = translator.Translate(command.Query);
            string conditionString = string.IsNullOrWhiteSpace(tranResult.ConditionString) ? string.Empty : $"WHERE {tranResult.ConditionString}";
            string preScript = tranResult.PreScript;
            string joinScript = tranResult.AllowJoin ? tranResult.JoinScript : string.Empty;

            #endregion

            #region script

            string objectName = translator.DataAccessContext.GetCommandEntityObjectName(command);
            string cmdText = $"{preScript}SELECT EXISTS(SELECT {string.Join(",", PostgreSqlManager.FormatQueryFields(translator.ObjectPetName, command.Query, command.EntityType, true, false))} FROM {PostgreSqlManager.WrapKeyword(objectName)} AS {translator.ObjectPetName} {joinScript} {conditionString} {tranResult.CombineScript})";

            #endregion

            #region parameter

            var parameters = PostgreSqlManager.ConvertCmdParameters(PostgreSqlManager.ConvertParameter(tranResult.Parameters));

            #endregion

            //Trace log
            PostgreSqlManager.LogScript(cmdText, tranResult.Parameters);

            using (var conn = PostgreSqlManager.GetConnection(server))
            {
                var tran = PostgreSqlManager.GetQueryTransaction(conn, command.Query);
                var cmdDefinition = new CommandDefinition(cmdText, parameters, transaction: tran, cancellationToken: command.Query?.GetCancellationToken() ?? default);
                int value = await conn.ExecuteScalarAsync<int>(cmdDefinition).ConfigureAwait(false);
                return value > 0;
            }
        }

        /// <summary>
        /// Query aggregation value
        /// </summary>
        /// <typeparam name="T">Data type</typeparam>
        /// <param name="server">Database server</param>
        /// <param name="command">Command</param>
        /// <returns>Return aggregation value</returns>
        public T AggregateValue<T>(DatabaseServer server, ICommand command)
        {
            return AggregateValueAsync<T>(server, command).Result;
        }

        /// <summary>
        /// Query aggregation value
        /// </summary>
        /// <typeparam name="T">Data type</typeparam>
        /// <param name="server">Database server</param>
        /// <param name="command">Command</param>
        /// <returns>Return aggregation value</returns>
        public async Task<T> AggregateValueAsync<T>(DatabaseServer server, ICommand command)
        {
            if (command.Query == null)
            {
                throw new EZNEWException($"{nameof(ICommand.Query)} is null");
            }

            #region query translation

            bool queryObject = command.Query.ExecutionMode == QueryExecutionMode.QueryObject;
            string funcName = PostgreSqlManager.GetAggregateFunctionName(command.OperationType);
            EntityField defaultField = null;
            if (queryObject)
            {
                if (string.IsNullOrWhiteSpace(funcName))
                {
                    throw new NotSupportedException($"Not support {command.OperationType}");
                }
                if (PostgreSqlManager.AggregateOperateMustNeedField(command.OperationType))
                {
                    if (command.Query.QueryFields.IsNullOrEmpty())
                    {
                        throw new EZNEWException($"You must specify the field to perform for the {funcName} operation");
                    }
                    defaultField = DataManager.GetField(CurrentDatabaseServerType, command.EntityType, command.Query.QueryFields.First());
                }
                else
                {
                    defaultField = DataManager.GetDefaultField(CurrentDatabaseServerType, command.EntityType);
                }
                //combine fields
                if (!command.Query.Combines.IsNullOrEmpty())
                {
                    var combineKeys = EntityManager.GetPrimaryKeys(command.EntityType).Union(new string[1] { defaultField.PropertyName }).ToArray();
                    command.Query.ClearQueryFields();
                    foreach (var combineItem in command.Query.Combines)
                    {
                        combineItem.Query.ClearQueryFields();
                        if (combineKeys.IsNullOrEmpty())
                        {
                            combineItem.Query.ClearNotQueryFields();
                            command.Query.ClearNotQueryFields();
                        }
                        else
                        {
                            command.Query.AddQueryFields(combineKeys);
                            if (combineItem.Type == CombineType.Union || combineItem.Type == CombineType.UnionAll)
                            {
                                combineItem.Query.AddQueryFields(combineKeys);
                            }
                        }
                    }
                }
            }
            IQueryTranslator translator = PostgreSqlManager.GetQueryTranslator(DataAccessContext.Create(server, command));
            var tranResult = translator.Translate(command.Query);

            #endregion

            #region script

            string cmdText;
            string joinScript = tranResult.AllowJoin ? tranResult.JoinScript : string.Empty;
            switch (command.Query.ExecutionMode)
            {
                case QueryExecutionMode.Text:
                    cmdText = tranResult.ConditionString;
                    break;
                case QueryExecutionMode.QueryObject:
                default:
                    string objectName = translator.DataAccessContext.GetCommandEntityObjectName(command);
                    cmdText = string.IsNullOrWhiteSpace(tranResult.CombineScript)
                        ? $"{tranResult.PreScript}SELECT {funcName}({PostgreSqlManager.FormatField(translator.ObjectPetName, defaultField, false)}) FROM {PostgreSqlManager.WrapKeyword(objectName)} AS {translator.ObjectPetName} {joinScript} {(string.IsNullOrWhiteSpace(tranResult.ConditionString) ? string.Empty : $"WHERE {tranResult.ConditionString}")}"
                        : $"{tranResult.PreScript}SELECT {funcName}({PostgreSqlManager.FormatField(translator.ObjectPetName, defaultField, false)}) FROM (SELECT {string.Join(",", PostgreSqlManager.FormatQueryFields(translator.ObjectPetName, command.Query, command.EntityType, true, false))} FROM {PostgreSqlManager.WrapKeyword(objectName)} AS {translator.ObjectPetName} {joinScript} {(string.IsNullOrWhiteSpace(tranResult.ConditionString) ? string.Empty : $"WHERE {tranResult.ConditionString}")} {tranResult.CombineScript}) AS {translator.ObjectPetName}";
                    break;
            }

            #endregion

            #region parameter

            var parameters = PostgreSqlManager.ConvertCmdParameters(PostgreSqlManager.ConvertParameter(tranResult.Parameters));

            #endregion

            //Trace log
            PostgreSqlManager.LogScript(cmdText, tranResult.Parameters);

            using (var conn = PostgreSqlManager.GetConnection(server))
            {
                var tran = PostgreSqlManager.GetQueryTransaction(conn, command.Query);
                var cmdDefinition = new CommandDefinition(cmdText, parameters, transaction: tran, commandType: PostgreSqlManager.GetCommandType(command as DefaultCommand), cancellationToken: command.Query?.GetCancellationToken() ?? default);
                return await conn.ExecuteScalarAsync<T>(cmdDefinition).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Query data set
        /// </summary>
        /// <param name="server">Database server</param>
        /// <param name="command">Command</param>
        /// <returns>Return data set</returns>
        public async Task<DataSet> QueryMultipleAsync(DatabaseServer server, ICommand command)
        {
            //Trace log
            PostgreSqlManager.LogScript(command.Text, command.Parameters);
            using (var conn = PostgreSqlManager.GetConnection(server))
            {
                var tran = PostgreSqlManager.GetQueryTransaction(conn, command.Query);
                DynamicParameters parameters = PostgreSqlManager.ConvertCmdParameters(PostgreSqlManager.ConvertParameter(command.Parameters));
                var cmdDefinition = new CommandDefinition(command.Text, parameters, transaction: tran, commandType: PostgreSqlManager.GetCommandType(command as DefaultCommand), cancellationToken: command.Query?.GetCancellationToken() ?? default);
                using (var reader = await conn.ExecuteReaderAsync(cmdDefinition).ConfigureAwait(false))
                {
                    DataSet dataSet = new DataSet();
                    while (!reader.IsClosed && reader.Read())
                    {
                        DataTable dataTable = new DataTable();
                        dataTable.Load(reader);
                        dataSet.Tables.Add(dataTable);
                    }
                    return dataSet;
                }
            }
        }

        #endregion

        #region Bulk

        /// <summary>
        /// Bulk insert datas
        /// </summary>
        /// <param name="server">Database server</param>
        /// <param name="dataTable">Data table</param>
        /// <param name="bulkInsertOptions">Insert options</param>
        public void BulkInsert(DatabaseServer server, DataTable dataTable, IBulkInsertionOptions bulkInsertOptions = null)
        {
            BulkInsertAsync(server, dataTable).Wait();
        }

        /// <summary>
        /// Bulk insert datas
        /// </summary>
        /// <param name="server">Database server</param>
        /// <param name="dataTable">Data table</param>
        /// <param name="bulkInsertOptions">Insert options</param>
        public async Task BulkInsertAsync(DatabaseServer server, DataTable dataTable, IBulkInsertionOptions bulkInsertOptions = null)
        {
            if (server == null)
            {
                throw new ArgumentNullException(nameof(server));
            }
            if (dataTable == null)
            {
                throw new ArgumentNullException(nameof(dataTable));
            }
            PostgreSqlBulkInsertionOptions postgreSqlBulkInsertOptions = bulkInsertOptions as PostgreSqlBulkInsertionOptions;
            postgreSqlBulkInsertOptions = postgreSqlBulkInsertOptions ?? new PostgreSqlBulkInsertionOptions();
            List<string> columnNames = new List<string>(dataTable.Columns.Count);
            foreach (DataColumn col in dataTable.Columns)
            {
                columnNames.Add(col.ColumnName);
            }
            string tableName = dataTable.TableName;
            var fields = columnNames;
            if (postgreSqlBulkInsertOptions.WrapWithQuotes)
            {
                tableName = PostgreSqlManager.WrapKeyword(tableName);
                fields = fields.Select(c => PostgreSqlManager.WrapKeyword(c)).ToList();
            }
            string copyString = $"COPY {tableName} ({string.Join(",", fields)}) FROM STDIN BINARY";
            using (var conn = new NpgsqlConnection(server?.ConnectionString))
            {
                try
                {
                    conn.Open();
                    using (var writer = conn.BeginBinaryImport(copyString))
                    {
                        foreach (DataRow row in dataTable.Rows)
                        {
                            writer.StartRow();
                            foreach (var col in columnNames)
                            {
                                await writer.WriteAsync(row[col]).ConfigureAwait(false);
                            }
                        }
                        await writer.CompleteAsync().ConfigureAwait(false);
                    }
                }
                catch (Exception ex)
                {
                    throw ex;
                }
                finally
                {
                    if (conn != null && conn.State != ConnectionState.Closed)
                    {
                        conn.Close();
                    }
                }
            }
        }

        #endregion
    }
}
