using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Dapper;
using EZNEW.Development.Entity;
using EZNEW.Development.Query;
using EZNEW.Development.Query.Translator;
using EZNEW.Development.Command;
using EZNEW.Development.Command.Modification;
using EZNEW.Exceptions;
using EZNEW.Data.Configuration;
using Npgsql;

namespace EZNEW.Data.PostgreSQL
{
    /// <summary>
    /// Imeplements database provider for the PostgreSQL
    /// </summary>
    public class PostgreSQLProvider : IDatabaseProvider
    {
        #region Execute

        /// <summary>
        /// Execute command
        /// </summary>
        /// <param name="server">Database server</param>
        /// <param name="executeOptions">Execute options</param>
        /// <param name="commands">Commands</param>
        /// <returns>Return the affected data numbers</returns>
        public int Execute(DatabaseServer server, CommandExecutionOptions executeOptions, IEnumerable<ICommand> commands)
        {
            return ExecuteAsync(server, executeOptions, commands).Result;
        }

        /// <summary>
        /// Execute command
        /// </summary>
        /// <param name="server">Database server</param>
        /// <param name="executeOptions">Execute options</param>
        /// <param name="commands">Commands</param>
        /// <returns>Return the affected data numbers</returns>
        public int Execute(DatabaseServer server, CommandExecutionOptions executeOptions, params ICommand[] commands)
        {
            return ExecuteAsync(server, executeOptions, commands).Result;
        }

        /// <summary>
        /// Execute command
        /// </summary>
        /// <param name="server">Database server</param>
        /// <param name="executeOptions">Execute options</param>
        /// <param name="commands">Commands</param>
        /// <returns>Return the affected data numbers</returns>
        public async Task<int> ExecuteAsync(DatabaseServer server, CommandExecutionOptions executeOptions, IEnumerable<ICommand> commands)
        {
            #region group execute commands

            IQueryTranslator translator = PostgreSqlFactory.GetQueryTranslator(server);
            List<DatabaseExecutionCommand> executeCommands = new List<DatabaseExecutionCommand>();
            var batchExecuteConfig = DataManager.GetBatchExecutionConfiguration(server.ServerType) ?? BatchExecutionConfiguration.Default;
            var groupStatementsCount = batchExecuteConfig.GroupStatementsCount;
            groupStatementsCount = groupStatementsCount < 0 ? 1 : groupStatementsCount;
            var groupParameterCount = batchExecuteConfig.GroupParametersCount;
            groupParameterCount = groupParameterCount < 0 ? 1 : groupParameterCount;
            StringBuilder commandTextBuilder = new StringBuilder();
            CommandParameters parameters = null;
            int statementsCount = 0;
            bool forceReturnValue = false;
            int cmdCount = 0;

            DatabaseExecutionCommand GetGroupExecuteCommand()
            {
                var executeCommand = new DatabaseExecutionCommand()
                {
                    CommandText = commandTextBuilder.ToString(),
                    CommandType = CommandType.Text,
                    ForceReturnValue = forceReturnValue,
                    Parameters = parameters
                };
                statementsCount = 0;
                translator.ParameterSequence = 0;
                commandTextBuilder.Clear();
                parameters = null;
                forceReturnValue = false;
                return executeCommand;
            }

            foreach (var cmd in commands)
            {
                DatabaseExecutionCommand executeCommand = GetExecuteDbCommand(translator, cmd as DefaultCommand);
                if (executeCommand == null)
                {
                    continue;
                }

                //Trace log
                PostgreSqlFactory.LogExecutionCommand(executeCommand);

                cmdCount++;
                if (executeCommand.PerformAlone)
                {
                    if (statementsCount > 0)
                    {
                        executeCommands.Add(GetGroupExecuteCommand());
                    }
                    executeCommands.Add(executeCommand);
                    continue;
                }
                commandTextBuilder.AppendLine(executeCommand.CommandText);
                parameters = parameters == null ? executeCommand.Parameters : parameters.Union(executeCommand.Parameters);
                forceReturnValue |= executeCommand.ForceReturnValue;
                statementsCount++;
                if (translator.ParameterSequence >= groupParameterCount || statementsCount >= groupStatementsCount)
                {
                    executeCommands.Add(GetGroupExecuteCommand());
                }
            }
            if (statementsCount > 0)
            {
                executeCommands.Add(GetGroupExecuteCommand());
            }

            #endregion

            return await ExecuteCommandAsync(server, executeOptions, executeCommands, executeOptions?.ExecuteByTransaction ?? cmdCount > 1).ConfigureAwait(false);
        }

        /// <summary>
        /// Execute command
        /// </summary>
        /// <param name="server">Database server</param>
        /// <param name="executeOptions">Execute options</param>
        /// <param name="commands">Commands</param>
        /// <returns>Return the affected data numbers</returns>
        public async Task<int> ExecuteAsync(DatabaseServer server, CommandExecutionOptions executeOptions, params ICommand[] commands)
        {
            IEnumerable<ICommand> cmdCollection = commands;
            return await ExecuteAsync(server, executeOptions, cmdCollection).ConfigureAwait(false);
        }

        /// <summary>
        /// Execute commands
        /// </summary>
        /// <param name="server">Database server</param>
        /// <param name="executeCommands">Execute commands</param>
        /// <param name="useTransaction">Use transaction</param>
        /// <returns>Return the affected data numbers</returns>
        async Task<int> ExecuteCommandAsync(DatabaseServer server, CommandExecutionOptions executeOptions, IEnumerable<DatabaseExecutionCommand> executeCommands, bool useTransaction)
        {
            int resultValue = 0;
            bool success = true;
            using (var conn = PostgreSqlFactory.GetConnection(server))
            {
                IDbTransaction transaction = null;
                if (useTransaction)
                {
                    transaction = PostgreSqlFactory.GetExecuteTransaction(conn, executeOptions);
                }
                try
                {
                    foreach (var cmd in executeCommands)
                    {
                        var cmdDefinition = new CommandDefinition(cmd.CommandText, PostgreSqlFactory.ConvertCmdParameters(cmd.Parameters), transaction: transaction, commandType: cmd.CommandType, cancellationToken: executeOptions?.CancellationToken ?? default);
                        var executeResultValue = await conn.ExecuteAsync(cmdDefinition).ConfigureAwait(false);
                        success = success && (cmd.ForceReturnValue ? executeResultValue > 0 : true);
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
        /// Get execute database execute command
        /// </summary>
        /// <param name="command">Command</param>
        /// <returns>Return execute command</returns>
        DatabaseExecutionCommand GetExecuteDbCommand(IQueryTranslator queryTranslator, DefaultCommand command)
        {
            DatabaseExecutionCommand GetTextCommand()
            {
                return new DatabaseExecutionCommand()
                {
                    CommandText = command.CommandText,
                    Parameters = PostgreSqlFactory.ParseParameters(command.Parameters),
                    CommandType = PostgreSqlFactory.GetCommandType(command),
                    ForceReturnValue = command.MustReturnValueOnSuccess,
                    HasPreScript = true
                };
            }
            if (command.ExecutionMode == CommandExecutionMode.CommandText)
            {
                return GetTextCommand();
            }
            DatabaseExecutionCommand executeCommand;
            switch (command.OperateType)
            {
                case CommandOperationType.Insert:
                    executeCommand = GetInsertExecuteDbCommand(queryTranslator, command);
                    break;
                case CommandOperationType.Update:
                    executeCommand = GetUpdateExecuteDbCommand(queryTranslator, command);
                    break;
                case CommandOperationType.Delete:
                    executeCommand = GetDeleteExecuteDbCommand(queryTranslator, command);
                    break;
                default:
                    executeCommand = GetTextCommand();
                    break;
            }
            return executeCommand;
        }

        /// <summary>
        /// Get insert execute DbCommand
        /// </summary>
        /// <param name="translator">Translator</param>
        /// <param name="command">Command</param>
        /// <returns>Return insert execute command</returns>
        DatabaseExecutionCommand GetInsertExecuteDbCommand(IQueryTranslator translator, DefaultCommand command)
        {
            string objectName = DataManager.GetEntityObjectName(DatabaseServerType.PostgreSQL, command.EntityType, command.ObjectName);
            var fields = DataManager.GetEditFields(DatabaseServerType.PostgreSQL, command.EntityType);
            var fieldCount = fields.GetCount();
            var insertFormatResult = PostgreSqlFactory.FormatInsertFields(fieldCount, fields, command.Parameters, translator.ParameterSequence);
            if (insertFormatResult == null)
            {
                return null;
            }
            string cmdText = $"INSERT INTO {PostgreSqlFactory.WrapKeyword(objectName)} ({string.Join(",", insertFormatResult.Item1)}) VALUES ({string.Join(",", insertFormatResult.Item2)});";
            CommandParameters parameters = insertFormatResult.Item3;
            translator.ParameterSequence += fieldCount;
            return new DatabaseExecutionCommand()
            {
                CommandText = cmdText,
                CommandType = PostgreSqlFactory.GetCommandType(command),
                ForceReturnValue = command.MustReturnValueOnSuccess,
                Parameters = parameters
            };
        }

        /// <summary>
        /// Get update execute command
        /// </summary>
        /// <param name="translator">Translator</param>
        /// <param name="command">Command</param>
        /// <returns>Return update execute command</returns>
        DatabaseExecutionCommand GetUpdateExecuteDbCommand(IQueryTranslator translator, DefaultCommand command)
        {
            #region query translate

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

            CommandParameters parameters = PostgreSqlFactory.ParseParameters(command.Parameters) ?? new CommandParameters();
            string objectName = DataManager.GetEntityObjectName(DatabaseServerType.PostgreSQL, command.EntityType, command.ObjectName);
            var fields = PostgreSqlFactory.GetFields(command.EntityType, command.Fields);
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
                    parameterName = PostgreSqlFactory.FormatParameterName(parameterName, parameterSequence);
                    parameters.Rename(field.PropertyName, parameterName);
                    if (parameterValue is IModificationValue)
                    {
                        var modifyValue = parameterValue as IModificationValue;
                        parameters.ModifyValue(parameterName, modifyValue.Value);
                        if (parameterValue is CalculationModificationValue)
                        {
                            var calculateModifyValue = parameterValue as CalculationModificationValue;
                            string calChar = PostgreSqlFactory.GetCalculateChar(calculateModifyValue.Operator);
                            newValueExpression = $"{translator.ObjectPetName}.{PostgreSqlFactory.WrapKeyword(field.FieldName)}{calChar}{PostgreSqlFactory.ParameterPrefix}{parameterName}";
                        }
                    }
                }
                if (string.IsNullOrWhiteSpace(newValueExpression))
                {
                    newValueExpression = $"{PostgreSqlFactory.ParameterPrefix}{parameterName}";
                }
                updateSetArray.Add($"{PostgreSqlFactory.WrapKeyword(field.FieldName)}={newValueExpression}");
            }
            string cmdText;
            string wrapObjName = PostgreSqlFactory.WrapKeyword(objectName);
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

            var queryParameters = PostgreSqlFactory.ParseParameters(tranResult.Parameters);
            parameters.Union(queryParameters);

            #endregion

            return new DatabaseExecutionCommand()
            {
                CommandText = cmdText,
                CommandType = PostgreSqlFactory.GetCommandType(command),
                ForceReturnValue = command.MustReturnValueOnSuccess,
                Parameters = parameters,
                HasPreScript = !string.IsNullOrWhiteSpace(preScript)
            };
        }

        /// <summary>
        /// Get delete execute command
        /// </summary>
        /// <param name="translator">Translator</param>
        /// <param name="command">Command</param>
        /// <returns>Return delete execute command</returns>
        DatabaseExecutionCommand GetDeleteExecuteDbCommand(IQueryTranslator translator, DefaultCommand command)
        {
            #region query translate

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

            string objectName = DataManager.GetEntityObjectName(DatabaseServerType.PostgreSQL, command.EntityType, command.ObjectName);
            string cmdText;
            string wrapObjectName = PostgreSqlFactory.WrapKeyword(objectName);
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

            CommandParameters parameters = PostgreSqlFactory.ParseParameters(command.Parameters) ?? new CommandParameters();
            var queryParameters = PostgreSqlFactory.ParseParameters(tranResult.Parameters);
            parameters.Union(queryParameters);

            #endregion

            return new DatabaseExecutionCommand()
            {
                CommandText = cmdText,
                CommandType = PostgreSqlFactory.GetCommandType(command),
                ForceReturnValue = command.MustReturnValueOnSuccess,
                Parameters = parameters,
                HasPreScript = !string.IsNullOrWhiteSpace(preScript)
            };
        }

        Tuple<IEnumerable<string>, IEnumerable<string>> FormatWrapJoinPrimaryKeys(Type entityType, string translatorObjName, string sourceObjName, string targetObjName)
        {
            var primaryKeyFields = DataManager.GetFields(DatabaseServerType.PostgreSQL, entityType, EntityManager.GetPrimaryKeys(entityType));
            if (primaryKeyFields.IsNullOrEmpty())
            {
                throw new EZNEWException($"{entityType?.FullName} not set primary key");
            }
            return FormatWrapJoinFields(primaryKeyFields, translatorObjName, sourceObjName, targetObjName);
        }

        Tuple<IEnumerable<string>, IEnumerable<string>> FormatWrapJoinFields(IEnumerable<EntityField> fields, string translatorObjName, string sourceObjName, string targetObjName)
        {
            var joinItems = fields.Select(field =>
            {
                string fieldName = PostgreSqlFactory.WrapKeyword(field.FieldName);
                return $"{sourceObjName}.{fieldName} = {targetObjName}.{fieldName}";
            });
            var queryItems = fields.Select(field =>
            {
                string fieldName = PostgreSqlFactory.WrapKeyword(field.FieldName);
                return $"{translatorObjName}.{fieldName}";
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
                throw new EZNEWException("ICommand.Query is null");
            }

            #region query translate

            IQueryTranslator translator = PostgreSqlFactory.GetQueryTranslator(server);
            var tranResult = translator.Translate(command.Query);
            string joinScript = tranResult.AllowJoin ? tranResult.JoinScript : string.Empty;

            #endregion

            #region script

            string cmdText;
            switch (command.Query.QueryType)
            {
                case QueryCommandType.Text:
                    cmdText = tranResult.ConditionString;
                    break;
                case QueryCommandType.QueryObject:
                default:
                    int size = command.Query.QuerySize;
                    string objectName = DataManager.GetEntityObjectName(DatabaseServerType.PostgreSQL, command.EntityType, command.ObjectName);
                    string orderString = string.IsNullOrWhiteSpace(tranResult.OrderString) ? string.Empty : $"ORDER BY {tranResult.OrderString}";
                    var queryFields = PostgreSqlFactory.GetQueryFields(command.Query, command.EntityType, true);
                    string outputFormatedField = string.Join(",", PostgreSqlFactory.FormatQueryFields(translator.ObjectPetName, queryFields, true));
                    if (string.IsNullOrWhiteSpace(tranResult.CombineScript))
                    {
                        cmdText = $"{tranResult.PreScript}SELECT {outputFormatedField} FROM {PostgreSqlFactory.WrapKeyword(objectName)} AS {translator.ObjectPetName} {joinScript} {(string.IsNullOrWhiteSpace(tranResult.ConditionString) ? string.Empty : $"WHERE {tranResult.ConditionString}")} {orderString} {(size > 0 ? $"LIMIT {size} OFFSET 0" : string.Empty)}";
                    }
                    else
                    {
                        string innerFormatedField = string.Join(",", PostgreSqlFactory.FormatQueryFields(translator.ObjectPetName, queryFields, false));
                        cmdText = $"{tranResult.PreScript}SELECT {outputFormatedField} FROM (SELECT {innerFormatedField} FROM {PostgreSqlFactory.WrapKeyword(objectName)} AS {translator.ObjectPetName} {joinScript} {(string.IsNullOrWhiteSpace(tranResult.ConditionString) ? string.Empty : $"WHERE {tranResult.ConditionString}")} {tranResult.CombineScript}) AS {translator.ObjectPetName} {orderString} {(size > 0 ? $"LIMIT {size} OFFSET 0" : string.Empty)}";
                    }
                    break;
            }

            #endregion

            #region parameter

            var parameters = PostgreSqlFactory.ConvertCmdParameters(PostgreSqlFactory.ParseParameters(tranResult.Parameters));

            #endregion

            //Trace log
            PostgreSqlFactory.LogScript(cmdText, tranResult.Parameters);

            using (var conn = PostgreSqlFactory.GetConnection(server))
            {
                var tran = PostgreSqlFactory.GetQueryTransaction(conn, command.Query);
                var cmdDefinition = new CommandDefinition(cmdText, parameters, transaction: tran, commandType: PostgreSqlFactory.GetCommandType(command as DefaultCommand), cancellationToken: command.Query?.GetCancellationToken() ?? default);
                return await conn.QueryAsync<T>(cmdDefinition).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Query data paging
        /// </summary>
        /// <typeparam name="T">Data type</typeparam>
        /// <param name="server">Databse server</param>
        /// <param name="command">Command</param>
        /// <returns>Return data paging</returns>
        public IEnumerable<T> QueryPaging<T>(DatabaseServer server, ICommand command)
        {
            return QueryPagingAsync<T>(server, command).Result;
        }

        /// <summary>
        /// Query data paging
        /// </summary>
        /// <typeparam name="T">Data type</typeparam>
        /// <param name="server">Databse server</param>
        /// <param name="command">Command</param>
        /// <returns>Return data paging</returns>
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
                throw new EZNEWException("ICommand.Query is null");
            }

            #region query translate

            IQueryTranslator translator = PostgreSqlFactory.GetQueryTranslator(server);
            var tranResult = translator.Translate(command.Query);

            #endregion

            #region script

            string joinScript = tranResult.AllowJoin ? tranResult.JoinScript : string.Empty;
            string cmdText;
            switch (command.Query.QueryType)
            {
                case QueryCommandType.Text:
                    cmdText = tranResult.ConditionString;
                    break;
                case QueryCommandType.QueryObject:
                default:
                    string limitString = $"LIMIT {size} OFFSET {offsetNum}";
                    string objectName = DataManager.GetEntityObjectName(DatabaseServerType.PostgreSQL, command.EntityType, command.ObjectName);
                    string defaultFieldName = PostgreSqlFactory.GetDefaultFieldName(command.EntityType);
                    var queryFields = PostgreSqlFactory.GetQueryFields(command.Query, command.EntityType, true);
                    string innerFormatedField = string.Join(",", PostgreSqlFactory.FormatQueryFields(translator.ObjectPetName, queryFields, false));
                    string outputFormatedField = string.Join(",", PostgreSqlFactory.FormatQueryFields(translator.ObjectPetName, queryFields, true));
                    string queryScript = $"SELECT {innerFormatedField} FROM {PostgreSqlFactory.WrapKeyword(objectName)} AS {translator.ObjectPetName} {joinScript} {(string.IsNullOrWhiteSpace(tranResult.ConditionString) ? string.Empty : $"WHERE {tranResult.ConditionString}")} {tranResult.CombineScript}";
                    cmdText = $"{(string.IsNullOrWhiteSpace(tranResult.PreScript) ? $"WITH {PostgreSqlFactory.PagingTableName} AS ({queryScript})" : $"{tranResult.PreScript},{PostgreSqlFactory.PagingTableName} AS ({queryScript})")}SELECT (SELECT COUNT({PostgreSqlFactory.WrapKeyword(defaultFieldName)}) FROM {PostgreSqlFactory.PagingTableName}) AS {DataManager.PagingTotalCountFieldName},{outputFormatedField} FROM {PostgreSqlFactory.PagingTableName} AS {translator.ObjectPetName} ORDER BY {(string.IsNullOrWhiteSpace(tranResult.OrderString) ? $"{translator.ObjectPetName}.{PostgreSqlFactory.WrapKeyword(defaultFieldName)} DESC" : $"{tranResult.OrderString}")} {limitString}";
                    break;
            }

            #endregion

            #region parameter

            var parameters = PostgreSqlFactory.ConvertCmdParameters(PostgreSqlFactory.ParseParameters(tranResult.Parameters));

            #endregion

            //Trace log
            PostgreSqlFactory.LogScript(cmdText, tranResult.Parameters);

            using (var conn = PostgreSqlFactory.GetConnection(server))
            {
                var tran = PostgreSqlFactory.GetQueryTransaction(conn, command.Query);
                var cmdDefinition = new CommandDefinition(cmdText, parameters, transaction: tran, commandType: PostgreSqlFactory.GetCommandType(command as DefaultCommand), cancellationToken: command.Query?.GetCancellationToken() ?? default);
                return await conn.QueryAsync<T>(cmdDefinition).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Query whether the data exists or not
        /// </summary>
        /// <param name="server">Database server</param>
        /// <param name="command">Command</param>
        /// <returns>Return whether data has existed</returns>
        public bool Query(DatabaseServer server, ICommand command)
        {
            return QueryAsync(server, command).Result;
        }

        /// <summary>
        /// Query whether the data exists or not
        /// </summary>
        /// <param name="server">Database server</param>
        /// <param name="command">Command</param>
        /// <returns>Return whether data has existed</returns>
        public async Task<bool> QueryAsync(DatabaseServer server, ICommand command)
        {
            var translator = PostgreSqlFactory.GetQueryTranslator(server);

            #region query translate

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

            string objectName = DataManager.GetEntityObjectName(DatabaseServerType.PostgreSQL, command.EntityType, command.ObjectName);
            string cmdText = $"{preScript}SELECT EXISTS(SELECT {string.Join(",", PostgreSqlFactory.FormatQueryFields(translator.ObjectPetName, command.Query, command.EntityType, true, false))} FROM {PostgreSqlFactory.WrapKeyword(objectName)} AS {translator.ObjectPetName} {joinScript} {conditionString} {tranResult.CombineScript})";

            #endregion

            #region parameter

            var parameters = PostgreSqlFactory.ConvertCmdParameters(PostgreSqlFactory.ParseParameters(tranResult.Parameters));

            #endregion

            //Trace log
            PostgreSqlFactory.LogScript(cmdText, tranResult.Parameters);

            using (var conn = PostgreSqlFactory.GetConnection(server))
            {
                var tran = PostgreSqlFactory.GetQueryTransaction(conn, command.Query);
                var cmdDefinition = new CommandDefinition(cmdText, parameters, transaction: tran, cancellationToken: command.Query?.GetCancellationToken() ?? default);
                int value = await conn.ExecuteScalarAsync<int>(cmdDefinition).ConfigureAwait(false);
                return value > 0;
            }
        }

        /// <summary>
        /// Query single value
        /// </summary>
        /// <typeparam name="T">Data type</typeparam>
        /// <param name="server">Database server</param>
        /// <param name="command">Command</param>
        /// <returns>Return the data</returns>
        public T AggregateValue<T>(DatabaseServer server, ICommand command)
        {
            return AggregateValueAsync<T>(server, command).Result;
        }

        /// <summary>
        /// Query single value
        /// </summary>
        /// <typeparam name="T">Data type</typeparam>
        /// <param name="server">Database server</param>
        /// <param name="command">Command</param>
        /// <returns>Return the data</returns>
        public async Task<T> AggregateValueAsync<T>(DatabaseServer server, ICommand command)
        {
            if (command.Query == null)
            {
                throw new EZNEWException("ICommand.Query is null");
            }

            #region query translate

            bool queryObject = command.Query.QueryType == QueryCommandType.QueryObject;
            string funcName = PostgreSqlFactory.GetAggregateFunctionName(command.OperateType);
            EntityField defaultField = null;
            if (queryObject)
            {
                if (string.IsNullOrWhiteSpace(funcName))
                {
                    throw new NotSupportedException($"Not support {command.OperateType}");
                }
                if (PostgreSqlFactory.AggregateOperateMustNeedField(command.OperateType))
                {
                    if (command.Query.QueryFields.IsNullOrEmpty())
                    {
                        throw new EZNEWException($"You must specify the field to perform for the {funcName} operation");
                    }
                    defaultField = DataManager.GetField(DatabaseServerType.PostgreSQL, command.EntityType, command.Query.QueryFields.First());
                }
                else
                {
                    defaultField = DataManager.GetDefaultField(DatabaseServerType.PostgreSQL, command.EntityType);
                }
                //combine fields
                if (!command.Query.CombineItems.IsNullOrEmpty())
                {
                    var combineKeys = EntityManager.GetPrimaryKeys(command.EntityType).Union(new string[1] { defaultField.PropertyName }).ToArray();
                    command.Query.ClearQueryFields();
                    foreach (var combineItem in command.Query.CombineItems)
                    {
                        combineItem.CombineQuery.ClearQueryFields();
                        if (combineKeys.IsNullOrEmpty())
                        {
                            combineItem.CombineQuery.ClearNotQueryFields();
                            command.Query.ClearNotQueryFields();
                        }
                        else
                        {
                            command.Query.AddQueryFields(combineKeys);
                            if (combineItem.CombineType == CombineType.Union || combineItem.CombineType == CombineType.UnionAll)
                            {
                                combineItem.CombineQuery.AddQueryFields(combineKeys);
                            }
                        }
                    }
                }
            }
            IQueryTranslator translator = PostgreSqlFactory.GetQueryTranslator(server);
            var tranResult = translator.Translate(command.Query);

            #endregion

            #region script

            string cmdText;
            string joinScript = tranResult.AllowJoin ? tranResult.JoinScript : string.Empty;
            switch (command.Query.QueryType)
            {
                case QueryCommandType.Text:
                    cmdText = tranResult.ConditionString;
                    break;
                case QueryCommandType.QueryObject:
                default:
                    string objectName = DataManager.GetEntityObjectName(DatabaseServerType.PostgreSQL, command.EntityType, command.ObjectName);
                    cmdText = string.IsNullOrWhiteSpace(tranResult.CombineScript)
                        ? $"{tranResult.PreScript}SELECT {funcName}({PostgreSqlFactory.FormatField(translator.ObjectPetName, defaultField, false)}) FROM {PostgreSqlFactory.WrapKeyword(objectName)} AS {translator.ObjectPetName} {joinScript} {(string.IsNullOrWhiteSpace(tranResult.ConditionString) ? string.Empty : $"WHERE {tranResult.ConditionString}")}"
                        : $"{tranResult.PreScript}SELECT {funcName}({PostgreSqlFactory.FormatField(translator.ObjectPetName, defaultField, false)}) FROM (SELECT {string.Join(",", PostgreSqlFactory.FormatQueryFields(translator.ObjectPetName, command.Query, command.EntityType, true, false))} FROM {PostgreSqlFactory.WrapKeyword(objectName)} AS {translator.ObjectPetName} {joinScript} {(string.IsNullOrWhiteSpace(tranResult.ConditionString) ? string.Empty : $"WHERE {tranResult.ConditionString}")} {tranResult.CombineScript}) AS {translator.ObjectPetName}";
                    break;
            }

            #endregion

            #region parameter

            var parameters = PostgreSqlFactory.ConvertCmdParameters(PostgreSqlFactory.ParseParameters(tranResult.Parameters));

            #endregion

            //Trace log
            PostgreSqlFactory.LogScript(cmdText, tranResult.Parameters);

            using (var conn = PostgreSqlFactory.GetConnection(server))
            {
                var tran = PostgreSqlFactory.GetQueryTransaction(conn, command.Query);
                var cmdDefinition = new CommandDefinition(cmdText, parameters, transaction: tran, commandType: PostgreSqlFactory.GetCommandType(command as DefaultCommand), cancellationToken: command.Query?.GetCancellationToken() ?? default);
                return await conn.ExecuteScalarAsync<T>(cmdDefinition).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Query data
        /// </summary>
        /// <param name="server">Database server</param>
        /// <param name="command">Command</param>
        /// <returns>Return data set</returns>
        public async Task<DataSet> QueryMultipleAsync(DatabaseServer server, ICommand command)
        {
            //Trace log
            PostgreSqlFactory.LogScript(command.CommandText, command.Parameters);
            using (var conn = PostgreSqlFactory.GetConnection(server))
            {
                var tran = PostgreSqlFactory.GetQueryTransaction(conn, command.Query);
                DynamicParameters parameters = PostgreSqlFactory.ConvertCmdParameters(PostgreSqlFactory.ParseParameters(command.Parameters));
                var cmdDefinition = new CommandDefinition(command.CommandText, parameters, transaction: tran, commandType: PostgreSqlFactory.GetCommandType(command as DefaultCommand), cancellationToken: command.Query?.GetCancellationToken() ?? default);
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
        public void BulkInsert(DatabaseServer server, DataTable dataTable, IBulkInsertOptions bulkInsertOptions = null)
        {
            BulkInsertAsync(server, dataTable).Wait();
        }

        /// <summary>
        /// Bulk insert datas
        /// </summary>
        /// <param name="server">Database server</param>
        /// <param name="dataTable">Data table</param>
        /// <param name="bulkInsertOptions">Insert options</param>
        public async Task BulkInsertAsync(DatabaseServer server, DataTable dataTable, IBulkInsertOptions bulkInsertOptions = null)
        {
            if (server == null)
            {
                throw new ArgumentNullException(nameof(server));
            }
            if (dataTable == null)
            {
                throw new ArgumentNullException(nameof(dataTable));
            }
            PostgreSQLBulkInsertOptions postgreSqlBulkInsertOptions = bulkInsertOptions as PostgreSQLBulkInsertOptions;
            postgreSqlBulkInsertOptions = postgreSqlBulkInsertOptions ?? new PostgreSQLBulkInsertOptions();
            List<string> columnNames = new List<string>(dataTable.Columns.Count);
            foreach (DataColumn col in dataTable.Columns)
            {
                columnNames.Add(col.ColumnName);
            }
            string tableName = dataTable.TableName;
            var fields = columnNames;
            if (postgreSqlBulkInsertOptions.WrapWithQuotes)
            {
                tableName = PostgreSqlFactory.WrapKeyword(tableName);
                fields = fields.Select(c => PostgreSqlFactory.WrapKeyword(c)).ToList();
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
