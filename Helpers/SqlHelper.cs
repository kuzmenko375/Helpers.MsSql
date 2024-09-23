using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using Helpers.MsSql.EventArgs;
using Helpers.MsSql.ExecParameters.ById;
using Helpers.MsSql.Interfaces;
using Helpers.MsSql.ExecParameters;
using System.Diagnostics;
using Microsoft.Data.SqlClient;
using System.Collections;

namespace Helpers.MsSql.Helpers
{
    public class SqlHelper : ISqlHelper
    {
        /// <summary>
        /// Соединение создано в хэлпере, а не передано в конструктор,
        /// поэтому будет открываться и закрываться во время каждого (!) запроса
        /// </summary>
        private readonly bool _selfConnection = false;

        public event EventHandler<LogEventArgs> Log;
        public event EventHandler<BeforeSqlRequestEventArgs> BeforeSqlRequest;
        public event EventHandler<AfterSqlRequestEventArgs> AfterSqlRequest;

        public SqlConnection Connection { get; private set; }

        public string DatabaseName { get; private set; }

        public string SchemaName { get; private set; }

        public string TableName { get; private set; }
        
        public bool IsNoTable => string.IsNullOrEmpty(TableName);

        /// <summary>
        /// Полное имя таблицы [НазваниеБД].[НазваниеСхемы].[НазваниеТаблицы]
        /// </summary>
        public string FullTableName
        {
            get
            {
                if (IsNoTable)
                {
                    return $"[{DatabaseName}].[{SchemaName}]";
                }

                return $"[{DatabaseName}].[{SchemaName}].[{TableName}]";
            }
        }

        /// <summary>
        /// Полное название ХП для выполнения
        /// </summary>
        /// <param name="command">Команда</param>
        /// <returns></returns>
        protected string GetStoredProcedureName(string command)
        {
            if (IsNoTable)
            {
                return $"{FullTableName}.[{command}]";
            }

            return $"[{DatabaseName}].[{SchemaName}].[{TableName}_{command}]";
        }

        /// <summary>
        /// Создание хэлпера для запросов с передачей соединения, соединение не открывается во время запроса
        /// </summary>
        /// <param name="sqlConnection"></param>
        /// <param name="schemaName"></param>
        /// <param name="tableName"></param>
        public SqlHelper(SqlConnection sqlConnection,
            string schemaName,
            string tableName)
        {
            Connection = sqlConnection;

            Connection.InfoMessage += ConnectionOnInfoMessage;

            DatabaseName = Connection.Database;
            SchemaName = schemaName;
            TableName = tableName;
        }

        public SqlHelper(string connectionString, string schemaName, string tableName)
        {
            if (string.IsNullOrEmpty(connectionString))
            {
                throw new Exception($"Передана пустая строка подключения");
            }
            
            Connection = new SqlConnection(connectionString);

            Connection.InfoMessage += ConnectionOnInfoMessage;

            _selfConnection = true;

            DatabaseName = Connection.Database;
            SchemaName = schemaName;
            TableName = tableName;
        }

        private void ConnectionOnInfoMessage(object sender, SqlInfoMessageEventArgs e)
        {
            OnLog(e.Message);
        }

        /// <summary>
        /// Выбирает строчку по Id
        /// </summary>
        /// <param name="transaction">Транзакция</param>
        /// <param name="id">Идентификатор</param>
        /// <returns></returns>
        public virtual DataTable SelectById(SqlTransaction transaction, Guid id)
        {
            return ExecuteCommand(transaction, new SelectById(id));
        }

        /// <summary>
        /// Выбирает строчку по Id
        /// </summary>
        /// <param name="id">Идентификатор</param>
        /// <returns></returns>
        public virtual DataTable SelectById(Guid id)
        {
            return ExecuteCommand(null, new SelectById(id));
        }

        /// <summary>
        /// Выбирает все записи
        /// </summary>
        /// <param name="transaction">Транзакция</param>
        /// <returns></returns>
        public virtual DataTable SelectAll(SqlTransaction transaction)
        {
            return ExecuteCommand(transaction, new SelectAll());
        }

        /// <summary>
        /// Выбирает все записи
        /// </summary>
        /// <returns></returns>
        public virtual DataTable SelectAll()
        {
            return SelectAll(null);
        }

        /// <summary>
        /// Выполняет указанную ХП
        /// </summary>
        /// <param name="transaction">Транзакция</param>
        /// <param name="storedProcName">Название ХП</param>
        /// <param name="parameters">Параметры ХП</param>
        /// <returns></returns>
        protected DataTable ExecuteStoredProcedure(SqlTransaction transaction, string storedProcName, params SqlParameter[] parameters)
        {
            try
            {
                var command = string.Format("Exec {1} {0}",
                    storedProcName,
                    string.Join(", ", parameters.Select(e => $"{e.ParameterName}={(e.Value == null ? "null" : e.Value.ToString().Length > 100 ? "[string > 100 chars]" : e.Value.ToString())}")));

                OnBeforeSqlRequest(new BeforeSqlRequestEventArgs($"START {command}"));

                var dt = new DataTable();

                var proc = new SqlCommand(storedProcName, Connection)
                {
                    CommandType = CommandType.StoredProcedure,
                    CommandTimeout = 0
                };

                if (transaction != null)
                {
                    proc.Transaction = transaction;
                }

                proc.Parameters.Clear();

                foreach (var sqlParameter in parameters)
                {
                    proc.Parameters.Add(sqlParameter);
                }

                if (_selfConnection && Connection.State == ConnectionState.Closed && transaction == null)
                {
                    Connection.Open();
                }

                var execResult = proc.ExecuteReader();

                try
                {
                    dt.Load(execResult);
                }
                finally
                {
                    execResult.Close();
                    execResult.Dispose();

                    proc.Dispose();

                    if (_selfConnection && transaction == null)
                    {
                        Connection.Close();
                    }
                }

                foreach (DataColumn col in dt.Columns)
                {
                    col.ReadOnly = false;
                }

                OnAfterSqlRequest(new AfterSqlRequestEventArgs($"COMPLETED {command}"));

                return dt;
            }
            catch (Exception ex)
            {
                OnLog($"{storedProcName}: {ex.Message}");
                throw;
            }
        }


        /// <summary>
        /// Выполняет подданый SQL запрос
        /// </summary>
        /// <param name="transaction">Транзакция</param>
        /// <param name="sqlCommand">SQL-Запрос</param>
        /// <param name="parameters">Параметры ХП</param>
        /// <returns></returns>
        protected DataTable ExecuteSqlCommand(SqlTransaction transaction, string sqlCommand, params SqlParameter[] parameters)
        {
            try
            {
                var command = string.Format("Exec {1} {0}",
                    sqlCommand,
                    string.Join(", ", (IEnumerable)parameters?.Select(e => $"{e.ParameterName}={(e.Value == null ? "null" : e.Value.ToString().Length > 100 ? "[string > 100 chars]" : e.Value.ToString())}") ?? ""));

                OnBeforeSqlRequest(new BeforeSqlRequestEventArgs($"START {command}"));

                var dt = new DataTable();

                var proc = new SqlCommand(sqlCommand, Connection)
                {
                    CommandType = CommandType.Text,
                    CommandTimeout = 0,
                    CommandText = sqlCommand
                };

                if (transaction != null)
                {
                    proc.Transaction = transaction;
                }

                if (parameters != null)
                {
                    foreach (var sqlParameter in parameters)
                    {
                        proc.Parameters.Add(sqlParameter);
                    }
                }

                if (Connection.State == ConnectionState.Closed && transaction == null)
                {
                    Connection.Open();
                }

                var execResult = proc.ExecuteReader();

                try
                {
                    dt.Load(execResult);
                }
                finally
                {
                    execResult.Close();
                    execResult.Dispose();

                    proc.Dispose();

                    if (transaction == null)
                    {
                        Connection.Close();
                    }
                }

                foreach (DataColumn col in dt.Columns)
                {
                    col.ReadOnly = false;
                }

                OnAfterSqlRequest(new AfterSqlRequestEventArgs($"COMPLETED {command}"));

                return dt;
            }
            catch (Exception ex)
            {
                OnLog($"{sqlCommand}: {ex.Message}");
                throw;
            }
        }

        /// <summary>
        /// Выполняет указанную ХП и возвращает все наборы данных
        /// </summary>
        /// <param name="transaction">Транзакция</param>
        /// <param name="storedProcName">Название ХП</param>
        /// <param name="parameters">Параметры ХП</param>
        /// <returns></returns>
        protected IList<DataTable> ExecuteStoredProcedureMultipleResultSet(SqlTransaction transaction, string storedProcName, params SqlParameter[] parameters)
        {
            try
            {
                var command = string.Format("Exec {1} {0}",
                    storedProcName,
                    string.Join(", ", parameters.Select(e => $"{e.ParameterName}={(e.Value == null ? "null" : e.Value.ToString().Length > 100 ? "[string > 100 chars]" : e.Value.ToString())}")));

                OnBeforeSqlRequest(new BeforeSqlRequestEventArgs($"START {command}"));

                var result = new List<DataTable>();

                var proc = new SqlCommand(storedProcName, Connection)
                {
                    CommandType = CommandType.StoredProcedure,
                    CommandTimeout = 0
                };

                if (transaction != null)
                {
                    proc.Transaction = transaction;
                }

                proc.Parameters.Clear();

                foreach (var sqlParameter in parameters)
                {
                    proc.Parameters.Add(sqlParameter);
                }

                if (_selfConnection && Connection.State == ConnectionState.Closed && transaction == null)
                {
                    Connection.Open();
                }

                var execResult = proc.ExecuteReader();

                try
                {
                    while (!execResult.IsClosed)
                    {
                        var dt = new DataTable();
                        dt.Load(execResult);

                        foreach (DataColumn col in dt.Columns)
                        {
                            col.ReadOnly = false;
                        }

                        result.Add(dt);
                    }
                }
                finally
                {
                    execResult.Close();
                    execResult.Dispose();

                    proc.Dispose();

                    if (_selfConnection && transaction == null)
                    {
                        Connection.Close();
                    }
                }

                OnAfterSqlRequest(new AfterSqlRequestEventArgs($"COMPLETED {command}"));

                return result;
            }
            catch (Exception ex)
            {
                OnLog($"{storedProcName}: {ex.Message}");
                throw;
            }
        }

        protected string ExecuteStoredProcedureXml(SqlTransaction transaction, string storedProcName, params SqlParameter[] parameters)
        {
            try
            {
                storedProcName = GetStoredProcedureName(storedProcName); 

                var command = string.Format("Exec {1} {0}",
                    storedProcName,
                    string.Join(", ", parameters.Select(e => $"{e.ParameterName}={e.Value}")));

                OnBeforeSqlRequest(new BeforeSqlRequestEventArgs($"START {command}"));

                var proc = new SqlCommand(storedProcName, Connection)
                {
                    CommandType = CommandType.StoredProcedure,
                    CommandTimeout = 0
                };

                if (transaction != null)
                {
                    proc.Transaction = transaction;
                }

                proc.Parameters.Clear();

                foreach (var sqlParameter in parameters)
                {
                    proc.Parameters.Add(sqlParameter);
                }

                if (_selfConnection)
                {
                    Connection.Open();
                }

                try
                {
                    OnAfterSqlRequest(new AfterSqlRequestEventArgs($"COMPLETED {command}"));

                    var execResult = proc.ExecuteXmlReader();

                    var str = "";

                    execResult.Read();

                    while (!execResult.EOF)
                    {
                        str += execResult.ReadOuterXml();
                    }

                    return str;
                }
                finally
                {
                    proc.Dispose();

                    if (_selfConnection)
                    {
                        Connection.Close();
                    }
                }
            }
            catch (Exception ex)
            {
                OnLog($"{storedProcName}: {ex.Message}");
                throw;
            }
        }

        /// <summary>
        /// Выполняет указанную ХП
        /// </summary>
        /// <param name="storedProcName"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        protected DataTable ExecuteStoredProcedure(string storedProcName, params SqlParameter[] parameters)
        {
            return ExecuteStoredProcedure(null, storedProcName, parameters);
        }

        /// <summary>
        /// Выполняет указанную команду НазваниеБД.НазваниеСхемы.НазваниеТаблицы_Команда
        /// </summary>
        /// <param name="transaction">Транзакция</param>
        /// <param name="parameters">Параметры</param>
        /// <returns></returns>
        public DataTable ExecuteCommand(SqlTransaction transaction, IExecParameters parameters)
        {
            return ExecuteCommand(transaction, parameters.GetCommandName(), parameters.GetParams());
        }

        /// <summary>
        /// Выполняет указанную команду НазваниеБД.НазваниеСхемы.НазваниеТаблицы_Команда
        /// </summary>
        /// <param name="parameters">Параметры</param>
        /// <returns></returns>
        public DataTable ExecuteCommand(IExecParameters parameters)
        {
            return ExecuteCommand(parameters.GetCommandName(), parameters.GetParams());
        }

        /// <summary>
        /// Выполняет указанную команду НазваниеБД.НазваниеСхемы.НазваниеТаблицы_Команда
        /// </summary>
        /// <param name="parameters">Параметры</param>
        /// <returns></returns>
        public void ExecuteCommandNonQuery(IExecParameters parameters)
        {
            ExecuteCommandNonQuery(null, parameters);
        }

        /// <summary>
        /// Выполняет указанную команду НазваниеБД.НазваниеСхемы.НазваниеТаблицы_Команда
        /// </summary>
        /// <param name="transaction">Транзакция</param>
        /// <param name="parameters">Параметры</param>
        /// <returns></returns>
        public void ExecuteCommandNonQuery(SqlTransaction transaction, IExecParameters parameters)
        {
            ExecuteCommand(transaction, parameters.GetCommandName(), parameters.GetParams());
        }

        /// <summary>
        /// Выполняет указанную команду НазваниеБД.НазваниеСхемы.НазваниеТаблицы_Команда
        /// </summary>
        /// <param name="transaction">Транзакция</param>
        /// <param name="command">Команда</param>
        /// <param name="parameters">Параметры</param>
        /// <returns></returns>
        protected DataTable ExecuteCommand(SqlTransaction transaction, string command,
            params SqlParameter[] parameters)
        {
            return ExecuteStoredProcedure(transaction, GetStoredProcedureName(command), parameters);
        }

        /// <summary>
        /// Выполняет указанную команду НазваниеБД.НазваниеСхемы.НазваниеТаблицы_Команда
        /// </summary>
        /// <param name="command">Команда</param>
        /// <param name="parameters">Параметры</param>
        /// <returns></returns>
        protected DataTable ExecuteCommand(string command, params SqlParameter[] parameters)
        {
            return ExecuteStoredProcedure(GetStoredProcedureName(command), parameters);
        }

        protected object GetScalar(DataTable dataTable)
        {
            if (dataTable.Rows.Count == 0 || dataTable.Columns.Count == 0)
            {
                return null;
            }

            return dataTable.Rows[0][0];
        }


        protected string GetXml(string command, params SqlParameter[] parameters)
        {
            var result = ExecuteStoredProcedureXml(null, command, parameters);

            return result;
        }

        /// <summary>
        /// Выполняет ХП и возвращает 1 колонку 1 строку, или NULL
        /// </summary>
        /// <param name="command">Команда</param>
        /// <param name="parameters">Параметры</param>
        /// <returns></returns>
        protected object ExecScalar(string command, params SqlParameter[] parameters)
        {
            return GetScalar(ExecuteCommand(command, parameters));
        }

   /// <summary>
        /// Выполняет ХП и возвращает 1 колонку 1 строку, или NULL
        /// </summary>
        /// <param name="transaction">Транзакция</param>
        /// <param name="command">Команда</param>
        /// <param name="parameters">Параметры</param>
        /// <returns></returns>
        protected object ExecScalar(SqlTransaction transaction, string command, params SqlParameter[] parameters)
        {
            return GetScalar(ExecuteCommand(transaction, command, parameters));
        }


        
        /// <summary>
        /// Выполняет запрос без возарата результата
        /// </summary>
        /// <param name="command">Команда</param>
        /// <param name="parameters">Параметры</param>
        protected void ExecNonQuery(string command, params SqlParameter[] parameters)
        {
            ExecNonQuery(null, command, parameters);
        }


        /// <summary>
        /// Выполняет запрос без возарата результата
        /// </summary>
        /// <param name="transaction">Транзакция</param>
        /// <param name="command">Команда</param>
        /// <param name="parameters">Параметры</param>
        protected void ExecNonQuery(SqlTransaction transaction, string command, params SqlParameter[] parameters)
        {
            ExecuteCommand(transaction, command, parameters);
        }

        /// <summary>
        /// Вставляет данные в таблицу
        /// </summary>
        /// <param name="dataTable">Откуда вставлять</param>
        public void Bulk(DataTable dataTable, Dictionary<string, string> mapping = null)
        {
            Bulk(null, dataTable, mapping);
        }

        public object GetScalar(SqlTransaction transaction, IExecParameters execParameters)
        {
            var value = ExecScalar(transaction, execParameters.GetCommandName(), execParameters.GetParams());

            return value;
        }

        public object GetScalar(IExecParameters execParameters)
        {
            return GetScalar(null, execParameters);
        }

        public TResult GetScalar<TResult>(SqlTransaction transaction, string command, params SqlParameter[] parameters) where TResult: struct
        {
            var value = ExecScalar(transaction, command, parameters);

            return (TResult?) value ?? default;
        }

        public TResult GetScalar<TResult>(SqlTransaction transaction, IExecParameters execParameters) where TResult : struct
        {
            return GetScalar<TResult>(transaction, execParameters.GetCommandName(), execParameters.GetParams());
        }

        public string GetString(SqlTransaction transaction, IExecParameters execParameters)
        {
            var value = GetScalar(transaction, execParameters);

            return value == null || value is DBNull ? string.Empty : (string)value;
        }

        /// <summary>
        /// Получение XML, в базе UTF8 лежит
        /// </summary>
        /// <param name="execParameters"></param>
        /// <returns></returns>
        public string GetXml(IExecParameters execParameters)
        {
            return GetXml(execParameters.GetCommandName(), execParameters.GetParams());
        }

        public string GetString(IExecParameters execParameters)
        {
            return GetString(null, execParameters);
        }

        public TResult GetScalar<TResult>(IExecParameters execParameters) where TResult : struct
        {
            return GetScalar<TResult>(null, execParameters);
        }

        /// <summary>
        /// Вставляет данные в таблицу с транзакцией
        /// </summary>
        /// <param name="transaction">Транзакция</param>
        /// <param name="dataTable">Откуда вставлять</param>
        /// <param name="mapping"></param>
        public virtual void Bulk(SqlTransaction transaction, DataTable dataTable, Dictionary<string, string> mapping = null)
        {
            try
            {
                var command = $"Bulk insert into {FullTableName}";
                
                OnBeforeSqlRequest(new BeforeSqlRequestEventArgs($"START {command}"));

                using (var bulkCopy = new SqlBulkCopy(Connection, SqlBulkCopyOptions.Default, transaction))
                {
                    bulkCopy.DestinationTableName = FullTableName;
                    bulkCopy.BatchSize = 1;
                    bulkCopy.BulkCopyTimeout = 0;

                    if (mapping != null)
                    {
                        foreach (var map in mapping)
                        {
                            bulkCopy.ColumnMappings.Add(map.Key, map.Value);
                        }
                    }
                    else
                    {
                        foreach (DataColumn column in dataTable.Columns)
                        {
                            bulkCopy.ColumnMappings.Add(column.ColumnName, column.ColumnName);
                        }
                    }

                    Debug.WriteLine($"start {DateTime.Now:dd.MM.yyyy HH:mm}");

                    bulkCopy.WriteToServer(dataTable);

                    Debug.WriteLine($"finish {DateTime.Now:dd.MM.yyyy HH:mm}");
                }

                OnAfterSqlRequest(new AfterSqlRequestEventArgs($"COMPLETED {command}"));
            }
            catch (Exception ex)
            {
                OnLog($"{FullTableName}: {ex.Message}");
                throw;
            }
        }

        public virtual void Bulk(SqlTransaction transaction, DataTable dataTable, string tableName, string schema, Dictionary<string, string> mapping = null)
        {
            var fullTableName = $"{schema}.{tableName}";

            try
            {
                if (Connection.State == ConnectionState.Closed)
                {
                    Connection.Open();
                }


                var command = $"Bulk insert into {fullTableName}";

                OnBeforeSqlRequest(new BeforeSqlRequestEventArgs($"START {command}"));

                using (var bulkCopy = new SqlBulkCopy(Connection, SqlBulkCopyOptions.Default, transaction))
                {
                    bulkCopy.DestinationTableName = fullTableName;

                    if (mapping != null)
                    {
                        foreach (var map in mapping)
                        {
                            bulkCopy.ColumnMappings.Add(map.Key, map.Value);
                        }
                    }
                    else
                    {
                        foreach (DataColumn column in dataTable.Columns)
                        {
                            bulkCopy.ColumnMappings.Add(column.ColumnName, column.ColumnName);
                        }
                    }

                    bulkCopy.BulkCopyTimeout = 0;

                    bulkCopy.WriteToServer(dataTable);
                }

                Connection.Close();

                OnAfterSqlRequest(new AfterSqlRequestEventArgs($"COMPLETED {command}"));
            }
            catch (Exception ex)
            {
                OnLog($"{fullTableName}: {ex.Message}");
                throw;
            }
        }
       
        protected virtual void OnLog(string message)
        {
            var handler = Log;
            if (handler != null)
            {
                var e = new LogEventArgs()
                {
                    Message = message
                };

                handler(this, e);
            }
        }

        protected virtual void OnBeforeSqlRequest(BeforeSqlRequestEventArgs e)
        {
            BeforeSqlRequest?.Invoke(this, e);
        }

        protected virtual void OnAfterSqlRequest(AfterSqlRequestEventArgs e)
        {
            AfterSqlRequest?.Invoke(this, e);
        }
    }
}
