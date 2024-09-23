using System;
using System.Collections.Generic;
using System.Data;
using Microsoft.Data.SqlClient;

namespace Helpers.MsSql.Interfaces
{
    public interface ISqlHelper
    {
        SqlConnection Connection { get; }

        DataTable SelectById(SqlTransaction transaction, Guid id);

        DataTable SelectById(Guid id); 

        DataTable ExecuteCommand(SqlTransaction transaction, IExecParameters parameters);

        DataTable ExecuteCommand(IExecParameters parameters);

        void ExecuteCommandNonQuery(SqlTransaction transaction, IExecParameters parameters);

        void ExecuteCommandNonQuery(IExecParameters parameters);

        void Bulk(SqlTransaction transaction, DataTable dataTable, Dictionary<string, string> mapping = null);

        void Bulk(DataTable dataTable, Dictionary<string, string> mapping = null);

        TResult GetScalar<TResult>(SqlTransaction transaction, IExecParameters execParameters) where TResult: struct;

        TResult GetScalar<TResult>(IExecParameters execParameters) where TResult : struct;

        object GetScalar(SqlTransaction transaction, IExecParameters execParameters);

        object GetScalar(IExecParameters execParameters);
    }
}