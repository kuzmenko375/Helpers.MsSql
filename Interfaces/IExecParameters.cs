using Microsoft.Data.SqlClient;

namespace Helpers.MsSql.Interfaces
{
    public interface IExecParameters
    {
        /// <summary>
        /// Название команды, например: Select, SelectByMedFileId
        /// </summary>
        string GetCommandName();

        /// <summary>
        /// Должно вернуть параметры запроса
        /// </summary>
        /// <returns></returns>
        SqlParameter[] GetParams();
    }
}