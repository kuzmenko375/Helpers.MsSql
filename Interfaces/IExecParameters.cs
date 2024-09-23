using Microsoft.Data.SqlClient;

namespace Helpers.MsSql.Interfaces
{
    public interface IExecParameters
    {
        /// <summary>
        /// �������� �������, ��������: Select, SelectByMedFileId
        /// </summary>
        string GetCommandName();

        /// <summary>
        /// ������ ������� ��������� �������
        /// </summary>
        /// <returns></returns>
        SqlParameter[] GetParams();
    }
}