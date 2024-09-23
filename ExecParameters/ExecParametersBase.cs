using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Microsoft.Data.SqlClient;

namespace Helpers.MsSql.ExecParameters
{
    /// <summary>
    /// Возвращает CommandName - название класса, 
    /// перечень параметров - преобразовывает свойства класса
    /// к объекту SqlParameter
    /// </summary>
    public abstract class ExecParametersBase
    {
        public virtual string GetCommandName()
        {
            return GetType().Name;
        }

        public SqlParameter[] GetParams()
        {
            var props = GetType().GetProperties();

            var resultList = new List<SqlParameter>();

            foreach (var propertyInfo in props)
            {
                resultList.Add(SqlParameterFromPropertyInfo(this, propertyInfo));
            }

            return resultList.ToArray();
        }

        /// <summary>
        /// Возвращает SqlParameter по переданному объекту и св-ву
        /// </summary>
        /// <param name="object"></param>
        /// <param name="propertyInfo"></param>
        /// <param name="deletePartFromFieldName">Удалить из названия поля часть указанную строки</param>
        /// <returns></returns>
        public static SqlParameter SqlParameterFromPropertyInfo(object @object, PropertyInfo propertyInfo, string deletePartFromFieldName)
        {
            object value = propertyInfo.GetValue(@object);

            if ((propertyInfo.PropertyType == typeof(DateTime?) || propertyInfo.PropertyType == typeof(DateTime))
                && (((DateTime?)propertyInfo.GetValue(@object)) == null || ((DateTime)propertyInfo.GetValue(@object)) == default(DateTime)))
            {
                value = null;
            }

            if (propertyInfo.PropertyType == typeof(string)
                && (string)propertyInfo.GetValue(@object) != null
                && (string)propertyInfo.GetValue(@object) == "")
            {
                value = null;
            }

            if (propertyInfo.PropertyType.BaseType == typeof(Array))
            {
                if (propertyInfo.PropertyType == typeof(Guid[]))
                {
                    var propVal = (Guid[])propertyInfo.GetValue(@object);
                    value = propVal != null && propVal.Any()
                        ? string.Join(",", propVal.Select(e => e.ToString()))
                        : null;
                }
                else
                {
                    if (propertyInfo.PropertyType != typeof(byte[]))
                    {
                        if (propertyInfo.PropertyType == typeof(int[]))
                        {
                            var propVal = (int[])propertyInfo.GetValue(@object);
                            value = propVal != null && propVal.Any()
                                ? string.Join(",", propVal.Select(Convert.ToInt32))
                                : null;
                        }
                        else
                            if (propertyInfo.PropertyType == typeof(long[]))
                            {
                                var propVal = (long[])propertyInfo.GetValue(@object);
                                value = propVal != null && propVal.Any()
                                    ? string.Join(",", propVal.Select(Convert.ToInt64))
                                    : null;
                            }
                    }
                }
            }

            if (propertyInfo.PropertyType.BaseType == typeof(Enum))
            {
                value = (int)propertyInfo.GetValue(@object);
            }

            return new SqlParameter($"@{(string.IsNullOrEmpty(deletePartFromFieldName) ? propertyInfo.Name : propertyInfo.Name.Replace(deletePartFromFieldName, ""))}", value);
        }

        public static SqlParameter SqlParameterFromPropertyInfo(object @object, PropertyInfo propertyInfo)
        {
            return SqlParameterFromPropertyInfo(@object, propertyInfo, string.Empty);
        }
    }
}