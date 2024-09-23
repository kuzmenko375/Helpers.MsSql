using System;
using Helpers.MsSql.Interfaces;

namespace Helpers.MsSql.ExecParameters.ById
{
    /// <summary>
    /// Выполняет операцию по Id объекта
    /// </summary>
    public abstract class ById : ExecParameters, IExecParametersById
    {
        public Guid Id { get; set; }

        protected ById()
        {
        }

        protected ById(Guid id)
        {
            Id = id;
        }
    }
}