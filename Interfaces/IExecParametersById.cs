using System;

namespace Helpers.MsSql.Interfaces
{
    public interface IExecParametersById : IExecParameters
    {
        Guid Id { get; set; }
    }
}