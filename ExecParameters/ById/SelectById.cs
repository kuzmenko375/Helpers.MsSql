using System;

namespace Helpers.MsSql.ExecParameters.ById
{
    public class SelectById : ById
    {
        public SelectById(Guid id) : base(id)
        {
        }
    }
}