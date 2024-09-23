using System;

namespace Helpers.MsSql.ExecParameters.ById
{
    public class DeleteById : ById
    {
        public DeleteById(Guid id) : base(id)
        {
        }
    }
}