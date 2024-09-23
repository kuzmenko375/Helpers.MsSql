namespace Helpers.MsSql.EventArgs
{
    public class AfterSqlRequestEventArgs : System.EventArgs
    {
        public AfterSqlRequestEventArgs(string command)
        {
            Command = command;
        }

        public string Command { get; set; }
    }
}