namespace Helpers.MsSql.EventArgs
{
    public class BeforeSqlRequestEventArgs : System.EventArgs
    {
        public BeforeSqlRequestEventArgs(string command)
        {
            Command = command;
        }

        public string Command { get; set; }
    }
}