using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace IBRLogging
{
    public enum Severity
    {
        Audit = 0,
        Exception = 1,
        Error = 2,
        Warning = 4,
        Info = 8,
        Timing = 16
    }

    public interface ILogging
    {
        string LogFile { get; set; }

        Severity LoggingLevel { get; set; }

        string ReadLog(string LogFile = "");

        Boolean ClearLog(DateTimeOffset? Before = null, string LogFile = "");

        Boolean Log(Severity EventSeverity, string Message, string Source, string Account = "", string ClientDetails = "", string SessionID = "", string LogFile = "");
    }
}