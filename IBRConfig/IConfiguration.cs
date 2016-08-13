using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using IBRLogging;

namespace IBRConfig
{
    public interface IConfiguration
    {
        IList<String> Settings {  get; }
        Boolean Load();
        Boolean Loaded { get; }
        string GetValue(string Name);
        string GetValueTable();
        Boolean UpsertDBValue(string Name, string Value);
        Boolean DeleteDBValue(string Name);
        Boolean SetValue(string Name, string Value);
        Boolean SetMemValue(string Name, string Value, string Source);
        Severity ParseLoggingLevel(string LoggingLevel);
    }
}
