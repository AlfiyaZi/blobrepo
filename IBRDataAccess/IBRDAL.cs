using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using IBRLogging;
using IBRConfig;
using IBLOBDocument;

namespace IBRDataAccess
{
    public interface IBRDAL
    {
        Boolean LogToDB(Severity EventSeverity, string Message, string Source, string Account = "", string ClientDetails = "");
        Boolean UpsertDocument(IBRDocument Document);
        Boolean LoadDBSettings(string SettingName = "");
    }
}
