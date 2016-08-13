using System;
using System.Diagnostics;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using IBRLogging;
using IBRConfig;
using BRDataAccess;

namespace BRLogging
{
    public class Logging : ILogging
    {
        public const string DefaultLogFile = "blobstore.log";

        private string logfile;
        private IConfiguration config;

        public string LogFile
        {
            get
            {
                if (logfile == "")
                    return DefaultLogFile;
                else
                    return logfile;
            }
            set { logfile = value; }
        }


        public Logging(IConfiguration Config)
        {
            if (!(Config.GetValue("LogFile")==null))
                this.logfile = Config.GetValue("LogFile");
            //set logging level
            if (!(Config.GetValue("LoggingLevel") == null))
                this.logginglevel = Config.ParseLoggingLevel(Config.GetValue("LoggingLevel"));
            else
                logginglevel = (Severity.Error | Severity.Warning | Severity.Exception | Severity.Info | Severity.Timing);
            config = Config;
        }

        private Severity logginglevel;

        public Severity LoggingLevel
        {
            get { return logginglevel; }
            set { logginglevel = value; }
        }

        public string ReadLog(string LogFile = "")
        {
            string log = null;
            if (LogFile == "" && config.GetValue("ConnectionString") != null)
            {
                BRDAL dal = new BRDAL(config, this);
                log = dal.ReadDBLog();
            }
            if (log == null)
            {
                try
                {
                    LogFile = GetLogFile(LogFile);
                    log = File.ReadAllText(Path.Combine(Directory.GetCurrentDirectory(), LogFile));
                }
                catch (Exception ex)
                {
                    Log(Severity.Error, "Failed to read log file contents - " + ex.Message, "Logging.ReadLog");
                    return "Failed to read log file contents from " + LogFile;
                }
            }
            return log;
        }

        private string GetLogFile(string LogFile)
        {
            if (LogFile == "")
                LogFile = logfile;

            if (LogFile == "")
            {
                LogFile = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Log\\" + DefaultLogFile);
            }
            else if (!LogFile.Contains("\\"))
            {
                LogFile = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Log\\" + LogFile);
            }

            return LogFile;
        }

        private string CurrentUser()
        {
            string username = "";
            try
            {
                username = Environment.UserDomainName;
            }
            catch (Exception ex)
            {
                username = "(NULL)";
            }
            try
            {
                username += "\\" + Environment.UserName;
            }
            catch (Exception ex)
            {
                username += "\\(NULL)";
            }
            return username;
        }

        public Boolean ClearLog(DateTimeOffset? Before = null,  string LogFile = "")
        {
            Boolean success = false;

            if (LogFile == "" && config.GetValue("ConnectionString") != null)
            {
                BRDAL dal = new BRDAL(config, this);
                success = dal.ClearDBLog(Before);
            }
            else
            {
                LogFile = GetLogFile(LogFile);
                string ClientDetails = "";
                if (config.GetValue("ClientDetails") != null)
                    ClientDetails = config.GetValue("ClientDetails"); 

                string Account = CurrentUser();

                string newevent = DateTimeOffset.UtcNow.ToString("s");
                newevent += '\t';
                newevent += Severity.Audit.ToString();
                newevent += '\t';
                newevent += "Log file cleared.";
                newevent += '\t';
                newevent += "Logging.ClearLog";
                newevent += '\t';
                newevent += Account.Replace("\t", "<TAB>");
                if (ClientDetails != "")
                {
                    newevent += '\t';
                    newevent += ClientDetails;
                }
                newevent += "\n";
                try
                {
                    File.WriteAllText(LogFile, newevent);
                    success = true;
                }
                catch (Exception ex)
                {
                    Log(Severity.Error, "Failed to clear log file - " + ex.Message, "Logging.ClearLog");
                }
            }
        return success;
        }

        private Boolean LogToFile(Severity EventSeverity, string Message, string Source, string Account = "", string ClientDetails = "", string SessionID = "", string LogFile = "")
        {
            Boolean success = false;

            LogFile = GetLogFile(LogFile);

            string newevent = DateTimeOffset.UtcNow.ToString("s");
            newevent += '\t';
            newevent += EventSeverity.ToString();
            newevent += '\t';
            newevent += Message.Replace("\t", "<TAB>");
            newevent += '\t';
            newevent += Source.Replace("\t", "<TAB>");
            newevent += '\t';
            newevent += Account.Replace("\t", "<TAB>");
            newevent += '\t';
            newevent += ClientDetails;
            newevent += '\t';
            newevent += SessionID;
            
            try
            {
                using (StreamWriter logappend = File.AppendText(LogFile))
                {
                    logappend.WriteLine(newevent);
                }
                success = true;
            }
            catch
            {
                //Nothing more we can do... we failed to log, and failed to log to the failover log
                throw;
            }
            return success;

        }

        private Boolean LogToDiag(Severity EventSeverity, string Message, string Source, string Account = "", string ClientDetails = "", string SessionID = "")
        {
            Boolean success = false;

            string newevent = DateTimeOffset.UtcNow.ToString("s");
            newevent += '\t';
            newevent += EventSeverity.ToString();
            newevent += '\t';
            newevent += Message.Replace("\t", "<TAB>");
            newevent += '\t';
            newevent += Source.Replace("\t", "<TAB>");
            newevent += '\t';
            newevent += Account.Replace("\t", "<TAB>");
            newevent += '\t';
            newevent += ClientDetails;
            newevent += '\t';
            newevent += SessionID;

            try
            {
                switch (EventSeverity)
                {
                    case Severity.Error:
                        System.Diagnostics.Trace.TraceError(newevent);
                        break;
                    case Severity.Warning:
                        System.Diagnostics.Trace.TraceWarning(newevent);
                        break;
                    default:
                        System.Diagnostics.Trace.TraceInformation(newevent);
                        break;
                }
                success = true;
            }
            catch
            {
                //Nothing more we can do... we failed to log, and failed to log to the failover log
                throw;
            }
            return success;

        }


        //Main logging call to log application and audit events
        //Will attempt to log to DB is DB is configured, and will failover to file or System.Diagnostics
        // or WindowsAzure.Diagnostics, depending on configuration 
        public Boolean Log(Severity EventSeverity, string Message, string Source, string Account = "", string ClientDetails = "", string SessionID = "", string LogFile = "")
        {
            Boolean success = false;
            string failoverlog = "FILE";

            if (config.GetValue("FailoverLog") != null)
                failoverlog = config.GetValue("FailoverLog").ToUpper();

            if ((EventSeverity == Severity.Audit) || ((EventSeverity & logginglevel) > 0))
            {

                if (Account == "")
                    Account = CurrentUser();

                if (SessionID == "")
                    if (config.GetValue("SessionGuid") != null)
                        SessionID = config.GetValue("SessionGuid");
                
                if (config != null)
                    if (config.GetValue("ConnectionString") != null)
                    {
                        try
                        {
                            BRDAL dal = new BRDAL(config, this);
                            success = dal.LogToDB(EventSeverity, Message, Source, Account, ClientDetails, SessionID);
                        }
                        catch (Exception ex)
                        {
                            if (failoverlog == "DIAG")
                                success = LogToDiag(Severity.Error, "Failed to log to database - " + ex.Message, "Logging.Log", Account, ClientDetails, SessionID);
                            else
                                success = LogToFile(Severity.Error, "Failed to log to database - " + ex.Message, "Logging.Log", Account, ClientDetails, SessionID, LogFile);
                        }
                    }

                if (!success)
                {
                    if (failoverlog == "DIAG")
                        success = LogToDiag(EventSeverity, Message, Source, Account, ClientDetails, SessionID);
                    else    
                        success = LogToFile(EventSeverity, Message, Source, Account, ClientDetails, SessionID, LogFile);
                }
                return success;
            }
            else
                return true;
        }

    }
}