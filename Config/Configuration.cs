using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using IBRConfig;
using IBRLogging;
using BRLogging;
using BRDataAccess;

namespace BRConfig
{
    public class Configuration: IConfiguration
    {

        private ILogging logger;
        public const string DefaultConfigFile = "blobstore.conf";
        private string configfile;

        private class ConfigItem
        {
            private string name;

            public string Name
            {
                get { return name; }
                set { name = value; }
            }
            private string value;

            public string Value
            {
                get { return value; }
                set { this.value = value; }
            }

            private string source;

            public string Source
            {
                get { return source; }
                set { source = value; }
            }


        }

        private List<ConfigItem> values = new List<ConfigItem>();

        public IList<String> Settings
        {
            get {
                List<String> settings = new List<string>();
                foreach(ConfigItem x in values)
                {
                    settings.Add(x.Name);
                }
                return settings.AsReadOnly(); }
        }

        //Loads all settings into the private values list.
        //The expectations here are that the conf file is fairly small
        //and most settings will be needed for most actions.
        //Reloading the settings on each method invocation allow conf changes to take
        //effect pretty quickly, whether they're edited through the API or directly in the file
        public Boolean Load()
        {
            string confdata = null;
            values.Clear();
            loaded = false;
            var appsettings = System.Configuration.ConfigurationManager.AppSettings;

            try
            {
                confdata = File.ReadAllText(configfile);
            }
            catch (Exception ex)
            {
                if (appsettings.Count == 0)
                {
                    //Since we failed early reading configuration data, the only possibility is to try to log the event
                    //this should go to the default log, since we couldn't read the logfile setting
                    logger.Log(Severity.Error, "Failed to read configuration file: " + configfile + " - " + ex.Message, "Configuration initializer");
                    return false;
                }
                else
                    logger.Log(Severity.Info, "Failed to read configuration file: " + configfile + " - " + ex.Message + " - but there are settings available from AppSettings.", "Configuration initializer");
            }

            if (confdata != null)
            {
                string[] lines = confdata.Split('\n');
                foreach (string line in lines)
                {
                    if (line.Contains('=') && (!line.StartsWith("#")))
                    {
                        try
                        {
                            //string[] details = line.Split('=');
                            string[] details = line.Split(new char[] { '=' }, 2);
                            SetMemValue(details[0].Trim(), details[1].Trim(), "File");

                            loaded = true;

                        }
                        catch (Exception ex)
                        {
                            //Failed to read or parse an entry in the conf file
                            string message = "Failed to parse line (";
                            if (line != null)
                                message += line;
                            else
                                message += "NULL LINE";
                            message += ") from configuration file: " + configfile + " - " + ex.Message;
                            logger.Log(Severity.Exception, message, "Configuration initializer");

                        }
                    }

                }
            }

            foreach (var key in appsettings.AllKeys)
            {
                SetMemValue(key.ToString(), appsettings[key].ToString(), "AppSettings");
            }

            loaded = (values.Count > 0);

            if (loaded)
            {
                logger.Log(Severity.Info, "Loaded configuration settings from " + configfile, "Configuration Load");
                BRDAL dal = new BRDAL(this, logger);
                dal.LoadDBSettings(); 
            }
            return loaded;
        }

        public Configuration(string ConfigFile = "")
        {
            logger = new Logging(this);

            if (ConfigFile == "")
            {
                ConfigFile = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Conf\\" + DefaultConfigFile);
            }
            else if (!ConfigFile.Contains("\\"))
            {
                ConfigFile = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "Conf\\" + ConfigFile);
            }

            configfile = ConfigFile;

            Load();
        }


        private Boolean loaded;

        public Boolean Loaded
        {
            get { return loaded; }
        }

        public string GetValue(string Name)
        {
            Boolean found = false;
            int i = 0;
            while (i < values.Count && !found)
                found = (values[i++].Name.ToUpper() == Name.ToUpper());
            if (found)
                return values[i - 1].Value;
            else return null;
        }

        public string GetValueTable()
        {
            string result = "";
            foreach (ConfigItem ci in values)
            {
                result += ci.Name + "\t" + ci.Value + "\t" + ci.Source + Environment.NewLine;
            }
            return result;
        }

        public Boolean UpsertDBValue(string Name, string Value)
        {
            BRDAL dal = new BRDAL(this, logger);
            return (dal.UpsertDBSetting(Name, Value));
        }

        public Boolean DeleteDBValue(string Name)
        {
            BRDAL dal = new BRDAL(this, logger);
            return (dal.DeleteDBSetting(Name));
        }

        //Sets or adds a setting to the configuration file and to the in-memory settings list
        //For Azure use, this function is probably not needed, as settings added through the API
        //will be persisted in the DB (UpsertDBValue).
        public Boolean SetValue(string Name, string Value)
        {
            string sourcename = "Configuration.SetValue";
            string confdata;
            Boolean success = false;

            if (loaded)
            {
                //referesh in-memory settings, and verify that config file can be read
                if (Load())
                {
                    if (GetValue(Name) == null)
                    {
                        //New value, just append it
                        try
                        {
                            using (StreamWriter confappend = File.AppendText(configfile))
                            {
                                confappend.WriteLine(Name + "=" + Value);
                            }
                        }
                        catch (Exception ex)
                        {
                            logger.Log(Severity.Error, "Failed to append setting: " + Name + " to configuration file: " + configfile + " - " + ex.Message, sourcename);
                        }
                    }
                    else
                    {
                        //existing value is being changed
                        Name = Name.Trim();
                        try
                        {
                            confdata = File.ReadAllText(configfile);
                            success = true;
                        }
                        catch (Exception ex)
                        {
                            logger.Log(Severity.Error, "Failed to read configuration file: " + configfile + " - " + ex.Message, sourcename);
                            return false;
                        }
                        string[] lines = confdata.Split('\n');
                        string content = "";
                        foreach (string line in lines)
                        {
                            if (line.Contains('=') && (!line.StartsWith("#")))
                            {
                                try
                                {
                                    string[] details = line.Split('=');
                                    if (details[0].Trim().ToUpper() == Name.ToUpper())
                                    {
                                        //Found the matching line
                                        content += Name + "=" + Value + '\n';
                                        success = true;
                                    }
                                    else
                                        content += line + '\n';
                                }
                                catch (Exception ex)
                                {
                                    //Failed to read or parse an entry in the conf file
                                    string message = "Failed to parse line (";
                                    if (line != null)
                                        message += line;
                                    else
                                        message += "NULL LINE";
                                    message += ") from configuration file: " + configfile + " - " + ex.Message;
                                    logger.Log(Severity.Exception, message, sourcename);
                                }
                            }
                            else
                                content += line + '\n';
                        }
                        try
                        {
                            File.WriteAllText(configfile, content);
                        }
                        catch (Exception ex)
                        {
                            logger.Log(Severity.Error, "Failed to write setting: " + Name + " to configuration file: " + configfile + " - " + ex.Message, sourcename);
                            logger.Log(Severity.Error, "Check configuration file. It may be missing or corrupted.", sourcename);
                            return false;
                        }
                    }
                }

            }
            return success;
        }

        //Sets or adds a setting to the in-memory settings list
        public Boolean SetMemValue(string Name, string Value, string Source)
        {
            string sourcename = "Configuration.SetMemValue";

            if (GetValue(Name) == null)
            {
                //New value, just append it
                try
                {
                    ConfigItem x = new ConfigItem();
                    x.Name = Name;
                    x.Value = Value;
                    x.Source = Source;
                    values.Add(x);
                }
                catch (Exception ex)
                {
                    logger.Log(Severity.Error, "Failed to append setting: " + Name + " to values collection. - " + ex.Message, sourcename);
                    return false;
                }
            }
            else
            {
                //existing value is being changed
                try
                {
                    Boolean found = false;
                    int i = 0;
                    while (i < values.Count && !found)
                        found = (values[i++].Name.ToUpper() == Name.ToUpper());
                    if (found)
                        values[i - 1].Value = Value;
                }
                catch (Exception ex)
                {
                    logger.Log(Severity.Error, "Failed to set in-memory configuration value: " + Name + " to: " + Value + " - " + ex.Message, sourcename);
                    return false;
                }
            }

            //If we just loaded a new log file setting, set it for the logger
            if (Name.ToUpper() == "LOGFILE")
                logger.LogFile = Value;

            //If we just loaded a new logging level, update that setting
            if (Name.ToUpper() == "LOGGINGLEVEL")
                logger.LoggingLevel = ParseLoggingLevel(Value);

            return true;
        }

        public Severity ParseLoggingLevel(string LoggingLevel)
        {
            Severity result = 0;

            foreach (string level in LoggingLevel.Split(','))
            {
                result = (result | (Severity)(Enum.Parse(typeof(Severity), level.Trim())));
            }
            return result;
        }

    }

}
