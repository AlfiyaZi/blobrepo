using System;
using System.IO;
using System.Web;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using BRLogging;
using BRConfig;
using BRDataAccess;
using IBRLogging;
using BLOBDocument;

namespace BLOBRepoService
{

    public class Log : BLOBRepoNoun
    {

        protected override void GetMethod(HttpContext Context)
        {
            try
            {
                Context.Response.Write(Logger.ReadLog());
            }
            catch (Exception ex)
            {
                Context.Response.Write ("Error: Failed to retrive log contents - " + ex.Message);
            }
        }

        protected override void DeleteMethod(HttpContext Context)
        {
            DateTimeOffset? Before = null;
            if (Context.Request["before"] != null)
                Before = Convert.ToDateTime(Context.Request["before"]);
            if (!Logger.ClearLog(Before))
                RaiseHTTPError(Context, "Failed to clear log file.", 500);
            else
            {
                Context.Response.Write("Log file cleared.");
                Context.Response.StatusCode = 200;
            }
        }
    }

    public class Config : BLOBRepoNoun
    {

        protected override void GetMethod(HttpContext Context)
        {
            try
            {
                Context.Response.Write(Settings.GetValueTable());
            }
            catch (Exception ex)
            {
                Logger.Log(Severity.Error, "Failed to retrive settings - " + ex.Message, "Config GET");
            }
        }

        protected override void PutMethod(HttpContext Context)
        {
            string setting = null;
            string value = null;

            if (Context.Request["setting"] != null)
                setting = Context.Request["setting"];
            if (Context.Request["value"] != null)
                value = Context.Request["value"];

            if (setting == null)
            {
                RaiseHTTPError(Context, "A setting parameter with the setting name must be provided.");
                return;
            }

            if (setting == "")
            {
                RaiseHTTPError(Context, "The setting name cannot be blank.");
                return;
            }

            if (value == null)
            {
                RaiseHTTPError(Context, "A value parameter with the value of the setting must be provided.");
                return;
            }

            if (!Settings.UpsertDBValue(setting, value))
                RaiseHTTPError(Context, "Failed to upsert configuration value. See logs for details of the failure.");
            else
            {
                Context.Response.Write("Successfully upserted the setting: " + setting + " in the database.");
                Context.Response.StatusCode = 200;
            }
        }

        protected override void DeleteMethod(HttpContext Context)
        {
            string setting = null;

            if (Context.Request["setting"] != null)
                setting = Context.Request["setting"];

            if (setting == null)
            {
                RaiseHTTPError(Context, "A setting parameter with the setting name must be provided.");
                return;
            }

            if (setting == "")
            {
                RaiseHTTPError(Context, "The setting name cannot be blank.");
                return;
            }

            if (!Settings.DeleteDBValue(setting))
                RaiseHTTPError(Context, "Failed to delete configuration value. See logs for details of the failure. One possibility is that a request was made to delete a non-existent setting. Also, only database settings can be added or deleted.");
            else
            {
                Context.Response.Write("Successfully deleted the setting: " + setting + " from the database.");
                Context.Response.StatusCode = 200;
            }
        }

    }

    public class Service : BLOBRepoNoun
    {
        protected override void GetMethod(HttpContext Context)
        {
            Context.Response.Write("Assembly name: " + typeof(Service).Assembly.GetName().Name + "<br/>");
            Context.Response.Write("Version: " + typeof(Service).Assembly.GetName().Version);
            Context.Response.StatusCode = 200;
        }

        protected override void PostMethod(HttpContext Context)
        {
            string action;

            if (Context.Request["action"] != null)
                action = Context.Request["action"].ToUpper();
            else
            {
                RaiseHTTPError(Context, "No action supplied.", 400);
                return;
            }

            BRDAL dal;

            switch (action)
            {
                case "ARCHIVE":
                case "DOARCHIVE":
                    dal = new BRDAL(Settings, Logger);
                    if (dal.DoArchive())
                    {
                        Context.Response.StatusCode = 200;
                        Context.Response.Write("Archive call processed successfully.");
                    }
                    else
                        RaiseHTTPError(Context, "Failed to process Archive call.", 500);
                    break;
                case "CLEANUP":
                case "DOCLEANUP":
                    dal = new BRDAL(Settings, Logger);
                    if (dal.DoCleanup())
                    {
                        Context.Response.StatusCode = 200;
                        Context.Response.Write("Cleanup call processed successfully.");
                    }
                    else
                        RaiseHTTPError(Context, "Failed to process Cleanup call.", 500);
                    break;
                default:
                    RaiseHTTPError(Context, "Action name is unrecognized.", 400);
                    break;
            }
        }
    }
    public class Document : BLOBRepoNoun
    {
        protected override void DeleteMethod(HttpContext Context)
        {
            string uuid = "";
            Guid docuuid;

            if (Context.Request["uuid"] != null)
                uuid = Context.Request["uuid"];
            if (!Guid.TryParse(uuid, out docuuid))
            {
                string message = "Malformed UUID provided.";
                Logger.Log(Severity.Error, message, "Document.Delete");
                RaiseHTTPError(Context, message);
                return;
            }

            BRDocument newdoc = new BRDocument(Settings, Logger);
            if (!newdoc.Delete(uuid))
            {
                RaiseHTTPError(Context, "Unable to delete data for requested document: " + uuid);
                return;
            }

            Context.Response.Write("Document UUID: " + uuid + " successfully deleted.");
            return;
        }

        private BRDocument Deserialize(HttpContext Context)
        {
            BRDocument newdoc;
            try
            {
                //string json = HttpContext.Current.Request.Form["json"];
                string json = "";

                Context.Request.InputStream.Position = 0;
                using (var inputStream = new StreamReader(Context.Request.InputStream))
                {
                    json = inputStream.ReadToEnd();
                }

                newdoc = BRSerializer.Deserialize(json, Settings, Logger);
            }
            catch (Exception ex)
            {
                Logger.Log(Severity.Error, "Failed to deserialize BLOB document from JSON - " + ex.Message, "Document.Put.Deserialize");
                RaiseHTTPError(Context, "Failed to deserialize BLOB document from JSON - " + ex.Message);
                return null;
            }
            if (newdoc == null)
            {
                Logger.Log(Severity.Error, "Failed to deserialize BLOB document from JSON", "Document.Put.Deserialize");
                RaiseHTTPError(Context, "Failed to deserialize BLOB document from JSON");
            }
            if (newdoc != null)
            {
                DateTimeOffset testdate;
                if (!DateTimeOffset.TryParse(newdoc.ArchiveDate, out testdate))
                {
                    Logger.Log(Severity.Error, "Invalid format for ArchiveDate in submitted JSON.", "Document.Put.Deserialize");
                    RaiseHTTPError(Context, "Invalid format for ArchiveDate in submitted JSON.");
                    newdoc = null;
                }
            }
            return newdoc;
        }

        protected override void GetMethod(HttpContext Context)
        {
            //Retrieve a particular document
            if (Context.Request["uuid"] != null)
            {
                string uuid = "";
                Guid docuuid;
                Boolean omitblob = false;
                uuid = Context.Request["uuid"];
                if (!Guid.TryParse(uuid, out docuuid))
                {
                    string message = "Malformed UUID provided.";
                    Logger.Log(Severity.Error, message, "Document.Put");
                    RaiseHTTPError(Context, message);
                    return;
                }

                if (Context.Request["omitblob"] != null)
                {
                    string obparm = Context.Request["omitblob"];
                    omitblob = ((obparm == "1") || (obparm.ToUpper() == "TRUE") || (obparm.ToUpper() == "YES"));
                }

                BRDocument newdoc = new BRDocument(Settings, Logger);
                if (!newdoc.Retrieve(uuid, omitblob))
                {
                    RaiseHTTPError(Context, "Unable to retrieve data for requested document: " + uuid);
                    return;
                }

                string docjson = BRSerializer.Serialize(newdoc);
                Context.Response.Write(docjson);
                return;
            }

            //Get a list of documents
            string accountname = "";
            if (Context.Request["account_name"] != null)
                accountname = Context.Request["account_name"];

            BRDocumentList doclist = new BRDocumentList(Settings, Logger);
            if (doclist.Populate(accountname))
            {
                string doclistjson = BRSerializer.Serialize(doclist);
                Context.Response.Write(doclistjson);
                return;
            }
            else
            {
                RaiseHTTPError(Context, "No documents found.", 404);
            }

        }

        protected override void PostMethod(HttpContext Context)
        {
            BRDocument newdoc;
            newdoc = Deserialize(Context);
            if (newdoc == null)
            {
                return;
            }
            Guid docguid; 
            if (Guid.TryParse(newdoc.UUID, out docguid))
            {
                string message = "POST method called for a document with UUID provided.";
                Logger.Log(Severity.Error, message, "Document.Post");
                RaiseHTTPError(Context, message);
                return;
            }
            try
            {
                if (!newdoc.Store())
                {
                    RaiseHTTPError(Context, "Failed to create new document, but no error raised.");
                }
                else
                {
                    Context.Response.Write(newdoc.UUID);
                    Context.Response.StatusCode = 201; 
                    Context.ApplicationInstance.CompleteRequest();
                }
            }
            catch (Exception ex)
            {
                string message = "Failed to create new document - " + ex.Message;
                Logger.Log(Severity.Error, message, "Document.Post");
            }
        }

        protected override void PutMethod(HttpContext Context)
        {
            BRDocument newdoc;
            newdoc = Deserialize(Context);
            if (newdoc == null)
            {
                return;
            }
            Guid docguid; 
            if (!Guid.TryParse(newdoc.UUID, out docguid))
            {
                string message = "PUT method called for a document with no UUID provided.";
                Logger.Log(Severity.Error, message, "Document.Put");
                RaiseHTTPError(Context, message);
                return;
            }
            try
            {
                if (!newdoc.Store())
                {
                    RaiseHTTPError(Context, "Failed to store document with UUID: " + newdoc.UUID + ", but no error raised.");
                }
                else
                {
                    Context.Response.StatusCode = 200; //May want to add conditional checking to set 201 for new objects
                    Context.ApplicationInstance.CompleteRequest();
                }
            }
            catch (Exception ex)
            {
                string message = "Failed to upsert document with UUID: " + newdoc.UUID + " - " + ex.Message;
                Logger.Log(Severity.Error, message, "Document.Put");
            }
       }
    }
}
