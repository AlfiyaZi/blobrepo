using System;
using System.Web;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using BRLogging;
using BRConfig;
using IBRLogging;
using IBRConfig;

namespace BLOBRepoService
{
    public class BLOBRepoNoun : IHttpHandler
    {
        protected void RaiseHTTPError(HttpContext Context, string Message, int StatusCode = 500)
        {
            Context.Response.TrySkipIisCustomErrors = true;
            Context.Response.StatusCode = StatusCode;
            Context.Response.Write(Message);
            Context.ApplicationInstance.CompleteRequest();
        }


        protected Logging Logger;
        protected Configuration Settings;

        bool IHttpHandler.IsReusable
        {
            get { return false; }
        }

        private Boolean AuthenticateSession(HttpContext Context)
        {
            //Need to add other authentication stuff here
            //For now, we'll just set up a session GUID to group log entries
            //and distinguish DB transaction records
            if (Settings.GetValue("SessionGuid") == null)
                Settings.SetMemValue("SessionGuid", Guid.NewGuid().ToString(), "Process");
            return true;
        }

        void IHttpHandler.ProcessRequest(HttpContext context)
        {
            try
            {
                string url = Convert.ToString(context.Request.Url);

                Settings = new Configuration();
                Logger = new Logging(Settings);

                if (!AuthenticateSession(context))
                {
                    RaiseHTTPError(context, "Failed to authenticate session.", 401);
                    return;
                }

                Logger.Log(Severity.Info, "Initialized new RESTMethod instance", "RESTMethod initializer");

                //Handle HTTP methods (to be overridden in derived classes)
                switch (context.Request.HttpMethod)
                {
                    case "GET":
                        GetMethod(context);
                        break;
                    case "POST":
                        PostMethod(context);
                        break;
                    case "PUT":
                        PutMethod(context);
                        break;
                    case "DELETE":
                        DeleteMethod(context);
                        break;
                    case "PATCH":
                        PatchMethod(context);
                        break;
                    default:
                        context.Response.Write(context.Request.HttpMethod + "  is not a supported method for this object.");
                        break;
                }
            }
            catch (Exception ex)
            {
                if (Logger != null)
                {
                    Logger.Log(Severity.Error, "Failed at top level while processing request: " + context.Request.HttpMethod + " - " + ex.Message, "BLOBRepoService");
                }
                //errHandler.ErrorMessage = ex.Message.ToString();
                context.Response.Write(ex.Message);
            }
        }

        protected virtual void GetMethod(HttpContext Context)
        {
            Context.Response.Write("GET is currently not implemented for this object.");
            Logger.Log(Severity.Warning, "Non-implemented GET method called", "Base BLOBRepoNoun GET function.");
        }

        protected virtual void PostMethod(HttpContext Context)
        {
            Context.Response.Write("POST is currently not implemented for this object.");
            Logger.Log(Severity.Warning, "Non-implemented POST method called", "Base BLOBRepoNoun POST function.");
        }

        protected virtual void PutMethod(HttpContext Context)
        {
            Context.Response.Write("PUT is currently not implemented for this object.");
            Logger.Log(Severity.Warning, "Non-implemented PUT method called", "Base BLOBRepoNoun PUT function.");
        }

        protected virtual void PatchMethod(HttpContext Context)
        {
            Context.Response.Write("PATCH is currently not implemented for this object.");
            Logger.Log(Severity.Warning, "Non-implemented PATCH method called", "Base BLOBRepoNoun PATCH function.");
        }

        protected virtual void DeleteMethod(HttpContext Context)
        {
            Context.Response.Write("DELETE is currently not implemented for this object.");
            Logger.Log(Severity.Warning, "Non-implemented DELETE method called", "Base BLOBRepoNoun DELETE function.");
        }
    }
}



