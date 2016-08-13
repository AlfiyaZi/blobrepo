using System;
using System.Data;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.OleDb;
using IBRLogging;
using IBRConfig;
using BRCommon;
using System.Security.Cryptography;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

/*
This module combines both generic data access abstractions (for executing queries), and 
business object specific functions for doing things like clearing the log and upserting a document.
Later it might make sense to break these up a bit... maybe by moving the abstraction functions
into a base class and making them protected rather than private. The main reason for restricting 
their access is because the connection needs to be kept open when working with Readers and their
lifecycle needs to be well controlled. 
*/

namespace BRDataAccess
{
    public class BRDAL
    {
        private IConfiguration config;
        private ILogging logger;
        private OleDbConnection oconn;

        public BRDAL(IConfiguration BRConfig, ILogging BRLogging)
        {
            config = BRConfig;
            logger = BRLogging;
        }

        private string FormatForSQL(string input)
        {
            string temp = input.Replace("\t", "<TAB>");
            temp = temp.Replace("'", "''");
            return ("'" + temp + "'");
        }

        //This function is a bit safer to use than ExecSQLForReader, since it 
        //populates the entire result set into a managed DataTable object in memory.
        //On the other hand, this also means more memory utilization... so this
        //is good for relatively small result sets to be passed between modules using the 
        //DataTable as a generic structure to encapsulate the result set.
        public DataTable ExecSQLForTable(string SQL)
        {
            string source = "ExecSQLForTable";

            OleDbDataReader reader = ExecSQLForReader(SQL);

            if (reader == null)
            {
                logger.Log(Severity.Error, "Failed to execute SQL.", source);
                DisposeReader(reader);
                return null;
            }
            if (!reader.HasRows)
            {
                logger.Log(Severity.Warning, "Query returned no rows.", source);
                DisposeReader(reader);
                return null;
            }

            DataTable result = new DataTable();
            DataTable schema = reader.GetSchemaTable();

            try {
                foreach (DataRow scolumn in schema.Rows)
                {
                    string columnname = scolumn["ColumnName"].ToString();
                    DataColumn column = new DataColumn(columnname, (Type)(scolumn["DataType"]));
                    result.Columns.Add(column);
                }
            }
            catch (Exception ex)
            {
                logger.Log(Severity.Error, "Failed to convert query schema to DataTable schema. - " + ex.Message, source);
                DisposeReader(reader);
                return null;
            }

            try
            {
                while (reader.Read())
                {
                    DataRow row = result.NewRow();
                    for (int i = 0; i < reader.FieldCount; i++)
                    {
                        row[i] = reader[i];
                    }
                    result.Rows.Add(row);
                }
            }
            catch (Exception ex)
            {
                logger.Log(Severity.Error, "Failed to load OleDBDataReader results into DataTable. - " + ex.Message, source);
                DisposeReader(reader);
                return null;
            }

            DisposeReader(reader);
            return result;

        }

        private void GetRetries(string Group, out int Retries, out int RetryDelay)
        {
            Retries = 0;
            RetryDelay = 0;
            string source = "GetRetries";
            string name = Group + "Retries";

            if (config.GetValue(name) != null)
            {
                try
                {
                    Retries = System.Convert.ToInt32(config.GetValue(name));
                }
                catch (Exception ex)
                {
                    logger.Log(Severity.Error, "Failed to retrieve or convert " + name + " setting value. - " + ex.Message, source);
                }
            }

            name = Group + "RetryDelay";
            if (config.GetValue(name) != null)
            {
                try
                {
                    RetryDelay = System.Convert.ToInt32(config.GetValue(name));
                }
                catch (Exception ex)
                {
                    logger.Log(Severity.Error, "Failed to retrieve or convert " + name + " setting value. - " + ex.Message, source);
                }
            }

            if (Retries < 0)
            {
                logger.Log(Severity.Exception, "Invalid (negative) value set for " + name, source);
                Retries = 0;
            }

            if (RetryDelay < 0)
            {
                logger.Log(Severity.Exception, "Invalid (negative) value set for " + name, source);
                RetryDelay = 0;
            }
        }

        //DisposeReader must always be called when the calling function is done with the reader produced 
        //by this function. This will ensure that the connection is closed and that the reader's 
        //resources are freed. Failure to do this will cause subsequent calls to ExecSQLForReader to fail.
        private OleDbDataReader ExecSQLForReader(string SQL)
        {
            string source = "ExecSQLForReader";
            OleDbDataReader odr = null;
            int retries;
            int retrydelay;

            int currenttry = 0;
            Boolean success = false;

            GetRetries("DB", out retries, out retrydelay);

            if (config.GetValue("ConnectionString") == null)
            {
                logger.Log(Severity.Error, "No connection string configured.", source);
                return null;
            }

            if (oconn != null)
            {
                logger.Log(Severity.Error, "Connection object already in-use. Some other function probably called ExecSQLForReader without calling DisposeReader afterwards.", source);
                return null;
            }
            oconn = new OleDbConnection();
            oconn.ConnectionString = config.GetValue("ConnectionString");

            while (!success)
            {
                try
                {
                    oconn.Open();
                    success = true;
                }
                catch (Exception ex)
                {
                    logger.Log(Severity.Error, "Failed to connect to DB - " + ex.Message, source);
                    oconn.Dispose();
                    oconn = null;
                    if (++currenttry > retries)
                        return null;
                    else
                    {
                        logger.Log(Severity.Exception, "Retrying DB connection after wait of " + retrydelay.ToString() + " seconds.", source);
                        System.Threading.Thread.Sleep(1000 * retrydelay);
                    }
                }
            }

            currenttry = 0;
            success = false;
            OleDbCommand odc = new OleDbCommand();
            odc.CommandText = SQL;
            odc.CommandType = System.Data.CommandType.Text;
            odc.Connection = oconn;
            while (!success)
            {
                try
                {
                    //In case of retry, the connection will have been closed after the previous failure
                    if (oconn.State == ConnectionState.Closed)
                        oconn.Open();
                    odr = odc.ExecuteReader(System.Data.CommandBehavior.CloseConnection);
                    success = true;
                }
                catch (Exception ex)
                {
                    logger.Log(Severity.Error, "Failed to execute SQL query - " + ex.Message, source);
                    if (++currenttry > retries)
                    {
                        if (oconn.State == System.Data.ConnectionState.Open)
                            oconn.Close();
                        oconn.Dispose();
                        return null;
                    }
                    {
                        logger.Log(Severity.Exception, "Retrying query execution after wait of " + retrydelay.ToString() + " seconds.", source);
                        System.Threading.Thread.Sleep(1000 * retrydelay);
                    }
                }
            }
            return odr;
        }

        private int ExecuteSQLDirect(string SQL)
        {
            //Typically used to execute DML (INSERT, UPDATE, DELETE)
            string source = "ExecSQLDirect";
            int result = -1;

            int retries;
            int retrydelay;

            int currenttry = 0;
            Boolean success = false;

            GetRetries("DB", out retries, out retrydelay);

            if (config.GetValue("ConnectionString") == null)
            {
                logger.Log(Severity.Error, "No connection string configured.", source);
                return -1;
            }

            using (OleDbConnection oconn = new OleDbConnection())
            {
                while (!success && currenttry <= retries)
                {
                    oconn.ConnectionString = config.GetValue("ConnectionString");
                    oconn.Open();
                    if (oconn.State == System.Data.ConnectionState.Open)
                    {
                        OleDbCommand odc = new OleDbCommand();
                        odc.CommandText = SQL;
                        odc.Connection = oconn;

                        try
                        {
                            result = odc.ExecuteNonQuery();
                            success = true;
                        }
                        catch (Exception ex)
                        {
                            logger.Log(Severity.Error, "Failed to execute SQL - " + ex.Message, source);
                            result = -1;
                            if (currenttry++ < retries)
                            {
                                logger.Log(Severity.Exception, "Retrying query execution after wait of " + retrydelay.ToString() + " seconds.", source);
                                System.Threading.Thread.Sleep(1000 * retrydelay);
                            }
                        }
                        try
                        {
                            if (oconn.State == System.Data.ConnectionState.Open)
                                oconn.Close();
                        }
                        catch (Exception ex)
                        {
                            logger.Log(Severity.Error, "Failed to close DB connection - " + ex.Message, source);
                        }
                    }
                }
            }
            return result;
        }

        private enum ScalarType
        {
            Int,
            String,
            BigInt,
            DateTime,
            Double    
        }

        private int ExecuteSQLForScalarInt(string SQL)
        {
            return (Int32)(ExecuteSQLForScalar(SQL, ScalarType.Int));
        }

        private string ExecuteSQLForScalarString(string SQL)
        {
            return (String)(ExecuteSQLForScalar(SQL, ScalarType.String));
        }

        private object ExecuteSQLForScalar(string SQL, ScalarType Type = ScalarType.Int)
        {
            //Typically used to execute single value queries (COUNT(*), MAX(), etc)
            string source = "ExecuteSQLForScalar";
            object result = 0;
            int retries;
            int retrydelay;

            int currenttry = 0;
            Boolean success = false;

            GetRetries("DB", out retries, out retrydelay);

            if (config.GetValue("ConnectionString") == null)
            {
                logger.Log(Severity.Error, "No connection string configured.", source);
                return 0;
            }

            using (OleDbConnection oconn = new OleDbConnection())
            {
                while (!success && currenttry <= retries)
                {
                    oconn.ConnectionString = config.GetValue("ConnectionString");
                    oconn.Open();
                    if (oconn.State == System.Data.ConnectionState.Open)
                    {
                        OleDbCommand odc = new OleDbCommand();
                        odc.CommandText = SQL;
                        odc.Connection = oconn;
                        try
                        {
                            switch (Type)
                            {
                                case ScalarType.Int:
                                    result = (Int32)odc.ExecuteScalar();
                                    break;
                                case ScalarType.BigInt:
                                    result = (Int64)odc.ExecuteScalar();
                                    break;
                                case ScalarType.DateTime:
                                    result = DateTime.Parse(odc.ExecuteScalar().ToString());
                                    break;
                                case ScalarType.String:
                                    result = odc.ExecuteScalar().ToString();
                                    break;
                                default:
                                    result = odc.ExecuteScalar();
                                    break;
                            }
                            success = true;
                        }
                        catch (Exception ex)
                        {
                            logger.Log(Severity.Error, "Failed to execute SQL or cast result - " + ex.Message, source);
                            if (currenttry++ < retries)
                            {
                                logger.Log(Severity.Exception, "Retrying query execution after wait of " + retrydelay.ToString() + " seconds.", source);
                                System.Threading.Thread.Sleep(1000 * retrydelay);
                            }
                        }
                        try
                        {
                            if (oconn.State == System.Data.ConnectionState.Open)
                                oconn.Close();
                        }
                        catch (Exception ex)
                        {
                            logger.Log(Severity.Error, "Failed to close DB connection - " + ex.Message, source);
                        }
                    }
                }
            }
            return result;
        }

        public DataTable RetrieveDocumentList(string StoreAccount = "")
        {
            string source = "RetrieveDocumentList";

            logger.Log(Severity.Info, "Querying for document list", source);

            string SQL = "SELECT d.uuid, d.created, d.modified, d.archive_after, d.blob_uuid, d.blob_size, d.blob_md5, s.account_name, s.tier, s.sku";
            SQL += " FROM dbo.Document d JOIN dbo.BLOBStore s ON d.store_id = s.store_id";
            if (StoreAccount != "")
                SQL += " WHERE s.account_name LIKE " + FormatForSQL(StoreAccount);

            DataTable doclist = ExecSQLForTable(SQL);
            return doclist;
        }

        private Boolean StoreDocumentProperties(List<BRProperty> Properties, int DocID)
        {
            string baseSQL = "INSERT dbo.DocumentMetaData (doc_id, property_name, property_value) VALUES (" + DocID.ToString() + ", ";
            string source = "StoreDocumentProperties";
            string SQL = "";
            Boolean result = true;

            foreach (BRProperty kvp in Properties)
            {
                SQL = baseSQL + FormatForSQL(kvp.Name) + ",";
                SQL += FormatForSQL(kvp.Value) + ")";
                if (ExecuteSQLDirect(SQL) != 1)
                {
                    logger.Log(Severity.Error, "Failed to insert " + kvp.Name + " into metadata table for doc_id: " + DocID.ToString(), source);
                    result = false;
                }
            }
            return result;
        }

        private Boolean RetrieveDocumentProperties(int DocID, out List<BRProperty> Properties)
        {
            string source = "RetrieveDocumentProperties";
            OleDbDataReader kvp;

            Properties = new List<BRProperty>();

            kvp = ExecSQLForReader("SELECT property_name, property_value FROM dbo.DocumentMetaData WHERE doc_id = " + DocID.ToString());

            if (kvp == null)
            {
                DisposeReader(kvp);
                logger.Log(Severity.Error, "Failed to query document metadata properties for doc_id = " + DocID.ToString(), source);
                return false;
            }

            while (kvp.Read())
            {
                try
                {
                    BRProperty newprop = new BRProperty();
                    newprop.Name = kvp[0].ToString();
                    newprop.Value = kvp[1].ToString();
                    Properties.Add(newprop);
                }
                catch (Exception ex)
                {
                    logger.Log(Severity.Error, "Failed to parse metadata properties read for doc_id: " + DocID.ToString() + " - " + ex.Message, source);
                    DisposeReader(kvp);
                    return false;
                }        
            }

            DisposeReader(kvp);
            return true;
        }

        private Boolean DeleteDocumentProperties(int DocID)
        {
            if (ExecuteSQLForScalarInt("SELECT COUNT(*) FROM dbo.DocumentMetaData WHERE doc_id = " + DocID.ToString()) > 0)
                if (ExecuteSQLDirect("DELETE FROM dbo.DocumentMetaData WHERE doc_id = " + DocID.ToString()) > 0)
                    return true;
                else
                {
                    logger.Log(Severity.Error, "Failed to delete metadata for doc_id: " + DocID.ToString(), "DeleteDocumentProperties");
                    return false;
                }
            else
                return true;
        }

        private Boolean DeleteDocumentRecord(int DocID)
        {
            if (ExecuteSQLForScalarInt("SELECT COUNT(*) FROM dbo.Document WHERE doc_id = " + DocID.ToString()) > 0)
                if (ExecuteSQLDirect("DELETE FROM dbo.Document WHERE doc_id = " + DocID.ToString()) > 0)
                    return true;
                else
                {
                    logger.Log(Severity.Error, "Failed to delete document record for doc_id: " + DocID.ToString(), "DeleteDocumentRecord");
                    return false;
                }
            else
                return true;
        }

        public Boolean LogToDB(Severity EventSeverity, string Message, string Source, string Account = "", string ClientDetails = "", string SessionID = "")
        {
            //This function does its own DB access, because exception handling for logging needs to happen in the logging module
            Boolean result = false;
            using (OleDbConnection oconn = new OleDbConnection())
            {
                oconn.ConnectionString = config.GetValue("ConnectionString");
                oconn.Open();
                if (oconn.State == System.Data.ConnectionState.Open)
                {
                    OleDbCommand odc = new OleDbCommand();
                    odc.CommandText = "INSERT dbo.EventLog (occurred, severity_id, message_text, event_source, account, app_details, session_id) VALUES (";
                    odc.CommandText += "'" + DateTimeOffset.UtcNow.ToString("s") + "',";
                    odc.CommandText += ((int)EventSeverity).ToString() + ",";
                    odc.CommandText += FormatForSQL(Message) + ",";
                    odc.CommandText += FormatForSQL(Source) + ",";
                    odc.CommandText += FormatForSQL(Account) + ",";
                    if (ClientDetails != "")
                        odc.CommandText += FormatForSQL(ClientDetails) + ",";
                    else
                        odc.CommandText += "NULL,";
                    if (SessionID != "")
                        odc.CommandText += FormatForSQL(SessionID) + ")";
                    else
                        odc.CommandText += "NULL)";
                    odc.Connection = oconn;
                    result = (odc.ExecuteNonQuery() > 0);
                    if (oconn.State == System.Data.ConnectionState.Open)
                        oconn.Close();    
                }
            }
            return result;       
        }

        private int AzureStoreBLOB(Byte[] BLOB, string BLOBUUID, AzureTier Tier)
        {
            string source = "AzureStoreBLOB";
            int retries;
            int retrydelay;

            int currenttry = 0;
            Boolean success = false;

            GetRetries("Azure", out retries, out retrydelay);

            int storeid = AzureSelectStore(BLOB.Length, Tier);
            if (storeid < 1)
            {
                //Try to create a new store/container
                //If that fails...
                logger.Log(Severity.Error, "No suitable store could be found or created for the request (size=" + BLOB.LongLength.ToString() + ", Tier=" + Enum.GetName(typeof(AzureTier), Tier), source);
                return -1;
            }

            CloudBlockBlob blobblock = AzureGetBLOBReference(storeid, BLOBUUID);
            if (blobblock != null)
                while (!success && currenttry <= retries)
                {
                    try
                    {
                        logger.Log(Severity.Timing, "Starting upload to Azure storeid: " + storeid.ToString() + ", for BLOB UUID: " + BLOBUUID + " for " + BLOB.Length.ToString() + " bytes.", source);
                        blobblock.UploadFromByteArray(BLOB, 0, BLOB.Length);
                        logger.Log(Severity.Timing, "Finished upload to Azure storeid: " + storeid.ToString() + ", for BLOB UUID: " + BLOBUUID + " for " + BLOB.Length.ToString() + " bytes.", source);
                        success = true;
                    }
                    catch (Exception ex)
                    {
                        logger.Log(Severity.Error, "Failed to upload BLOB data to Azure storeid: " + storeid.ToString() + " - " + ex.Message, source);
                        if (currenttry++ < retries)
                        {
                            logger.Log(Severity.Exception, "Retrying Azure BLOB copy after " + retrydelay.ToString() + " seconds.", source);
                            System.Threading.Thread.Sleep(1000 * retrydelay);
                        }
                        else
                            storeid = -1;
                    }
                }
            else
                storeid = -1;

            if (storeid > 0)
            {
                //Update store capacity info
                string SQL = "UPDATE dbo.BLOBStore SET space_used = space_used + ";
                SQL += BLOB.Length.ToString();
                SQL += " WHERE store_id = " + storeid.ToString();
                if (ExecuteSQLDirect(SQL) != 1)
                    logger.Log(Severity.Error, "Failed to update space_used for store_id: " + storeid.ToString(), source);
            }
            return storeid;
        }

        private int AzureCopyBLOB(string SourceBLOBUUID, ref string DestBLOBUUID, AzureTier Tier, int PreferredStore = 0)
        {
            //This will need to call out to select the store, possibly create a storage account, 
            //and insert/update the BLOB
            //In the case where the current location of the BLOB is the best location for the BLOB
            //the function will return the current store_id, and will set DestBLOBUUID to SourceBLOBUUID.
            //PreferredStore can be used to force the function to copy the BLOB to a particular store.

            string source = "AzureCopyBLOB";
            long blobsize = -1;
            int sourcestoreid = -1;
            int deststoreid = -1;
            int retries;
            int retrydelay;

            int currenttry = 0;
            Boolean success = false;

            GetRetries("Azure", out retries, out retrydelay);

            if (!GetBLOBDetails(SourceBLOBUUID, out sourcestoreid, out blobsize))
            {
                logger.Log(Severity.Error, "Failed to execute query to retrieve source BLOB details for blob_uuid: " + SourceBLOBUUID, source);
                return -1;
            }

            CloudBlockBlob sourceblobblock = AzureGetBLOBReference(sourcestoreid, SourceBLOBUUID);
            if (sourceblobblock == null)
            {
                logger.Log(Severity.Error, "Failed to obtain reference for source BLOB: " + SourceBLOBUUID, source);
                return -1;
            }

            if (PreferredStore > 0)
                deststoreid = PreferredStore;
            else
                deststoreid = AzureSelectStore((int)blobsize, Tier);

            if (deststoreid < 1)
            {
                //Try to create a new store/container
                //If that fails...
                logger.Log(Severity.Error, "No suitable store could be found or created for the request (size=" + blobsize.ToString() + ", Tier=" + Enum.GetName(typeof(AzureTier), Tier), source);
                return -1;
            }

            if (deststoreid == sourcestoreid)
            {
                logger.Log(Severity.Warning, "Current BLOB store is optimal location for the BLOB.", source);
                DestBLOBUUID = SourceBLOBUUID;
                return sourcestoreid;
            }

            CloudBlockBlob destblobblock = AzureGetBLOBReference(deststoreid, DestBLOBUUID);
            if (destblobblock != null)
                while (!success && currenttry <= retries)
                {
                    try
                    {
                        var sas = sourceblobblock.Container.GetSharedAccessSignature(new SharedAccessBlobPolicy()
                        {
                            SharedAccessStartTime = DateTime.UtcNow.AddMinutes(-5),
                            SharedAccessExpiryTime = DateTime.UtcNow.AddMinutes(5),
                            Permissions = SharedAccessBlobPermissions.Read,
                        });
                        //Create a SAS URI for the blob
                        string srcBlockBlobSasUri = string.Format("{0}{1}", sourceblobblock.Uri, sas);
                        logger.Log(Severity.Timing, "Starting Azure copy from storeid: " + sourcestoreid.ToString() + ", for BLOB UUID: " + SourceBLOBUUID + " for " + blobsize.ToString() + " bytes to destination store_id: " + deststoreid.ToString() + " and destination BLOB UUID: " + DestBLOBUUID, source);
                        //destblobblock.StartCopy(sourceblobblock);
                        //Need to use SAS, since we're copying between accounts...
                        destblobblock.StartCopy(new Uri(srcBlockBlobSasUri));
                        logger.Log(Severity.Timing, "Finished Azure copy from storeid: " + sourcestoreid.ToString() + ", for BLOB UUID: " + SourceBLOBUUID + " for " + blobsize.ToString() + " bytes to destination store_id: " + deststoreid.ToString() + " and destination BLOB UUID: " + DestBLOBUUID, source);
                        success = true;
                    }
                    catch (Exception ex)
                    {
                        logger.Log(Severity.Error, "Failed to copy BLOB data from storeid: " + sourcestoreid.ToString() + ", for BLOB UUID: " + SourceBLOBUUID + " for " + blobsize.ToString() + " bytes to destination store_id: " + deststoreid.ToString() + " and destination BLOB UUID: " + DestBLOBUUID + " - " + ex.Message, source);
                        logger.Log(Severity.Timing, "Finished Azure copy from storeid: " + sourcestoreid.ToString() + ", for BLOB UUID: " + SourceBLOBUUID + " for " + blobsize.ToString() + " bytes to destination store_id: " + deststoreid.ToString() + " and destination BLOB UUID: " + DestBLOBUUID, source);
                        if (currenttry++ < retries)
                        {
                            logger.Log(Severity.Exception, "Retrying Azure BLOB copy after " + retrydelay.ToString() + " seconds.", source);
                            System.Threading.Thread.Sleep(1000 * retrydelay);
                        }
                        else
                            deststoreid = -1;
                    }
                }
            else
                deststoreid = -1;

            if (deststoreid > 0)
            {
                //Update store capacity info
                string SQL = "UPDATE dbo.BLOBStore SET space_used = space_used + ";
                SQL += blobsize.ToString();
                SQL += " WHERE store_id = " + deststoreid.ToString();
                if (ExecuteSQLDirect(SQL) != 1)
                    logger.Log(Severity.Error, "Failed to update space_used for store_id: " + deststoreid.ToString(), source);
            }
            return deststoreid;
        }

        //This function checks for abandoned transactions and attempts to roll them back
        //Its back-off period is configurable (AbandonedTransactionTimeout). By default it 
        //will not attempt to cleanup any transactions whose last activity was withing the last
        //30 minutes
        public Boolean DoCleanup()
        {
            string source = "DoCleanup";
            Boolean result = true;
            int backoff = -30;

            if (config.GetValue("AbandonedTransactionTimeout") != null)
            {
                try
                {
                    backoff = System.Convert.ToInt32(config.GetValue("AbandonedTransactionTimeout"));
                }
                catch (Exception ex)
                {
                    logger.Log(Severity.Error, "Failed to retrieve or convert AbandonedTransactionTimeout setting value. - " + ex.Message, source);
                }
            }

            string baseSQL = " FROM dbo.TransactionState ";
            baseSQL += " WHERE last_status < '" + DateTimeOffset.Now.AddMinutes(backoff).ToString("s") + "'";

            string SQL = "SELECT COUNT(*) " + baseSQL;

            logger.Log(Severity.Info, "Starting check for abandoned transactions.", source);

            int xtoclear = ExecuteSQLForScalarInt(SQL);
            if (xtoclear < 1)
            {
                logger.Log(Severity.Info, "No transactions found that need to be cleared.", source);
                return true;
            }

            logger.Log(Severity.Timing, "Starting to process transaction cleanup for " + xtoclear.ToString() + " transactions.", source);

            //Use a datatable, so the reader DB connection will remain available for other queries needed 
            //for the BLOB copy operations
            SQL = "SELECT transaction_id " + baseSQL + " ORDER BY last_status ASC";
            DataTable trans = ExecSQLForTable(SQL);
            if (trans == null)
            {
                logger.Log(Severity.Error, "Failed to get list of transactions to clear.", source);
                logger.Log(Severity.Timing, "Ending processing of transaction cleanup for " + xtoclear.ToString() + " transactions.", source);
                return false;
            }

            int xid = -1;
            foreach (DataRow transaction in trans.Rows)
            {
                try
                {
                    xid = (int)transaction[0];
                }
                catch (Exception ex)
                {
                    logger.Log(Severity.Error, "Failed to parse details of document to archive. - " + ex.Message, source);
                    xid = -1;
                }
                if (xid > 0)
                {
                    result = result && RollbackTransaction(xid);
                }
            }

            //Depending on what we find in system testing and production
            //we might need to add additional housekeeping code here
            //Possibilities include things like querying Azure stores for orphaned BLOBs to delete,
            //and querying the Document table for superseded versions of documents that, for some reason,
            //weren't deleted at the time they were superseded.

            return result;
        }

        public Boolean DoArchive()
        {
            string source = "DoArchive";
            Boolean result = true;

            string baseSQL = " FROM dbo.Document d JOIN dbo.BLOBStore b ON d.store_id = b.store_id";
            baseSQL += " WHERE d.archive_after < '" + DateTimeOffset.Now.ToString("s") + "' AND b.tier = 'Hot'";

            string SQL = "SELECT COUNT(*) " + baseSQL;

            logger.Log(Severity.Info, "Starting check for documents to archive.", source);

            int docstoarchive = ExecuteSQLForScalarInt(SQL);
            if (docstoarchive < 1)
            {
                logger.Log(Severity.Info, "No documents found that need to be archived.", source);
                return true;
            }

            logger.Log(Severity.Timing, "Starting to process archiving for " + docstoarchive.ToString() + " documents.", source);

            //Use a datatable, so the reader DB connection will remain available for other queries needed 
            //for the BLOB copy operations
            SQL = "SELECT doc_id, blob_uuid " + baseSQL;
            DataTable docs = ExecSQLForTable(SQL);
            if (docs == null)
            {
                logger.Log(Severity.Error, "Failed to get list of documents to archive.", source);
                logger.Log(Severity.Timing, "Ending processing of archiving for " + docstoarchive.ToString() + " documents.", source);
                return false;
            }

            int docid = -1;
            string blobuuid = "";
            foreach (DataRow archivedoc in docs.Rows)
            {
                try
                {
                    docid = (int)archivedoc[0];
                    blobuuid = archivedoc[1].ToString();
                }
                catch (Exception ex)
                {
                    logger.Log(Severity.Error, "Failed to parse details of document to archive. - " + ex.Message, source);
                    docid = -1;
                }
                if (docid > 0)
                {
                    result = result && ArchiveDocument(docid, blobuuid, AzureTier.Cool);
                }
            }
            return result;
        }

        private Boolean ArchiveDocument(int DocID, string BLOBUUID, AzureTier Tier = AzureTier.Cool)
        {
            string source = "ArchiveDocument";
            string SQL = "";
            int newdocid = -1;

            int transactionid = StartTransaction(TransactionType.ArchiveDocument, TransactionStep.CopyBLOB);
            if (transactionid < 1)
            {
                logger.Log(Severity.Error, "Failed to get a transaction ID.", source);
                return false;
            }

            string destblobuuid = Guid.NewGuid().ToString();
            int deststoreid = AzureCopyBLOB(BLOBUUID, ref destblobuuid, Tier);
            if (deststoreid > 0 && (BLOBUUID == destblobuuid))
            {
                //BLOB was not copied... already at correct location; nothing else to do
                logger.Log(Severity.Exception, "Document was passed for archive, but BLOB was already at best store. BLOBGUID: " + BLOBUUID, source);
                CloseTransaction(transactionid);
                return true;
            }
            if (deststoreid < 1)
            {
                logger.Log(Severity.Error, "BLOB was not copied. Check other events in the log for additional details. BLOBGUID: " + BLOBUUID, source);
                CloseTransaction(transactionid);
                return false;
            }

            if (!UpdateTransaction(TransactionStep.InsertDocRecord, transactionid, 0, deststoreid, destblobuuid))
            {
                logger.Log(Severity.Error, "Failed to update transaction.", source);
                return false;
            }

            SQL = "INSERT dbo.Document (uuid, created, modified, archive_after, store_id, blob_uuid, blob_size, blob_md5) ";
            SQL += "SELECT uuid, created, modified, archive_after, ";
            SQL += deststoreid.ToString() + ", '" + destblobuuid + "', ";
            SQL += "blob_size, blob_md5 ";
            SQL += "FROM dbo.Document WHERE doc_id = " + DocID.ToString();     
            if (ExecuteSQLDirect(SQL) != 1)
            {
                logger.Log(Severity.Error, "Failed to insert document data in DB for new copy of document doc_id: " + DocID.ToString(), source);
                if (!RollbackTransaction(transactionid))
                    logger.Log(Severity.Error, "Failed to rollback transaction.", source);
                return false;
            }
            else
            {
                //This is not as concurrency foolproof as SCOPE_IDENTITY, but it is more portable and 
                //is still quite safe. 
                SQL = "SELECT MAX(doc_id) FROM dbo.Document WHERE blob_uuid = '" + destblobuuid + "'";
                SQL += " AND uuid = (SELECT uuid FROM dbo.Document WHERE doc_id = " + DocID.ToString() + ")";
                SQL += " AND store_id = " + deststoreid.ToString();
                newdocid = ExecuteSQLForScalarInt(SQL);
                if (newdocid < 1)
                {
                    logger.Log(Severity.Error, "Failed to obtain doc_id of newly added Document table row for copy of doc_id: " + DocID.ToString(), source);
                    RollbackTransaction(transactionid);
                    return false;
                }
            }

            if (!UpdateTransaction(TransactionStep.InsertDocProperties, transactionid, newdocid, deststoreid, destblobuuid))
            {
                logger.Log(Severity.Error, "Failed to update transaction.", source);
                return false;
            }


            //Insert document properties list
            SQL = "SELECT COUNT(*) FROM dbo.DocumentMetaData WHERE doc_id = " + DocID.ToString();
            if (ExecuteSQLForScalarInt(SQL) > 0)
            {
                SQL = "INSERT dbo.DocumentMetaData (doc_id, property_name, property_value) ";
                SQL += "SELECT " + newdocid.ToString() + ", property_name, property_value ";
                SQL += "FROM dbo.DocumentMetaData WHERE doc_id = " + DocID.ToString();
                if (ExecuteSQLDirect(SQL) < 1)
                {
                    logger.Log(Severity.Error, "Failed to copy document properties.", source);
                    RollbackTransaction(transactionid);
                    return false;
                }
            }

            if (!CloseTransaction(transactionid))
            {
                logger.Log(Severity.Error, "Failed to close transaction after successful archive of doc_id: " + DocID.ToString(), source);
                return false;
            }

            if (!DeleteDocument(DocID: DocID))
            {
                logger.Log(Severity.Error, "Successfully archived doc_id: " + DocID.ToString() + " to doc_id: " + newdocid.ToString() + ", but failed to delete old version.", source);
                return false;
            }

            return true;
        }

        private Boolean GetBLOBDetails(string BLOBUUID, out int StoreID, out long Length)
        {
            string source = "GetBLOBDetails";
            StoreID = 0;
            Length = -1;

            OleDbDataReader blobdetails = ExecSQLForReader("SELECT store_id, blob_size FROM dbo.Document WHERE blob_uuid = '" + BLOBUUID + "'");
            if (blobdetails == null)
            {
                logger.Log(Severity.Error, "Failed to execute query to retrieve BLOB details for blob_uuid: " + BLOBUUID, source);
                DisposeReader(blobdetails);
                return false;
            }
            if (!blobdetails.HasRows)
            {
                logger.Log(Severity.Error, "No BLOB details found for for blob_uuid: " + BLOBUUID, source);
                DisposeReader(blobdetails);
                return false;
            }

            try
            {
                blobdetails.Read();
                StoreID = blobdetails.GetInt32(0);
                Length = blobdetails.GetInt64(1);
                DisposeReader(blobdetails);
            }
            catch (Exception ex)
            {
                logger.Log(Severity.Error, "Failed to parse BLOB details. - " + ex.Message, source);
                DisposeReader(blobdetails);
                return false;
            }
            return true;
        }

        private Boolean AzureRetrieveBLOB(string BLOBUUID, out Byte[] BLOB)
        {
            string source = "AzureRetrieveBLOB";
            BLOB = null;
            long blobsize = -1;
            int storeid = 0;
            int retries;
            int retrydelay;

            int currenttry = 0;
            Boolean success = false;

            GetRetries("Azure", out retries, out retrydelay);

            if (!GetBLOBDetails(BLOBUUID, out storeid, out blobsize))
            {
                logger.Log(Severity.Error, "Failed to execute query to retrieve BLOB details for blob_uuid: " + BLOBUUID, source);
                return false;
            }

            CloudBlockBlob blobblock = AzureGetBLOBReference(storeid, BLOBUUID);
            if (blobblock != null)
                while (!success)
                {
                    try
                    {
                        //Better to use local BLOB size information, or BLOB size information from Azure?
                        //Initial testing shows they match. Added a warning so we can see if there's a concern here.

                        //Also note that CloudBlockBlob Byte Array methods seem to be limited to 4GB (32-bit, rather than 64-bit, length arguments)
                        //Probably because memory source/destination for more the 4GB would be problematic.

                        blobblock.FetchAttributes();
                        long length = blobblock.Properties.Length;
                        BLOB = new byte[length];

                        if (blobsize != length)
                            logger.Log(Severity.Warning, "Azure BLOB length property does not match local DB length property.", source);

                        logger.Log(Severity.Timing, "Starting download from Azure storeid: " + storeid.ToString() + ", for BLOB UUID: " + BLOBUUID + " for " + length.ToString() + " bytes.", source);
                        blobblock.DownloadToByteArray(BLOB, 0);
                        logger.Log(Severity.Timing, "Finished download from Azure storeid: " + storeid.ToString() + ", for BLOB UUID: " + BLOBUUID + " for " + length.ToString() + " bytes.", source);
                        success = true;
                    }
                    catch (Exception ex)
                    {
                        logger.Log(Severity.Error, "Failed to download BLOB data from Azure storeid: " + storeid.ToString() + ", for BLOB UUID: " + BLOBUUID + " - " + ex.Message, source);
                        if (currenttry++ < retries)
                        {
                            logger.Log(Severity.Exception, "Retrying Azure BLOB delete after " + retrydelay.ToString() + " seconds.", source);
                            System.Threading.Thread.Sleep(1000 * retrydelay);
                        }
                        else
                            return false;
                    }
                }
            else
                return false;
            return true;
        }

        private CloudBlockBlob AzureGetBLOBReference(int StoreID, string BLOBUUID, string ContainerName = "")
        {
            string source = "AzureGetBLOBReference";
            CloudBlockBlob blobblock = null;
            string accountname;
            string accountkey;
            string containername;
            int retries;
            int retrydelay;

            int currenttry = 0;
            Boolean success = false;

            GetRetries("DB", out retries, out retrydelay);

            if (AzureGetStoreAccount(StoreID, out accountname, out accountkey))
            {
                while (!success && currenttry <= retries)
                {
                    try
                    {
                        CloudStorageAccount azureaccount;

                        //Could also do this with 
                        //StorageCredentials credentials = new StorageCredentials(accountName, accountKey);
                        //CloudStorageAccount storageAccount = new CloudStorageAccount(credentials, true);
                        //Need to see which one offers more flexibility...
                        string csconnectionstring;
                        if (config.GetValue("AzureConnectionString") != null)
                            csconnectionstring = config.GetValue("AzureConnectionString");
                        else
                            csconnectionstring = "DefaultEndpointsProtocol=https";
                        csconnectionstring += ";AccountName=" + accountname;
                        csconnectionstring += ";AccountKey=" + accountkey;

                        if (ContainerName != "")
                            containername = ContainerName;
                        else
                        {
                            if (config.GetValue("AzureDefaultContainerName") != null)
                                containername = config.GetValue("AzureDefaultContainerName").ToLower();
                            else
                                containername = "defaultcontainer";
                        }

                        azureaccount = CloudStorageAccount.Parse(csconnectionstring);

                        CloudBlobClient blobstore = azureaccount.CreateCloudBlobClient();
                        CloudBlobContainer blobcontainer = blobstore.GetContainerReference(containername);
                        blobcontainer.CreateIfNotExists();
                        blobblock = blobcontainer.GetBlockBlobReference(BLOBUUID);
                        success = true;
                    }
                    catch (Exception ex)
                    {
                        logger.Log(Severity.Error, "Failed to get reference for Azure storeid: " + StoreID.ToString() + " and BLOB UUID: " + BLOBUUID + " - " + ex.Message, source);
                        if (currenttry++ < retries)
                        {
                            logger.Log(Severity.Exception, "Retrying Azure BLOB connection after " + retrydelay.ToString() + " seconds.", source);
                            System.Threading.Thread.Sleep(1000 * retrydelay);
                        }
                    }
                }
            }
            if (blobblock == null)
                logger.Log(Severity.Error, "Failed to get reference for Azure storeid: " + StoreID.ToString() + " and BLOB UUID: " + BLOBUUID, source);
            return blobblock; 
        }

        public Boolean AzureGetStoreAccount(int StoreID, out string AccountName, out string AccountKey)
        {
            string source = "AzureGetStoreAccount";
            AccountKey = "";
            AccountName = "";

            OleDbDataReader storedetails;
            storedetails = ExecSQLForReader("SELECT account_name, account_key FROM dbo.BLOBStore where store_id = " + StoreID.ToString());

            if (storedetails != null)
                if (storedetails.HasRows)
                    try
                    {
                        storedetails.Read();
                        AccountName = storedetails[0].ToString();
                        AccountKey = storedetails[1].ToString();
                    }
                    catch (Exception ex)
                    {
                        logger.Log(Severity.Error, "Failed to retrieve details for store_id: " + StoreID.ToString() + " - " + ex.Message, source);
                        DisposeReader(storedetails);
                        return false;
                    }
                else
                {
                    logger.Log(Severity.Error, "No details found for store_id: " + StoreID.ToString(), source);
                    DisposeReader(storedetails);
                    return false;
                }
            else
            {
                logger.Log(Severity.Error, "Failed to execute query to retrieve store details.", source);
                DisposeReader(storedetails);
                return false;
            }

            DisposeReader(storedetails);
            return true;
        }

        private Boolean AzureDeleteBLOB(string BLOBUUID)
        {
            string source = "AzureDeleteBLOB";
            long length = -1;
            int storeid = -1;
            int retries;
            int retrydelay;

            int currenttry = 0;
            Boolean success = false;

            GetRetries("Azure", out retries, out retrydelay);

            if (!GetBLOBDetails(BLOBUUID, out storeid, out length))
            {
                logger.Log(Severity.Error, "Failed to execute query to retrieve source BLOB details for blob_uuid: " + BLOBUUID, source);
                return false;
            }

            CloudBlockBlob blobblock = AzureGetBLOBReference(storeid, BLOBUUID);
            if (blobblock != null)
                while (!success)
                {
                    try
                    {
                        {
                            blobblock.FetchAttributes();
                            length = blobblock.Properties.Length;
                            blobblock.Delete(DeleteSnapshotsOption.IncludeSnapshots);
                            success = true;
                        }
                    }
                    catch (Exception ex)
                    {
                        logger.Log(Severity.Error, "Failed to delete BLOB data from Azure storeid: " + storeid.ToString() + ", for BLOB UUID: " + BLOBUUID + " - " + ex.Message, source);
                        if (currenttry++ < retries)
                        {
                            logger.Log(Severity.Exception, "Retrying Azure BLOB delete after " + retrydelay.ToString() + " seconds.", source);
                            System.Threading.Thread.Sleep(1000 * retrydelay);
                        }
                        else
                            return false;
                    }
                }
            else
                return false;

            //Update store capacity info
            if (length > -1)
            {
                string SQL = "UPDATE dbo.BLOBStore SET space_used = space_used - ";
                SQL += length.ToString();
                SQL += " WHERE store_id = " + storeid.ToString();
                if (ExecuteSQLDirect(SQL) != 1)
                    logger.Log(Severity.Error, "Failed to update space_used for store_id: " + storeid.ToString(), source);
            }
             
            return true;
        }

        private int StartTransaction(TransactionType TransactionType = TransactionType.AddDocument, TransactionStep NextStep = TransactionStep.StoreBLOB)
        {
            int result = 0;
            string SQL;
            string source = "StartTransaction";
            string sessionguid;

            if (config.GetValue("SessionGuid") == null)
                config.SetMemValue("SessionGuid", Guid.NewGuid().ToString(), "Process");
            sessionguid = config.GetValue("SessionGuid");
            SQL = "INSERT dbo.TransactionState (first_status, transaction_type, next_step, session_guid) VALUES ('";
            SQL += DateTimeOffset.UtcNow.ToString("s") + "', ";
            SQL += FormatForSQL(TransactionType.ToString()) + ", ";
            SQL += FormatForSQL(NextStep.ToString()) + ", '";
            SQL += sessionguid + "')";
            result = ExecuteSQLDirect(SQL);
            if (result != 1)
            {
                logger.Log(Severity.Error, "Failed to insert start of transaction record.", source);
                return 0;
            }
            SQL = "SELECT MAX(transaction_id) FROM dbo.TransactionState WHERE session_guid = '" + sessionguid + "'";
            result = ExecuteSQLForScalarInt(SQL);
            return result;
        }

        private Boolean UpdateTransaction(TransactionStep NextStep, int TransactionID, int DocID = 0, int StoreID = 0, string BLOBUUID = "")
        {
            int qresult;
            string SQL;
            string source = "UpdateTransaction";

            SQL = "UPDATE dbo.TransactionState SET ";
            SQL += "last_status = '" + DateTimeOffset.UtcNow.ToString("s") + "', ";
            if (DocID > 0)
                SQL += "doc_id = " + DocID.ToString() + ", ";
            if (StoreID > 0)
                SQL += "store_id = " + StoreID.ToString() + ", ";
            if (BLOBUUID != "")
                SQL += "blob_uuid = '" + BLOBUUID + "', ";
            SQL += "next_step = " + FormatForSQL(NextStep.ToString());
            SQL += " WHERE transaction_id = " + TransactionID.ToString();
            qresult = ExecuteSQLDirect(SQL);
            if (qresult != 1)
            {
                logger.Log(Severity.Error, "Failed to update transaction record for transaction_id: " + TransactionID.ToString(), source);
                return false;
            }
            return true;
        }

        private Boolean CloseTransaction(int TransactionID)
        {
            int qresult;
            string SQL;
            string source = "CloseTransaction";

            SQL = "DELETE dbo.TransactionState WHERE transaction_id = " + TransactionID.ToString();
            qresult = ExecuteSQLDirect(SQL);
            if (qresult != 1)
            {
                logger.Log(Severity.Error, "Failed to delete transaction record for transaction_id: " + TransactionID.ToString(), source);
                return false;
            }
            return true;
        }

        private Boolean TouchTransaction(int TransactionID)
        {
            int qresult;
            string SQL;
            string source = "TouchTransaction";

            SQL = "UPDATE dbo.TransactionState SET ";
            SQL += "last_status = '" + DateTimeOffset.UtcNow.ToString("s") + "'";
            SQL += " WHERE transaction_id = " + TransactionID.ToString();
            qresult = ExecuteSQLDirect(SQL);
            if (qresult != 1)
            {
                logger.Log(Severity.Error, "Failed to update transaction record for transaction_id: " + TransactionID.ToString(), source);
                return false;
            }
            return true;
        }

        static string MD5Hash(byte[] BLOB)
        {
            MD5 hasher = MD5.Create();

            // Convert the input string to a byte array and compute the hash.
            byte[] data = hasher.ComputeHash(BLOB);

            // Create a new Stringbuilder to collect the bytes
            // and create a string.
            StringBuilder sBuilder = new StringBuilder();

            // Loop through each byte of the hashed data 
            // and format each one as a hexadecimal string.
            for (int i = 0; i < data.Length; i++)
            {
                sBuilder.Append(data[i].ToString("x2"));
            }

            // Return the hexadecimal string.
            return sBuilder.ToString();
        }

        private enum TransactionType
        {
            AddDocument,
            UpdateDocument,
            DeleteDocument,
            ArchiveDocument
        }

        private enum TransactionStep
        {
            InsertDocRecord,
            InsertDocProperties,
            StoreBLOB,
            DeleteBLOB,
            DeleteDocProperties,
            DeleteDocRecord,
            CopyBLOB,
            UpdateDocRecord
        }

        //A bit of a mis-named function, because it doesn't necessarily roll things back.
        //It will instead attempt to restore state at the document level. 
        //This means undoing insert/update that were completed, and retrying delete 
        //actions that were not completed.
        private Boolean RollbackTransaction(int TransactionID)
        {
            string source = "RollbackTransaction";
            Boolean success = false;
            int docid = 0;

            if (ExecuteSQLForScalarInt("SELECT COUNT(*) FROM dbo.TransactionState WHERE transaction_id = " + TransactionID.ToString()) == 0)
            {
                logger.Log(Severity.Warning, "Transaction ID: " + TransactionID.ToString() + " rollback initiated, but not found in TransactionState table.", source);
                return false;
            }

            string strxtype = ExecuteSQLForScalarString("SELECT transaction_type FROM dbo.TransactionState WHERE transaction_id = " + TransactionID.ToString());
            TransactionType xtype = (TransactionType)(Enum.Parse(typeof(TransactionType), strxtype));
            if (xtype != TransactionType.AddDocument && xtype != TransactionType.UpdateDocument && xtype != TransactionType.DeleteDocument && xtype != TransactionType.ArchiveDocument)
            { 
                logger.Log(Severity.Exception, "Unhandled transaction type submitted for rollback", source);
                    return false;
            }

            logger.Log(Severity.Info, "Attempting to rollback transaction_id: " + TransactionID.ToString(), source);

            //Update the last activity for the transaction, so that it will at least go to the end
            //of the processing list if cleanup fails early... not sure what to do if Touch fails...
            if (!TouchTransaction(TransactionID))
                return false;

            string strnextstep = ExecuteSQLForScalarString("SELECT next_step FROM dbo.TransactionState WHERE transaction_id = " + TransactionID.ToString());
            TransactionStep nextstep = (TransactionStep)(Enum.Parse(typeof(TransactionStep), strnextstep));
            switch (nextstep)
            {
                case TransactionStep.InsertDocRecord:
                case TransactionStep.DeleteBLOB:
                    //Delete BLOB
                    string blobuuid = "";
                    int storeid = 0;
                    OleDbDataReader blobdetails = ExecSQLForReader("SELECT store_id, blob_uuid FROM TransactionState WHERE transaction_id  = " + TransactionID.ToString());
                    if (blobdetails == null)
                    {
                        logger.Log(Severity.Error, "Failed to read details from DB for transaction_id: " + TransactionID.ToString(), source);
                        return false;
                    }
                    else
                    {
                        try
                        {
                            blobdetails.Read();
                            storeid = blobdetails.GetInt32(0);
                            blobuuid = blobdetails[1].ToString();
                        }
                        catch (Exception ex)
                        {
                            logger.Log(Severity.Exception, "Attempting rollback of transaction, but failed to retrieve details - " + ex.Message, source);
                            DisposeReader(blobdetails);
                            return false;
                        }
                        DisposeReader(blobdetails);
                    }
                    if (storeid > 0)
                    AzureDeleteBLOB(blobuuid);
                    break;
                case TransactionStep.DeleteDocProperties:
                    //Delete doc properties
                    docid = ExecuteSQLForScalarInt("SELECT doc_id FROM dbo.TransactionState WHERE transaction_id = " + TransactionID.ToString());
                    if (docid < 1)
                    {
                        logger.Log(Severity.Error, "Failed to retrieve doc_id for transaction_id: " + TransactionID.ToString(), source);
                    }
                    success = DeleteDocumentProperties(docid);
                    if (!success)
                        break;
                    goto case TransactionStep.StoreBLOB;
                case TransactionStep.DeleteDocRecord:
                case TransactionStep.InsertDocProperties:
                    //Delete doc record
                    docid = ExecuteSQLForScalarInt("SELECT doc_id FROM dbo.TransactionState WHERE transaction_id = " + TransactionID.ToString());
                    if (docid < 1)
                    {
                        logger.Log(Severity.Error, "Failed to retrieve doc_id for transaction_id: " + TransactionID.ToString(), source);
                    }
                    success = DeleteDocumentRecord(docid);
                    if (!success)
                        break;
                    goto case TransactionStep.StoreBLOB;
                case TransactionStep.CopyBLOB:
                case TransactionStep.StoreBLOB:
                    success = CloseTransaction(TransactionID);
                    break;
                default:
                    logger.Log(Severity.Exception, "Unhandled next_step encountered while rolling back transaction_id: " + TransactionID.ToString(), source);
                    break;
            }

            if (!success)
            {
                logger.Log(Severity.Error, "Failed to rollback transaction_id: " + TransactionID.ToString(), source);
            }
            return success;
        }

        public Boolean UpsertDocument(string UUID, DateTimeOffset ArchiveDate, Byte[] BLOB, List<BRProperty> MetaData, int PreferredStore=0)
        {
            string source = "UpsertDocument";
            string SQL = "";
            int olddocid = 0;
            string oldblobuuid = "";
            int oldstoreid = 0;
            string oldblobmd5 = "";
            int docid = 0;
            int storeid;
            string blobuuid = Guid.NewGuid().ToString();
            string blobmd5;
            DateTimeOffset oldarchivedate = DateTimeOffset.UtcNow;
            DateTimeOffset created = DateTimeOffset.UtcNow;
            Boolean update = false;
            Boolean updateblob = true;
            int transactionid;

            logger.Log(Severity.Audit, "Upsert called for document with UUID: " + UUID, source);

            if (ExecuteSQLForScalarInt("SELECT COUNT(*) FROM dbo.Document WHERE uuid = '" + UUID + "'") > 0)
            {
                OleDbDataReader existing = ExecSQLForReader("SELECT doc_id, created, archive_after, store_id, blob_uuid, blob_md5 FROM dbo.Document WHERE uuid = '" + UUID + "'");
                if (existing == null)
                {
                    logger.Log(Severity.Error, "Failed to read details from DB for existing document: " + UUID, source);
                    return false;
                }
                else
                    try
                    {
                        existing.Read();
                        olddocid = existing.GetInt32(0);
                        created = existing.GetDateTime(1);
                        oldarchivedate = existing.GetDateTime(2);
                        oldstoreid = existing.GetInt32(3);
                        oldblobuuid = existing[4].ToString();
                        oldblobmd5 = existing[5].ToString();
                        update = true;
                    }
                    catch (Exception ex)
                    {
                        logger.Log(Severity.Exception, "Attempting upsert of document, but failed to retrieve details of previous version - " + ex.Message, source);
                        DisposeReader(existing);
                        return false;
                    }
                DisposeReader(existing);
            }

            //Start the transaction
            if (update)
                transactionid = StartTransaction(TransactionType.UpdateDocument, TransactionStep.StoreBLOB);
            else
                transactionid = StartTransaction(TransactionType.AddDocument, TransactionStep.StoreBLOB);

            if (transactionid < 1)
            {
                logger.Log(Severity.Error, "Failed to get a transaction ID.", source);
                return false;
            }

            blobmd5 = MD5Hash(BLOB);

            if (update)
            {
                if (PreferredStore < 1)
                    PreferredStore = oldstoreid;
                //reevaluate BLOB storage if the BLOB has changed, 
                //or a PreferredStore is provided (may be a future expansion item), 
                //or the archive date has changed 
                //to a value that would be incompatible with the object's current location
                updateblob = ((blobmd5 != oldblobmd5) || oldstoreid != PreferredStore 
                    || ((oldarchivedate != ArchiveDate) 
                    && (((oldarchivedate < DateTimeOffset.UtcNow) && (ArchiveDate > DateTimeOffset.UtcNow)) 
                    || ((ArchiveDate < DateTimeOffset.UtcNow) && (oldarchivedate > DateTimeOffset.UtcNow)))));
            }

            //Store the new BLOB data
            if (updateblob)
            {
                //For now, tier is only Hot or Cool, and based on whether the document's ArchiveDate is in the past
                AzureTier tier;
                if (ArchiveDate <= DateTimeOffset.UtcNow)
                    tier = AzureTier.Cool;
                else
                    tier = AzureTier.Hot;

                storeid = AzureStoreBLOB(BLOB, blobuuid, tier);
                if (storeid < 1)
                {
                    //Exception handling for BLOB storage happens in and under the StoreBLOB call
                    CloseTransaction(transactionid);
                    return false;
                }
                if (!UpdateTransaction(TransactionStep.InsertDocRecord, transactionid, 0, storeid, blobuuid))
                {
                    logger.Log(Severity.Error, "Failed to update transaction.", source);
                    return false;
                }
            }
            else
                {
                storeid = oldstoreid;
                blobuuid = oldblobuuid;

            if (!UpdateTransaction(TransactionStep.InsertDocRecord, transactionid, 0, 0, ""))
                {
                    logger.Log(Severity.Error, "Failed to update transaction.", source);
                    return false;
                }

            }

            DateTimeOffset modified = DateTimeOffset.UtcNow;

            //Note that UUID and ArchiveDate are validated or generated
            //in the service module when parsing the JSON
            SQL = "INSERT dbo.Document (uuid, created, modified, archive_after, store_id, blob_uuid, blob_size, blob_md5) VALUES (";
            SQL += "'" + UUID + "', ";
            SQL += "'" + created.ToString("s") + "', ";
            if (update)
                SQL += "'" + modified.ToString("s") + "',";
            else
                SQL += "NULL, ";
            SQL += "'" + ArchiveDate.ToString("s") + "',";
            SQL += storeid.ToString() + ",";
            SQL += "'" + blobuuid + "', ";
            SQL += BLOB.Length.ToString() + ", ";
            SQL += "'" + blobmd5 + "')";
            if (ExecuteSQLDirect(SQL) != 1)
            {
                logger.Log(Severity.Error, "Failed to insert document data in DB for document UUID: " + UUID, source);
                if (!RollbackTransaction(transactionid))
                    logger.Log(Severity.Error, "Failed to rollback transaction.", source);
                return false;
            }
            else
            {
                //This is not as concurrency foolproof as SCOPE_IDENTITY, but it is more portable and 
                //is still quite safe. 
                SQL = "SELECT doc_id FROM dbo.Document WHERE blob_uuid = '" + blobuuid + "'"
                    + " AND uuid = '" + UUID + "'";
                if (update)
                    SQL += " AND modified = '" + modified.ToString("s") + "'";
                else
                    SQL += " AND modified IS NULL";
                docid = ExecuteSQLForScalarInt(SQL);
                if (docid < 1)
                {
                    logger.Log(Severity.Error, "Failed to obtain doc_id of newly added Document table row for document UUID: " + UUID, source);
                    RollbackTransaction(transactionid);
                    return false;
                }
            }

            if (!UpdateTransaction(TransactionStep.InsertDocProperties, transactionid, docid, storeid, blobuuid))
            {
                logger.Log(Severity.Error, "Failed to update transaction.", source);
                return false;
            }


            //Insert document properties list
            if (!StoreDocumentProperties(MetaData, docid))
            {
                logger.Log(Severity.Error, "Failed to store document properties.", source);
                RollbackTransaction(transactionid);
                return false;
            }

            //If this is an update, do cleanup of old versions
            if (update)
            {
                if (!CloseTransaction(transactionid))
                {
                    logger.Log(Severity.Error, "Failed to close otherwise successful update transaction.", source);
                    RollbackTransaction(transactionid);
                    return false;
                }

                if (!DeleteDocument(UUID, olddocid))
                {
                    logger.Log(Severity.Error, "Failed to delete old version of document.", source);
                    return false;
                }
            }
            else
                if (!CloseTransaction(transactionid))
                {
                    logger.Log(Severity.Error, "Failed to close otherwise successful insert transaction.", source);
                    RollbackTransaction(transactionid);
                    return false;
                }
            return true;
        }

        //This function is used both for deleting a document through the API, and for deleting the old
        //version of a document that has been updated. For the second case, the old DocID must be 
        //provided to distinguish which version of data to delete. A corner case to consider would be
        //if we somehow leave multiple document records around for a single UUID... in this case, 
        //the "user" would be unable to delete the document through the API.
        public Boolean DeleteDocument(string UUID = "", int DocID = 0)
        {
            Boolean success = false;
            string source = "DeleteDocument";
            Boolean deleteblob;
            string blobuuid;
            int storeid;

            if (UUID == "" && DocID == 0)
            {
                logger.Log(Severity.Error, "Either UUID or DocID must be provided when calling DeleteDocument.", source);
                return false;
            }

            if (UUID == "")
                UUID = ExecuteSQLForScalarString("SELECT uuid FROM dbo.Document WHERE doc_id = " + DocID.ToString());

            logger.Log(Severity.Audit, "Delete requested for document: " + UUID, source);

            int records = ExecuteSQLForScalarInt("SELECT COUNT(*) FROM dbo.Document WHERE uuid = '" + UUID + "'");
            if (records == 0)
            {
                logger.Log(Severity.Warning, "No matching record found for delete of UUID: " + UUID, source);
                return false;
            }

            //Sanity check
            if (DocID == 0 && records > 1)
            {
                logger.Log(Severity.Warning, "Multiple matching records found for delete of UUID: " + UUID + ", and DocID was not specified.", source);
                return false;
            }

            //To be safe, we'll grab the doc_id of the document we plan to delete now, using MIN, just in
            //case a newer version is added concurrently with the delete operation
            if (DocID == 0)
                DocID = ExecuteSQLForScalarInt("SELECT MIN(doc_id) FROM dbo.Document WHERE uuid = '" + UUID + "'");

            int cleanupxid = StartTransaction(TransactionType.DeleteDocument, TransactionStep.DeleteBLOB);
            if (cleanupxid < 1)
            {
                logger.Log(Severity.Error, "Failed to get a transaction for the delete request.", source);
                return false;
            }

            //Get BLOB details
            OleDbDataReader blobdetails = ExecSQLForReader("SELECT store_id, blob_uuid FROM dbo.Document WHERE doc_id = " + DocID.ToString());
            if (blobdetails == null)
            {
                logger.Log(Severity.Error, "Failed to execute query to retrieve BLOB details for doc_id: " + DocID.ToString(), source);
                DisposeReader(blobdetails);
                return false;
            }

            if (!blobdetails.HasRows)
            {
                logger.Log(Severity.Error, "Failed to find BLOB details for doc_id: " + DocID.ToString(), source);
                DisposeReader(blobdetails);
                return false;
            }

            if (blobdetails.Read())
            {
                try
                {
                    storeid = blobdetails.GetInt32(0);
                    blobuuid = blobdetails[1].ToString();
                }
                catch (Exception ex)
                {
                    logger.Log(Severity.Exception, "Failed to parse BLOB details for doc_id: " + DocID.ToString() + " - " + ex.Message, source);
                    DisposeReader(blobdetails);
                    return false;
                }
            }
            else
            {
                logger.Log(Severity.Error, "Failed to retrieve BLOB details for doc_id: " + DocID.ToString(), source);
                DisposeReader(blobdetails);
                return false;
            }
            DisposeReader(blobdetails);

            //Only delete the blob if there is exactly one document record using it
            deleteblob = (ExecuteSQLForScalarInt("SELECT COUNT(*) FROM dbo.Document WHERE blob_uuid = '" + blobuuid + "'") == 1);

            if (deleteblob)
            {
                UpdateTransaction(TransactionStep.DeleteBLOB, cleanupxid, DocID, storeid, blobuuid);
                success = AzureDeleteBLOB(blobuuid);
                UpdateTransaction(TransactionStep.DeleteDocProperties, cleanupxid);
            }
            else
            {
                UpdateTransaction(TransactionStep.DeleteDocProperties, cleanupxid);
                success = true;
            }

            success = (success && DeleteDocumentProperties(DocID));
            UpdateTransaction(TransactionStep.DeleteDocRecord, cleanupxid);
            success = (success && DeleteDocumentRecord(DocID));
            if (success)
                CloseTransaction(cleanupxid);

            return success;
        }

        public int AzureSelectStore(int Size, AzureTier Tier)
        {
            int result = -1;
            string source = "AzureSelectStore";

            string SQL = "SELECT store_id FROM dbo.BLOBStore ";
            SQL += "WHERE(capacity - space_used) > " + Size.ToString();
            SQL += " AND tier = '" + Enum.GetName(typeof(AzureTier), Tier) + "'";
            SQL += " ORDER BY COST ASC, (capacity - space_used) DESC";

            OleDbDataReader stores;
            stores = ExecSQLForReader(SQL);
            if (stores != null)
            {
                try
                {
                    if (stores.HasRows)
                        if (stores.Read())
                        {
                            result = stores.GetInt32(0);
                        }
                }
                catch (Exception ex)
                {
                    logger.Log(Severity.Error, "Failed to retrieve the selected store - " + ex.Message, source);
                }
            }
            else
                logger.Log(Severity.Error, "Failed to execute the query to select a store.", source);
            DisposeReader(stores);
            return result;
        }

        public Boolean RetrieveDocument(string UUID, out DateTimeOffset ArchiveDate, out Byte[] BLOB, out List<BRProperty> MetaData, Boolean OmitBLOB = false)
        {
            string source = "RetrieveDocument";
            int docid = 0;
            int storeid;
            long blobsize = 0;
            string blobuuid = Guid.NewGuid().ToString();
            string SQL;

            ArchiveDate = DateTimeOffset.Now;
            BLOB = null;
            MetaData = null;

            logger.Log(Severity.Audit, "Retrieve called for document with UUID: " + UUID, source);

            SQL = "SELECT COUNT(*) FROM dbo.Document d WHERE uuid = '" + UUID + "'";
            SQL += " AND NOT EXISTS (SELECT * FROM dbo.TransactionState ts WHERE ts.doc_id = d.doc_id)";

            if (ExecuteSQLForScalarInt(SQL) > 0)
            {
                //Get the newest version that doesn't have an open transaction
                SQL = "SELECT doc_id, archive_after, store_id, blob_uuid, blob_size FROM dbo.Document d WHERE uuid = '" + UUID + "'";
                SQL += " AND NOT EXISTS (SELECT * FROM dbo.TransactionState ts WHERE ts.doc_id = d.doc_id)";
                SQL += " ORDER BY doc_id DESC"; 
                OleDbDataReader existing = ExecSQLForReader(SQL);
                if (existing == null)
                {
                    logger.Log(Severity.Error, "Failed to execute query to retrieve details from DB for existing document: " + UUID, source);
                    DisposeReader(existing);
                    return false;
                }
                if (!existing.HasRows)
                {
                    logger.Log(Severity.Error, "Failed to read details from DB for existing document: " + UUID, source);
                    DisposeReader(existing);
                    return false;
                }
                else
                    try
                    {
                        existing.Read();
                        docid = existing.GetInt32(0);
                        ArchiveDate = existing.GetDateTime(1);
                        storeid = existing.GetInt32(2);
                        blobuuid = existing[3].ToString();
                        blobsize = existing.GetInt64(4);
                    }
                    catch (Exception ex)
                    {
                        logger.Log(Severity.Exception, "Attempting retrieval of document, but failed to retrieve details - " + ex.Message, source);
                        DisposeReader(existing);
                        return false;
                    }
                DisposeReader(existing);
            }
            else
            {
                SQL = "SELECT COUNT(*) FROM dbo.Document d JOIN dbo.TransactionState ts";
                SQL += " ON ts.doc_id = d.doc_id WHERE d.uuid = '" + UUID + "'";
                int transactions = ExecuteSQLForScalarInt(SQL);

                if (transactions > 0)
                    logger.Log(Severity.Warning, "No unlocked data found in Document table for UUID: " + UUID + ". There are " + transactions.ToString() + " open transactions for this document.", source);
                else
                    logger.Log(Severity.Warning, "No data found in Document table for UUID: " + UUID, source);
                return false;
            }

            if (!RetrieveDocumentProperties(docid, out MetaData))
            {
                logger.Log(Severity.Error, "Failed to retrieve MetaData data for UUID: " + UUID, source);
                return false;
            }

            if (!OmitBLOB)
                if (!AzureRetrieveBLOB(blobuuid, out BLOB))
                {
                    logger.Log(Severity.Error, "Failed to retrieve BLOB data for UUID: " + UUID, source);
                    return false;
                }

            return true;
        }

        private void DisposeReader(OleDbDataReader Reader)
        {
            if (Reader == null)
                return;

            if (!Reader.IsClosed)
                try
                {
                    Reader.Close();
                }
                catch (Exception ex)
                {
                    logger.Log(Severity.Error, "Failed to close reader - " + ex.Message, "DisposeReader");
                }
            if (oconn != null)
            {
                if (oconn.State == System.Data.ConnectionState.Open)
                    try
                    {
                        oconn.Close();
                    }
                    catch (Exception ex)
                    {
                        logger.Log(Severity.Error, "Failed to close connection - " + ex.Message, "DisposeReader");
                    }
                try
                {
                    oconn.Dispose();
                    oconn = null;
                }
                catch (Exception ex)
                {
                    logger.Log(Severity.Error, "Failed to dispose of connection - " + ex.Message, "DisposeReader");
                }
            }
        }

        public Boolean LoadDBSettings(string SettingName = "")
        {
            OleDbDataReader settings = null;
            string source = "LoadDBSettings";
            string SQL = "SELECT setting_name, setting_value FROM dbo.ConfigurationSetting";
            if (SettingName != "")
                SQL += " WHERE setting_name = '" + SettingName + "'";
            try
            {
                settings = ExecSQLForReader(SQL);
                if (settings != null)
                {
                    while (settings.Read())
                    {
                        config.SetMemValue(settings[0].ToString(), settings[1].ToString(), "DB");
                    }

                }
            }
            catch (Exception ex)
            {
                logger.Log(Severity.Error, "Failed to read DB settings - " + ex.Message, source);
                DisposeReader(settings);
                return false;
            }
            DisposeReader(settings);
            return true;
        }

        public Boolean UpsertDBSetting(string SettingName, string SettingValue)
        {
            string source = "UpsertDBSetting";
            string SQL = "";

            logger.Log(Severity.Audit, "Upsert requested for DB setting: " + SettingName + ", new value: " + SettingValue, source);

            if (ExecuteSQLForScalarInt("SELECT COUNT(*) FROM dbo.ConfigurationSetting WHERE setting_name = '") == 0)
            {   //new setting
                SQL = "INSERT dbo.ConfigurationSetting (setting_name, setting_value) VALUES (";
                SQL += FormatForSQL(SettingName) + ", ";
                SQL += FormatForSQL(SettingValue) + ")";
            }
            else
            {
                SQL = "UPDATE dbo.ConfigurationSetting SET setting_value = ";
                SQL += FormatForSQL(SettingValue);
                SQL += " WHERE setting_name = " + FormatForSQL(SettingName);
            }

            if (ExecuteSQLDirect(SQL) < 1)
            {
                logger.Log(Severity.Error, "Failed to upsert DB setting: " + SettingName + ", new value: " + SettingValue, source);
                return false;
            }
            else
                logger.Log(Severity.Info, "Successfully upserted DB setting: " + SettingName + ", new value: " + SettingValue, source);

            return true;
        }

        public Boolean DeleteDBSetting(string SettingName)
        {
            string source = "DeleteDBSetting";
            string SQL = "";

            logger.Log(Severity.Audit, "Delete requested for DB setting: " + SettingName, source);

            if (ExecuteSQLForScalarInt("SELECT COUNT(*) FROM dbo.ConfigurationSetting WHERE setting_name = " + FormatForSQL(SettingName)) == 0)
            {
                logger.Log(Severity.Warning, "No matching setting found in ConfigurationSetting for requested delete: " + SettingName, source);
                return false;
            }

            SQL = "DELETE dbo.ConfigurationSetting";
            SQL += " WHERE setting_name = " + FormatForSQL(SettingName);

            if (ExecuteSQLDirect(SQL) < 1)
            {
                logger.Log(Severity.Error, "Failed to delete DB setting: " + SettingName, source);
                return false;
            }
            else
                logger.Log(Severity.Info, "Successfully deleted DB setting: " + SettingName, source);

            return true;
        }

        public string ReadDBLog(DateTimeOffset? Start = null, DateTimeOffset? End = null)
        {
            StringBuilder result = new StringBuilder();
            if (Start == null)
                Start = DateTimeOffset.Parse("1/1/1980");
            if (End == null)
                End = DateTimeOffset.UtcNow;
            OleDbDataReader events = null;
            string source = "ReadDBLog";
            string SQL = "SELECT occurred, severity_id, message_text, event_source, account, app_details FROM dbo.EventLog";
            SQL += " WHERE occurred >= '" + ((DateTimeOffset)Start).ToString("s") + "'";
            SQL += " AND occurred <= '" + ((DateTimeOffset)End).ToString("s") + "'";
            try
            {
                events = ExecSQLForReader(SQL);
                if (events != null)
                {
                    while (events.Read())
                    {
                        result.Append(events.GetDateTime(0).ToString("s") + "\t");
                        result.Append(((Severity)events.GetInt32(1)).ToString() + "\t");
                        result.Append(events[2].ToString() + "\t");
                        for (int i = 3; i<6; i++)
                        {
                            if (events.IsDBNull(i))
                                result.Append("\t");
                            else
                                result.Append(events[i].ToString() + "\t");
                        }
                        result.Append(Environment.NewLine);
                    }
                }
            }
            catch (Exception ex)
            {
                logger.Log(Severity.Error, "Failed to read DB events - " + ex.Message, source);
                DisposeReader(events);
                return null;
            }
            DisposeReader(events);
            return result.ToString();
        }

        public Boolean ClearDBLog(DateTimeOffset? Before = null)
        {
            Boolean result = false;
            string message;
            if (Before == null)
            {
                Before = DateTimeOffset.UtcNow;
                message = "Event log cleared.";
            }
            else
                message = "Event log purged of events up to " + ((DateTimeOffset)Before).ToString("s") + " UTC.";
            string source = "ClearDBLog";
            string SQL = "DELETE FROM dbo.EventLog";
            SQL += " WHERE occurred <= '" + ((DateTimeOffset)Before).ToString("s") + "'";
            if (ExecuteSQLDirect(SQL) > -1)
            {
                logger.Log(Severity.Audit, "Event log cleared.", source);
                result = true;
            }
            else
                logger.Log(Severity.Error, "Failed to clear DB log.", source);
            return result;
        }
    }
}
