using System;
using System.Data;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using BRDataAccess;
using IBRLogging;
using IBRConfig;
using BRCommon;

namespace BLOBDocument
{

    public class BRDocumentListEntry
    {
        private string uuid;

        public string UUID
        {
            get { return uuid; }
            set { uuid = value; }
        }

        private DateTimeOffset created;

        public string Created
        {
            get { return created.ToString("s"); }
            set
            {
                try
                {
                    created = DateTimeOffset.Parse(value);
                }
                catch
                { }
            }
        }

        private DateTimeOffset modified;

        public string Modified
        {
            get { return modified.ToString("s"); }
            set
            {
                try
                {
                    modified = DateTimeOffset.Parse(value);
                }
                catch
                { }
            }
        }

        private DateTimeOffset archivedate;

        public string ArchiveDate
        {
            get { return archivedate.ToString("s"); }
            set
            {
                try
                {
                    archivedate = DateTimeOffset.Parse(value);
                }
                catch
                { }
            }
        }

        private string tier;

        public string Tier
        {
            get { return tier; }
            set { tier = value; }
        }

        private string sku;

        public string SKU
        {
            get { return sku; }
            set { sku = value; }
        }

        private string storeaccount;

        public string StoreAccount
        {
            get { return storeaccount; }
            set { storeaccount = value; }
        }

        private string blobuuid;

        public string BlobUUID
        {
            get { return blobuuid; }
            set { blobuuid = value; }
        }
        
        private long size;

        public long Size
        {
            get { return size; }
            set { size = value; }
        }

        private string md5;

        public string MD5
        {
            get { return md5; }
            set { md5 = value; }
        }

    }

    public class BRDocumentList
    {
        private ILogging logger;

        [JsonIgnoreAttribute]
        public ILogging BRLogging
        {
            get { return logger; }
            set { logger = value; }
        }

        private IConfiguration config;

        [JsonIgnoreAttribute]
        public IConfiguration BRConfig
        {
            get { return config; }
            set { config = value; }
        }

        private List<BRDocumentListEntry> documents;

        public List<BRDocumentListEntry> Documents
        {
            get { return documents; }
            set { documents = value; }
        }


        public BRDocumentList(IConfiguration BRConfig, ILogging BRLogging)
        {
            config = BRConfig;
            logger = BRLogging;
            documents = new List<BRDocumentListEntry>();
        }

        public Boolean Populate(string StoreAccount = "")
        {
            string source = "BRDocumentList.Populate";

            BRDAL dal = new BRDAL(config, logger);
            DataTable documents = dal.RetrieveDocumentList(StoreAccount);
            if (documents == null)
            {
                if (StoreAccount == "")
                    logger.Log(Severity.Warning, "No document list information found.", source);
                else
                    logger.Log(Severity.Warning, "No document list information found for Account Names matching: " + StoreAccount, source);
                return false;
            }

            try
            {
                foreach (DataRow row in documents.Rows)
                {
                    BRDocumentListEntry doc = new BRDocumentListEntry();
                    doc.StoreAccount = row["account_name"].ToString();
                    doc.UUID = row["uuid"].ToString();
                    doc.MD5 = row["blob_md5"].ToString();
                    doc.Created = row["created"].ToString();
                    doc.Modified = row["modified"].ToString();
                    doc.SKU = row["sku"].ToString();
                    doc.Tier = row["tier"].ToString();
                    doc.Size = (long)row["blob_size"];
                    doc.BlobUUID = row["blob_uuid"].ToString();
                    doc.ArchiveDate = row["archive_after"].ToString();
                    this.documents.Add(doc);
                }
            }
            catch (Exception ex)
            {
                logger.Log(Severity.Error, "Failed to parse document list information. - " + ex.Message, source);
                return false;
            }
            return true;
        }

    }

    public class BRDocument
    {
        private byte[] blob;

        public byte[] BLOB
        {
            get { return blob; }
            set { blob = value; }
        }

        //Used by JSON.NET. Don't include BLOB property if BLOB is null.
        public Boolean ShouldSerializeBLOB()
        {
            return (blob != null);
        }

        private string uuid;

        public string UUID
        {
            get { return uuid; }
            set { uuid = value; }
        }

        private DateTimeOffset archivedate;

        public string ArchiveDate
        {
            get { return archivedate.ToString("s"); }
            set
            {
                try
                {
                    archivedate = DateTimeOffset.Parse(value);
                }
                catch
                { }
            }
        }

        private List<BRProperty> metadata;

        public List<BRProperty> MetaData
        {
            get { return metadata; }
            set { metadata = value; }
        }

        private ILogging logger;

        [JsonIgnoreAttribute]
        public ILogging BRLogging
        {
            get { return logger; }
            set { logger = value; }
        }

        private IConfiguration config;

        [JsonIgnoreAttribute]
        public IConfiguration BRConfig
        {
            get { return config; }
            set { config = value; }
        }

        public BRDocument(IConfiguration BRConfig, ILogging BRLogging)
        {
            config = BRConfig;
            logger = BRLogging;
            metadata = new List<BRProperty>();
        }

        public Boolean Store()
        {
            //Upsert this
            Guid docguid;
            if (!Guid.TryParse(this.UUID, out docguid))
            {
                docguid = Guid.NewGuid();
                this.UUID = docguid.ToString();
            }
            BRDAL dal = new BRDAL(config, logger);
            return (dal.UpsertDocument(uuid.ToString(), archivedate, blob, metadata));
        }

        public Boolean Retrieve(string UUID, Boolean OmitBLOB = false)
        {
            BRDAL dal = new BRDAL(config, logger);
            if (dal.RetrieveDocument(UUID, out this.archivedate, out this.blob, out this.metadata, OmitBLOB))
            {
                this.uuid = UUID;
                return true;
            }
            return false;
        }

        public Boolean Delete(string UUID)
        {
            BRDAL dal = new BRDAL(config, logger);
            if (dal.DeleteDocument(UUID))
            {
                return true;
            }
            return false;
        }
    }

    public class BRSerializer
    {
        static public BRDocument Deserialize(string JSON, IConfiguration BRConfig, ILogging BRLogging)
        {
            var jssettings = new JsonSerializerSettings();
            jssettings.DateParseHandling = DateParseHandling.None;

            BRDocument loaddoc = Newtonsoft.Json.JsonConvert.DeserializeObject<BRDocument>(JSON, jssettings);
            loaddoc.BRConfig = BRConfig;
            loaddoc.BRLogging = BRLogging;    
            return loaddoc;
        }

        static public string Serialize(BRDocument Document)
        {
            string json = Newtonsoft.Json.JsonConvert.SerializeObject(Document);
            return json;
        }

        static public string Serialize(BRDocumentList DocumentList)
        {
            string json = Newtonsoft.Json.JsonConvert.SerializeObject(DocumentList);
            return json;
        }

    }

}
