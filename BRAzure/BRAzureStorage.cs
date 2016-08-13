using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json;
using Newtonsoft.Json.Converters;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;

namespace BRAzure
{
    public enum AccessTiers
    {
        Cool,
        Hot
    }

    public enum SKUs
    {
        Standard_LRS,
        Standard_ZRS,
        Standard_GRS,
        Standard_RAGRS,
        Premium_LRS
    }

    public enum Kinds
    {
        Storage,
        BlobStorage
    }

    public class Domain
    {
        private string p_name;

        public string name
        {
            get { return p_name; }
            set { p_name = value; }
        }

        private Boolean p_usesubdomainname;

        public Boolean useSubDomainName
        {
            get { return p_usesubdomainname; }
            set { p_usesubdomainname = value; }
        }

    }

    public class BLOB
    {
        private Boolean p_enabled;

        public Boolean enabled
        {
            get { return p_enabled; }
            set { p_enabled = value; }
        }

        public BLOB()
        {
            enabled = true;
        }

    }

    public class Services
    {
        private BLOB p_blob;

        public BLOB blob
        {
            get { return p_blob; }
            set { p_blob = value; }
        }

        public Services()
        {
            p_blob = new BLOB();
        }

    }

    public class Encryption
    {
        private Services p_services;

        public Services services
        {
            get { return p_services; }
            set { p_services = value; }
        }


        private string p_keysource;

        public string keySource
        {
            get { return p_keysource; }
            set { p_keysource = value; }
        }

        public Encryption()
        {
            p_services = new Services();
            p_keysource = "Microsoft.Storage";
        }

    }


    public class Properties
    {
        private Domain p_customdomain;

        public Domain customDomain
        {
            get { return p_customdomain; }
            set { p_customdomain = value; }
        }

        public Boolean ShouldSerializecustomDomain()
        {
            return (p_customdomain.name != null);
        }

        private Encryption p_encryption;

        public Encryption encryption
        {
            get { return p_encryption; }
            set { p_encryption = value; }
        }

        private AccessTiers p_accesstier;

        [JsonConverter(typeof(StringEnumConverter))]
        public AccessTiers accessTier
        {
            get { return p_accesstier; }
            set { p_accesstier = value; }
        }

        public Properties()
        {
            p_customdomain = new Domain();
            p_encryption = new Encryption();
            p_accesstier = AccessTiers.Hot;
        }
    }

    public class AzureStorageAcccountBody
    {
        private string p_location;

        public string location
        {
            get { return p_location; }
            set { p_location = value; }
        }

        private Dictionary<string, string> p_tags;

        public Dictionary<string, string> tags
        {
            get { return p_tags; }
            set { p_tags = value; }
        }

        public Boolean ShouldSerializetags()
        {
            return (p_tags.Count > 0);
        }

        private Properties p_properties;

        public Properties properties
        {
            get { return p_properties; }
            set { p_properties = value; }
        }

        private SKUs p_sku;

        [JsonConverter(typeof(StringEnumConverter))]
        public SKUs sku
        {
            get { return p_sku; }
            set { p_sku = value; }
        }

        private Kinds p_kind;

        [JsonConverter(typeof(StringEnumConverter))]
        public Kinds kind
        {
            get { return p_kind; }
            set { p_kind = value; }
        }

        public AzureStorageAcccountBody()
        {
            p_tags = new Dictionary<string, string>();
            p_properties = new Properties();
            kind = Kinds.BlobStorage;
            sku = SKUs.Standard_GRS;
        }
    }
    public class BRAzureStorage
    {
        static public class Serializer
        {
            static public AzureStorageAcccountBody Deserialize(string JSON)
            {
                var jssettings = new JsonSerializerSettings();
                jssettings.DateParseHandling = DateParseHandling.None;

                AzureStorageAcccountBody loadaccount = Newtonsoft.Json.JsonConvert.DeserializeObject<AzureStorageAcccountBody>(JSON, jssettings);
                return loadaccount;
            }

            static public string Serialize(AzureStorageAcccountBody Account)
            {
                string json = Newtonsoft.Json.JsonConvert.SerializeObject(Account);
                return json;
            }
        }

        public Microsoft.WindowsAzure.Storage.Auth.StorageCredentials

    }
}
