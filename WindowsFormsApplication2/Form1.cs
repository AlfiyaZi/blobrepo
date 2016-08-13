using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
using System.IO;
using BRLogging;
using BRConfig;
using IBRLogging;
using IBRConfig;
using System.Data.OleDb;
using IBLOBDocument;
using BLOBDocument;
using BRCommon;
using BRDataAccess;
//using System.Configuration;

namespace WindowsFormsApplication2
{
    public partial class Form1 : Form
    {
        public Form1()
        {
            InitializeComponent();
        }

        private void button2_Click(object sender, EventArgs e)
        {
            textBox2.Text = "";
            textBox2.Text = File.ReadAllText(Path.Combine(Directory.GetCurrentDirectory(), "blobstore.log"));
        }

        private void button1_Click(object sender, EventArgs e)
        {
            Configuration config = new Configuration();
            textBox1.Text = config.GetValueTable();
        }

        private void button3_Click(object sender, EventArgs e)
        {
            if (txtName.Text != "" && txtValue.Text != "")
            {
                Configuration config = new Configuration();
                config.SetValue(txtName.Text, txtValue.Text);        
            }

        }

        private void label3_Click(object sender, EventArgs e)
        {

        }

        private void button4_Click(object sender, EventArgs e)
        {
            Configuration settings = new Configuration();
            settings.SetMemValue("ConnectionString", "Provider=sqlncli11;Server=.;Database=BLOBRepo;Trusted_Connection=yes;", "TestApp");
            Logging logger;
            logger = new Logging(settings);
            logger.Log(Severity.Error, txtMessage.Text, "BLOBStore Test Module");

        }

        private void textBox3_TextChanged(object sender, EventArgs e)
        {

        }

        private void button5_Click(object sender, EventArgs e)
        {
            textBox2.Text = "";
            Configuration settings = new Configuration();
            Logging logger = new Logging(settings);
            textBox2.Text = logger.ReadLog();
        }

        private void Form1_Load(object sender, EventArgs e)
        {

        }

        private void button6_Click(object sender, EventArgs e)
        {
            Configuration settings = new Configuration();
            settings.SetMemValue("ConnectionString", "Provider=sqlncli11;Server=.;Database=BLOBRepo;Trusted_Connection=yes;", "TestApp");
            Logging logger = new Logging(settings);
            logger.ClearLog();            
        }

        private void button7_Click(object sender, EventArgs e)
        {
            string result = "";
            var appSettings = System.Configuration.ConfigurationManager.AppSettings;

            if (appSettings.Count == 0)
            {
                result = "AppSettings is empty";
            }
            else
            {
                foreach (var key in appSettings.AllKeys)
                {
                    result += key.ToString() + " : " +  appSettings[key].ToString() + Environment.NewLine;
                }
            }
        System.Windows.Forms.MessageBox.Show(result);
        }

        private void button8_Click(object sender, EventArgs e)
        {
            string result = "";

            OleDbConnection oconn = new OleDbConnection();
            oconn.ConnectionString = "Provider=sqlncli11;Server=.;Database=BLOBRepo;Trusted_Connection=yes;";
            oconn.Open();
            OleDbDataReader odr;
            OleDbCommand odc = new OleDbCommand();
            odc.CommandText = "SELECT * FROM ConfigurationSetting";
            odc.CommandType = CommandType.Text;
            odc.Connection = oconn;
            odr = odc.ExecuteReader();
            while (odr.Read())
            {
                result += odr[0].ToString() + " = " + odr[1].ToString() + Environment.NewLine;
            }
            textBox1.Text = result;
        }

        private void button9_Click(object sender, EventArgs e)
        {
            //BRSerializer serializer = new BRSerializer();
            Configuration settings = new Configuration();
            Logging logger = new Logging(settings);
            BRDocument newdoc = new BRDocument(settings, logger);
            newdoc.ArchiveDate = DateTimeOffset.UtcNow.AddYears(1).ToString("s");
            newdoc.UUID = Guid.NewGuid().ToString();
            //newdoc.BLOB = File.ReadAllBytes("C:\\DSC02630-Edit.jpg");
            newdoc.BLOB = File.ReadAllBytes("C:\\install.exe");
            BRProperty prop1 = new BRProperty();
            prop1.Name = "AccountID";
            prop1.Value = "1234567890";
            newdoc.MetaData.Add(prop1);
            BRProperty prop2 = new BRProperty();
            prop2.Name = "Custodian";
            prop2.Value = "Luigi";
            newdoc.MetaData.Add(prop2);
            System.Windows.Forms.Clipboard.SetText(BRSerializer.Serialize(newdoc));
        }

        /*
        private BRDocument Deserialize(string json)
        {
            BRDocument newdoc;
            try
            {
                newdoc = BRSerializer.Deserialize(json);
            }
            catch (Exception ex)
            {
                System.Windows.Forms.MessageBox.Show("Failed to deserialize BLOB document from JSON - " + ex.Message);
                return null;
            }
            if (newdoc == null)
            {
                System.Windows.Forms.MessageBox.Show("Failed to deserialize BLOB document from JSON");
            }
            if (newdoc != null)
            {
                DateTimeOffset testdate;
                if (!DateTimeOffset.TryParse(newdoc.ArchiveDate, out testdate))
                {
                    System.Windows.Forms.MessageBox.Show("Invalid format for ArchiveDate in submitted JSON.");
                    newdoc = null;
                }
            }
            return newdoc;
        }
        */

        private void button10_Click(object sender, EventArgs e)
        {
            Configuration config = new Configuration();
            Logging logger = new Logging(config);
            string json = System.Windows.Forms.Clipboard.GetText();
            BRDocument newdoc = BRSerializer.Deserialize(json, config, logger);
            if (newdoc != null)
                if (!newdoc.Store())
                    System.Windows.Forms.MessageBox.Show("Store() failed"); 
        }

        private void button11_Click(object sender, EventArgs e)
        {
            string uuid = "";
            Guid docuuid;

            Configuration Settings = new Configuration();
            Logging Logger = new Logging(Settings);

            uuid = txtUUID.Text;
            if (!Guid.TryParse(uuid, out docuuid))
            {
                string message = "Malformed UUID provided.";
                Logger.Log(Severity.Error, message, "Document.Put");
                System.Windows.Forms.MessageBox.Show(message);
                return;
            }

            BRDocument newdoc = new BRDocument(Settings, Logger);
            if (!newdoc.Retrieve(uuid))
            {
                System.Windows.Forms.MessageBox.Show("Unable to retrieve data for requested document: " + uuid);
                return;
            }

            string docjson = BRSerializer.Serialize(newdoc);
            System.Windows.Forms.Clipboard.SetText(docjson);
            return;
        }

        private void button12_Click(object sender, EventArgs e)
        {
/*
            AzureStorageAcccountBody account = new AzureStorageAcccountBody();
            Configuration settings = new Configuration();
            account.kind = Kinds.BlobStorage;
            account.location = settings.GetValue("AzureGEO");
            account.properties.encryption.services.blob.enabled = (settings.GetValue("AzureDefaultEncryption") == "1");
            account.sku = (SKUs)(Enum.Parse(typeof(SKUs), settings.GetValue("AzureDefaultSKU")));
            string json = BRAzure.BRAzureStorage.Serializer.Serialize(account);
            System.Windows.Forms.Clipboard.SetText(json);
            */
    }

        private void CmdTestSQLForDataTable_Click(object sender, EventArgs e)
        {
            Configuration settings = new Configuration();
            Logging logger = new Logging(settings);
            BRDAL dal = new BRDAL(settings, logger);
            DataTable dttest = dal.ExecSQLForTable(txtSQL.Text);
            if (dttest != null)
            {
                string result = "";
                for (int i = 0; i < dttest.Columns.Count; i++)
                {
                    result += dttest.Columns[i].ColumnName + '\t';
                }
                result += '\n';
                foreach (DataRow row in dttest.Rows)
                {
                    for (int i = 0; i < dttest.Columns.Count; i++)
                        result += row[i].ToString() + '\t';
                    result += '\n';
                }
                System.Windows.Forms.Clipboard.SetText(result);
            }
            else
                System.Windows.Forms.MessageBox.Show("Query failed.");
        }

        private void button13_Click(object sender, EventArgs e)
        {
            Configuration settings = new Configuration();
            Logging logger = new Logging(settings);
            BRDocumentList doclist = new BRDocumentList(settings, logger);
            doclist.Populate();

        }

        private void button14_Click(object sender, EventArgs e)
        {
            Configuration settings = new Configuration();
            Logging logger = new Logging(settings);
            BRDAL dal = new BRDAL(settings, logger);
            System.Windows.Forms.MessageBox.Show(dal.DoArchive().ToString());
        }

        private void button15_Click(object sender, EventArgs e)
        {

        }
    }
}
