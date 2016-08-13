using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace IBLOBDocument
{

    public interface IBRProperty
    {
        string Name { get; set; }
        string Value { get; set; }
    }

    public interface IBRDocument
    {
        byte[] BLOB { get; set; }
        string UUID { get; set; }
        string ArchiveDate { get; set; }
        List<IBRProperty> MetaData { get; set; }
        Boolean Store();
    }

    public interface IBRSerializer
    {
        IBRDocument Deserialize(string JSON);
        string Serialize(IBRDocument Document);
    }

}
