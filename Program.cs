using LinqToDB;
using LinqToDB.Data;
using LinqToDB.Mapping;
using System.Linq.Expressions;

namespace l2db_bug
{
    class Db : DataConnection
    {
        public Db(string path) :
            base("SQLite", $@"Data Source = {path}; foreign keys = true; Version = 3;")
        { }
    }

class Client
{
    public int Id { get; set; }
    public string Name { get; set; }
}

class Contract
{
    public int Id { get; set; }
    public int IdClient { get; set; }
}

class Bill
{
    public int Id { get; set; }
    public int? IdContract { get; set; }
    public int? IdClient { get; set; }
    public decimal Sum { get; set; }
}

interface IBill
{
    int Id { get; set; }
    int? IdContract { get; set; }
    int? IdClient { get; set; }
    decimal Sum { get; set; }
}

interface IBillWithClient : IBill
{
    public Client Client { get; set; }
}

class BillWithClient : Bill
{
    public Client Client { get; set; }
}

class LegacyBill : Bill, IBillWithClient
{
    [Association(ThisKey = nameof(IdClient), OtherKey = nameof(Client.Id), CanBeNull = false)]
    public Client Client { get; set; }
}

class ModernBill : Bill, IBillWithClient
{
    [Association(ThisKey = nameof(IdContract), OtherKey = nameof(Contract.Id), CanBeNull = false)]
    public Contract Contract { get; set; }

    [Association(ExpressionPredicate = nameof(ModernBill_Client_Expr), CanBeNull = false)]
    public Client Client { get; set; }

    static Expression<Func<ModernBill, Client, bool>> ModernBill_Client_Expr
        => (b, cl) => b.Contract.IdClient == cl.Id;
}

    class Program
    {
        static void Main(string[] args)
        {
            string dbPath = "db.db";

            using (File.Create(dbPath)) { }

            using (var db = new Db(dbPath))
            {
                db.CreateTable<Client>();
                db.CreateTable<Contract>();
                db.CreateTable<Bill>();

                IQueryable<IBillWithClient> q_union =
                    db
                        .GetTable<LegacyBill>().TableName(nameof(Bill))
                        .Where(b => b.IdClient != null)
                    .UnionAll<IBillWithClient>(
                    db
                        //.GetTable<LegacyBill>().TableName(nameof(Bill))
                        .GetTable<ModernBill>().TableName(nameof(Bill))
                        .Where(b => b.IdContract != null));

                // var q_union = 
                //     db
                //         .GetTable<LegacyBill>().TableName(nameof(Bill))
                //         .Where(b => b.IdClient != null)
                //         .Select(b => new BillWithClient { Id = b.Id, IdClient = b.IdClient, IdContract = b.IdContract, Sum = b.Sum, Client = b.Client })
                //     .UnionAll<BillWithClient>(
                //     db
                //         .GetTable<ModernBill>().TableName(nameof(Bill))
                //         .Where(b => b.IdContract != null)
                //         .Select(b => new BillWithClient { Id = b.Id, IdClient = b.IdClient, IdContract = b.IdContract, Sum = b.Sum, Client = b.Client }));

                var q_result = q_union.Select(b => b.Client.Name);
                Console.WriteLine(q_result.ToString());

                var q_result2 = q_union.Select(b => b.Sum);
                Console.WriteLine(q_result2.ToString());
            }
        }
    }
}
