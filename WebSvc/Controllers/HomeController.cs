using System.Data.Common;
using Microsoft.AspNetCore.Mvc;
using MySql.Data.MySqlClient;

namespace SphinxQueryCore.Net.Controllers
{
	public class HomeController : Controller
	{
		public async Task<IActionResult> Index(string host, int? port, string statement)
		{
			var model = await LoadModel(host, (uint)(port ?? 0), statement);

			return View(model);
		}

		private async Task<IndexModel> LoadModel(string host, uint port, string statement)
		{
			if (string.IsNullOrEmpty(host)) return new IndexModel();

			try
			{
				var connStrBuilder = new MySqlConnectionStringBuilder();
				connStrBuilder.Server = host;
				connStrBuilder.Port = port;
				connStrBuilder.CharacterSet = "utf8";
				connStrBuilder.Pooling = false;
				connStrBuilder.SslMode = MySqlSslMode.Disabled;

				using (var conn = new MySqlConnection(connStrBuilder.ToString()))
				{
					try
					{
						await conn.OpenAsync();
					}
					catch
					{
						// workaround http://sphinxsearch.com/bugs/view.php?id=2196
						// for version of Sphinx earlier than 2.2.9
						if (conn.State != System.Data.ConnectionState.Open)
						{
							throw;
						}
					}

					var m = new IndexModel
					{
						Indexes = await MakeQueryAsync("show tables", conn),
					};

					if (!string.IsNullOrEmpty(statement))
					{
						m.Rows = MakeQueryAsync(statement, conn).Result;
						m.Meta = MakeQueryAsync("show meta", conn).Result;
						m.Status = MakeQueryAsync("show status", conn).Result;
					}

					return m;
				}
			}
			catch (Exception x)
			{
				return new IndexModel { Exception = x };
			}
		}

		private async Task<QueryResult> MakeQueryAsync(string statement, MySqlConnection conn)
		{
			var cmd = new MySqlCommand(statement, conn);
			using (var r = await cmd.ExecuteReaderAsync())
			{
				return new QueryResult
				{
					ColumnNames = Enumerable.Range(0, r.FieldCount)
						.Select(i => r.GetName(i))
						.ToArray(),
					Values = ReadAllValues(r)
						.ToArray(),
				};
			}
		}

		private IEnumerable<object[]> ReadAllValues(DbDataReader r)
		{
			var row = new List<object>();

			while (r.Read())
			{
				row.Clear();

				for (int i = 0; i < r.FieldCount; i++)
				{
					row.Add(r.GetValue(i));
				}

				yield return row.ToArray();
			}
		}
	}
}
