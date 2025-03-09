using Microsoft.Data.Sqlite;
using Npgsql;
using System.Net;
using System.Runtime.CompilerServices;

const string SQLITE_CONNECTION_STRING = "Data Source=YOUR_SQLITE_DB_PATH";
const string PGSQL_CONNECTION_STRING = "Host=localhost;Username=YOUR_USERNAME;Password=YOUR_PASSWORD;Database=YOUR_DB_NAME";

using var postgreSqlDataSource = NpgsqlDataSource.Create(PGSQL_CONNECTION_STRING);

var postTags = new List<KeyValuePair<int, string>>();
using (var cmd = postgreSqlDataSource.CreateCommand(@"
    select pt.""PostId"", t.""DisplayName""
    from public.""Tag"" as t
    join public.""PostTag"" as pt on t.""Id"" = pt.""TagId"""))
using (var reader = await cmd.ExecuteReaderAsync())
{
    while (await reader.ReadAsync())
    {
        var postId = reader.GetInt32(0);
        var tagName = reader.GetString(1);

        postTags.Add(new(postId, tagName));
    }
}

var postIdMap = new Dictionary<int, int>();

using (var cmd = postgreSqlDataSource.CreateCommand("SELECT * FROM public.\"Post\" order by \"PublishedOn\";"))
using (var reader = await cmd.ExecuteReaderAsync())
{
    while (await reader.ReadAsync())
    {
        var postId = reader.GetInt32(0);
        var title = reader.GetString(1);
        var excerpt = reader.GetString(2);
        var content = reader.GetString(3);
        var linkName = reader.GetString(4);
        var publishedOn = reader.GetDateTime(5);

        using var connection = new SqliteConnection(SQLITE_CONNECTION_STRING);
        connection.Open();

        var insertSlipCommand = connection.CreateCommand();
        insertSlipCommand.CommandText =
        @"
            INSERT INTO slips(content, createdAt, friendlyLinkName, status, title, excerpt)
            VALUES (@content, @publishedOn, @linkName, 1, @title, @excerpt)
        ";
        insertSlipCommand.Parameters.AddWithValue("@content", content);
        insertSlipCommand.Parameters.AddWithValue("@publishedOn", publishedOn.Ticks);
        insertSlipCommand.Parameters.AddWithValue("@linkName", linkName);
        insertSlipCommand.Parameters.AddWithValue("@title", title);
        insertSlipCommand.Parameters.AddWithValue("@excerpt", excerpt);

        insertSlipCommand.ExecuteNonQuery();

        var getSlipIdCommand = connection.CreateCommand();
        getSlipIdCommand.CommandText = "SELECT LAST_INSERT_ROWID()";

        using var slipIdReader = getSlipIdCommand.ExecuteReader();
        if (slipIdReader.Read())
        {
            var slipId = slipIdReader.GetInt32(0);
            postIdMap.Add(postId, slipId);
        }

        AddRedirect(connection, $"posts/{linkName}", $"p/{linkName}", HttpStatusCode.Moved);
    }
}

using (var connection = new SqliteConnection(SQLITE_CONNECTION_STRING))
{
    connection.Open();

    var insertTagCommand = connection.CreateCommand();
    var values = new List<string>();

    var parameterIndex = 0;
    foreach (var postTag in postTags)
    {
        values.Add($"(@slipId{parameterIndex}, @tag{parameterIndex})");

        insertTagCommand.Parameters.AddWithValue($"@slipId{parameterIndex}", postIdMap[postTag.Key]);
        insertTagCommand.Parameters.AddWithValue($"@tag{parameterIndex}", postTag.Value);

        parameterIndex++;
    }

    insertTagCommand.CommandText = @$"
        INSERT INTO sliptags(slipId, tag)
        VALUES {String.Join(",", values)}
    ";

    insertTagCommand.ExecuteNonQuery();
}

static void AddRedirect(SqliteConnection connection, string source, string target, HttpStatusCode statusCode)
{
    var insertRedirectCommand = connection.CreateCommand();
    insertRedirectCommand.CommandText =
    @"
            INSERT INTO RedirectRules(source, target, httpStatus)
            VALUES (@source, @target, @httpStatus)
        ";
    insertRedirectCommand.Parameters.AddWithValue("@source", source);
    insertRedirectCommand.Parameters.AddWithValue("@target", target);
    insertRedirectCommand.Parameters.AddWithValue("@httpStatus", (int)statusCode);

    insertRedirectCommand.ExecuteNonQuery();
}