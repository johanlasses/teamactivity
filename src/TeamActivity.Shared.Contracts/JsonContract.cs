using System.Text.Json;

namespace TeamActivity.Shared.Contracts;

public static class JsonContract
{
    public static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web)
    {
        WriteIndented = false
    };
}
