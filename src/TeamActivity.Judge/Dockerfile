FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /source

# Copy csproj files first for layer-cached restore
COPY TeamActivity.ServiceDefaults/TeamActivity.ServiceDefaults.csproj TeamActivity.ServiceDefaults/
COPY src/TeamActivity.Shared.Contracts/TeamActivity.Shared.Contracts.csproj src/TeamActivity.Shared.Contracts/
COPY src/TeamActivity.Judge/TeamActivity.Judge.csproj src/TeamActivity.Judge/

RUN dotnet restore src/TeamActivity.Judge/TeamActivity.Judge.csproj

# Copy source
COPY TeamActivity.ServiceDefaults/ TeamActivity.ServiceDefaults/
COPY src/TeamActivity.Shared.Contracts/ src/TeamActivity.Shared.Contracts/
COPY src/TeamActivity.Judge/ src/TeamActivity.Judge/

RUN dotnet publish src/TeamActivity.Judge/TeamActivity.Judge.csproj \
    -c Release \
    -o /app

FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS runtime
WORKDIR /app
COPY --from=build /app .
ENTRYPOINT ["dotnet", "TeamActivity.Judge.dll"]
