# ============================================================
# BUILD STAGE
# ============================================================

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build

WORKDIR /src

COPY . .

RUN dotnet restore

RUN dotnet publish \
    -c Release \
    -o /app/publish \
    --no-restore


# ============================================================
# RUNTIME STAGE
# ============================================================

FROM mcr.microsoft.com/dotnet/runtime:10.0 AS runtime

WORKDIR /app

COPY --from=build /app/publish .

ENTRYPOINT ["dotnet", "Datalogger_Linux.dll"]