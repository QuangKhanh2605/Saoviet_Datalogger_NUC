# ============================================================
# BUILD STAGE
# ============================================================

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build

WORKDIR /src

COPY Modbus.csproj ./

RUN dotnet restore Modbus.csproj

COPY . .

# Remove Windows build artifacts
RUN rm -rf bin obj

# Restore again after cleaning copied build artifacts
RUN dotnet restore Modbus.csproj

RUN dotnet publish Modbus.csproj \
    -c Release \
    -o /app/publish \
    --no-restore


# ============================================================
# RUNTIME STAGE
# ============================================================

FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS runtime

WORKDIR /app

COPY --from=build /app/publish .

ENTRYPOINT ["dotnet", "Modbus.dll"]