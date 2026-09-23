# ============================================================
# BUILD STAGE
# ============================================================

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build

WORKDIR /src

COPY Mqtt.csproj ./

RUN dotnet restore Mqtt.csproj

COPY . .

# Remove Windows build artifacts
RUN rm -rf bin obj

# Restore again after cleaning copied build artifacts
RUN dotnet restore Mqtt.csproj

RUN dotnet publish Mqtt.csproj \
    -c Release \
    -o /app/publish \
    --no-restore


# ============================================================
# RUNTIME STAGE
# ============================================================

FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS runtime

WORKDIR /app

COPY --from=build /app/publish .

ENTRYPOINT ["dotnet", "Mqtt.dll"]