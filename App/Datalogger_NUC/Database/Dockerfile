# ============================================================
# BUILD STAGE
# ============================================================

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build

WORKDIR /src

COPY Database.csproj ./

RUN dotnet restore Database.csproj

COPY . .

# Remove Windows build artifacts
RUN rm -rf bin obj

# Restore again after cleaning copied build artifacts
RUN dotnet restore Database.csproj

RUN dotnet publish Database.csproj \
    -c Release \
    -o /app/publish \
    --no-restore


# ============================================================
# RUNTIME STAGE
# ============================================================

FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS runtime

WORKDIR /app

COPY --from=build /app/publish .

ENTRYPOINT ["dotnet", "Database.dll"]