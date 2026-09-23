# ============================================================
# BUILD STAGE
# ============================================================

FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build

WORKDIR /src

COPY Ftp.csproj ./

RUN dotnet restore Ftp.csproj

COPY . .

# Remove Windows build artifacts
RUN rm -rf bin obj

# Restore again after cleaning copied build artifacts
RUN dotnet restore Ftp.csproj

RUN dotnet publish Ftp.csproj \
    -c Release \
    -o /app/publish \
    --no-restore


# ============================================================
# RUNTIME STAGE
# ============================================================

FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS runtime

WORKDIR /app

COPY --from=build /app/publish .

ENTRYPOINT ["dotnet", "Ftp.dll"]