FROM mcr.microsoft.com/dotnet/sdk:10.0

WORKDIR /app

ENTRYPOINT ["dotnet"]
CMD ["test", "--no-restore"]

COPY XtdbTest.csproj packages.lock.json ./
RUN dotnet restore --locked-mode

COPY . .
