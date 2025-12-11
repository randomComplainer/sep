param(
    [Parameter(Mandatory=$true)][string]$server
)

$ErrorActionPreference = "Stop"

$Username = "ftpuser"
$Password = "sep-user"
$LocalFile = "$PSScriptRoot\sep-client.exe"
$RemoteFile = "ftp://$server/sep-client.exe"
$LogDir = "$PSScriptRoot\log"

$Req = [System.Net.FileWebRequest]::Create("$RemoteFile")
$Req.Credentials = New-Object System.Net.NetworkCredential($Username, $Password)
$Req.Method = [System.Net.WebRequestMethods+Ftp]::DownloadFile

$Res = $Req.GetResponse()
$Stream = $Res.GetResponseStream()

$LocalStream = New-Object IO.FileStream($LocalFile, [IO.FileMode]::Create)
[byte[]] $Buf = New-Object byte[] 1024

do {
    $Len = $Stream.Read($Buf, 0, 1024)
    $LocalStream.Write($Buf, 0, $Len)
} while ($Len -ne 0)
$Stream.Close()
$LocalStream.Close()

New-Item -Path $LogDir -ItemType Directory -Force

$Timestamp = Get-Date -Format "yyyy-MM-dd_HHmm"
$LogFile = "$LogDir\$Timestamp"
iex "$LocalFile --key test-pass --server ${server}:1081 --port 1082 --log-format json | Tee-Object -FilePath $LogFile"
