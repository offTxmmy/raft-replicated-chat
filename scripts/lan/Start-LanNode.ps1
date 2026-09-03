param(
    [ValidateSet("directory", "broker", "client")]
    [string] $Role,
    [string] $Voters,
    [string] $DirectoryHost,
    [int] $DirectoryBrokerPort = 60000,
    [int] $DirectoryClientPort = 60001,
    [int] $NodeId,
    [int] $RpcPort,
    [int] $ClientPort,
    [int] $BroadcastPort = 7100,
    [string] $ClusterId = "lan-demo",
    [int] $UdpMaxPayload = 1300
)

$ErrorActionPreference = "Stop"
$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot "..\..")).Path
$classes = Join-Path $projectRoot "target\classes"

if (-not (Test-Path $classes)) {
    throw "Missing target/classes. Run 'mvn clean package -DskipTests' first."
}

switch ($Role) {
    "directory" {
        if ([string]::IsNullOrWhiteSpace($Voters)) {
            throw "-Voters is required for the directory role."
        }
        & java -cp $classes it.polimi.ds.chat.directory.DirectoryService `
            $Voters $DirectoryBrokerPort $DirectoryClientPort
    }
    "broker" {
        foreach ($value in @($Voters, $DirectoryHost)) {
            if ([string]::IsNullOrWhiteSpace($value)) {
                throw "-Voters and -DirectoryHost are required for the broker role."
            }
        }
        & java -cp $classes it.polimi.ds.chat.broker.core.BrokerMain `
            raft $NodeId $RpcPort $ClientPort $BroadcastPort $ClusterId `
            $UdpMaxPayload $DirectoryHost $DirectoryBrokerPort
    }
    "client" {
        if ([string]::IsNullOrWhiteSpace($DirectoryHost)) {
            throw "-DirectoryHost is required for the client role."
        }
        & java -cp $classes it.polimi.ds.chat.client.ClientMain `
            $DirectoryHost $DirectoryClientPort
    }
}