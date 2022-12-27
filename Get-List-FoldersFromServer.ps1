Function Log {
    param(
        [Parameter(Mandatory=$true)][String]$msg
    )
    
    Add-Content log_list_shared_folder_by_server.txt $msg
}

$servers = Get-Content -Path "$PSScriptRoot\servers.txt"
Write-Verbose "Working with [$servers]!"
#$arrROOTs      = @() #Initialize array used to store custom object output
$ServerShares = 
ForEach ($Server in $servers){
    Log "Loop server: $("{0} - Start scan: {1}" -f (Get-Date), $server)"
    ForEach ($Share in (Get-WmiObject -Class Win32_Share -Computername $Server -filter "Type=0"| Select-Object Name,Path,Description,Caption)){
        [PSCustomObject]@{
            Server          = $Server
            'Share Name'    = $Share.Name
            'Source Folder' = $Share.Path
            Description     = $Share.Description
            Caption         = $Share.Caption
        }
    }
}
$ServerShares | Format-Table -auto
#$ServerShares | Out-GridView
$ServerShares | Export-Csv -Path ("$PSScriptRoot\Data\ServerShares.csv" -f (Get-Date)) -NoTypeInformation