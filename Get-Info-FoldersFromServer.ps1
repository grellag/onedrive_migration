Function Log {
    param(
        [Parameter(Mandatory=$true)][String]$msg
    )
    Add-Content log_Info_Folders.txt $msg
}

[System.Collections.ArrayList]$resultsArray = @()
[System.Collections.ArrayList]$resultsAnomsArray = @()

$basePath = ''
$servers = Get-Content -Path "$PSScriptRoot\servers.txt"

$listfolder = ("LubranoA", "LubranoR")

#Import the module - be sure to change this path to where the psd1 file is on your machine
#Not needed if you already have the module imported
Import-Module -Name "$PSScriptRoot\PSFolderSize.psd1"
Write-Verbose "Working with [$servers]!"

foreach ($server in $servers) {
    Log "Loop server: $("{0} - Start scan: {1}" -f (Get-Date), $server)"
    $curPath       = $null
    $result        = $null
    ForEach ($Share in (Get-WmiObject -Class Win32_Share -Computername $Server -filter "Type=0"| Select-Object Name,Path,Description,Caption)) {
        $basePath = $Share.Caption
        $curPath = "\\$server\$basePath"  
        #if ($listfolder -contains $Share.Caption ) { 
        if ($Share.Caption.Substring(0,1)  -match '^[d-d,D-D]' ) {
            Write-Verbose "Working with [$curPath]!"
            Log "Loop server: $("{0} - Start scan: {1}" -f (Get-Date), $curPath)"
            if (Test-Path -Path $curPath) {
                $result = Get-FolderSize -AddFileTotal -AddTotal -BasePath $curPath 
                Log "Loop server: $("{0} - Start scan: {1}" -f (Get-Date), $curPath)"
                if ($result) {
                    #Add the basepath as a property, so we can sort by it later if needed
                    $result | Add-Member -MemberType NoteProperty -Name "BasePath" -Value $curPath
                } 
                #Add our result object to the array
                $resultsArray.Add($result) | Out-Null
                Log "Loop server: $("{0} - End scan: {1}" -f (Get-Date), $curPath)" 
                $combined = $result | ForEach-Object {$_}
                $combined | Export-Csv -Path ("{1}\Data\Logs\results_{2}_{0:MMddyy_HHmm}.csv" -f (Get-Date), $PSScriptRoot, $basePath.Replace("\", "_")) -NoTypeInformation -Force
                $combined | Export-Csv -Path ("{0}\Data\Output\results_{1}.csv" -f $PSScriptRoot, $basePath.Replace("\", "_")) -NoTypeInformation -Force
                $resultsArray.Add($result) | Out-Null
            } else {
                $result_anomalies = [PSCustomObject]@{
                    BasePath = $curPath
                    Result   = "Unable to access path [$curPath]!"
                }
                $resultsAnomsArray.Add($result_anomalies) | Out-Null
            }    
            if (!$result) {
                $result_anomalies = [PSCustomObject]@{
                    BasePath     = $curPath
                    Result       = "No results!"
                }
                $resultsAnomsArray.Add($result_anomalies) | Out-Null
            }   
            
        } else {
            $result_anomalies = [PSCustomObject]@{
                BasePath = $curPath
                Result   = "Skipped path by developer [$curPath]!"
            }
            $resultsAnomsArray.Add($result_anomalies) | Out-Null
        }
        $curPath = ''
        $basePath = ''
    }
}

#Example of exporting combined results as CSV flatten array
#$combined = $resultsArray | ForEach-Object {$_}
#Export combined array
$combined_anomalies = $resultsAnomsArray | ForEach-Object {$_}
$combined_anomalies | Export-Csv -Path ("{1}\Data\results_anomalies_{0:MMddyy_HHmm}.csv" -f (Get-Date), $PSScriptRoot) -NoTypeInformation -Force 
#$combined_anomalies | Export-Csv -Path ("{1}\Data\Log\anomalies_results_{0:MMddyy_HHmm}.csv" -f (Get-Date), $PSScriptRoot) -NoTypeInformation -Force
$resultsArray | Export-Csv -Path ("{1}\results_{2}_{0:MMddyy_HHmm}.csv" -f (Get-Date), $PSScriptRoot, $server.Replace(".", "_")) -NoTypeInformation -Force 

$basePath = ''
#Return results array
return $resultsArray
