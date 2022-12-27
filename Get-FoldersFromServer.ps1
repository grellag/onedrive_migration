Function Log {
    param(
        [Parameter(Mandatory=$true)][String]$msg
    )
    
    Add-Content log.txt $msg
}

[System.Collections.ArrayList]$resultsArray = @()

$basePath = 'edidoc'

#Import the servers from a text file
$servers = Get-Content -Path "$PSScriptRoot\servers.txt"

#Import the module - be sure to change this path to where the psd1 file is on your machine
#Not needed if you already have the module imported
Import-Module -Name "$PSScriptRoot\PSFolderSize.psd1"
Write-Verbose "Working with [$servers]!"



foreach ($server in $servers) {
    Log "Loop server: $("{0} - Start scan: {1}" -f (Get-Date), $curPath)"
    $curPath       = $null
    $result        = $null

    $curPath = "\\$server\$basePath"
    
        Write-Verbose "Working with [$curPath]!"
        
        if (Test-Path -Path $curPath) {
            
            #$result = Get-FolderSize -BasePath $curPath -AddFileTotal -OmitFolders '\\10.1.63.58\Public\Accounting','\\10.1.63.58\Public\Biblioteca' 
            $result = Get-FolderSize -AddFileTotal -AddTotal -BasePath $curPath 
            if ($result) {
    
                #Add the basepath as a property, so we can sort by it later if needed
                $result | Add-Member -MemberType NoteProperty -Name "BasePath" -Value $curPath
            }  
        } else {
            #If we can't access the path, let it be known in the result
            $result = [PSCustomObject]@{
                BasePath = $curPath
                Result   = "Unable to access path [$curPath]!"
            }
        }    
        if (!$result) {
            $result = [PSCustomObject]@{
                BasePath     = $curPath
                Result       = "No results!"
            }
        }        
        $resultsArray.Add($result) | Out-Null
        Log "Loop server: $("{0} - End scan: {1}" -f (Get-Date), $curPath)"
}

$combined = $resultsArray | ForEach-Object {$_}
$combined | Export-Csv -Path ("{1}\results_{2}_{0:MMddyy_HHmm}.csv" -f (Get-Date), $PSScriptRoot, $basePath.Replace("\", "_")) -NoTypeInformation -Force 
$basePath = ''
#Return results array
return $resultsArray
