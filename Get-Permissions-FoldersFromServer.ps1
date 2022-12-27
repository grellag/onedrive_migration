Function Log {
    param(
        [Parameter(Mandatory=$true)][String]$msg
    )
    
    Add-Content log_permissions.txt $msg
}

[System.Collections.ArrayList]$Report = @()

$basePath = 'AbondiG'

$servers = Get-Content -Path "$PSScriptRoot\servers.txt"
$curPath       = $null
$result        = $null
$curPath = '\\$server\$basePath'

Write-Verbose "Working with [$curPath]!"

Log "Loop permissions: $("{0} - Start scan: {1}" -f (Get-Date), $basePath)"

$FolderPath = dir -Directory -Path $curPath -Recurse -Force

Foreach ($Folder in $FolderPath) {

    
    try {
        $Acl = Get-Acl -Path $Folder.FullName -ErrorAction Stop 
    } catch [System.UnauthorizedAccessException] {
        $pathWithProblem = $_.TargetObject
        #do what you like with it after this
        $descriptionOfProblem = $_.Exception.Message
        Write-Warning "$descriptionOfProblem : $pathWithProblem"
        throw
    }




    foreach ($Access in $acl.Access) { 
        $Properties = [ordered]@{
            'FolderName'       = $Folder.FullName;
            'AD Group or User' = $Access.IdentityReference;
            'Permissions'      = $Access.FileSystemRights;
            'Inherited'        = $Access.IsInherited
        }
    $Report += New-Object -TypeName PSObject -Property $Properties
    }
}








$Report | Export-Csv -path ("{1}\FolderPermissions_{2}_{0:MMddyy_HHmm}.csv" -f (Get-Date), $PSScriptRoot, $basePath.Replace("\", "_")) -NoTypeInformation -Force

Log "Loop permissions: $("{0} - End scan: {1}" -f (Get-Date), $curPath)"
