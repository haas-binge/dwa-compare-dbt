$env:DWH_DWS_DATABASE = ''
$env:DWH_DWS_PASSWORD = ''
$env:DWH_DWS_USER = ''


[System.Environment]::SetEnvironmentVariable('DWH_DWS_DATABASE','')
[System.Environment]::SetEnvironmentVariable('DWH_DWS_PASSWORD', '')
[System.Environment]::SetEnvironmentVariable('DWH_DWS_USER', '')

dir env:DWH*
