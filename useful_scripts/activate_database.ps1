
$env:DWH_DATABASE = ''
$env:DWH_SOURCE_DATABASE = ''
$env:DWH_PASSWORD = ''
$env:DWH_DATABASE = ''
$env:DWH_PASSWORD = ''
$env:DWH_SNOWFLAKE_ACCOUNT = ''
$env:DWH_DWS_USER = ''

[System.Environment]::SetEnvironmentVariable('DWH_DATABASE','')
[System.Environment]::SetEnvironmentVariable('DWH_SOURCE_DATABASE','')
[System.Environment]::SetEnvironmentVariable('DWH_PASSWORD', '')
[System.Environment]::SetEnvironmentVariable('DWH_USER', '')
[System.Environment]::SetEnvironmentVariable('DWH_SNOWFLAKE_ACCOUNT', '')

dir env:DWH_*
dir env:DWH_SNOWFLAKE*