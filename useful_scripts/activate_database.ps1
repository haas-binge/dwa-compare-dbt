
$env:DWH_DWS_DATABASE = ''
$env:DWH_DWS_PASSWORD = ''
$env:DWH_SNOWFLAKE_ACCOUNT = ''
$env:DWH_DWS_USER = ''

[System.Environment]::SetEnvironmentVariable('DWH_DWS_DATABASE','')
[System.Environment]::SetEnvironmentVariable('DWH_DWS_PASSWORD', '')
[System.Environment]::SetEnvironmentVariable('DWH_DWS_USER', '')
[System.Environment]::SetEnvironmentVariable('DWH_SNOWFLAKE_ACCOUNT', '')

dir env:DWH_DWS*
dir env:DWH_SNOWFLAKE*