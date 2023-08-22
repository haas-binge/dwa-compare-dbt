
$env:DWH_DATABASE = ''
$env:DWH_PASSWORD = ''
$env:DWH_SNOWFLAKE_ACCOUNT = ''
$env:DWH_USER = ''

[System.Environment]::SetEnvironmentVariable('DWH_DATABASE',$env:DWH_DATABASE)
[System.Environment]::SetEnvironmentVariable('DWH_PASSWORD', $env:DWH_PASSWORD)
[System.Environment]::SetEnvironmentVariable('DWH_USER', $env:DWH_USER )
[System.Environment]::SetEnvironmentVariable('DWH_SNOWFLAKE_ACCOUNT', $env:DWH_SNOWFLAKE_ACCOUNT)

dir env:DWH_*
