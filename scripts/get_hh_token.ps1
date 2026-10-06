# Run from CMD: powershell -NoExit -ExecutionPolicy Bypass -File .\get_hh_token.ps1
# Credentials stay in this PowerShell process; nothing is saved to disk.

Set-Location -LiteralPath (Split-Path -Parent $PSScriptRoot)

if ($env:HH_ACCESS_TOKEN) {
    Write-Host 'HH_ACCESS_TOKEN is already set in this session. No new token requested.'
    Write-Host 'Check access: .\.venv\Scripts\python.exe collect.py hh --check-access'
    return
}

try {
    $hhClientId = Read-Host 'Client ID'
    $hhSecret = Read-Host 'Client Secret' -AsSecureString
    $hhEmail = Read-Host 'Your contact email'

    if ([string]::IsNullOrWhiteSpace($hhClientId) -or $hhSecret.Length -eq 0 -or
        [string]::IsNullOrWhiteSpace($hhEmail)) {
        throw 'All three fields are required.'
    }

    $env:HH_USER_AGENT = "VacancyResearchPilot/0.1 ($($hhEmail.Trim()))"
    $hhForm = @{
        grant_type = 'client_credentials'
        client_id = $hhClientId.Trim()
        client_secret = [System.Net.NetworkCredential]::new('', $hhSecret).Password
    }
    $hhResponse = Invoke-RestMethod `
        -Method Post `
        -Uri 'https://api.hh.ru/token' `
        -ContentType 'application/x-www-form-urlencoded' `
        -Headers @{ 'HH-User-Agent' = $env:HH_USER_AGENT } `
        -Body $hhForm `
        -MaximumRedirection 0 `
        -TimeoutSec 30 `
        -ErrorAction Stop

    if ([string]::IsNullOrWhiteSpace($hhResponse.access_token)) {
        throw 'HH did not return an access_token.'
    }

    $env:HH_ACCESS_TOKEN = $hhResponse.access_token
    Write-Host 'Token received. It is set for this PowerShell session.'
    Write-Host 'Next: .\.venv\Scripts\python.exe collect.py hh --check-access'
}
catch {
    # Do not echo the exception body: it could contain credentials or a token.
    $hhStatus = $null
    if ($_.Exception.Response) {
        $hhStatus = [int]$_.Exception.Response.StatusCode
    }
    if ($hhStatus) {
        Write-Host "Token request failed: HTTP $hhStatus. No new token configured."
    }
    else {
        Write-Host 'Token request failed. Check the input fields and network connection.'
    }
    Write-Host 'You may share the HTTP status, but never Client Secret or access_token.'
}
finally {
    if ($hhForm) { $hhForm.Clear() }
    if ($hhSecret) { $hhSecret.Dispose() }
    Remove-Variable hhForm, hhResponse, hhSecret, hhClientId, hhEmail -ErrorAction SilentlyContinue
}
