# StopIIS.ps1

# Stop IIS
Stop-Service -Name W3SVC

# Optional: Add a delay if needed
Start-Sleep -Seconds 600

Write-Host "IIS has been stopped."


# StartIIS.ps1

# Start IIS
Start-Service -Name W3SVC

Write-Host "IIS has been started."

