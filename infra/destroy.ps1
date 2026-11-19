param(
    [string]$Region = "ap-south-1",
    [string]$StackName = "web-log-analytics"
)

$ErrorActionPreference = "Stop"
Write-Host "This removes the Glue crawler, job, catalog database and IAM role." -ForegroundColor Yellow
Write-Host "It intentionally leaves your S3 bucket and its data untouched."
$confirm = Read-Host "Type DELETE-GLUE-STACK to continue"
if ($confirm -ne "DELETE-GLUE-STACK") { Write-Host "Cancelled."; exit 0 }
aws cloudformation delete-stack --stack-name $StackName --region $Region
if ($LASTEXITCODE -ne 0) { throw "Could not request stack deletion." }
aws cloudformation wait stack-delete-complete --stack-name $StackName --region $Region
if ($LASTEXITCODE -ne 0) { throw "Stack deletion did not complete." }
Write-Host "Glue resources deleted. S3 data retained." -ForegroundColor Green
