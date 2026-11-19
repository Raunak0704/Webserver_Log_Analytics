param(
    [Parameter(Mandatory = $true)]
    [string]$BucketName,
    [string]$Region = "ap-south-1",
    [string]$StackName = "web-log-analytics",
    [string]$ProjectPrefix = "web-log-analytics"
)

$ErrorActionPreference = "Stop"

if (-not (Get-Command aws -ErrorAction SilentlyContinue)) {
    throw "AWS CLI v2 was not found. Install it, then reopen PowerShell."
}

$ProjectRoot = Split-Path -Parent $PSScriptRoot
$GlueScript = Join-Path $ProjectRoot "glue_jobs\glue_web_log_etl.py"
$Template = Join-Path $PSScriptRoot "glue-stack.yaml"

Write-Host "Checking AWS credentials..." -ForegroundColor Cyan
aws sts get-caller-identity --region $Region | Out-Host
if ($LASTEXITCODE -ne 0) { throw "AWS credentials are not configured or lack permission." }

Write-Host "Checking S3 bucket $BucketName..." -ForegroundColor Cyan
aws s3api head-bucket --bucket $BucketName --region $Region 2>$null
if ($LASTEXITCODE -ne 0) {
    Write-Host "Bucket not found/accessible; attempting to create it..." -ForegroundColor Yellow
    if ($Region -eq "us-east-1") {
        aws s3api create-bucket --bucket $BucketName --region $Region | Out-Host
    } else {
        aws s3api create-bucket --bucket $BucketName --region $Region `
            --create-bucket-configuration "LocationConstraint=$Region" | Out-Host
    }
    if ($LASTEXITCODE -ne 0) { throw "Could not create bucket. Bucket names are globally unique; choose another name." }
}

aws s3api put-public-access-block --bucket $BucketName --region $Region `
    --public-access-block-configuration BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true | Out-Null
if ($LASTEXITCODE -ne 0) { throw "Could not enable S3 Block Public Access." }

$Encryption = '{"Rules":[{"ApplyServerSideEncryptionByDefault":{"SSEAlgorithm":"AES256"}}]}'
aws s3api put-bucket-encryption --bucket $BucketName --region $Region --server-side-encryption-configuration $Encryption | Out-Null
if ($LASTEXITCODE -ne 0) { throw "Could not configure default S3 encryption." }

$ScriptKey = "$ProjectPrefix/scripts/glue_web_log_etl.py"
Write-Host "Uploading Glue script to s3://$BucketName/$ScriptKey ..." -ForegroundColor Cyan
aws s3 cp $GlueScript "s3://$BucketName/$ScriptKey" --region $Region
if ($LASTEXITCODE -ne 0) { throw "Could not upload Glue script." }

Write-Host "Deploying Glue database, crawler, IAM role and job..." -ForegroundColor Cyan
aws cloudformation deploy `
    --template-file $Template `
    --stack-name $StackName `
    --region $Region `
    --parameter-overrides "ExistingBucketName=$BucketName" "ProjectPrefix=$ProjectPrefix" `
    --capabilities CAPABILITY_NAMED_IAM
if ($LASTEXITCODE -ne 0) { throw "CloudFormation deployment failed. Inspect the stack Events in AWS Console." }

Write-Host "Deployment complete." -ForegroundColor Green
aws cloudformation describe-stacks --stack-name $StackName --region $Region `
    --query "Stacks[0].Outputs[*].[OutputKey,OutputValue]" --output table
