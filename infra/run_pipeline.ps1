param(
    [string]$Region = "ap-south-1",
    [string]$StackName = "web-log-analytics"
)

$ErrorActionPreference = "Stop"
if (-not (Get-Command aws -ErrorAction SilentlyContinue)) {
    throw "AWS CLI v2 was not found."
}

$CrawlerName = aws cloudformation describe-stacks --stack-name $StackName --region $Region `
    --query "Stacks[0].Outputs[?OutputKey=='GlueCrawlerName'].OutputValue | [0]" --output text
$JobName = aws cloudformation describe-stacks --stack-name $StackName --region $Region `
    --query "Stacks[0].Outputs[?OutputKey=='GlueJobName'].OutputValue | [0]" --output text
if ($LASTEXITCODE -ne 0 -or -not $CrawlerName -or -not $JobName) {
    throw "Could not read crawler/job names from CloudFormation stack $StackName."
}

Write-Host "Starting crawler: $CrawlerName" -ForegroundColor Cyan
$CrawlerState = aws glue get-crawler --name $CrawlerName --region $Region --query "Crawler.State" --output text
if ($CrawlerState -eq "RUNNING" -or $CrawlerState -eq "STOPPING") {
    throw "Crawler is already active. Wait for it to finish, then rerun this script."
}
aws glue start-crawler --name $CrawlerName --region $Region
if ($LASTEXITCODE -ne 0) { throw "Could not start crawler." }

do {
    Start-Sleep -Seconds 15
    $CrawlerState = aws glue get-crawler --name $CrawlerName --region $Region --query "Crawler.State" --output text
    Write-Host "Crawler state: $CrawlerState"
} while ($CrawlerState -eq "RUNNING" -or $CrawlerState -eq "STOPPING")

$CrawlerStatus = aws glue get-crawler --name $CrawlerName --region $Region --query "Crawler.LastCrawl.Status" --output text
if ($CrawlerStatus -ne "SUCCEEDED") {
    throw "Crawler did not succeed (LastCrawl.Status=$CrawlerStatus). Check Glue crawler logs."
}

Write-Host "Starting Glue ETL job: $JobName" -ForegroundColor Cyan
$RunId = aws glue start-job-run --job-name $JobName --region $Region --query "JobRunId" --output text
if ($LASTEXITCODE -ne 0 -or -not $RunId) { throw "Could not start Glue job." }
Write-Host "Job run ID: $RunId"

do {
    Start-Sleep -Seconds 20
    $JobState = aws glue get-job-run --job-name $JobName --run-id $RunId --region $Region `
        --query "JobRun.JobRunState" --output text
    Write-Host "Job state: $JobState"
} while ($JobState -eq "STARTING" -or $JobState -eq "WAITING" -or $JobState -eq "RUNNING" -or $JobState -eq "STOPPING")

if ($JobState -ne "SUCCEEDED") {
    aws glue get-job-run --job-name $JobName --run-id $RunId --region $Region `
        --query "JobRun.[ErrorMessage,GlueVersion,ExecutionTime,LogGroupName]" --output table
    throw "Glue job finished with state $JobState. Inspect CloudWatch logs."
}
Write-Host "Glue ETL completed successfully." -ForegroundColor Green
