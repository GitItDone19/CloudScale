$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

function Invoke-Git {
    git @args
    if ($LASTEXITCODE -ne 0) { throw "git $args failed (exit $LASTEXITCODE)" }
}

$branch = git rev-parse --abbrev-ref HEAD

Invoke-Git add -A

if (git status --porcelain) {
    Invoke-Git commit -m "auto: sync $(Get-Date -Format 'yyyy-MM-dd HH:mm')"
}

# Pick up remote commits first so the push is not rejected
Invoke-Git pull --rebase origin $branch

# Push even when there was nothing new to commit but local commits are pending
Invoke-Git push origin $branch
