## Branch Workflow

This is the standard development and deployment workflow.

Create branch:

    git checkout -b feature/short-description

Commit:

    git add .
    git commit -m "message"
    git push origin feature/short-description

Merge to main:

    git checkout main
    git merge feature/short-description
    git push origin main

Deploy to runtime host:

    ssh $ELT_SERVER_USER@$ELT_SERVER_IP
    cd /opt/elt-canonical-data
    git pull origin main
    docker compose restart
