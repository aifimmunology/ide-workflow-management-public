argo submit set-iam-policy.yaml \
 -p userEmail="user@example.org" \
 -p serviceAccountEmail="user-sa@example-project.iam.gserviceaccount.com" \
 -p podName="example-ide-ingress" \
 -p projectId="example-project" \
 -p ideUrl="https://ide.example.org/example-ide/lab" --log
