argo submit ide-health-route-workflow.yaml \
 -p ideName="makingpixi3" \
 -p podName="makingpixi3-0" \
 -p ideId="ac19412d-d940-4050-a4fc-e6158808c844" \
 -p ideBaseURI="example.org" \
 -p authClientID="CLIENT_ID.apps.googleusercontent.com" \
 -p authSecretName="backend-service-oauth-client-secret" \
 -p includeDashApp="false" \
 -p project="example-project" \
 -p podTimeout="1200" --log