sam build
sam deploy \
    --s3-bucket=$SAM_ARTIFACTS_BUCKET \
    --stack-name=ees-tests \
    --region=us-east-1 \
    --no-confirm-changeset \
    --capabilities="CAPABILITY_IAM" \
    --parameter-overrides=""