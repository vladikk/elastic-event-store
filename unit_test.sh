export AWS_SAM_STACK_NAME=ees-python-tests
export AWS_DEFAULT_REGION=us-east-1
pytest -v -m "not slow"