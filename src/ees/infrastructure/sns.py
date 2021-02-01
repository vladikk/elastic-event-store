import boto3
import json
import logging

logger = logging.getLogger("ees.infrastructure.sns")


class SNS:
    def __init__(self, topic):
        self.topic = topic
        self.sns = boto3.client('sns') 
    
    def publish(self, message, group):
        logger.debug(f"Publishing message to {self.topic}: {message}")

        response = self.sns.publish(
            TopicArn=self.topic,
            Message=message,
            MessageGroupId=group)
        
        logger.debug(f"Publishing result: {response}")