import json
import requests
import boto3

# Define API endpoint and SQS queue URL 
API_URL = "https://jsonplaceholder.typicode.com/users"
SQS_QUEUE_URL = "https://sqs.us-west-2.amazonaws.com/298788153511/user_data_processing_queue"

def lambda_handler(event, context):
  """
  This Lambda function retrieves data from the specified API and sends it to an SQS queue.

  Args:
      event: AWS Lambda event object (unused in this example).
      context: AWS Lambda context object (unused in this example).

  Returns:
      A dictionary containing a success message upon successful execution.
  """

  # Fetch data from the API
  response = requests.get(API_URL)
  data = response.json()

  # Check for successful API response
  if response.status_code != 200:
    raise Exception(f"Error retrieving data from API: {response.status_code}")

  # Create SQS client
  sqs_client = boto3.client('sqs')

  # Send each user data as a message to the SQS queue
  for user in data:
    try:
      message_body = json.dumps(user)  # Convert user data to JSON string
      response = sqs_client.send_message(
          QueueUrl=SQS_QUEUE_URL,
          MessageBody=message_body
      )
      print(f"Sent message to SQS queue: {message_body}")
    except Exception as e:
      print(f"Error sending message to SQS: {e}")

  # Return success message
  return {
      'statusCode': 200,
      'body': json.dumps('Data sent to SQS queue successfully!')
  }

