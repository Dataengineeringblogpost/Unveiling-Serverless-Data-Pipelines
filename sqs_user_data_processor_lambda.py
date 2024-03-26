import json
import boto3

# Replace with your SQS queue URL
SQS_QUEUE_URL = "https://sqs.us-west-2.amazonaws.com/298788153511/user_data_processing_queue"
#Creating an S3 client object 
s3_client = boto3.client('s3')
def lambda_handler(event, context):

  """
  This Lambda function receives messages from an SQS queue, processes the data,
  and stores it in a  S3 bucket .

  Args:
      event: AWS Lambda event object containing SQS message data.
      

  Returns:
      A dictionary containing a success message upon successful execution.
  """

  messages = event['Records'][0]['body']
  
  try:
    # Extract message body
    message_body = json.loads(messages)

    # Process the user data (replace with your logic)
    processed_data = process_user_data(message_body)
    print(processed_data)
    store_data(processed_data)
      

  except Exception as e:
    print(f"Error processing message: {e}")
    # Consider retrying or logging errors for further analysis

  return {
      'statusCode': 200,
      'body': json.dumps('Data processed and stored successfully!')
  }

# Define your data processing and storage functions here
def process_user_data(user_data):
  # Implementing data preprocessing logic
  processed_data = {
      "username": user_data["username"],
      "email": user_data["email"],
      "city": user_data["address"]["city"]  # Extracting a specific field
  }
  
  return processed_data



def store_data(processed_data):
  #Storing the preprocessed data into s3 object 
  s3_client.put_object(Bucket='processed-user-data-2521', Key='user_data_'+str(processed_data['username']), Body=json.dumps(processed_data))
