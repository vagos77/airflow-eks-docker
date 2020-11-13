# Use this code snippet in your app.
# If you need more information about configurations or implementing the sample code, visit the AWS docs:
# https://aws.amazon.com/developers/getting-started/python/

import boto3
import base64
import logging
from botocore.exceptions import ClientError


class SecretsManager(object):
    def __init__(self, session=None, credentials=None, region='eu-west-1'):
        """
         Different sessions are supported which allows access to multiple AWS accounts. To set them up you need to
         edit ~/.aws/config and ~/.aws/credentials or use the aws cli to configure the different profiles

         :param session: The AWS profile as set up in AWS config
         :param credentials: Dictionary with AccessKeyId, SecretAccessKey, SessionToken
         """
        if session is not None:
            self.session = boto3.Session(profile_name=session)
            self.client = self.session.client('secretsmanager')
        elif credentials is not None:
            self.client = boto3.client('secretsmanager', region_name=region,
                                       aws_access_key_id=credentials['AccessKeyId'],
                                       aws_secret_access_key=credentials['SecretAccessKey'],
                                       aws_session_token=credentials['SessionToken'])
        else:
            # For use in AWS Lambda
            session = boto3.session.Session()
            self.client = session.client(
                service_name='secretsmanager',
                region_name=region
            )

        self.logger = logging.getLogger(__name__ + '.secretsmanager')

    def get_secret(self, name):
        # In this sample we only handle the specific exceptions for the 'GetSecretValue' API.
        # See https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        # We rethrow the exception by default.

        try:
            self.logger.info('Retrieving secret {}'.format(name))

            get_secret_value_response = self.client.get_secret_value(
                SecretId=name
            )
        except ClientError as e:
            if e.response['Error']['Code'] == 'DecryptionFailureException':
                # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            elif e.response['Error']['Code'] == 'InternalServiceErrorException':
                # An error occurred on the server side.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            elif e.response['Error']['Code'] == 'InvalidParameterException':
                # You provided an invalid value for a parameter.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            elif e.response['Error']['Code'] == 'InvalidRequestException':
                # You provided a parameter value that is not valid for the current state of the resource.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            elif e.response['Error']['Code'] == 'ResourceNotFoundException':
                # We can't find the resource that you asked for.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
        else:
            # Decrypts secret using the associated KMS CMK.
            # Depending on whether the secret is a string or binary, one of these fields will be populated.
            if 'SecretString' in get_secret_value_response:
                secret = get_secret_value_response['SecretString']

                return secret
            else:
                decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])

                return decoded_binary_secret
