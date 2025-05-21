import boto3
from typing import List, Dict, Any, Optional
from botocore.exceptions import ClientError

class S3Bucket:
    """
    A class for interacting with AWS S3 buckets.
    Provides methods to read, write, list, and manage objects in S3.
    """
    
    def __init__(self, bucket_name: str, region_name: Optional[str] = None, 
                 aws_access_key_id: Optional[str] = None, 
                 aws_secret_access_key: Optional[str] = None):
        """
        Initialize an S3 bucket connection.
        
        Args:
            bucket_name: Name of the S3 bucket
            region_name: AWS region name (optional)
            aws_access_key_id: AWS access key (optional, uses environment/config if not provided)
            aws_secret_access_key: AWS secret key (optional, uses environment/config if not provided)
        """
        self.bucket_name = bucket_name
        
        # Initialize S3 client
        session_params = {'region_name': region_name} if region_name else {}
        if aws_access_key_id and aws_secret_access_key:
            session_params.update({
                'aws_access_key_id': aws_access_key_id,
                'aws_secret_access_key': aws_secret_access_key
            })
        
        self.s3_client = boto3.client('s3', **session_params)
        self.s3_resource = boto3.resource('s3', **session_params)
        self.bucket = self.s3_resource.Bucket(bucket_name)
        
    def put_bytes(self, key: str, data: bytes, metadata: Optional[Dict[str, str]] = None) -> Dict:
        """
        Upload bytes data to S3.
        
        Args:
            key: S3 object key (path)
            data: Bytes data to upload
            metadata: Optional metadata dictionary
            
        Returns:
            Response from S3 put_object operation
        """
        params = {
            'Bucket': self.bucket_name,
            'Key': key,
            'Body': data
        }
        
        if metadata:
            params['Metadata'] = metadata
            
        return self.s3_client.put_object(**params)
    
    def get_bytes(self, key: str) -> bytes:
        """
        Get object from S3 and return it as bytes.
        
        Args:
            key: S3 object key (path)
            
        Returns:
            Object content as bytes
            
        Raises:
            ClientError: If the object does not exist or other AWS error occurs
        """
        response = self.s3_client.get_object(Bucket=self.bucket_name, Key=key)
        return response['Body'].read()
    
    def put_file(self, key: str, file_path: str, metadata: Optional[Dict[str, str]] = None) -> Dict:
        """
        Upload a file to S3.
        
        Args:
            key: S3 object key (path)
            file_path: Local file path to upload
            metadata: Optional metadata dictionary
            
        Returns:
            Response from S3 upload_file operation
        """
        extra_args = {'Metadata': metadata} if metadata else None
        self.s3_client.upload_file(file_path, self.bucket_name, key, ExtraArgs=extra_args)
        return {"Key": key, "Bucket": self.bucket_name}
    
    def get_file(self, key: str, file_path: str) -> None:
        """
        Download an object from S3 to a local file.
        
        Args:
            key: S3 object key (path)
            file_path: Local file path to save the downloaded object
            
        Raises:
            ClientError: If the object does not exist or other AWS error occurs
        """
        self.s3_client.download_file(self.bucket_name, key, file_path)
    
    def list_objects(self, prefix: str = "", delimiter: str = "/") -> List[Dict[str, Any]]:
        """
        List objects in a specific prefix (folder) in the bucket.
        
        Args:
            prefix: Path prefix to list objects from
            delimiter: Delimiter for hierarchical listing (typically '/')
            
        Returns:
            List of objects with their metadata
        """
        paginator = self.s3_client.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(
            Bucket=self.bucket_name,
            Prefix=prefix,
            Delimiter=delimiter
        )
        
        result = []
        for page in page_iterator:
            # Add files
            if 'Contents' in page:
                for obj in page['Contents']:
                    result.append({
                        'key': obj['Key'],
                        'size': obj['Size'],
                        'last_modified': obj['LastModified'],
                        'type': 'file'
                    })
            
            # Add folders (common prefixes)
            if 'CommonPrefixes' in page:
                for prefix_obj in page['CommonPrefixes']:
                    result.append({
                        'key': prefix_obj['Prefix'],
                        'type': 'folder'
                    })
                    
        return result
    
    def object_exists(self, key: str) -> bool:
        """
        Check if an object exists in the bucket.
        
        Args:
            key: S3 object key (path)
            
        Returns:
            True if object exists, False otherwise
        """
        try:
            self.s3_client.head_object(Bucket=self.bucket_name, Key=key)
            return True
        except ClientError:
            return False
    
    def delete_object(self, key: str) -> Dict:
        """
        Delete an object from the bucket.
        
        Args:
            key: S3 object key (path) to delete
            
        Returns:
            Response from S3 delete_object operation
        """
        return self.s3_client.delete_object(Bucket=self.bucket_name, Key=key)
    
    def get_object_metadata(self, key: str) -> Dict:
        """
        Get metadata for a specific object.
        
        Args:
            key: S3 object key (path)
            
        Returns:
            Object metadata
            
        Raises:
            ClientError: If the object does not exist or other AWS error occurs
        """
        response = self.s3_client.head_object(Bucket=self.bucket_name, Key=key)
        return {
            'content_type': response.get('ContentType'),
            'content_length': response.get('ContentLength'),
            'last_modified': response.get('LastModified'),
            'metadata': response.get('Metadata', {})
        }
    
    def generate_presigned_url(self, key: str, expiration: int = 3600) -> str:
        """
        Generate a presigned URL for temporary access to an object.
        
        Args:
            key: S3 object key (path)
            expiration: URL expiration time in seconds (default: 1 hour)
            
        Returns:
            Presigned URL string
        """
        return self.s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': self.bucket_name, 'Key': key},
            ExpiresIn=expiration
        )