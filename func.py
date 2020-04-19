import io
import json
import logging

from fdk import response
import oci


def handler(ctx, data: io.BytesIO=None):
    try:
        # authenticate via Instance Principals
        signer = oci.auth.signers.get_resource_principals_signer()
        
        # get the info from the event service
        body = json.loads(data.getvalue())

        logging.getLogger().info("Body is: " + str(body))
        
        # get compartment id and instance name
        compartment_id = body["data"]["compartmentId"]
        resource_name = body["data"]["resourceName"]
                      
        # see if instance was created or terminated
        action_type = body["eventType"]
        
        logging.getLogger().info("action_type is: " + str(action_type))
        
        # initiate the object storage client
        object_storage_client = oci.object_storage.ObjectStorageClient({}, signer=signer)
        
        # create or delete bucket based on event type
        if action_type == "com.oraclecloud.computeapi.launchinstance.end":
            create_bucket(object_storage_client, compartment_id, resource_name)
        elif action_type == "com.oraclecloud.computeapi.terminateinstance.end":
            delete_bucket(object_storage_client, compartment_id, resource_name)
        
    except (Exception, ValueError) as ex:
        logging.getLogger().error("There was an error: " + str(ex))

    return response.Response(
        ctx, response_data=json.dumps(
            {"message": "The function executed successfully"}),
        headers={"Content-Type": "application/json"}
    )


def create_bucket(object_storage_client, compartment_id, resource_name):
    logging.getLogger().info("Create bucket")
    try:
        # get the namespace (tenancy name)
        namespace = object_storage_client.get_namespace().data
        logging.getLogger().info("Namespace is: "+ str(namespace))
        
        # create the bucket
        create_bucket_response = object_storage_client.create_bucket(
            namespace,
            oci.object_storage.models.CreateBucketDetails(
                name=resource_name,
                compartment_id=compartment_id,
                public_access_type='ObjectRead',
                storage_tier='Standard',
                object_events_enabled=True
            )
        )
    except Exception as ex:
        logging.getLogger().error("There was an error creating the bucket: " + str(ex))
        
    logging.getLogger().info(f"Bucket {resource_name} was created")


def delete_bucket(object_storage_client, compartment_id, resource_name):
    logging.getLogger().info("Delete bucket")
    try:
        # get the namespace (tenancy name)
        namespace = object_storage_client.get_namespace().data
        
        # delete the bucket
        object_storage_client.delete_bucket(namespace, resource_name)
        
    except Exception as ex:
        logging.getLogger().error("There was an error deleting the bucket: " + str(ex))

    logging.getLogger().info(f"Bucket {resource_name} was deleted")




