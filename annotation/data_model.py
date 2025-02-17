import logging
from datetime import datetime, timezone
from typing import Dict, Any, List
import json
import os
import requests

ODS_TYPE = "ods:type"
AT_TYPE = "@type"
ODS_ID = "ods:ID"
AT_ID = "@id"
MAS_ID = os.environ.get('MAS_ID')
MAS_NAME = os.environ.get('MAS_NAME')


def timestamp_now() -> str:
    """
    Create a timestamp in the correct format
    :return: The timestamp as a string
    """
    timestamp = str(
        datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f"))
    timestamp_cleaned = timestamp[:-3]
    timestamp_timezone = timestamp_cleaned + 'Z'
    return timestamp_timezone


def mark_job_as_running(job_id: str):
    """
    Calls DiSSCo's RUNNING endpoint to inform the system that the message has
    been received by the MAS. Doing so will update the status of the job to
    "RUNNING" for any observing users.
    :param job_id: the job id from the kafka message
    """
    query_string = f"{os.environ.get('RUNNING_ENDPOINT')}/{MAS_ID}/{job_id}/running"
    requests.get(query_string)


def build_agent() -> Dict[str, Any]:
    """
    Builds agent based on MAS ID and name
    :return: Agent object
    """
    return {
        AT_ID: f"https://hdl.handle.net/{MAS_ID}",
        AT_TYPE: 'as:Application',
        'schema:name': MAS_NAME,
        'ods:hasIdentifier': [
            {
                AT_TYPE: "ods:Identifier",
                'dcterms:title': 'handle',
                'dcterms:identifier': f"https://hdl.handle.net/{MAS_ID}"
            }
        ]
    }


def map_to_assertion(timestamp: str, measurement_type: str,
                     measurement_value: str, measurement_unit: str,
                     assertion_protocol: str, protocol_id: str) -> Dict[str, Any]:
    """
    Maps the result of some computation to an Assertion object
    :param timestamp: timestamp of the annotation
    :param measurement_type: The nature of the measurement, fact, characteristic, or assertion.
    :param measurement_value: The value of the measurement, fact, characteristic, or assertion.
    :param measurement_unit: Recommended best practice is to use a controlled vocabulary such as the Ontology of Units of Measure
    :param assertion_protocol: The protocol used to make the assertion
    :param protocol_id: The ID of the protocol used to make the assertion
    :return: Formatted assertion object
    """
    return {
        AT_TYPE: 'ods:Assertion',
        'dwc:measurementDeterminedDate': timestamp,
        'dwc:measurementType': measurement_type,
        'dwc:measurementValue': measurement_value,
        'ods:AssertionByAgent': build_agent(),
        'dwc:measurementUnit': measurement_unit,
        'ods:assertionProtocol': assertion_protocol,
        'ods:assertionProtocolID': protocol_id
    }


def map_to_entity_relationship(relationship_type: str, resource_id: str,
                               timestamp: str) -> Dict[str, Any]:
    """
    Maps the result of some search to an Entity Relationship object
    :param relationship_type: Maps to dwc:relationshipOfResource
    :param resource_id: Id of related resource, maps to dwc:relatedResourceID and ods:relatedResourceURI
    :param timestamp: timestamp of ER creation
    :return: formatted Entity relationship annotation
    """
    return {
        AT_TYPE: 'ods:EntityRelationship',
        'dwc:relationshipOfResource': relationship_type,
        'dwc:relatedResourceID': resource_id,
        'ods:relatedResourceURI': resource_id,
        'dwc:relationshipEstablishedDate': timestamp,
        'ods:RelationshipAccordingToAgent': build_agent()
    }


def map_to_annotation_event(annotations: List[Dict[str, Any]], job_id: str) -> \
Dict[str, Any]:
    """
    Maps to annotation event to be sent to DiSSCo
    :param annotations: List of annotations produced by DiSSCo
    :param job_id: Job ID sent to MAS
    :return: annotation event
    """
    return {
        'annotations': annotations,
        'jobId': job_id
    }


def map_to_annotation_event_batch(annotations: List[Dict[str, Any]],
                                  job_id: str,
                                  batch_metadata: List[Dict[str, Any]]) -> \
Dict[str, Any]:
    """
    Builds annotation event with batch metadata to send to DiSSCo
    :param annotations: Annotations produced by MAS
    :param job_id: Job ID sent to MAS
    :param batch_metadata: List of batch metadata. Each entry corresponds to an annotation. Ignored if empty
    :return: annotation event
    """
    event = {
        'annotations': annotations,
        'jobId': job_id
    }
    if batch_metadata:
        event['batchMetadata'] = batch_metadata
    return event


def map_to_annotation(timestamp: str,
                      oa_value: Dict[str, Any], oa_selector: Dict[str, Any], target_id: str,
                      target_type: str,
                      dcterms_ref: str) -> Dict[str, Any]:
    """
    Map the result of the API call to an annotation
    :param timestamp: A formatted timestamp of the time the annotation was initialized
    :param oa_value: Value of the body of the annotation, the result of the computation
    :param oa_selector: selector of this annotation
    :param target_id: ID of target maps to ods:ID
    :param target_type: target Type, maps to ods:type
    :param dcterms_ref: maps tp dcterms:references, http://purl.org/dc/terms/references
    :return: Returns a formatted annotation Record
    """
    annotation = {
        AT_TYPE: 'ods:Annotation',
        'oa:motivation': 'ods:adding',
        'dcterms:creator': build_agent(),
        'dcterms:created': timestamp,
        'oa:hasTarget': {
            ODS_ID: target_id,
            AT_ID: target_id,
            ODS_TYPE: target_type,
            AT_TYPE: target_type,
            'oa:hasSelector': oa_selector,
        },
        'oa:hasBody': {
            AT_TYPE: 'oa:TextualBody',
            'oa:value': [json.dumps(oa_value)],
            'dcterms:references': dcterms_ref,
        },
    }
    return annotation


def build_class_selector(oa_class: str) -> Dict[str, str]:
    """
    Builds Selector for annotations that affect classes
    :param oa_class: The full jsonPath of the class being annotate
    :return: class selector object
    """
    return {
        AT_TYPE: 'ods:ClassSelector',
        'ods:class': oa_class,
    }


def build_field_selector(ods_field: str) -> Dict[str, str]:
    """
    A selector for an individual field.
    :param ods_field: The full jsonPath of the field being annotated
    :return: field selector object
    """
    return {
        AT_TYPE: 'ods:FieldSelector',
        'ods:field': ods_field
    }


def build_fragment_selector(bounding_box: Dict[str, int], width: int,
                            height: int) -> Dict[str, Any]:
    """
    A selector for a specific Region of Interest (ROI). ROI is expressed in the AudioVisual Core
    standard: https://ac.tdwg.org/termlist/. This selector type is only applicable on media objects.
    :param bounding_box: object containing the bounding box of the ROI, in the following format,
    where values are expressed as pixels:
    {
        'x_min': 98,
        'y_min': 104,
        'x_max': 429,
        'y_max': 345
    },
    :param width: The width of the image, used to calculate the ROI
    :param height: the height of the image, used to calculate the ROI
    :return: Formatted fragment selector
    :raises: ValueError: if width or height is zero
    """
    if width <= 0 or height <= 0:
        raise ValueError(
            f"Invalid dimensions: width ({width}) and height ({height}) must be greater than zero.")

    return {
        AT_TYPE: 'oa:FragmentSelector',
        'dcterms:conformsTo': 'https://www.w3.org/TR/media-frags/',
        'ac:hasROI': {
            'ac:xFrac': bounding_box['x_min'] / width,
            'ac:yFrac': bounding_box['y_max'] / height,
            'ac:widthFrac': (bounding_box['x_max'] -
                             bounding_box['x_min']) / width,
            'ac:heightFrac': (bounding_box['y_max']
                              - bounding_box['y_min']) / height
        }
    }
