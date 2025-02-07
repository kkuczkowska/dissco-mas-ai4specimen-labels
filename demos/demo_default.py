import json
import logging
import os
from typing import Dict, List, Any

import requests
from requests.auth import HTTPBasicAuth
from kafka import KafkaConsumer, KafkaProducer

from annotation import data_model as ods

logging.basicConfig(format='%(asctime)s - %(message)s', level=logging.INFO)

"""
This is a template for a simple MAS that calls an API and sends a list of annotations back to DiSSCo. 
"""


def start_kafka() -> None:
    """
    Start a kafka listener and process the messages by unpacking the image.
    When done it will republish the object, so it can be validated and stored by the processing service
    """
    consumer = KafkaConsumer(os.environ.get('KAFKA_CONSUMER_TOPIC'),
                             group_id=os.environ.get('KAFKA_CONSUMER_GROUP'),
                             bootstrap_servers=[
                                 os.environ.get('KAFKA_CONSUMER_HOST')],
                             value_deserializer=lambda m: json.loads(
                                 m.decode('utf-8')),
                             enable_auto_commit=True)
    producer = KafkaProducer(
        bootstrap_servers=[os.environ.get('KAFKA_PRODUCER_HOST')],
        value_serializer=lambda m: json.dumps(m).encode('utf-8'))
    for msg in consumer:
        try:
            logging.info(f"Received message: {str(msg.value)}")
            json_value = msg.value
            # Indicates to DiSSCo the message has been received by the mas and the job is running.
            # DiSSCo then informs the user of this development
            ods.mark_job_as_running(job_id=json_value.get('jobId'))
            digital_object = json_value.get('object')
            annotations = map_result_to_annotation(digital_object)
            event = ods.map_to_annotation_event(annotations,
                                                json_value['jobId'])
            logging.info(f"Publishing annotation event: {json.dumps(event)}")
            publish_annotation_event(event, producer)
        except Exception as e:
            logging.error(f"Failed to publish annotation event: {e}")


def build_query_string(digital_object: Dict[str, Any]) -> str:
    """
    Builds the query based on the digital object. Fill in your API call here
    :param digital_object: Target of the annotation
    :return: query string to some example API
    """
    access_uri = digital_object.get("ac:accessURI")
    return f"https://n8n.svc.gbif.no/webhook/9fa39dd6-63ea-4ed8-b4e1-904051e8a41a/?uri={access_uri}"  # Use your API here


def publish_annotation_event(annotation_event: Dict[str, Any],
                             producer: KafkaProducer) -> None:
    """
      Send the annotation to the Kafka topic
      :param annotation_event: The formatted list of annotations
      :param producer: The initiated Kafka producer
      :return: Will not return anything
      """
    logging.info(f"Publishing annotation: {str(annotation_event)}")
    producer.send(os.environ.get('KAFKA_PRODUCER_TOPIC'), annotation_event)


def map_result_to_annotation(digital_object: Dict[str, Any]) -> List[
    Dict[str, Any]]:
    """
    Given a target object, computes a result and maps the result to an openDS annotation
    :param digital_object: the target object of the annotation
    :return: List of annotations
    """
    # Your query here
    query_string = build_query_string(digital_object)
    timestamp = ods.timestamp_now()
    # Run API call and compute value(s) of the annotation
    oa_values = run_api_call(timestamp, query_string)
    annotations = list()
    '''
    It is up to the developer to determine the most appropriate selector. A Class 
    Selector is used if the annotation targets a whole class (or entire specimen), 
    a Field Selector is used if the annotation targets a specific field, and a Fragment
    Selector is used if the annotation targets a Region of Interest (media only). 
    Templates for all 3 selectors are provided. 
    '''
    selector = ods.build_class_selector("$ods:hasAssertion[0]")
    selector_entity_relationship = ods.build_class_selector(
        "$ods:hasEntityRelationship[0]")
    selector_field = ods.build_field_selector('$ods:topicDomain')

    # Make an annotation for each oa:value produced
    for value in oa_values:
        annotations.append(
            ods.map_to_annotation(timestamp, value, selector_assertion,
                              digital_object[ods.ODS_ID],
                              digital_object[ods.ODS_TYPE], query_string))
    # annotations.append(
    #     ods.map_to_annotation(timestamp, oa_values[0], selector_assertion,
    #                           digital_object[ods.ODS_ID],
    #                           digital_object[ods.ODS_TYPE], query_string))
    # annotations.append(
    #     ods.map_to_annotation(timestamp, oa_values[1],
    #                           selector_entity_relationship,
    #                           digital_object[ods.ODS_ID],
    #                           digital_object[ods.ODS_TYPE], query_string))
    # annotations.append(
    #     ods.map_to_annotation(timestamp, oa_values[2], selector_field,
    #                           digital_object[ods.ODS_ID],
    #                           digital_object[ods.ODS_TYPE], query_string))

    return annotations


def run_api_call(timestamp: str, query_string: str) -> List[Dict[str, Any]]:
    """
    Run API call or performs some computation on the target object
    :param digital_object: Object (digital specimen or media) adhering to openDS
    standard. This may be a digital specimen or digital media object.
    :return: Value of the annotation (maps to oa:value)
    :raises: request exception on failed API call
    """
    try:
        auth = HTTPBasicAuth(os.environ.get("API_USER"), os.environ.get("API_PASSWORD"))
        response = requests.get(query_string, auth=auth)
        response.raise_for_status()
    except requests.RequestException as e:
        logging.error(f"API call failed: {e}")
        raise requests.RequestException

    response_json = json.loads(response.content)
    '''
    It is up to the MAS developer to determine the best format for the value of the annotation.
    It may be appropriate to send an annotation as an Entity Relationship, if it describes
    a relationship to an external resource, or an Assertion, if the mas has performed 
    some computation or measurement. It may also map to the Georeference class, examples
    of which can be found on our demo GitHub page: https://github.com/DiSSCo/demo-enrichment-service-image/tree/main/osm-georeferencing
    '''
    # Return this if response is a computation or measurement
    # assertion = ods.map_to_assertion(timestamp, 'pixelWidthX',
    #                                  response_json['pixelWidthX'], 'pixels')
    # Return this if response is an entity relationship
    # entity_relationship = ods.map_to_entity_relationship(
    #     'hasRelatedResourceIdentifier', response_json['identifier'], timestamp)
    # Return something else - such as the raw response - if the response can not be structured in another way (not recommended)

    '''
    It is possible your MAS produces multiple annotations - this is supported. 
    One annotation can be created for each oa:value computed in this step
    '''
    return [response_json]


def run_local(dig_object_url):
    digital_object = requests.get(dig_object_url)
    annotations = map_result_to_annotation(digital_object)
    event = ods.map_to_annotation_event(annotations, "abcd")
