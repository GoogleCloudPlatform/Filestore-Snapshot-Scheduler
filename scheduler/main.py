# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


"""The Snapshot Scheduler main module.

Used to validate the user inputs,
and calls other modules for keep the snapshot retention policy.

The solution code should be created as a GCP Cloud Function.
The function is triggered by a GCP Scheduler job which runs periodically per
a user-defined JSON file.
The JSON file consists a list of Filestore instances to run the function on.
"""

import logging
import filestore_instance
import jsonschema
import werkzeug

logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

# The retention policy name maximum length of characters
RETENTION_NAME_LENGTH = 50
# Maximum number of Filestore instances per JSON file
MAX_INSTANCES = 8


def validate_json_schema(request_json: dict[str:str]) -> bool:
  """Check if the JSON file user input meets the schema.

  Args:
    request_json: The JSON request user input.

  Returns:
    An indication if the JSON file user input meets the schema.
  """
  schema = {
      "type": "object",
      "properties": {
          "retention_policy": {
              "type": "string"
          },
          "instances": {
              "type": "array"
          },
      },
  }
  try:
    jsonschema.validate(instance=request_json, schema=schema)
  except jsonschema.exceptions.ValidationError as err:
    logger.error("JSON schema validation error. Details: %s", err.message)
    return False
  return True


def validate_json(request_json: dict[str:str]) -> bool:
  """Check if the JSON file user input is valid.

  Valid JSON file has the following mandatory keys:
  1. 'retention_policy'- a string for retention policy name.
  2. 'instances'- a list of Filestore instances.

  Args:
    request_json: The JSON request user input.

  Returns:
    An indication if the JSON file user input is valid.
  """
  if not validate_json_schema(request_json):
    return False
  retention_policy = request_json.get("retention_policy")
  if not retention_policy:
    logger.error(
        "JSON configuration file is missing a required key named 'retention_policy'."
    )
    return False
  if len(retention_policy) > RETENTION_NAME_LENGTH:
    logger.error(
        "Retention policy name must be less than or equal to %i characters long.",
        RETENTION_NAME_LENGTH)
    return False
  if not request_json.get("instances"):
    logger.error(
        "JSON configuration file is missing a required key named 'instances'.")
    return False
  if len(request_json.get("instances")) > MAX_INSTANCES:
    logger.error(
        "There are more than %i Filestore instances in the JSON configuration file.",
        MAX_INSTANCES)
    return False
  return True


def validate_instance_input(instance_data: dict[str:str]) -> bool:
  """Check if the Filestore instance user input is valid.

  Args:
    instance_data: The Filestore instance user input.

  Returns:
    An indication if the Filestore instance user input is valid.
  """
  if not instance_data.get("instance_path"):
    logger.warning(
        "Skip item %s which is missing a required key named 'instance_path'.",
        instance_data)
    return False
  instance_path = instance_data["instance_path"]
  requested_snapshots = instance_data.get("snapshots")
  if not requested_snapshots:
    logger.warning(
        "Skip %s which is a missing a required key named 'snapshots'.",
        instance_path)
    return False
  if int(requested_snapshots) <= 0:
    logger.error(
        "Skip %s which is requiring %s snapshots. The number of snapshots should be more than 0.",
        instance_path, requested_snapshots)
    return False
  if int(requested_snapshots) > filestore_instance.MAX_NUMBER_OF_SNAPSHOTS:
    logger.error(
        "Skip %s which is requiring %s snapshots. The maximum number of snapshots is %i.",
        instance_path, requested_snapshots,
        filestore_instance.MAX_NUMBER_OF_SNAPSHOTS
    )
    return False
  return True


def main(request):
  try:
    request_json = request.get_json(force=True)
  except werkzeug.exceptions.BadRequest:
    logger.error("Failed to load scheduler body. Please validate the format.")
    return "done with error"
  logger.info("Start job cycle")
  if not validate_json(request_json):
    return "done with error"
  retention_policy = request_json.get("retention_policy")
  for instance_data in request_json["instances"]:
    if validate_instance_input(instance_data):
      instance_path = instance_data["instance_path"]
      logger.info("Start executing function on instance %s.", instance_path)
      try:
        filer = filestore_instance.FilestoreInstance(instance_data,
                                                     retention_policy)
      except filestore_instance.InstanceNotFoundError:
        logger.error("Failed to retrieve Filestore instance details")
        logger.info("Finish executing function with error on instance %s.",
                    instance_path)
        continue
      if filer.validate_instance_requirements():
        filer.increment_retention()
      logger.info("Finish executing function on instance %s.", instance_path)
  logger.info("Finish job cycle")
  return "done"


if __name__ == "__main__":
  main(0)

