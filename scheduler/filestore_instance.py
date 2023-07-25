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


"""GCP Filestore Instance Management Module.

Used for managing a Filestore instance object,
to meet user-defined retention policy of snapshots.
This module uses the application default credentials
of the GCP Cloud Function ServiceAccount.
"""

import logging
import socket
import time
import googleapiclient
from googleapiclient import discovery
import oauth2client
logging.basicConfig(
    format="%(asctime)s [%(levelname)s] %(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

# The service client library
SERVICE = "file"
# The API version
# Might change as new preview features launched
API_VERSION = "v1"
# Time pattern format. e.g: 20220303-153000
TIME_PATTERN = "%Y%m%d-%H%M%S"
# Filestore snapshot name prefix
SNAP_PREFIX = "sched-"
# Filestore tiers support snapshots feature.
# Refer https://cloud.google.com/filestore/docs/create-snapshots#supported_tiers
SUPPORTED_TIERS = ["ENTERPRISE","HIGH_SCALE_SSD"]
# Maximum number of snapshots per Filestore instance
# Refer https://cloud.google.com/filestore/docs/limits#number_of_snapshots
MAX_NUMBER_OF_SNAPSHOTS = 240
# Maximum number of attempts for snapshot creation/deletion
MAX_RETRIES = 3


def retry(func, retries=MAX_RETRIES, backoff=2):
  """A retry decorator.

  Calls a function and re-executes it if it failed.

  Args:
    func: the function to execute.
    retries: the maximum number of attempts.
    backoff: multiplier applied to delay between attempts.

  Returns:
    A retry decorator.
  """

  def retry_wrapper(*args):
    attempts = 0
    while attempts < retries:
      try:
        return func(*(args))
      except googleapiclient.errors.Error as err:
        logger.error("Attempt %i out of %i failed", attempts + 1, retries)
        log_gcp_api_err(err)
      except socket.timeout:
        logger.error("Attempt %i out of %i failed", attempts + 1, retries)
        logger.error("Timeout reached. Failed to complete operation.")
      sleep = backoff**(attempts + 1)
      logger.info("Waiting %i seconds before next retry", sleep)
      time.sleep(sleep)
      attempts += 1
    logger.error("Reached a maximum number of %i retries.", retries)

  return retry_wrapper


def log_gcp_api_err(err):
  logger.error("Error %s: %s", err.status_code, err.reason)


class InstanceNotFoundError(Exception):
  pass


class FilestoreInstance(object):
  """A Filestore instance object.

  An object built based on the user input and its GCP properties.

  Attributes:
    retention_policy: The retention policy name.
    url: The instance_path value taken from the user JSON input.
    name: The instance name taken from the url attribute.
    max_snapshots: The user input for number of snapshots to keep.
    project: Filestore client API project resource.
    location: Filestore client API location resource.
    operation: Filestore client API operation resource.
    instance: Filestore client API instance resource.
    snapshot: Filestore client API snapshot resource.
    filestore_instance_json: The instance details.
    snapshots: The instance list of snapshots.
    scheduler_snapshots: The instance list of snapshots part of the running
      retention_policy.
    oldest_sched_snapshot: The oldest snapshot in the scheduler_snapshots.
    tier: The Filestore instance tier type.
    state: The Filestore instance state.
  """

  def __init__(self, instance_data: dict[str:str],
               retention_policy: str) -> None:
    """Inits a FilestoreInstance object.

    Args:
      instance_data: The Filestore instance data taken from the user JSON input.
      retention_policy: The scheduler retention policy name.

    Raises:
      InstanceNotFoundError: The instance details were not received.
    """

    self.retention_policy = retention_policy
    self.url = instance_data.get("instance_path").lstrip("/").strip("/")
    self.name = get_resource_name(self.url)
    self.max_snapshots = int(instance_data.get("snapshots"))
    self.project = self._filestore_build().projects()
    self.location = self.project.locations()
    self.operation = self.location.operations()
    self.instance = self.location.instances()
    self.snapshot = self.instance.snapshots()
    self.filestore_instance_json = self._get_instance()
    if not self.filestore_instance_json:
      raise InstanceNotFoundError
    self.snapshots = self._list_snapshots()
    self.scheduler_snapshots = self.get_scheduler_snapshots_list()
    self.oldest_sched_snapshot = self.get_oldest_scheduler_snapshot()

  def _filestore_build(self) -> discovery.Resource:
    """Builds cloud Filestore API client.

    Returns:
      A Filestore API Resource
    """
    credentials = oauth2client.client.GoogleCredentials.get_application_default(
    )
    filestore_api_resource = discovery.build(
        SERVICE, API_VERSION, credentials=credentials, cache_discovery=False)
    return filestore_api_resource

  @retry
  def _get_instance(self) -> dict[str:str]:
    """Gets the details of a specific Filestore instance.

    Returns:
      A dict includes the Filestore instance details.
    """
    request = self.instance.get(name=self.url)
    response = request.execute()
    return response

  @retry
  def _get_operation(self, operation_url: str) -> dict[str:str]:
    """Gets the details of a specific Filestore operation.

    Args:
      operation_url: The operation url to get.

    Returns:
      A dict includes the Filestore operation details.
    """
    request = self.operation.get(name=operation_url)
    response = request.execute()
    return response

  @retry
  def _list_snapshots(self) -> list[dict[str:str]]:
    """Lists all snapshots for a specific Filestore instance.

    Returns:
      A list includes the Filestore instance's snapshots and their details.
    """
    request = self.snapshot.list(parent=self.url)
    response = request.execute()
    return response.get("snapshots", [])

  @retry
  def _create_snapshot(self) -> str:
    """Create a retention snapshot on the given Filestore instance.

    Returns:
      An indication if the operation is received successfully.
    """
    snap_name = f"{SNAP_PREFIX}{self.retention_policy}-{time.strftime(TIME_PATTERN)}"
    request = self.snapshot.create(parent=self.url, snapshotId=snap_name)
    response = request.execute()
    operation_url = response["name"]
    if operation_url:
      logger.info(
          "Snapshot creation of %s is running as part of %s",
          snap_name, get_resource_name(operation_url))
      return operation_url
    return None

  def _monitor_operation(self, operation_url: str) -> bool:
    """Check if the requested Filestore operation is completed successfully.

    Args:
      operation_url: The operation url to monitor.

    Returns:
      An indication if the operation is completed successfully.
    """
    logger.info(
        "Start monitoring operation %s.", get_resource_name(operation_url))
    logger.info("This might take a few minutes...")
    monitor_attempts = 8
    attempt = 1
    while attempt <= monitor_attempts:
      operation_details = self._get_operation(operation_url)
      if operation_details:
        if operation_details.get("done", False):
          if operation_details.get("response", False):
            logger.info(
                "Snapshot creation as part of %s is completed successfully.",
                get_resource_name(operation_url))
            return True
          if operation_details.get("error", False):
            err = operation_details["error"]
            logger.error("Error %i: %s", err["code"], err["message"])
            return False
        if attempt < monitor_attempts:
          time.sleep(2 ** attempt)
        attempt += 1
      else:
        logger.error("Could not receive the operation details")
        return False
    logger.error("Reached a maximum number of %i monitor retries.",
                 monitor_attempts)
    return False

  @retry
  def _delete_snapshot(self) -> bool:
    """Delete the oldest_sched_snapshot from the given Filestore instance.

    Returns:
      An indication if the operation is received successfully.
    """
    request = self.snapshot.delete(name=self.oldest_sched_snapshot)
    response = request.execute()
    operation_id = get_resource_name(response["name"])
    logger.info(
        "Snapshot deletion of %s is running as part of %s.",
        get_resource_name(self.oldest_sched_snapshot), operation_id)
    return bool(operation_id)

  def deletion_needed(self) -> bool:
    """Check if the Filestore instance retention policy is met and a snapshot should be deleted.

    Returns:
      An indication if a snapshot should be deleted for the Filestore instance
      retention policy.
    """
    if len(self.scheduler_snapshots) == self.max_snapshots:
      logger.info("A single snapshot should be deleted.")
      return True
    elif len(self.scheduler_snapshots) > self.max_snapshots:
      logger.warning(
          "The retention policy doesn't match the number of snapshots.")
      logger.info("Still going to delete a single snapshot.")
      return True
    else:
      logger.info(
          "The number of snapshots does not reach the retention policy.")
      logger.info("No need to delete snapshots.")
      return False

  def validate_instance_requirements(self) -> bool:
    """Validate if the Filestore instance meets requirements.

    The requirements are:
    1. Instance tier is supported.
    2. Instance does not reach the number of snapshot limitation.
    3. Instance is in READY state.

    Returns:
      An indication if the Filestore instance meets the above requirements.
    """
    if self.tier not in SUPPORTED_TIERS:
      logger.error(
          "Instance %s is not of %s tier.",
          self.name, ", ".join(SUPPORTED_TIERS))
      return False
    if len(self.snapshots) == MAX_NUMBER_OF_SNAPSHOTS:
      logger.error(
          "Instance %s reached maximum number of %i snapshots.",
          self.name, MAX_NUMBER_OF_SNAPSHOTS)
      return False
    if self.state != "READY":
      logger.error("Instance %s is not in a READY state.", self.name)
      return False
    return True

  def get_scheduler_snapshots_list(self) -> list[str]:
    """Filter the Filestore instance snapshots which are part of the retention policy.

    Returns:
      A filtered list of the retention policy READY snapshots only.
    """
    scheduler_snapshots_list = []
    for snapshot in self.snapshots:
      if f"{SNAP_PREFIX}{self.retention_policy}-" in snapshot["name"]:
        if snapshot["state"] == "READY":
          scheduler_snapshots_list.append(snapshot["name"])
    return scheduler_snapshots_list

  def get_oldest_scheduler_snapshot(self) -> str or None:
    """Detect the oldest snapshot out of the instance snapshot list.

    Returns:
      The oldest snapshot name or None if scheduler_snapshots_list is empty.
    """
    epoch_dict = {}
    if self.scheduler_snapshots:
      snapshot_string_len = len(SNAP_PREFIX + self.retention_policy) + 1
      for snapshot in self.scheduler_snapshots:
        snapshot_date = get_resource_name(snapshot)[snapshot_string_len:]
        epoch_dict[snapshot] = int(time.mktime(time.strptime(
            snapshot_date, TIME_PATTERN)))
      return min(epoch_dict, key=epoch_dict.get)
    else:
      return None

  def increment_retention(self) -> None:
    """Create a new snapshot for the requested Filestore instance and delete one if needed."""
    operation_url = self._create_snapshot()
    if operation_url:
      creation_completed = self._monitor_operation(operation_url)
      if creation_completed:
        logger.info(
            "%i %s scheduler snapshots are found.",
            len(self.scheduler_snapshots), self.retention_policy)
        logger.info(
            "The retention policy is set to %i snapshots.", self.max_snapshots)
        if self.deletion_needed():
          self._delete_snapshot()
      else:
        logger.error(
            "Snapshot creation failed. Not going to delete one either.")

  @property
  def tier(self) -> str or None:
    return self.filestore_instance_json.get("tier")

  @property
  def state(self) -> str or None:
    return self.filestore_instance_json.get("state")


def get_resource_name(resource_url: str) -> str:
  """Returns the GCP resource name, excluding the full URI."""
  return resource_url.split("/")[-1]

