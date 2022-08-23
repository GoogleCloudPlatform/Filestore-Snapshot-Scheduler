# GCP Filestore Snapshots Scheduler Solution

A cloud solution for automating a snapshots scheduling process for GCP
Filestore managed-service.

## Introduction

GCP Filestore Snapshots Scheduler Solution helps GCP Filestore users to
automate the snapshots scheduling process.
GCP Filestore Enterprise tier offers manual creation of snapshots.
This solution schedule snapshots creation operations based on user-defined
retention policy, configured in a JSON file.
The policy defines number of snapshots to preserve per Filestore instance,
while deleting the oldest one if the retention is reached.


## About the Solution

The solution is based on Python3.9 language.
The solution includes a few GCP resources:
1. GCP Filestore Instance Enterprise tier
2. GCP Cloud Scheduler
3. GCP Cloud Function


## Getting Started

Refer to this public tutorial.


## Capabilities

1. A single user-defined retention policy can be applied on multiple
Filestore instances.
2. Multiple user-defined retention policies can be applied on a single
Filestore instance.


## Known Limitations

1. Filestore Enterprise instance supports up to 240 snapshots.
2. The function only deletes a single snapshot when needed, even if there are
more scheduler snapshots than defined in the retention policy configuration file,
or if the retention policy configuration file is updated to keep fewer snapshots
than before.
3. Reducing the number of snapshots in the configuration file, will not result
in deleting multiple snapshots in the instance.
Instead, delete the redundant snapshots manually.


## Solution Code Components

* main.py
Used to validate the user inputs,
and calls other modules to keep the snapshot retention policy.
* filestore_instance.py
Used for managing a Filestore instance object,
to meet user-defined retention policy of snapshots.
This module uses the application default credentials
of the GCP Cloud Function ServiceAccount.
* __init__.py
A regular package of the FilestoreInstance object.
* requirements.txt
A file listing all the dependencies for a specific Python project.


## Troubleshooting

### GCP Cloud Scheduler Logs

1. Enter the GCP Cloud Scheduler dashboard
2. Locate the relevant scheduler job entry and click on its 3 dots button
under the Actions column.
3. Choose the 'View logs' option.

### GCP Cloud Function Logs

1. Enter the GCP Cloud Function dashboard
2. Locate the relevant function entry and click on its 3 dots button
under the Actions column.
3. Choose the 'View logs' option.


**This is not an officially supporting Google product.**
