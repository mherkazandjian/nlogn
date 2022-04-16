# Pipelines

A pipeline is composed of one or more stages.

The stages can be specified at the top of the `.yml` file as such:

```yaml
stages:
  - disks
  - nfs
  - ceph
```

A stage is just a bunch of tasks.

A pipeline outputs the status of every task.
The output is a dictionary (can be dumped in debug mode to stdout or logged as json).

## Task/Job

A task or a job is used interchangebly. A job is just an alias for a task.
A task can have sub-tasks.

A task does the following:

   - Performs any action. 
   - Does not necessarily publish data.
   - Defines the `output_columns` key is expected to publish data.
   - Is considered complete and hence perform an action if an execution
     module is defined in it.
   - A task can `extend` another task by defining the `extends` keyword.

The output is a dictionary (can be dumped in debug mode to stdout or logged as json).

### Task execution modules

  - nlogn.builtin.shell
  - nlogn.builtin.df
  - nlogn.builtin.ping

## Outputs

The output is done through an output object that can be through one or more channels:
The following is the list of channels:

  - stdout
  - file
      - text
      - pickle (to /dev/shm/unique_filename to communicate with a consumer process)
  - logger
  - socket
  - websocket
  - post/put

each output can be appended with a checksum.