# nlogn


the relay needs a ssl/tls key + cert
openssl req -x509 -nodes -newkey rsa:4096 -keyout key.pem -out cert.pem -sha256 -days 365 -nodes


## Pipelines

A pipeline is composed of one or more tasks and can have multiple stages.
Configuration files for pipeline are inspired from the way pipelines are
defined in `gitlab`. Pipelines support stages just like gitlab pipelines.
The tasks in each pipeline are slightly modified version of gitlab tasks
where of course not the full functionality is implemented here. One of 
the main moidifications over gitlab tasks is that the actions pipelines
execute are specified similar to how `ansible` does it, i.e by specifing
execution modules such as `ansible.builtin.shell`. This provides more
flexibiltiy in specifying custom functions to be executed.

## Tasks

A task defines the specifications of a certain component that needs to
be executed in a pipeline. A task when rendered into a fully defined
task by for example replacing variables an is in a state that can be
executed is refered to as a composed task. An instance of a task is
a job. This distinction is important since a task can specify that 
multiple jobs can be spawned with different parameter values if that
is defined in the task itself (this feature is not implemented yet).

## Variables

Tasks can define their own variables through an attribute. A variables
section can be defined in the pipeline too. These variables are scoped
at the full pipeline level and the values are shared within all the
tasks. Variables defined in the task have a higher precedance than the 
variables defined in the pipeline file.