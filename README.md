# nlogn


the relay needs a ssl/tls key + cert
openssl req -x509 -nodes -newkey rsa:4096 -keyout key.pem -out cert.pem -sha256 -days 365 -nodes


## pipelines

A pipeline is composed of one or more tasks and can have multiple stages.
Configuration files for pipeline are inspired from the way pipelines are
defined in `gitlab`. Pipelines support stages just like gitlab pipelines.
The tasks in each pipeline are slightly modified version of gitlab tasks
where of course not the full functionality is implemented here. One of 
the main moidifications over gitlab tasks is that the actions pipelines
execute are specified similar to how `ansible` does it, i.e by specifing
execution modules such as `ansible.builtin.shell`. This provides more
flexibiltiy in specifying custom functions to be executed.