class Pipeline:
    pass

# get the stages
# find all the jobs for a certain stage
# identify the job that outputs out of the pipeline
# find the hidden stages that are defined to be used as 'extends'
# crawl the directory and see who includes who
# assembel the full pipelines
#   - find the unique pipelines
# for each pipeline:
#   - assembel and execution flow
#   - configure the parameters of the jobs
#       - which function gets called...etc..
#   - configure the objects that gets called by the agent in the main
#     loop or as the schedule function