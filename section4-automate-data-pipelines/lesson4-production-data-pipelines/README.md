## Key Terms
In this lesson, we'll be learning about how to build maintainable and reusable pipelines in Airflow.
- Focus on elevating your DAGs to reliable, production-quality data pipelines.
- Extending airflow with custom plug-ins to create your own hooks and operators.
- How to design task boundaries so that we maximize visibility into our tasks.
- subDAGs and how we can actually reuse DAGs themselves.
- Monitoring in Airflow

#### Airflow Plugins
- Airflow was built with the intention of allowing its users to extend and customize its functionality through plugins. The most common types of user-created plugins for Airflow are Operators and Hooks. These plugins make DAGs reusable and simpler to maintain.
- To create custom operator, follow the steps:
    - Identify Operators that perform similar functions and can be consolidated
    - Define a new Operator in the plugins folder
    - Replace the original Operators with your new custom one, re-parameterize, and instantiate them.

#### Airflow Contrib
- Airflow has a rich and vibrant open source community. This community is constantly adding new functionality and extending the capabilities of Airflow. As an Airflow user, you should always check Airflow contrib before building your own airflow plugins, to see if what you need already exists.
- Operators and hooks for common data tools like Apache Spark and Cassandra, as well as vendor specific integrations for Amazon Web Services, Azure, and Google Cloud Platform can be found in Airflow contrib. If the functionality exists and its not quite what you want, that’s a great opportunity to add that functionality through an open source contribution.

#### Task Boundaries
- DAG tasks should be designed such that they are:
    - Atomic and have a single purpose
    - Maximize parallelism
    - Make failure states obvious
- Every task in your dag should perform only one job.
    - “Write programs that do one thing and do it well.” - Ken Thompson’s Unix Philosophy
- Benefits of Task Boundaries
    - Re-visitable: Task boundaries are useful for you if you revisit a pipeline you wrote after a 6 month absence. You'll have a much easier time understanding how it works and the lineage of the data if the boundaries between tasks are clear and well defined. This is true in the code itself, and within the Airflow UI.
    - Tasks that do just one thing are often more easily parallelized. This parallelization can offer a significant speedup in the execution of our DAGs.

#### SubDAGs
- Commonly repeated series of tasks within DAGs can be captured as reusable SubDAGs. Benefits include:
    - Decrease the amount of code we need to write and maintain to create a new DAG
    - Easier to understand the high level goals of a DAG
    - Bug fixes, speedups, and other enhancements can be made more quickly and distributed to all DAGs that use that SubDAG

#### Drawbacks of Using SubDAGs
- Limit the visibility within the Airflow UI
    - Abstraction makes understanding what the DAG is doing more difficult
    - Encourages premature optimization
- Common Questions
    - Can Airflow nest subDAGs? - Yes, you can nest subDAGs. However, you should have a really good reason to do so because it makes it much harder to understand what's going on in the code. Generally, subDAGs are not necessary at all, let alone subDAGs within subDAGs.

#### Pipeline Monitoring
Airflow can surface metrics and emails to help you stay on top of pipeline issues.
- SLAs
    - Airflow DAGs may optionally specify an SLA, or “Service Level Agreement”, which is defined as a time by which a DAG must complete. For time-sensitive applications these features are critical for developing trust amongst your pipeline customers and ensuring that data is delivered while it is still meaningful. Slipping SLAs can also be early indicators of performance problems, or a need to scale up the size of your Airflow cluster
- Emails and Alerts
    - Airflow can be configured to send emails on DAG and task state changes. These state changes may include successes, failures, or retries. Failure emails can allow you to easily trigger alerts. It is common for alerting systems like PagerDuty to accept emails as a source of alerts. If a mission-critical data pipeline fails, you will need to know as soon as possible to get online and get it fixed.
- Metrics
    - Airflow comes out of the box with the ability to send system metrics using a metrics aggregator called statsd. Statsd can be coupled with metrics visualization tools like Grafana to provide you and your team high level insights into the overall performance of your DAGs, jobs, and tasks. These systems can be integrated into your alerting system, such as pagerduty, so that you can ensure problems are dealt with immediately. These Airflow system-level metrics allow you and your team to stay ahead of issues before they even occur by watching long-term trends.

## Key Points
- Airflow Hooks: Hooks provide a reusable interface to external systems and databases. With hooks, you don’t have to worry about how and where to store the connection strings and secrets in your code.
- Data Lineage: The data lineage of a dataset describes the discrete steps involved in the creation, movement, and calculation of that dataset.
- Data Validation: Data Validation is the process of ensuring that data is present, correct & meaningful. Ensuring the quality of your data through automated validation checks is a critical step in building data pipelines at any organization.
- Database (Components of Airflow): Saves credentials, connections, history, and configuration Directed Acyclic Graphs (DAGs): DAGs are a special subset of graphs in which the edges between nodes have a specific direction, and no cycles exist. When we say “no cycles exist” what we mean is the nodes cant create a path back to themselves.
- Edges: The dependencies or relationships other between nodes. End Date: Airflow pipelines can also have end dates. You can use an end_date with your pipeline to let Airflow know when to stop running the pipeline. End_dates can also be useful when you want to perform an overhaul or redesign of an existing pipeline. Update the old pipeline with an end_date and then have the new pipeline start on the end date of the old pipeline.
- Logical partitioning: Conceptually related data can be partitioned into discrete segments and processed separately. This process of separating data based on its conceptual relationship is called logical partitioning. With logical partitioning, unrelated things belong in separate steps. Consider your dependencies and separate processing around those boundaries.
- Nodes: A step in the data pipeline process. Operators: Operators define the atomic steps of work that make up a DAG. Airflow comes with many Operators that can perform common operations. Schedule partitioning: Not only are schedules great for reducing the amount of data our pipelines have to process, but they also help us guarantee that we can meet timing guarantees that our data consumers may need.
- Scheduler (Components of Airflow:) orchestrates the execution of jobs on a trigger or schedule
- Schedules: Data pipelines are often driven by schedules which determine what data should be analyzed and when.
- Size Partitioning: Size partitioning separates data for processing based on desired or required storage limits. This essentially sets the amount of data included in a data pipeline run. Size partitioning is critical to understand when working with large datasets, especially with Airflow.
- Start Date: Airflow will begin running pipelines on the start date selected. Whenever the start date of a DAG is in the past, and the time difference between the start date and now includes more than one schedule intervals, Airflow will automatically schedule and execute a DAG run to satisfy each one of those intervals.
- Task Dependencies: Describe the tasks or nodes in the Airflow DAGs in the order in which they should execute
- Web Interface (Components of Airflow): provides a control dashboard for users and maintainers
- Work Queue (Components of Airflow): holds the state of running DAGS and Tasks
- Worker processes (Components of Airflow): that execute the operations defined in each DAG