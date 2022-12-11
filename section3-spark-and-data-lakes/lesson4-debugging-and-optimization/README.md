## Key Points
- Why debugging Spark is hard
- Debugging code errors
- Debugging data errors
- How to use Accumulators
- How to use Spark Broadcast variables
- Using the Spark WebUI
- Understanding data skewness
- Optimizing for data skewness

#### Debugging Spark is harder on Standalone mode
- Previously, we ran Spark codes in the local mode where you can easily fix the code on your laptop because you can view the error in your code on your local machine.
- For Standalone mode, the cluster (group of manager and executor) load data, distribute the tasks among them and the executor executes the code. The result is either a successful output or a log of the errors. The logs are captured in a separate machine than the executor, which makes it important to interpret the syntax of the logs - this can get tricky.
- One other thing that makes the standalone mode difficult to deploy the code is that your laptop environment will be completely different than AWS EMR or other cloud systems. As a result, you will always have to test your code rigorously on different environment settings to make sure the code works.

#### Introduction to Errors
Let's say you've written your Spark program but there's a bug somewhere in your code.
- The code seems to work just fine, but Spark uses lazy evaluation.
- Spark waits as long as it can before running your code on data, so you won't discover an error right away.
- This can be very different from what you've seen in traditional Python.

#### Tips for Debugging Code Errors
Typos are probably the simplest errors to identify
- A typo in a method name will generate a short attribute error
- An incorrect column name will result in a long analysis exception error
- Typos in variables can result in lengthy errors
- While Spark supports the Python API, its native language is Scala. That's why some of the error messages are referring to Scala, Java, or JVM issues even when we are running Python code.
- Whenever you use collect, be careful how much data are you collecting
- Mismatched parentheses can cause end-of-file (EOF) errors that may be misleading

#### Data Errors
When you work with big data, some of the records might have missing fields or have data that's malformed or incorrect in some other unexpected way.
- If data is malformed, Spark populates these fields with nulls.
- If you try to do something with a missing field, nulls remain nulls

#### Debugging Code
If you were writing a traditional Python script, you might use print statements to output the values held by variables. These print statements can be helpful when debugging your code, but this won't work on Spark. Think back to how Spark runs your code.
- You have a driver node coordinating the tasks of various worker nodes.
Code is running on those worker nodes and not the driver, so print statements will only run on those worker nodes.
- You cannot directly see the output from them because you're not connected directly to them.
- Spark makes a copy of the input data every time you call a function. So, the original debugging variables that you created won't actually get loaded into the worker nodes. Instead, each worker has their own copy of these variables, and only these copies get modified.
To get around these limitations, Spark gives you special variables known as accumulators. Accumulators are like global variables for your entire cluster.

#### What are Accumulators?
- As the name hints, accumulators are variables that accumulate. Because Spark runs in distributed mode, the workers are running in parallel, but asynchronously. For example, worker 1 will not be able to know how far worker 2 and worker 3 are done with their tasks. With the same analogy, the variables that are local to workers are not going to be shared to another worker unless you accumulate them. Accumulators are used for mostly sum operations, like in Hadoop MapReduce, but you can implement it to do otherwise.

#### What is Spark Broadcast?
- Spark Broadcast variables are secured, read-only variables that get distributed and cached to worker nodes. This is helpful to Spark because when the driver sends packets of information to worker nodes, it sends the data and tasks attached together which could be a little heavier on the network side. Broadcast variables seek to reduce network overhead and to reduce communications. Spark Broadcast variables are used only with Spark Context.

#### Transformations and Actions
There are two types of functions in Spark:
- Transformations
- Actions

Spark uses lazy evaluation to evaluate RDD and dataframe. Lazy evaluation means the code is not executed until it is needed. The action functions trigger the lazily evaluated functions. For example,
```
df = spark.read.load("some csv file")
df1 = df.select("some column").filter("some condition")
df1.write("to path")
```
- In this code, select and filter are transformation functions, and write is an action function.
- If you execute this code line by line, the second line will be loaded, but you will not see the function being executed in your Spark UI.
- When you actually execute using action write, then you will see your Spark program being executed:
    - select --> filter --> write chained in Spark UI
    - But you will only see Writeshow up under your tasks.
- This is significant because you can chain your RDD or dataframe as much as you want, but it might not do anything until you actually trigger with some action words. And if you have lengthy transformations, then it might take your executors quite some time to complete all the tasks.

#### Spark WebUI
Spark has a built-in user interface that you can access from your web browser. This interface, known as the web UI, helps you understand what's going on in your cluster without looking at individual workers.
- The web UI provides the current configuration for the cluster which can be useful for double-checking that your desired settings went into effect.
- The web UI shows you the DAG, the recipe of steps for your program. You'll see the DAG broken up into stages, and within each stage, there are individual tasks. Tasks are the steps that the individual worker nodes are assigned. In each stage, the worker node divides up the input data and runs the task for that stage.
- The web UI only shows pages related to current Spark jobs that are running. For example, you won't see any pages related to other libraries like Spark Streaming unless you are also running a streaming job.

#### Connecting to the Spark UI
It's useful to have several ways to connect data with a machine. When you transfer private data through a secure shell known as SSH, you follow a different protocol than when transferring public HTML data for a webpage using HTTP.
- Use port 22 for SSH and port 80 for HTTP to indicate that you're transferring data using different networking protocols.
- We usually connect to Jupyter notebooks on port 8888.
- Another important port is 4040 which shows active Spark jobs.
- The most useful port for you to memorize is the web UI for your master node on port 8080. The web UI on 8080 shows the status of your cluster, configurations, and the status of any recent jobs.

#### Introduction to Code Optimization
- Up to now, the issues you've debugged have been traditional coding difficulties, problems with the Spark syntax or settings. Even when there were mistakes in the data like an error and a few lines of the logs, these are similar to the input errors you might run into while using traditional Python.
- You'll now debug some issues that are unique to working with big data. For these issues, your code would work fine on small or medium datasets but will fail when you try to scale up the data. In these cases, you'll have to optimize your code to find a better approach. Let's check out a few common issues and learn how to solve them.

#### Introduction to Data Skew
- Skewed data means due to non-optimal partitioning, the data is heavy on few partitions. This could be problematic.
- Imagine you’re processing a dataset, and the data is distributed through your cluster by partition.
    - In this case, only a few partitions will continue to work, while the rest of the partitions do not work.
    - If you were to run your cluster like this, you will get billed by the time of the data processing, which means you will get billed for the duration of the longest partitions working.
    - We would like to re-distribute the data in a way so that all the partitions are working.
- In order to look at the skewness of the data:
    - Check for MIN, MAX and data RANGES
    - Examine how the workers are working
    - Identify workers that are running longer and aim to optimize it.

#### So how do we solve skewed data problems?
The goal is to change the partitioning columns to take out the data skewness (e.g., the year column is skewed).
1. Use Alternate Columns that are more normally distributed:
- E.g., Instead of the year column, we can use Issue_Date column that isn’t skewed.
2. Make Composite Keys:
- For e.g., you can make composite keys by combining two columns so that the new column can be used as a composite key. For e.g, combining the Issue_Date and State columns to make a new composite key titled Issue_Date + State. The new column will now include data from 2 columns, e.g., 2017-04-15-NY. This column can be used to partition the data, create more normally distributed datasets (e.g., distribution of parking violations on 2017-04-15 would now be more spread out across states, and this can now help address skewness in the data.
3. Partition by number of Spark workers:
- Another easy way is using the Spark workers. If you know the number of your workers for Spark, then you can easily partition the data by the number of workers df.repartition(number_of_workers) to repartition your data evenly across your workers. For example, if you have 8 workers, then you should do df.repartition(8) before doing any operations.

#### Optimizing skewness
Let’s recap two different ways to solve the skewed data problem:
- Assign a new, temporary partition key before processing any huge shuffles.
- The second method is using repartition.

## Key Terms
- Debugging is hard
- Code errors
- Data errors
- How to use Accumulators
- How to use Spark Broadcast variables
- Understanding data skewness
- Optimizing for data skewness











