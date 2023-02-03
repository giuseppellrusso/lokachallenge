# Case study - Loka

In this README I've written down the mental processes I did while writing the code.
First of all, I divided the problem into 3 main problems:

- Ingesting data
- Processing and modeling data
- Elaborate business-logic oriented solutions

### Ingestion

Since I couldn't access the mentioned S3 bucket, here I'm emulating the bucket behavior with a folder inside the container (the /data folder). The idea would be to read the data from this external S3 bucket and to store them into our S3 datalake in the rawest format as possible.

In particular, my datalake will have a raw layer structured in folders containing specific days data. In this folder, there will be present one json newline delimited file containing all the events of a specific day (for 35k rows per day this is definitely a good solution, if we had to handle much more data probably we would have needed a more structured solution).

### Processing and modeling

From the raw layer of the datalake we want to go to a more complex layer where data is processed, integrated and modeled and is already stored in a consumption-ready format. We will store this data inside of the Postgres SQL database.

I decided to build two basic dimension tables, which will define the two main entities of our data domain: vehicles and operating_periods. Here there will the most generic attributes of the two entities and some specific measurement which could help in collecting some first insights.
As for the whole set of measurements (the vehicle updates), I built a dedicated fact table connected to the two main dimension and show all the measurements.

To see the details of how I'm transforming the data, look at the code documentation.

### Marts layer

In the marts layer we store tables which will make querying the datawarehouse extremely easier for the business stakeholder in order to let them collect their insights.


## Technologies

As an orchestrator for the pipeline, Airflow is one of the best solution. With such a tool we can define the correct dependency between the tasks. Moreover, by ensuring that all of the designed tasks are atomic and idempotent, we can use Airflow to schedule our runs both on daily basis, but also for full refrehs or eventual historical reloads.
It also allows you to write some Python-based pipelines, which enables the using of libraries such as pandas, which is one of the most suitable solutions for cleaning and transforming raw data with such a volume as the one presented in this study case.
With Airflow and Python we can also easily interact with our datalake on S3 and our datawarehouse on Postgres. Also, the building of the marts layer is done with a SQL procedure in the datawarehouse, which is definitely the best tool to handle complex transformations which need to follow the business logic, which can become easily very complex.

## How to run the solution

- Inside the main folder create a virtual environment with python3 -m virtualenv venv/
- Activate it with source venv/bin/activate
- Install the requirements with pip3 install -r requirements_dev.txt
- Switch inside the docker folder
- Start the containers with use docker-compose up -d --build
- Access airflow at localhost:8080 (user airflow, password airflow)
- Run the dag (with standard configuration, which is a reload of the 1st of June 2019)
- Access the datawarehouse at localhost:5433 with user postgres and password postgres