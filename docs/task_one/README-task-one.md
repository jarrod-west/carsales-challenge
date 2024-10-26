# Data Platforms & Engineering Homework Assignment
Thank you for taking the time look at this assignment. We've tried to create a scenario that reflects the sorts of challenges you'll be working on here. **It is important that you read the context and instructions that follow**. Enjoy!

## About the carsales Data team
Data at carsales is at an exciting part of its journey that reflects carsales' competitive advantage from its data. We are known for being the number #1 Digital Marketplace for buying and selling vehicles, and our data is key to giving our customers an edge with data.

## This problem
carsales' dealer facing teams are at the heart of helping our dealer customers make the most of their subscription to carsales solutions. One area our dealer account teams often help dealer customers with is assisting them with meeting their payment obligations, within payment terms. Our teams intuitively understand how likely our customers to pay their invoices on time, however this exercise is mostly manually done. We are considering providing support capabilities to help all teams use data to more systematically identify and prioritise customers to speak to about invoice payments.


### The Data
The DBAs have delivered extracts from 4 tables from the production databases:

- **accounts.csv**: Contains account information for our customers
- **skus.csv**: Contains the details about our product lines
- **invoices.csv**: Contains the invoices issued to each account
- **invoice_line_items.csv**: Contains the details of the SKUs sold on each invoice

The data can be found in the `task_one/data_extracts` folder in this repository.

### Task One: Building an ETL pipeline to support our Analytics team
You've been working closely with the Analysts in the team to understand what features might be predictive for late invoice payment. Now, the Analysts have requested that a dataset be produced on a regular basis. As part of your response, please add to the repository the code and documentation that:

- Uses Spark (or an equivalent MapReduce / Big Data framework) to transform the raw data into the features required by the scientists
- Tests your ETL job using the CSV extracts provided
- Highlights any intermediate data models you might use to store the data and why
- Highlights any design choices you've made, as well as scaling considerations

Additional details related to the task are provided in the `task_one` folder.

### Task Two: System Design
Having written the ETL job to do the data transformation required, we now need to design the overall system architecture that will allow the Data Team to operationalise the ETL job and maintain it as a long-lived production system. As part of your response, please add to the repository the documentation that:

- Shows the architecture of a production system that could run your (and similar) ETL jobs, before exposing the resulting datasets to the various consumers.
- Indicates how your architecture would evolve to support near real-time updating of data as the souce engineering team enables CDC.
- Highlights any key architectural decisions made, and explains them.

Additional details related to the task are provided in the `task_two` folder.