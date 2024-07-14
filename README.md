# GIC DATA ENGINEERING ASSESSMENT<br/>

external_funds - Contains all .csv formatted funds report.<br/>

first - Approach for the first assessment.<br/>

second - Approach for the second assessment.<br/>

sql_schemas - Schemas for creating top performing funds in months and all-time<br/>

Steps-To-Load:<br/>
  1. Create PostgreSQL database.
  2. Adjust environment variables value in .env file.
  3. Run Database.py to load data into db for 1st & 2nd assessment (Please run only once to prevent duplicated inserts).
  4. Run all schemas .sql in schemas folder (master-reference-sql.sql and top-performing-funds-views.sql).
  5. Run ReportGenerator.py.
  6. Reports will be generated in reports folder.
  7. Views for Top Performing Equities for Funds (Monthly & All-Time) both are shown in the output. Views are retrieved from PostgreSQL Views.
  8. WRITE_TO_DB (Only String "TRUE" or "FALSE" is acceptable) can be set to toggle update generated report data to db via .env file (data can be access by querying price_difference table).

Gap-Required-For-Production:<br/>
  1. UnitTest to be completed.
  2. More robust PostgreSQL CRUD methods/class.
  3. Store server/db keys to more secured platform (AWS KMS).
  4. May use Docker for deployment (needs docker configurations).
  5. May need API integration for storing generated reports to AWS S3.
  6. Setup ETL pipeline to inject/load pre-requisited data Eg. (master-reference data or funds report data).
  7. CI/CD pipeline can be setup for continuos integration and deployment.

Answers-Questions:<br/>
  Q: Show the break with instrument data ref price vs fund price.<br/>
  A: ![image](https://github.com/user-attachments/assets/ec2d422c-fca5-4e06-aac1-2af6a57882fb)

  Q: Describe scalability approach for "N" numbers of fund reports in the future.<br/>
  A: Suggest to use AWS Lambda for horizontal scaling or AWS ECS to host application and utilizes the Auto Scaling ability.<br/>
  
  Q: SQL view/views which gives a report as to which was the best performing fund for equities every month and the cumulative profit/loss for that fund for equities.<br/>
  A1: Monthly Best Performing Funds and their cumulative profit/loss
  ![image](https://github.com/user-attachments/assets/f36bd0c4-19cf-4aeb-9ee1-e8dbfef1aeea)

  A2: All-Time Top Performing Funds<br/>
  ![image](https://github.com/user-attachments/assets/83fb4b52-89f6-438a-bb7d-054153002234)<br/>


  




