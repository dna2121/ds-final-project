# Final Project
This final project is about Dockerize ETL Pipeline using ETL tools Airflow that extract Public API data from PIKOBAR, then load into MySQL (Staging Area) and finally aggregate the data and save into PostgreSQL.

## Project Flow
<img width="327" alt="Screenshot 2023-11-27 at 22 06 26" src="https://github.com/dna2121/ds-final-project/assets/80125535/26dd3972-5bd1-4bc4-873d-6bfe4437c8eb">

## Output - airflow
DAG:
<img width="1262" alt="Screenshot 2023-11-27 at 22 10 07" src="https://github.com/dna2121/ds-final-project/assets/80125535/b568c506-71ad-4828-9ae3-54a9f6ead1d3">


graph:
<img width="1262" alt="Screenshot 2023-11-27 at 22 10 52" src="https://github.com/dna2121/ds-final-project/assets/80125535/23f36992-1bc0-4b7a-99fe-029b78290dac">

## DWH - dimension table
dim_province

<img width="360" alt="Screenshot 2023-11-27 at 21 28 46" src="https://github.com/dna2121/ds-final-project/assets/80125535/a92b435f-ce4a-4cfc-9662-16f867d4b078">


dim_case

<img width="589" alt="Screenshot 2023-11-27 at 21 26 53" src="https://github.com/dna2121/ds-final-project/assets/80125535/726a727d-8161-4ad5-bd87-d66f1f72970c">


dim_district

<img width="496" alt="Screenshot 2023-11-27 at 21 28 15" src="https://github.com/dna2121/ds-final-project/assets/80125535/d33b4de0-6bb7-4d05-87d6-a8d9e5c282b5">


## DWH - fact table
province

<img width="571" alt="Screenshot 2023-11-27 at 21 35 08" src="https://github.com/dna2121/ds-final-project/assets/80125535/ea2d03bd-f4be-4b58-a2a0-fc1656ff7871">


district

<img width="559" alt="Screenshot 2023-11-27 at 21 33 40" src="https://github.com/dna2121/ds-final-project/assets/80125535/74889e9b-81cd-4769-b317-af856a323b33">

