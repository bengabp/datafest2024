# Datafest2024 - Team DataFusion
This repository contains code and assets for datafest 2024, datafusion team

#### `datasets` - files used to create data tables in supabase
#### `src/scripts` - data etl and generation scripts
#### `src/pipeline` - code for ai model

## Running data generation script
```bash
python3 -m src.scripts.generate_data
```

The 2 included notebooks contains code for data cleaning for training machine learning model
datafest-datathon-2024 notebook contains code for training machine learning model and student_academic_guardian.csv is dataset used for training model

## How we generated our data
Our data was generated completely using a custom written algorithm. 
First we generate the students, subjects, teachers and parents while maintaining a good model to model relationship.
Then we generate time_allocation data for each subject, in each class and for each term. Based on the time allocation and the income_level of their parents, we generate assessment data for different types of assessments including
exam, test and home_work. Based on that data we are able to determine specific trends and visualize that

## Visualization data
The /visualization directory contains files generated during visualization