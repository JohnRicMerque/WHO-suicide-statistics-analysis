![Cover Image](cover.jpg)

# WHO Suicide Statistics Analysis

## Project Overview

This project involves analyzing the WHO Suicide Statistics dataset using PySpark. The analysis aims to identify patterns and trends in suicide rates based on various demographic factors such as age, gender, and geographic location, specifically focusing on the Philippines. The insights gained are intended to support the development of evidence-based interventions and mental health initiatives.

## Dataset Description

The dataset contains historical suicide data from 1979 to 2016, categorized by country, year, age group, and gender. It is sourced from the World Health Organization (WHO).

## Objectives

1. Identify high-risk groups by demographic analysis (e.g., age, gender).
2. Monitor trends over time to understand underlying causes.
3. Highlight insights to somehow reveal the effectiveness of public mental health interventions in the Philippines.

## Data Dictionary

- **Country**: Country name
- **Year**: Year of the record
- **Age Group**: Age group of the population
- **Gender**: Gender of the population
- **Suicide Rate**: Number of suicides per 100,000 population

## Data Processing Steps

### Using RDD (Resilient Distributed Datasets)

1. **Header Removal**:
   - **Transformations**: `filter()`
   - **Actions**: `first()`, `count()`

2. **Transformation to Dictionary**:
   - **Transformations**: `map()`, `split()`
   - **Actions**: `take()`

3. **Filtering Philippine Data**:
   - **Transformations**: `filter()`
   - **Actions**: `collect()`

4. **Removing Null Values**:
   - **Transformations**: `filter()`
   - **Actions**: `count()`

5. **Grouping by Age**:
   - **Transformations**: `flatMap()`, `map()`, `groupBy()`, `combineByKey()`
   - **Actions**: `collect()`

6. **Sorting by Suicide Rate**:
   - **Transformations**: `sortBy()`
   - **Actions**: `collect()`

### Using DataFrame and SQL

1. **Removing Null Values**:
   - Function: `dropna()`

2. **Filtering Philippine Data**:
   - Function: `filter()`

3. **Adding Suicide Rate Column**:
   - Function: `withColumn()`

4. **Selecting Distinct Age Groups**:
   - Functions: `select()`, `distinct()`

5. **Grouping by Age and Ordering by Suicide Rate**:
   - Functions: `groupBy()`, `orderBy()`

6. **Grouping by Gender and Ordering by Suicide Rate**:
   - Functions: `groupBy()`, `orderBy()`
   - Visualization: Seaborn Bar Plot

7. **Grouping by Year and Ordering by Ascending Years**:
   - Functions: `groupBy()`, `orderBy()`
   - Visualization: Seaborn Line Plot

## Key Insights

1. **Highest Suicide Rate by Age Group**: Filipinos aged 75+ have the highest suicide rate.
2. **Gender Comparison**: Filipino men are approximately three to four times more likely to commit suicide than Filipino women.
3. **Yearly Trends**:
   - Highest number of suicides: 2011
   - Lowest number of suicides: 1992
   - Highest suicide rate: 2011
   - Lowest suicide rate: 1992

## Reflections

- **Data Processing**: RDDs require meticulous data cleaning and are powerful but complex to manage. DataFrames are more intuitive and efficient for handling large datasets.
- **Insights**: The analysis reveals critical insights into suicide trends in the Philippines, highlighting the need for targeted mental health interventions.

## Moving Forward

- **Learning Application**: Apply the technical and analytical skills gained to future big data projects.
- **Areas for Improvement**: Explore cross-country comparisons, improve code efficiency, and incorporate real-world contextual insights.

## References

- Kaggle "WHO Suicide Statistics" Accessed: May 24, 2024. [Online](https://www.kaggle.com/datasets/szamil/who-suicide-statistics)

