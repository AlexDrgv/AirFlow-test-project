This project is a try to use Apache Airflow to solve Analyst tasks and make it more 
We need to create a DAG with several tasks to find answers to the following questions:

**1**What was the best-selling game worldwide this year?
**2**Which genre of games was the best-selling in Europe? List all if there are several.
**3**On which platform were there the most games that sold more than one million copies in North America? List all if there are several.
**4**Which publisher has the highest average sales in Japan? List all if there are several.
**5**How many games sold better in Europe than in Japan?
The DAG can be structured in any way, but the final task should log the answer to each question. The DAG is expected to have 7 tasks: one for each question, one for loading the data, and a final task that collects all the answers
