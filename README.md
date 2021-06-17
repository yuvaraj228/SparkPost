# SparkPost

1. Created a temporary table on the input files.
2. If files have 1 to many relationship then those datatypes need to be flattened
3. We did that as part of our crawl_json and crawl_parquet methods.
4. We can enhance crawl to one method for crawling different file formats
5. Generate_extract() - I used spark sql to handle this use case. But we could also use spark native methods select(),groupBy(),reduceByKey(), etc.
6. Parameterized most of the input variables. We could as well, parameterize the extract sql
7. filter based on group column - The basic use case talks about filter by group = 'verizon_media' where as the other section talks about all the media groups. So, I added additional logic in extract sql where if we give '*' as group then it pulls data for all the attributes and if we switch it to 'verizon_media' then it only pulls data belonging to that group.
8. Added logic for injection traffic percentage per customer based on my understanding. We can tweak this logic if there's more to it.
9. I wrote all the code in a single script since the size of the code wasn't that big. However modularizing it to small chunks would improve readability
10. I didn't include any aws components in this. I completely used open source
11. We can implement the same in AWS using different services. We can talk about the AWS implementation of this code in our call
12. I tried to maintain a balance between the scripting and sql part in this assignment since both of these are like heart and brain of Data Engineering
13. We can talk about the architecture, deployments/CICD aspect of it in our call/meeting.