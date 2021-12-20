Download the data - https://s3-eu-west-1.amazonaws.com/csparkdata/ol_cdump.json using following command:

wget --continue https://s3-eu-west-1.amazonaws.com/csparkdata/ol_cdump.json -O /tmp/ol_cdump.json

Update the path in line 13 to point to the location you download this data in

Ensure that Python and Spark are installed on the machine

Please use the JSON file (140MB, ol_cdump.json) and perform the following: 
1. Load the data and print/make yourself familiar with the schema. Count the rows in the (raw) data set.
2. Apply some data profiling on the data set and suggest three data quality measures to improve the data set for the purpose of this case study
3. Make sure your data set is cleaned, so we for example don't include in results with empty/null "titles" and "number of pages" is greater than 20 and "publishing year" is after 1950. If you decided to add additional filters due to the data profiling exercise above, pls. state them as well. Count the rows in the filtered dataset.

Run the following queries on the cleaned dataset:

4. Get the first book / author which was published - and the last one. 
5. Find the top 5 genres with most published books.
6. Retrieve the top 5 authors who (co-)authored the most books.
7. Per publish year, get the number of authors that published at least one book
8. Find the number of authors and number of books published per month for years between 1950 and 1970!

Let's assume that you want to create a very simple interactive website (pagesbyauthor.com) where a user can provide an author and the website will provide the total number of pages that this author has written across all the books. Please create an architecture to achieve this (no need to do any implementation on this), keeping the following in mind:

9. How will you handle changing or evolving schema? 
10. How do you want to store/partition the data?
11. Would you go for on-premise or for a cloud-based solution? Why?
12. How would you do the reconciliation testing?
