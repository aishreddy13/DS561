# DS561_hw2

Steps to run this project:

if you have already generated the files and copied to your bucket then you dont have to run the generate-content.py file
Now:
1. download the tasks.py file to you local computer. then upload the file to your bucket directory where you have the html files stored.
2. then just have to run the .py files (please check the permission of you bucket as it has to be publicly accessible)
3. Once you run the file you get the below out put, first the statistics of the incoming_links and outgoing_links, then followed by the pagerank and graph:

IMPORTANT NOTES:
1.	The project name is ->u62138442-hw2
2.	Bucket name -> u62138442_hw2_b2
You just need to download the tasks.py file and run it the cloud shell with having the html files within the same bucket.

Github link:
1. url : https://github.com/aishreddy13/DS561_hw2 (but you have to accept the invite before you access)
2. SSH: git@github.com:aishreddy13/DS561_hw2.git
3. HTTP: https://github.com/aishreddy13/DS561_hw2.git
   
Below I have explained how I wrote the code and also what ways I tried ti run the long data files:

1.	In the first part of the code it processes all the html files and calculates the asked operations like finding Average, Median, Max, Min and Quintiles of incoming and outgoing links across all the html files and the output is something like this:

   ![image](https://github.com/aishreddy13/DS561_hw2/assets/118329497/05a6350c-dc35-4019-aad1-7efca65860f7)

2.	And in the second part of the code I have calculated the PageRank, but took very long time then I created a VM with name ‘instance-1’

I tried every way to run my code but due to the long running time it did not work but I am gonna explain all the ways I tried to make it work.
Steps in code:
•	first initialize the Google Cloud Storage client and specify the bucket name.
•	then, list all blobs (files) in the specified bucket.
•	Then, iterate through the blobs and analyze HTML files:
•	Download the HTML content.
•	Parse the HTML with BeautifulSoup.
•	Find all anchor (<a>) tags in the HTML.
•	Count incoming and outgoing links.
•	Store link counts in dictionaries (incoming_counts and outgoing_counts).
•	You calculate statistics (average, median, max, min, and quintiles) for incoming and outgoing links and print them.
•	You then proceed to calculate PageRank:
•	Initialize PageRank values for all pages.
•	Define a convergence threshold.
•	Perform an iterative PageRank calculation.
•	Calculate the contribution from incoming links and update PageRank values.
•	Check for convergence based on the total change in PageRank values.
•	Output the top 5 pages by PageRank score.

VM ssh browser where I have cloned the git:
 ![image](https://github.com/aishreddy13/DS561_hw2/assets/118329497/1ce36fac-3470-4a7c-8262-03d42decc36e)


Cloud shell terminal where I have runned the code multiple times in a different ways:
 ![image](https://github.com/aishreddy13/DS561_hw2/assets/118329497/daf63d99-2c60-4245-966c-0a5fb796b5f6)


Billing information: used VM for this and I cost me around 7.79 it is because I used it with the image disk (ubuntu) as I was getting very long running period.
 ![image](https://github.com/aishreddy13/DS561_hw2/assets/118329497/391cd678-7155-4a76-b588-acc3cc4b582f)


