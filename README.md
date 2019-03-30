# PagerRank-FlightData
Implementation of Page Rank Algorithm for domestic flight network data in US
References:
<https://www.mongodb.com/blog/post/pagerank-on-flights-dataset>
<https://cdn.iiit.ac.in/cdn/search.iiit.ac.in/cloud/presentations/3.pdf>
<https://www.transtats.bts.gov/DL_SelectFields.asp?Table_ID=236>
<https://github.com/mongodb-labs/big-data-exploration>

The project focuses on the implementation of Page Rank Algorithm on the flight data to come up with a flight network that can be used to apply analytics. The project aims at:  
1.	Exploring and cleansing the data 
2.	Feature engineering and customized to match the need of the algorithm 
3.	Implementation of Page Rank Algorithm to find the feature importance. 

We use the page rank algorithm on flight data set to determine:
1. Most Connected Airport

![connected](https://github.com/taniyariar/PagerRank-FlightData/blob/master/mostconnected.PNG)



2. Airports to which most flights were "cancelled" 

![cancelled](https://github.com/taniyariar/PagerRank-FlightData/blob/master/cancelled.PNG)



3. Airports with most "delayed" flights 

![delayed](https://github.com/taniyariar/PagerRank-FlightData/blob/master/delayed.PNG)
