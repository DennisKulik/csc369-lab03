Dennis Kulik

I chose to do a reduce side join because each mapper works as it typically would but puts an identifying tag, and the join reducer just has to check each inputs tag and then treat it accordingly. This is also more appealing than having to worry about how the size of one dataset compares to another, and whether either could fit in main memory. 

CountryRequestCount outputs the total request count for each country in descending order. 
LogMapper accepts access logs as input with type <LongWritable, Text>, and outputs <hostname, "1,1"> with type <Text, Text>. The first "1" is to act as a count, the second is used to associate the output with LogMapper.
HostMapper accepts "hostname,country" as input with type <LongWritable, Text>, and outputs <hostname, "country,2"> with type <Text, Text>. "2" is used to associate the output with HostMapper.
JoinReducer takes in <Text, Text> shuffled by hostname, and extracts either the country name or count from the value based on the numerical identifier. It outputs <country, count> with types <Text, IntWritable>.
SumMapper accepts <LongWritable, Text> and outputs <country, count> with type <Text, IntWritable>. It basically just passes its inputs through.
SumReducer takes in <country, count> pairs and sums all counts associated with each country. It outputs <sum, country> with types <IntWritable, Text>, producing the total number of requests per country.
SortMapper accepts input with type <LongWritable, Text> and outputs <count, country> with type <IntWritable, Text>. It prepares the data so that countries can be ordered by their total request counts.
SortReducer takes in <count, country> pairs and outputs <country, count> with types <Text, IntWritable>. Using a custom DecreasingComparator, it sorts the final results by total request count in descending order.

CountryURLCount lists the count of each url visited for each country, sorted in alphabetical order by country and descending order by count. 
LogMapper accepts <LongWritable, Text> and outputs <hostname, "url,1"> with type <Text, Text>. 
HostMapper accepts <LongWritable, Text> and outputs <hostname, "country,2"> with type <Text, Text>.
JoinReducer takes in <Text, Text> pairs shuffled by hostname and outputs <"country,url", 1> with types <Text, IntWritable>. It matches hostnames to their countries and URLs for counting.
SumMapper accepts <LongWritable, Text> and outputs <"country,url", count> with type <Text, IntWritable>. It just passes its inputs through.
SumReducer takes in <"country,url", 1> pairs and sums all counts associated with each URL per country. It outputs <(country,count), url> with types <CountryUrl, Text>. CountryUrl is a custom type for future sorting by country ascending and count descending.
SortMapper accepts <LongWritable, Text> and outputs <CountryUrl, Text>. It just passes its inputs through.
SortReducer takes in <CountryUrl, Text> and outputs <Text, IntWritable>. It sorts the final results by country in alphabetical order and count in descending order

URLCountryList lists the country names of all visitors per URL, sorted in alphabetical order without duplicates.
LogMapper accepts <LongWritable, Text> and outputs <hostname, "url,1"> with type <Text, Text>. 
HostMapper accepts <LongWritable, Text> and outputs <hostname, "country,2"> with type <Text, Text>.
JoinReducer takes in <Text, Text> pairs shuffled by hostname and outputs <url, country> with types <Text, Text>. It pairs each country with its url for counting.
SortMapper accepts <LongWritable, Text> and outputs <Text, Text>. It just passes its inputs through.
SortReducer takes in <url, country> with types <Text, Text> and outputs <url, "comma-serparated country list"> with types <Text, Text>. It uses a TreeSet to sort the list of countries and remove duplicates, but this is fine because the list of countries will never be large enough to impact scalability.