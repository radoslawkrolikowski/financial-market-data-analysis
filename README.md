# Real-Time Financial Market Data Processing and Prediction

<i>“Without big data, you are blind and deaf and in the middle of a freeway.”</i> – Geoffrey Moore

Nowadays Financial Markets produce a tremendous amount of data and to stay ahead of your competitors and beat the Market you need to perform data processing and prediction in a very rapid and efficient way. 

In this project, as a response to the aforementioned requirements, I developed a Real-Time Financial Market Data Processing and Prediction application that encompasses the following features:
- extraction of financial market data from various sources using Scrapy Spiders and API calls
- use of Apache Kafka to stream real-time data between internal components of an application
- utilize of distributed Pyspark Structured Streaming program to ingest market data, perform feature extraction and merging of the data
- use of MariaDB database for data warehousing and additional feature extraction
- implementation of Pytorch bidirectional Gated Recurrent Unit neural network model
- perform biGRU model training and evaluation
- make a real-time prediction of future price movement of Stock Indexes, ETFs, Currencies or Commodities (whether the price will go up, down or stall)
- use of various types of financial market data such for instance:
	- The SPDR S&P 500 order book (aggregated size of resting displayed orders at a price and side)
	- Economic indicators such as Nonfarm Payrolls, Unemployment Rate, Building Permits, Core Retail Sales and others
	- Commitment of Traders reports data
	- CBOE Volatility Index
	- Volume Imbalance, Delta Indictor, Bid-Ask Spread
	- Weighted average for bid's and ask's side orders
	- Bollinger Bands, Stochastic Oscillator, Average True Range
	- Open, high, low, close prices, volume and wick percentage
	- Volume, price and Delta indicator moving averages

Data provided by [IEX Cloud]( https://iexcloud.io)

### Architecture

![Architecture](https://github.com/radoslawkrolikowski/financial-market-data-analysis/blob/master/assets/app-architecture.png)

### Table of contents

* [getMarketData](https://github.com/radoslawkrolikowski/financial-market-data-analysis/blob/master/getMarketData.py)
 	
    Performs call to IEX Cloud and Alpha Vantage API to retrieve specified financial data and also returns market calendar of the current month.

	IEX Cloud is the financial data platform that allows you to access fundamentals, ownership, international equities, mutual funds, options, real-time data, and alternative data from one fast and easy to use API.

	To find out the full set of possibilities check out the IEX Cloud docs under the following link:
	<https://iexcloud.io/docs/api/>

	Alpha Vantage is the provider of free APIs for realtime and historical data on stocks, forex (FX), and digital/crypto currencies. To get more information browse through the AV docs here: <https://www.alphavantage.co/documentation/>


* [economic_indicators_spider](https://github.com/radoslawkrolikowski/financial-market-data-analysis/blob/master/economic_indicators_spider.py)
 	
	The implementation of Scrapy Spider that extracts economic indicators from Investing.com Economic Calendar. You can select particular indicators by specifying Countries of interest, importance level and list of events to be considered. Allows to fetch economic indicators such as:
	- Core Retail Sales
	- Fed Interest Rate Decision
	- Core CPI
	- Crude Oil Inventories
	- Building Permits
	- Unemployment Rate
	- Nonfarm Payrolls
	- New Home Sales
	- and many others 

* [vix_spider](https://github.com/radoslawkrolikowski/financial-market-data-analysis/blob/master/vix_spider.py)
 	
	The implementation of the Scrapy Spider that extracts VIX data from cnbc.com.

	VIX (CBOE Volatility Index)  is a calculation designed to produce a measure of constant, 30-day expected volatility of the U.S. stock market, derived from real-time, mid-quote prices of S&P 500® Index (SPXSM) call and put options (<http://www.cboe.com/vix>)

* [cot_reports_spider](https://github.com/radoslawkrolikowski/financial-market-data-analysis/blob/master/cot_reports_spider.py)
 	
	The Scrapy Spider that extracts Commitment of Traders (COT) Reports data from tradingster.com. 

	The COT report is a weekly publication that shows the aggregate holdings of different participants in the U.S. futures market. Published every Friday by the Commodity Futures Trading Commission (CFTC) at 3:30 E.T., the COT report is a snapshot of the commitment of the classified trading groups as of Tuesday that same week (<https://www.investopedia.com/terms/c/cot.asp>).

	There are available COT reports that regard futures on Currencies (British Pound Sterling, Swiss Franc, Japanese Yen), Stock Indexes (S&P 500 STOCK INDEX, NASDAQ-100 STOCK INDEX (MINI)), Grains (Soybeans, Corn), Metals (Gold, Silver), Softs (Cocoa, Coffee, Sugar), Energies (Crude Oil, Natural Gas).
	
	In case of Currencies or Stock Indexes Spider extracts from specified COT report following data that pertains Asset Manager/Institutional and  Leveraged Funds traders:
   	     
    - Long and Short Positions
    - Long and Short Positions Change (with respect to previous data release)
    - Long and Short Open Interest

	Where Asset Managers includes mutual funds, endowments, and pension funds. The Leveraged includes CTAs, CPOs, and hedge funds.
    
	COT reports are most commonly used in long-term trading (weekly, daily time frames)

* [config](https://github.com/radoslawkrolikowski/financial-market-data-analysis/blob/master/config.py)
 	
	The configuration file that includes: 
	- IEX and Alpha Vantage tokens
 	- Kafka brokers addresses and topics
 	- Scrapy user agent
 	- Database (MySQL/MariaDB) properties
 	- Financial data properties:
		- Number of order book price levels to include
		- Whether to use COT, VIX, volume data or Stochastic Oscillator
		- List of economic indicators to use
		- Specify period and number of standard deviations for Bollinger Bands
		- Specify Moving Averages periods

* [producer](https://github.com/radoslawkrolikowski/financial-market-data-analysis/blob/master/producer.py)
 	
   	The producer instantiates day session and gets the intraday market data from Alpha Vantage and IEX Cloud APIs, it also runs Scrapy Spiders to fetch economic indicators, COT Reports data and VIX. The producer will call the source API and extract data from web sources with the frequency specified by the user (interval) until the market is closed. The collected data subsequently creates a set of streams that are published to corresponding Kafka topics.


* [spark_consumer](https://github.com/radoslawkrolikowski/financial-market-data-analysis/blob/master/spark_consumer.py)

  	The distributed streaming Pyspark application that is responsible for following tasks:

    - subscribe to a stream of records in given Kafka topic and create a streaming Data Frame based on the pre-defined schema
    - fill missing values
    - perform real-time financial data feature extraction:

        - weighted average for bid's and ask's side orders      

        - Order Volume Imbalance

        - Micro-Price (according to Gatheral and Oomen)

        - Delta indicator

        - Bid-Ask Spread

        - calculate the bid and ask price relative to best values

        - day of the week

        - week of the month

        - start session (first 2 hours following market opening)

    - perform oneHotEncoding
    - join streaming data frames
    - write stream to MySQL/MariaDB
    - signal to Pytorch model readiness to make a prediction for current datapoint

	For testing we will run Spark locally with one worker thread (.master("local")).
	Other options to run Spark  (locally, on cluster) can be found here:
	<http://spark.apache.org/docs/latest/submitting-applications.html#master-urls>

	SPARK STRUCTURED STREAMING LIMITATIONS:

	In Spark Structured Streaming 2.4.4 several operations are not supported on Streaming DataFrames. The most significant constraint pertaining this application is that multiple streaming aggregations (a chain of aggregations on a streaming DataFrame) are not yet supported, thus the feature extraction process that requires multiple window aggregations will be moved from Spark to MariaDB.

	All unsupported operations are listed here <https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html#unsupported-operations>


* [create_database](https://github.com/radoslawkrolikowski/financial-market-data-analysis/blob/master/create_database.py)

	 Creates a 'stock_data' database that stores in the main table, processed by Spark application data, but also performs further feature extraction using SQL views. The following are the created additional features:

	- Volume Moving Averages
	- Price Moving Averages
	- Delta indicator Moving Averages
	- Bollinger Bands
	- Stochastic Oscillator
	- Average True Range

	Creates a VIEW with target variables, that are determined using following manner:

	|                Condition                    |  up1  |  up2  | down1 | down2 |
	| :------------------------------------------:|:-----:|:-----:|:-----:|:-----:|
	| 8th bar p8_close >= p0_close + (n1 * ATR)   |   1   |   0   |   0   |   0   |
	| 15th bar p15_close >= p0_close + (n2 * ATR) |   0   |   1   |   0   |   0   |
	| 8th bar p8_close <= p0_close - (n1 * ATR)   |   0   |   0   |   1   |   0   |
	| 15th bar p15_close <= p0_close - (n2 * ATR) |   0   |   0   |   0   |   1   |

	You can generate different target variables by modifying SQL <i>target_statement</i>

* [biGRU_model](https://github.com/radoslawkrolikowski/financial-market-data-analysis/blob/master/biGRU_model.py)

	Implementation of the bidirectional Gated Recurrent Unit neural network Pytorch model.

* [sql_pytorch_dataloader](https://github.com/radoslawkrolikowski/financial-market-data-analysis/blob/master/sql_pytorch_dataloader.py)

  Implementation of the custom Pytorch Dataset that loads data from MySQL/MariaDB database and consists of MySQLChunkLoader and MySQLBatchLoader. 

  MySQLChunkLoader is responsible for generating indices of database rows that form a chunk of MySQL/MariaDB database parameterized by chunk_size (chunking is used to diminish memory usage while parsing). Chunk loader also calculates data chunk's normalization parameters - minimum and maximum, pass them to MySQLBatchLoader and save locally. Normalization parameters subsequently can be used by MySQLBatchLoader to normalize training batches (according to MIN and MAX of a chunk to which given batch belongs to) as well as to normalize validation and test sets during evaluation or real-time inference.

 	The file also includes implementation of the TrainValTestSplit class that performs Train/Validation/Test splitting of a set of data chunks.

* [predict](https://github.com/radoslawkrolikowski/financial-market-data-analysis/blob/master/predict.py)

  Reads the latest data point from MySQL/MariaDB database based on the current timestamp value that is sent from Spark application through Kafka, performs real-time data normalization and prediction using trained Pytroch model. 

* [biGRU_model_training](https://nbviewer.jupyter.org/github/radoslawkrolikowski/financial-market-data-analysis/blob/master/biGRU_model_training.ipynb)

  Jupyter notebook showing the process of creating and training the BiGRU model.

### Installing

#### Apache Spark:
1. Download Apache Spark from <https://spark.apache.org/downloads.html>
2. Go to the directory where spark zip file was downloaded and unpack it:
   - `tar -zxvf spark-2.3.4-bin-hadoop2.7.tgz`
3. Set $JAVA_HOME environmental variable in .bashrc file:
   - `export JAVA_HOME='/usr/lib/jvm/java-1.8.0-openjdk-amd64'`
4. In .bashrc file configure other environmental variables for Spark:
   - `export SPARK_HOME='spark-2.3.4-bin-hadoop2.7'`
   - `export PATH=$SPARK_HOME:$PATH`
   - `export PATH=$PATH:$SPARK_HOME/bin`
   - `export PYTHONPATH=$SPARK_HOME/python;%SPARK_HOME%\python\lib\py4j-0.10.7-src.zip:%PYTHONPATH%`
   - `export PYSPARK_DRIVER_PYTHON="python" `
   - `export PYSPARK_PYTHON=python3`
   - `export SPARK_YARN_USER_ENV=PYTHONHASHSE`

#### Apache ZooKeeper
1. Manually download the ZooKeeper binaries to the /opt directory:
   - `cd opt/`
   - `wget https://www-eu.apache.org/dist/zookeeper/zookeeper-3.5.6/apache-zookeeper-3.5.6-bin.tar.gz`
2. Unpack ZooKeeper repository:
   - `tar -xvf apache-zookeeper-3.5.6-bin.tar.gz`
3. Create a symbolic link:
   - `ln -s apache-zookeeper-3.5.6-bin zookeeper`
4. Use sample properties:
   - `cd zookeeper/`
   - `cat conf/zoo_sample.cfg >> conf/zookeeper.properties`

#### Apache Kafka
1. Donwload Kafka:
   - `wget https://www-us.apache.org/dist/kafka/2.3.0/kafka_2.12-2.3.0.tgz`
2. Unpack Kafka repository:
   - `tar -xvf kafka_2.12-2.3.0.tgz`
3. Create a symbolic link:
   - `ln -s kafka_2.12-2.3.0 kafka`

Setting up a multi-broker cluster:
1. Create a config file for each of the brokers using sample properties:
   - `cp config/server.properties config/server-1.properties`
   - `cp config/server.properties config/server-2.properties`
2. Now edit these new files and set the following properties:
	
    config/server-1.properties:
	delete.topic.enable=true
        broker.id=1
        listeners=PLAINTEXT://:9093
        log.dirs=/tmp/kafka-logs-1
 
    config/server-2.properties:
	delete.topic.enable=true
        broker.id=2
        listeners=PLAINTEXT://:9094
        log.dirs=/tmp/kafka-logs-2

#### Python packages:
Install all packages included in requirements.txt

1. Create a virtual environment (conda, virtualenv etc.).
   - `conda create -n <env_name> python=3.7`
2. Activate your environment.
   - `conda activate <env_name>`
3. Install requirements.
   - `pip install -r requirements.txt `
4. Restart your environment.
    - `conda deactivate`
    - `conda activate <env_name>`

#### Dependencies
All indispensable JAR files can be found in jar_files directory.

### Usage

1. Start MySQL server:
   - service mysql start
2. Before each run of the application we have to start the ZooKeeper and Kafka brokers:

    1. Start ZooKeeper:
        - `cd zookeeper/`
        - `bin/zkServer.sh start conf/zookeeper.properties`
    2. Check if it started correctly:
        - `bin/zkServer.sh status conf/zookeeper.properties`

    3. Start kafka nodes:
       - `cd kafka/`
       - `bin/kafka-server-start.sh config/server.properties`
       - `bin/kafka-server-start.sh config/server-1.properties`
       - `bin/kafka-server-start.sh config/server-2.properties`

 3. Create Kafka topics if run the application for the first time (list of sample topics can be found in config.py file):
 
 	1. Create topic:
		- `bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 3 --partitions 1 --topic topic_name`
 
	2. List available topics:
		- `bin/kafka-topics.sh --list --bootstrap-server localhost:9092`
    
 4. Specify your configuration by modifying config.py file:
 	- Add your IEX and Alpha Vantage tokens
 	- Specify Kafka brokers addresses and topics
 	- Specify Scrapy user agent
 	- Add database (MySQL/MariaDB) properties
 	- Specify other properties regarding financial data
 5. Create a MariaDB database by running create_database.py file (it is necessary only with the first use of the application)   
 6. Run spark_consumer (it have to be launched before data producer).
 7. Then we can run producer.py to fetch financial data and send it through Kafka to Pyspark.
 8. To make a real-time prediction run predict.py file (only data that comes after predict.py is launched is going to be considered)
 

### References

* <https://pytorch.org/docs/stable/index.html>
* <https://iexcloud.io/docs/api/>
* <https://www.alphavantage.co/documentation/>
* <https://www.investing.com/economic-calendar/>
* <https://www.cnbc.com/>
* <http://www.cboe.com/vix>
* <https://www.investopedia.com/terms/c/cot.asp>
* <https://arxiv.org/pdf/1901.10534.pdf>
* <https://www.cis.upenn.edu/~mkearns/papers/KearnsNevmyvakaHFTRiskBooks.pdf>
* <https://www.math.fsu.edu/~aluffi/archive/paper462.pdf>
* <http://www.math.lsa.umich.edu/seminars_events/fileupload/4044_Microprice_Michigan.pdf>
