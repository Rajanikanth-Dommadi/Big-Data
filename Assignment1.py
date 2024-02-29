import requests
import redis
import json
import matplotlib.pyplot as plt
import pandas as pd


class DataConnector:
    """
    class to connect to data sources and perform data operations.
      
    """

    def acquire_data_from_api(self, url):
        """
        Acquire data from an API.

        Args:
            url (str): The URL of the API endpoint.

        Returns:
            dict: The retrieved data in dictionary format.
        """
        try:
            response = requests.get(url)
            data = response.json()
            return data
        except Exception as e:
            print("Data acquisition error:", e)
            return None

    def load_data_to_redis(self, data, redis_host=None, redis_port=None, redis_username=None, redis_password=None,
                          redis_db=None):
        """
        Load data into Redis.

        Args:
            data (dict): The data to be loaded into Redis.
            redis_host (str): The host address of the Redis server.
            redis_port (int): The port of the Redis server.
            redis_username (str): The username for authentication (if required).
            redis_password (str): The password for authentication (if required).
            redis_db (str): The name of the Redis database.

        Returns:
            None
        """
        try:
            # Connect to Redis
            r = redis.Redis(host=redis_host, port=redis_port,
                            username=redis_username, password=redis_password)

            # Convert the data to JSON string
            json_data = json.dumps(data)

            # Store the JSON data in Redis
            r.set('data_key', json_data)

            print("Data loaded into Redis successfully.")
        except Exception as e:
            print("Error loading data into Redis:", e)

    def read_data_from_redis(self, redis_host=None, redis_port=None, redis_username=None, redis_password=None,
                            redis_db=None):
        """
        Read data from Redis.

        Args:
            redis_host (str): The host address of the Redis server.
            redis_port (int): The port of the Redis server.
            redis_username (str): The username for authentication (if required).
            redis_password (str): The password for authentication (if required).
            redis_db (str): The name of the Redis database.

        Returns:
            dict: The retrieved data from Redis in dictionary format.
        """
        try:
            # Connect to Redis
            r = redis.Redis(host=redis_host, port=redis_port, username=redis_username, password=redis_password)

            # Retrieve data from Redis
            json_data = r.get('data_key')

            # Decode JSON data
            if json_data:
                data = json.loads(json_data)
                return data
            else:
                print("No data found in Redis.")
                return None
        except Exception as e:
            print("Error reading data from Redis:", e)
            return None


class Analytics:
    """
    A class to perform data analytics operations.

    Attributes:
        data (dict): The data for analysis.
        dataframe (DataFrame): The data in pandas DataFrame format.
    """

    def __init__(self, data):
        """
        Initialize Analytics class with data.

        Args:
            data (dict): The data for analysis.
        """
        self.data = data
        self.dataframe = pd.DataFrame(self.data)

    def generate_graph(self, dataframe, column_x, column_y):
        """
        Generate a bar graph.

        Args:
            dataframe (DataFrame): The data in pandas DataFrame format.
            column_x (str): The column to be plotted on the x-axis.
            column_y (str): The column to be plotted on the y-axis.

        Returns:
            None
        """
        plt.figure(figsize=(10, 6))
        plt.bar(dataframe[column_x], dataframe[column_y])
        plt.xlabel(column_x)
        plt.ylabel(column_y)
        plt.title(f'{column_y} vs {column_x}')
        plt.show()

    def search_data(self, dataframe, column, value):
        """
        Search data based on a specific value in a column.

        Args:
            dataframe (DataFrame): The data in pandas DataFrame format.
            column (str): The column to search within.
            value: The value to search for.

        Returns:
            DataFrame: The subset of data matching the search criteria.
        """
        return dataframe[dataframe[column] == value]

    def aggregate_data(self, dataframe, column):
        """
        Perform aggregation operations (min, max, average) on a column.

        Args:
            dataframe (DataFrame): The data in pandas DataFrame format.
            column (str): The column on which aggregation is performed.

        Returns:
            dict: A dictionary containing the aggregation results (min, max, average).
        """
        return {
            'min': dataframe[column].min(),
            'max': dataframe[column].max(),
            'average': dataframe[column].mean()
        }


if __name__ == "__main__":
    dl = DataConnector()
    url = "https://apis-ugha.onrender.com/products"
    data = dl.acquire_data_from_api(url)
    print("Data retrieval successful")

    # Load data into Redis
    redis_host = 'redis-18241.c267.us-east-1-4.ec2.cloud.redislabs.com'
    redis_port = 18241  # Your Redis Cloud port
    redis_password = 'Rowan@123'  # Your Redis Cloud password
    redis_db = 'BigData'
    username = 'default'
    dl.load_data_to_redis(data, redis_host=redis_host,
                          redis_port=redis_port,
                          redis_username=username,
                          redis_password=redis_password,
                          redis_db=redis_db)

    # Read data from Redis
    redis_data = dl.read_data_from_redis(redis_host=redis_host,
                                         redis_port=redis_port,
                                         redis_username=username,
                                         redis_password=redis_password,
                                         redis_db=redis_db)

    if redis_data:
        # Initialize Analytics object with the retrieved data
        analytics = Analytics(redis_data)

        # Process only the top 10 columns for analysis
        top_10_columns = analytics.dataframe.columns[:10]
        df = analytics.dataframe[top_10_columns]
        df = df[df['p_brand'].isin(['Xiaomi', 'Dell', 'Apple', 'Lenovo', 'Samsung'])]

        # Generate a graph
        analytics.generate_graph(df, 'p_brand', 'p_rate3star')

        # Search for data
        search_result = analytics.search_data(df, 'p_brand', 'Xiaomi')
        print("Search Result:")
        print(search_result)

        # Aggregate data
        agg_result = analytics.aggregate_data(df, 'p_rate2star')
        print("\nAggregation Result:")
        print(agg_result)

    else:
        print("Failed to read data from Redis.")