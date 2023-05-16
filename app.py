from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALSModel
from flask import Flask, request, render_template
from flask_wtf import FlaskForm
from pyspark.ml.feature import IndexToString
from pyspark.sql import SparkSession
import numpy as np
import kafka
model_path = "model_ex"
# spark = SparkSession.builder.appName("load-cfData").getOrCreate()
spark = SparkSession.builder \
    .appName("MongoDB Data Loading") \
    .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
    .getOrCreate()
model = ALSModel.load(model_path)
producer=kafka.KafkaProducer(bootstrap_servers=['localhost:9092'])
consumer=kafka.KafkaConsumer('test-topic',bootstrap_servers=['localhost:9092'])
app = Flask(__name__)
consumer.subscribe(topics='test-topic')

@app.route('/') 
def home():
    return render_template('home.html')


@app.route('/recommendation', methods=['GET','POST'])
def recommendation():
    option=request.form['optradio']
    if option=='Random':
                user_id=np.random.randint(1,200)
    if option=='Ratings':
                user_id=201
    
    num_recs = 5
    user_recs = model.recommendForUserSubset(df1.filter(df1.userIndex == user_id), num_recs)
    print('user recs')
    recs_col = user_recs.select('recommendations')
    # Extract item indices
    recs_list = [{'asin': row[0], 'rating': row[1]} for row in recs_col.collect()[0]['recommendations']]
    recs_df = spark.createDataFrame(recs_list)

# write the DataFrame to MongoDB
    recs_df.write.format("com.mongodb.spark.sql.DefaultSource")\
        .option("uri", "mongodb://localhost:27017/BDA.ratings")\
        .mode("append").save()
    print (recs_df)

    item_indices = [row['recommendations'][0]['itemIndex'] for row in user_recs.select('recommendations').collect()]

     # Convert item indices to original ASINs
    asins_df = spark.createDataFrame([(index,) for index in item_indices], ['itemIndex'])
    asins_reversed = (
        IndexToString(inputCol="itemIndex", outputCol="original_asin", labels=df2.select('asin').distinct().rdd.flatMap(lambda x: x).collect())
        .transform(asins_df)
        .select('original_asin')
        .collect()
     )

     # Get list of original ASINs
    original_asins = [r.original_asin for r in asins_reversed]
    print(asins_reversed)
    return render_template('home.html', user_id=user_id, recommendations=recs_list)


def stream_template(template_name, **context):
    """Enabling streaming back results to app"""
    app.update_template_context(context)
    template = app.jinja_env.get_template(template_name)
    streaming = template.stream(context)
    return streaming

@app.route('/get-recommendation', methods=['GET','POST'])
def get_recommendation():
        consumer.subscribe(topics='test-topic')
        if consumer:
            for message in consumer:
                option=message.value.decode('utf-8')
            if option=='Random':
                user_id=np.random.randint(1,200)
            if option=='Ratings':
                user_id=201
            else:
                user_id==None
        num_recs = 5
        user_recs = model.recommendForUserSubset(df1.filter(df1.userIndex == user_id), num_recs)
        print('user recs')
        recs_col =user_recs.select('recommendations')

        # extract the list of dictionaries from the DataFrame
        recs_list = recs_col.collect()[0]['recommendations']

        # create a new DataFrame from the list of dictionaries
        recs_df = spark.createDataFrame(recs_list)
        recs_df.show()
        recs_df.write.format("com.mongodb.spark.sql.DefaultSource")\
        .option("uri", "mongodb://localhost:27017/BDA.ratings")\
        .mode("append").save()

        item_indices = [row['recommendations'][0]['itemIndex'] for row in user_recs.select('recommendations').collect()]

        # Convert item indices to original ASINs
        asins_df = spark.createDataFrame([(index,) for index in item_indices], ['itemIndex'])
        asins_reversed = (
            IndexToString(inputCol="itemIndex", outputCol="original_asin", labels=df2.select('asin').distinct().rdd.flatMap(lambda x: x).collect())
            .transform(asins_df)
            .select('original_asin')
            .collect()
        )

        # Get list of original ASINs
        original_asins = [r.original_asin for r in asins_reversed]
        print(asins_reversed)
        print (user_id,recs_list)
        return render_template('get-recommendation', user_id=user_id, recommendations=recs_list)

if __name__ == '__main__':
    
    df1 = spark.read.json("cfData_ex.json")
    df2 = spark.read.json("subdf_asin.json")
    app.run(debug=True)
