from flask import Flask, request, render_template
from pyspark.ml.recommendation import ALSModel
from pyspark.ml.feature import IndexToString
from pyspark.sql import SparkSession

app = Flask(__name__)
model_path = "model_ex"
spark = SparkSession.builder.appName("load-cfData").getOrCreate()
model = ALSModel.load(model_path)

@app.route('/')
def home():
    return render_template('home.html')

@app.route('/recommendation', methods=['POST'])
def recommendations():
    user_id = int(request.form['user_id'])
    num_recs = 5
    user_recs = model.recommendForUserSubset(df1.filter(df1.userIndex == user_id), num_recs)
    print('user recs')
    recs_col = user_recs.select('recommendations')
    # Extract item indices
    recs_list = [{'asin': row[0], 'rating': row[1]} for row in recs_col.collect()[0]['recommendations']]

    # print (recs_list)

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

    return render_template('recommendation.html', user_id=user_id, recommendations=recs_list)

if __name__ == '__main__':
    
    df1 = spark.read.json("cfData_ex.json")
    df2 = spark.read.json("subdf_asin.json")
  


    app.run(debug=True)
