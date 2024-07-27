# API function for outlier check
def outlier_check(spark, db, tbl, col):
    """
    Function to count the number records within IQR range
    
    Args
    ----
    spark: Name of the spark session
    db: Name of the database
    tbl: Name of the table
    col: Name of the column

    Returns
    -------
    int - Number of records within IQR range
    """

    # Import required libraries
    import pyspark.sql.functions as F

    # Load required data
    df = spark.sql(f'select {col} from {db}.{tbl} where {col} is not null')
    
    # calculate lower bound and upper bound from first and third quartiles
    q1, q3 = df.approxQuantile(col, [0.25, 0.75], 0.01)
    iqr = q3 - q1
    lb = q1 - (3*iqr)
    ub = q3 + (3*iqr)
    
    # count the number of records between IQR
    dq_val = df.filter(F.col(col).between(lb, ub)).count()
    return dq_val

#  API function to distinct records using fuzzy matching
def fuzzy_duplicate_check(spark, db, tbl, col, threshold, col_list =''):
    """
    Function to count the distinct records using fuzzy matching
    
    Args
    ----
    spark: Name of the spark session
    db: Name of the database
    tbl: Name of the table
    col: Name of the column
    threshold: Threshold for fuzzy matching
    col_list: List of additional columns

    Returns
    -------
    int - Number of distinct records after fuzzy matching
    """ 
    # Import required libraries
    from pyspark.ml import Pipeline
    from pyspark.ml.feature import StopWordsRemover, Tokenizer, NGram, HashingTF, MinHashLSH, RegexTokenizer, SQLTransformer
    import pyspark.sql.functions as F
    
    # check if mandatory params present
    if not((threshold >= 0) and (threshold <= 1)):
            raise Exception('fuzzy_duplicate_check threshold should between 0 to 1')

    # construct select query
    if col_list != '':
        sel_query = f"select distinct(concat_ws(' ', {col}, {col_list})) as subject_col from {db}.{tbl}"
    else:
        sel_query = f"select distinct({col}) as subject_col from {db}.{tbl}"
    df = spark.sql(sel_query)
    
    # count the unique records
    unique_rec = df.count()

    # Create pipeline and fit the model
    model = Pipeline(stages=[
        SQLTransformer(statement="SELECT *, lower(subject_col) lower_col FROM __THIS__"),
        Tokenizer(inputCol="lower_col", outputCol="token_col"),
        StopWordsRemover(inputCol="token_col", outputCol="stop_col"),
        SQLTransformer(statement="SELECT *, concat_ws(' ', stop_col) concat_col FROM __THIS__"),
        RegexTokenizer(pattern="", inputCol="concat_col", outputCol="char_col", minTokenLength=1),
        NGram(n=2, inputCol="char_col", outputCol="ngram_col"),
        HashingTF(inputCol="ngram_col", outputCol="vector"),
        MinHashLSH(inputCol="vector", outputCol="lsh", numHashTables=3)
    ]).fit(df)

    # Transform the data with fitted model
    df_result = model.transform(df)
    df_result = df_result.filter(F.size(F.col("ngram_col")) > 0)

    # perform similarity join
    result = model.stages[-1].approxSimilarityJoin(df_result, df_result, threshold, "jaccardDist")
    result = result.selectExpr('datasetA.subject_col as a_col', 'datasetB.subject_col as b_col', 'jaccardDist')
    
    # count fuzzy duplicates
    fuzzy_match_cnt = result.filter(result.a_col != result.b_col).select('a_col').distinct().count()
    distinct_rec_cnt = unique_rec - fuzzy_match_cnt
    return distinct_rec_cnt 

#  API function to find non-anomaly records
def anomaly_check(spark, db, tbl, col, contamination, col_list =''):
    """
    Function to count the non anomaly records using isolation forest
    
    Args
    ----
    spark: Name of the spark session
    db: Name of the database
    tbl: Name of the table
    col: Name of the column
    contamination: Threshold for isolocation forest
    col_list: List of additional columns

    Returns
    -------
    int - Number of normal records
    """ 
    # Import required libraries
    from synapse.ml.isolationforest import IsolationForest
    from pyspark.ml.feature import VectorAssembler
    import pyspark.sql.functions as F
    
    # check if mandatory params present
    if not((contamination >= 0) and (contamination <= 1)):
            raise Exception('anomaly_check contamination should between 0 to 1')

    # construct select query
    if col_list == '':
        raise Exception('Add more than one column for anamoly check')

    sel_query = f"select {col}, {col_list} from {db}.{tbl}"
    df = spark.sql(sel_query)
    
    # drop null records
    df = df.na.drop()

    # Initiate model
    isolationForest = (IsolationForest()
      .setNumEstimators(50)
      .setBootstrap(False)
      .setMaxSamples(256)
      .setMaxFeatures(1.0)
      .setFeaturesCol("features")
      .setPredictionCol("predictedLabel")
      .setScoreCol("outlierScore")
      .setContamination(contamination)
      .setContaminationError(0.02 * contamination)
      .setRandomSeed(1))
    
    # create vectors
    inputCols = col_list.split(',')
    inputCols += [col] 
    outputCol = 'features'
    df_va = VectorAssembler(inputCols = inputCols, outputCol = outputCol)
    df_features = df_va.transform(df)
    
    # Build the model and predict the anomaly records
    model = isolationForest.fit(df_features)
    pred = model.transform(df_features)

    # count normal records
    normal_cnt = pred.filter(pred.predictedLabel == 0).count()
    return normal_cnt 