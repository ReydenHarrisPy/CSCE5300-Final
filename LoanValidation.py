import tkinter.messagebox

import pandas as pd
import csv
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StringIndexer
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from tkinter import *
import tkinter as tk
# import seaborn as sns
# import matplotlib.pyplot as plt
from tkinter import simpledialog
from LoanDetails import x
from LoanDetails import y
from LoanDetails import z
from LoanDetails import a
from tkinter import messagebox
from tkinter import *



conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf=conf)

# Creating a spark session
from pyspark.sql.types import StructField, StructType, StringType, IntegerType

spark = SparkSession.builder \
    .master("local[1]") \
    .appName("LoanValidation1") \
    .config("spark.pyspark.python", "python3") \
    .getOrCreate()


def loanApproval(user_Data):
    # Reading the file into dataframe
    filename = "Loan_Approval_Information.csv"
    df = spark.read.options(header='true', inferSchema='true') \
        .csv(filename)

    # Removing duplicate records
    df = df.dropDuplicates()

    # Removing rows with all values as 0
    df.na.drop("all")

    # # Creating schema for user input
    # data = [(Credit_Score, Annual_Income, Loan_Required, Loan_Tenure)]
    # columns = ["Credit_Score", "Annual_Income", "Loan_Required", "Loan_Tenure"]
    #
    # df_test = spark.createDataFrame(data).toDF

    # Creating Features using vector assembler
    numCols = ['Credit_Score', 'Annual_Income', 'Loan_Required', 'Loan_Tenure']
    VA = VectorAssembler(inputCols=numCols, outputCol="Features")
    df = VA.transform(df)
    df_test = VA.transform(user_Data)

    # Creating target using string indexer to encode
    SI = StringIndexer(inputCol='Loan_Status', outputCol='Target')
    df = SI.fit(df).transform(df)

    # # Splitting the dataset to test and train
    # train, test = df.randomSplit([0.7, 0.3])

    # Setting the whole dataset for train
    train = df

    # Training the model using the train dataset
    RF = RandomForestClassifier(numTrees=4, featuresCol="Features", labelCol="Target")
    RFmodel = RF.fit(train)

    # Testing the model
    predictions = RFmodel.transform(df_test)
    #predictions.show()
    p = predictions.toPandas()

    if p.loc[0, "prediction"] == 0.0:
        msg = InterestRate(df_test)
        return msg

    else:
        msg = "We are Unable to approve your loan from the information you provided"
        return msg

    # # Printing performance metrics of the model
    # acc_evaluator_rf = MulticlassClassificationEvaluator(labelCol="Target", predictionCol="prediction",
    #                                                      metricName="accuracy", )
    # acc_rf = acc_evaluator_rf.evaluate(predictions)
    # print("accuracy:" + str(acc_rf))
    #
    # pr_evaluator_rf = MulticlassClassificationEvaluator(labelCol="Target", predictionCol="prediction",
    #                                                     metricName="precisionByLabel")
    # precision_rf = pr_evaluator_rf.evaluate(predictions)
    # print("precision:" + str(precision_rf))
    #
    # f_evaluator_rf = MulticlassClassificationEvaluator(labelCol="Target", predictionCol="prediction", metricName="f1")
    # f1_score_rf = f_evaluator_rf.evaluate(predictions)
    # print("f1 score:" + str(f1_score_rf))
    #
    # recall_evaluator_rf = MulticlassClassificationEvaluator(labelCol="Target", predictionCol="prediction",
    #                                                         metricName="recallByLabel")
    # recall_rf = recall_evaluator_rf.evaluate(predictions)
    # print("recall:" + str(recall_rf))


def InterestRate(user_data):
    # Reading the file into dataframe
    filename = "Interest_Rates.csv"
    df = spark.read.options(header='true', inferSchema='true') \
        .csv(filename)

    # Removing duplicate records
    df = df.dropDuplicates()

    # Removing rows with all values as 0
    df.na.drop("all")

    # Removing rows with interest rate as 0 as they are rejected loans
    df = df.where(df.Interest_Rate != 0.0)

    # Plotting the clean data
    # df2 = df.toPandas()
    #
    # sns.pairplot(data=df2, diag_kind='kde')
    # plt.show()

    # Creating Features using vector assembler
    numCols = ['Credit_Score', 'Loan_Required', 'Loan_Tenure']
    VA = VectorAssembler(inputCols=numCols, outputCol="Features")
    df = VA.transform(df)

    # Creating target using string indexer to encode
    SI = StringIndexer(inputCol='Interest_Rate', outputCol='Target')
    df = SI.fit(df).transform(df)

    # # Splitting the dataset to test and train
    # train, test = df.randomSplit([0.7, 0.3])
    train = df

    # Training the model using the train dataset
    RF = RandomForestClassifier(numTrees=4, featuresCol="Features", labelCol="Target")
    RFmodel = RF.fit(train)

    # Testing the model
    predictions = RFmodel.transform(user_data)
    #predictions.show()
    p = predictions.toPandas()

    if p.loc[0, "prediction"] == 0.0:
        Irate = 7.5
    elif p.loc[0, "prediction"] == 1.0:
        Irate = 8.0
    elif p.loc[0, "prediction"] == 2.0:
        Irate = 7.0
    elif p.loc[0, "prediction"] == 3.0:
        Irate = 8.5
    else:
        Irate = 6.5
    msg = "Congratulations, your loan can be approved with " + str(Irate) + "% interest rate"
    return msg

    # # Printing performance metrics of the model
    # acc_evaluator_rf = MulticlassClassificationEvaluator(labelCol="Target", predictionCol="prediction",
    #                                                      metricName="accuracy", )
    # acc_rf = acc_evaluator_rf.evaluate(predictions)
    # print("accuracy:" + str(acc_rf))
    #
    # pr_evaluator_rf = MulticlassClassificationEvaluator(labelCol="Target", predictionCol="prediction",
    #                                                     metricName="precisionByLabel")
    # precision_rf = pr_evaluator_rf.evaluate(predictions)
    # print("precision:" + str(precision_rf))
    #
    # f_evaluator_rf = MulticlassClassificationEvaluator(labelCol="Target", predictionCol="prediction", metricName="f1")
    # f1_score_rf = f_evaluator_rf.evaluate(predictions)
    # print("f1 score:" + str(f1_score_rf))
    #
    # recall_evaluator_rf = MulticlassClassificationEvaluator(labelCol="Target", predictionCol="prediction",
    #                                                         metricName="recallByLabel")
    # recall_rf = recall_evaluator_rf.evaluate(predictions)
    # print("recall:" + str(recall_rf))


def loanValidation(CS, AI, LR, LT):

    # Writing data into a csv file

    # Defining the data for csv file
    columns = ['Credit_Score', 'Annual_Income', 'Loan_Required', 'Loan_Tenure']
    data = [CS, AI, LR, LT]
    # Writing the data into the file
    with open('Customer_Loan_Info.csv', 'w') as file:
        writer = csv.writer(file)
        writer.writerow(columns)
        writer.writerow(data)

    # Creating a dataframe based on user data for the models
    user_data = spark.read.options(header='true', inferSchema='true').csv("Customer_Loan_Info.csv")
    msg = loanApproval(user_data)
    return msg

loan = tkinter.Tk()




msg = loanValidation(x,y,z,a)

def showMessage():
    tkinter.messagebox.showinfo("Thanks for Applying", msg)


showMessage()

#loan.mainloop()