package it.polito.bigdata.spark.example;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.feature.*;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SparkDriver {

    @SuppressWarnings("resource")
    public static void main(String[] args) {

        String inputPath;
        inputPath = args[0];

        // Create a Spark Session object and set the name of the application
        // We use some Spark SQL transformation in this program
        SparkSession ss = SparkSession.builder().appName("Spark Machine Learning").getOrCreate();

        // Create a Java Spark Context from the Spark Session
        // When a Spark Session has already been defined this method
        // is used to create the Java Spark Context
        JavaSparkContext sc = new JavaSparkContext(ss.sparkContext());


        //EX 1: READ AND FILTER THE DATASET AND STORE IT INTO A DATAFRAME

        // To avoid parsing the comma escaped within quotes, you can use the following regex:
        // line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
        // instead of the simpler
        // line.split(",");
        // this will ignore the commas followed by an odd number of quotes.

        // read the input file, remove lines with denominator equal to zero
        JavaRDD<String> rawInFile = sc.textFile(inputPath).filter(line -> {
            // split the line
            String[] fields = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
            // fields[0] = Id
            // fields[1] = ProductId
            // fields[2] = UserId
            // fields[3] = ProfileName
            // fields[4] = HelpfulnessNumerator
            // fields[5] = HelpfulnessDenominator
            // fields[6] = Score
            // fields[7] = Time
            // fields[8] = Summary
            // fields[9] = Text

            return !(fields[0].equals("Id")) && (Integer.parseInt(fields[5]) > 0);
        }).cache();

        JavaRDD<LabeledPoint> preProcessedData = rawInFile.map(line -> {
            // split the line
            String[] fields = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
            // fields[0] = Id
            // fields[1] = ProductId
            // fields[2] = UserId
            // fields[3] = ProfileName
            // fields[4] = HelpfulnessNumerator
            // fields[5] = HelpfulnessDenominator
            // fields[6] = Score
            // fields[7] = Time
            // fields[8] = Summary
            // fields[9] = Text

            // extract the label
            double label = ((Double.parseDouble(fields[4]) / Double.parseDouble(fields[5])) > 0.9) ? 1 : 0;

            // extract the features
            double[] features = new double[7];

            // features:
            // 0) length of text
            // 1) number of words in text
            // 2) length of summary
            // 3) number of words in summary
            // 4) score
            // 5) number of ! in text
            // 6) number of ! in summary

            features[0] = fields[9].length();
            features[1] = fields[9].split("\\s+").length;
            features[2] = fields[8].length();
            features[3] = fields[8].split("\\s+").length;
            features[4] = Double.parseDouble(fields[6]);
            features[5] = fields[9].split("!").length;
            features[6] = fields[8].split("!").length;

            // return the labeledPoint
            return new LabeledPoint(label, Vectors.dense(features));

        });

        // create the DataFrame and cache it
        Dataset<Row> schemaReviews = ss.createDataFrame(preProcessedData, LabeledPoint.class);


        // Display 5 example rows.
        schemaReviews.show(5);


        // Split the data into training and test sets (30% held out for testing)
        Dataset<Row>[] splits = schemaReviews.randomSplit(new double[]{0.7, 0.3});
        Dataset<Row> trainingData = splits[0];
        Dataset<Row> testData = splits[1];


        //EX 2: CREATE THE PIPELINE THAT IS USED TO BUILD THE CLASSIFICATION MODEL


        // logistic regression part

        // create the estimator
        LogisticRegression lr = new LogisticRegression();

        // create the pipeline
        Pipeline pipeline = new Pipeline().setStages(new PipelineStage[]{lr});

        // Train model. Use the training set
        PipelineModel model = pipeline.fit(trainingData);

        /*==== EVALUATION ====*/

        // Make predictions for the test set.
        Dataset<Row> predictions = model.transform(testData);

        // Select example rows to display.
        predictions.show(5);

        // Retrieve the quality metrics.
        MulticlassMetrics metrics = new MulticlassMetrics(predictions.select("prediction", "label"));

        // Confusion matrix
        Matrix confusion = metrics.confusionMatrix();
        System.out.println("Confusion matrix for logistic regression \n" + confusion);

        double accuracy = metrics.accuracy();
        System.out.println("Accuracy for logistic regression = " + accuracy + "\n\n");


        // decision tree classifier part

        // create the estimator
        DecisionTreeClassifier dc = new DecisionTreeClassifier();

        // create the pipeline
        pipeline = new Pipeline().setStages(new PipelineStage[]{dc});

        // Train model. Use the training set
        model = pipeline.fit(trainingData);

        /*==== EVALUATION ====*/

        // Make predictions for the test set.
        predictions = model.transform(testData);

        // Select example rows to display.
        predictions.show(5);

        // Retrieve the quality metrics.
        metrics = new MulticlassMetrics(predictions.select("prediction", "label"));

        // Confusion matrix
        confusion = metrics.confusionMatrix();
        System.out.println("Confusion matrix for decision tree: \n" + confusion);

        accuracy = metrics.accuracy();
        System.out.println("Accuracy for decision tree= " + accuracy + "\n\n");


        // EX 4: use the content of the review

        // start again from the input lines
        JavaRDD<LabeledDocument> preProcessedDocuments = rawInFile.map(line -> {
            // split the line
            String[] fields = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
            // fields[0] = Id
            // fields[1] = ProductId
            // fields[2] = UserId
            // fields[3] = ProfileName
            // fields[4] = HelpfulnessNumerator
            // fields[5] = HelpfulnessDenominator
            // fields[6] = Score
            // fields[7] = Time
            // fields[8] = Summary
            // fields[9] = Text

            // extract the label
            double label = ((Double.parseDouble(fields[4]) / Double.parseDouble(fields[5])) > 0.9) ? 1 : 0;

            // extract the text
            String text = fields[9];

            // return the labeled document
            return new LabeledDocument(label, text);
        });

        // create the dataframe
        Dataset<Row> schemaDocuments = ss.createDataFrame(preProcessedDocuments, LabeledDocument.class);

        // Display 5 example rows.
        schemaDocuments.show(5);

        // Split the data into training and test sets (30% held out for testing)
        Dataset<Row>[] splitDocuments = schemaDocuments.randomSplit(new double[]{0.7, 0.3});
        Dataset<Row> trainingDocuments = splitDocuments[0];
        Dataset<Row> testDocuments = splitDocuments[1];

        // create the pipeline composed by
        // Tokenizer, StopWordsRemover, TF-IDF, logistic regression
        Tokenizer tokenizer = new Tokenizer()
                .setInputCol("text").setOutputCol("words");

        StopWordsRemover remover = new StopWordsRemover()
                .setInputCol("words").setOutputCol("filteredWords");

        HashingTF hashingTF = new HashingTF()
                .setNumFeatures(1000).setInputCol("filteredWords").setOutputCol("rawFeatures");

        IDF idf = new IDF()
                .setInputCol("rawFeatures").setOutputCol("features");

        LogisticRegression lrDocument = new LogisticRegression();

        Pipeline pipelineDocument = new Pipeline()
                .setStages(new PipelineStage[]{tokenizer, remover, hashingTF, idf, lrDocument});

        // train the model
        PipelineModel modeDocument = pipelineDocument.fit(trainingDocuments);

        // apply the model on test Data
        Dataset<Row> predictionsDocument = modeDocument.transform(testDocuments);

        // Select example rows to display.
        predictionsDocument.show(5);

        // Retrieve the quality metrics.
        MulticlassMetrics metricsDocument = new MulticlassMetrics(
                predictionsDocument.select("prediction", "label"));

        // Confusion matrix
        Matrix confusionDocument = metricsDocument.confusionMatrix();
        System.out.println("Confusion matrix for Documents \n" + confusionDocument);

        double accuracyDocument = metricsDocument.accuracy();
        System.out.println("Accuracy for logistic regression = " + accuracyDocument + "\n\n");

        // Close the Spark context
        sc.close();
    }
}
