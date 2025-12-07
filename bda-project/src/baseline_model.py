"""
Baseline Model: Bitcoin Price Direction Prediction
Uses Logistic Regression for binary classification (up/down)
"""

import os
import yaml
import csv
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator


class BaselineModel:
    """Baseline predictor for Bitcoin price movement"""
    
    def __init__(self, config_path: str = "config/bda_project_config.yml"):
        self.config = self._load_config(config_path)
        self.spark = self._create_spark_session()
        self.metrics_log = []
        
    def _load_config(self, path: str) -> dict:
        full_path = os.path.expanduser(f"~/bda-project/{path}")
        with open(full_path, 'r') as f:
            return yaml.safe_load(f)
    
    def _create_spark_session(self) -> SparkSession:
        spark_conf = self.config['spark']
        spark = SparkSession.builder \
            .appName(f"{spark_conf['app_name']}-Baseline") \
            .master(spark_conf['master']) \
            .config("spark.executor.memory", spark_conf['executor_memory']) \
            .config("spark.driver.memory", spark_conf['driver_memory']) \
            .getOrCreate()
        return spark
    
    def load_data(self):
        """Load train and test sets"""
        base_path = os.path.expanduser("~/bda-project/data/processed")
        
        train_df = self.spark.read.parquet(f"{base_path}/train_features.parquet")
        test_df = self.spark.read.parquet(f"{base_path}/test_features.parquet")
        
        print(f" Train: {train_df.count()} | Test: {test_df.count()}")
        
        return train_df, test_df
    
    def build_pipeline(self, feature_cols: list):
        """Build ML pipeline"""
        
        # Assemble features into a vector
        assembler = VectorAssembler(
            inputCols=feature_cols,
            outputCol="features_raw"
        )
        
        # Scale features
        scaler = StandardScaler(
            inputCol="features_raw",
            outputCol="features",
            withStd=True,
            withMean=True
        )
        
        # Logistic Regression
        lr = LogisticRegression(
            featuresCol="features",
            labelCol="target_direction",
            maxIter=100,
            regParam=0.01
        )
        
        # Create pipeline
        pipeline = Pipeline(stages=[assembler, scaler, lr])
        
        return pipeline
    
    def train(self, train_df, feature_cols: list):
        """Train the model"""
        print("🔧 Training baseline model...")
        
        # Build pipeline
        pipeline = self.build_pipeline(feature_cols)
        
        # Train
        start_time = datetime.now()
        model = pipeline.fit(train_df)
        train_time = (datetime.now() - start_time).total_seconds()
        
        print(f" Training completed in {train_time:.2f}s")
        
        self._log_metric("baseline", "train", "train_time_sec", train_time)
        
        return model
    
    def evaluate(self, model, test_df, split_name: str = "test"):
        """Evaluate model performance"""
        print(f"\n Evaluating on {split_name} set...")
        
        # Predictions
        predictions = model.transform(test_df)
        
        # Binary classification metrics
        evaluator_auc = BinaryClassificationEvaluator(
            labelCol="target_direction",
            metricName="areaUnderROC"
        )
        auc = evaluator_auc.evaluate(predictions)
        
        # Multiclass metrics
        evaluator_acc = MulticlassClassificationEvaluator(
            labelCol="target_direction",
            predictionCol="prediction",
            metricName="accuracy"
        )
        accuracy = evaluator_acc.evaluate(predictions)
        
        evaluator_f1 = MulticlassClassificationEvaluator(
            labelCol="target_direction",
            predictionCol="prediction",
            metricName="f1"
        )
        f1 = evaluator_f1.evaluate(predictions)
        
        # Baseline (always predict majority class)
        total = predictions.count()
        majority_class = predictions.groupBy("target_direction").count().orderBy("count", ascending=False).first()[0]
        baseline_acc = predictions.filter(col("target_direction") == majority_class).count() / total
        
        # Print results
        print(f"\n{'='*50}")
        print(f" {split_name.upper()} RESULTS")
        print(f"{'='*50}")
        print(f"Accuracy:          {accuracy:.4f}")
        print(f"F1 Score:          {f1:.4f}")
        print(f"AUC-ROC:           {auc:.4f}")
        print(f"Baseline Accuracy: {baseline_acc:.4f} (always predict {majority_class})")
        print(f"{'='*50}\n")
        
        # Log metrics
        self._log_metric("baseline", split_name, "accuracy", accuracy)
        self._log_metric("baseline", split_name, "f1_score", f1)
        self._log_metric("baseline", split_name, "auc_roc", auc)
        self._log_metric("baseline", split_name, "baseline_accuracy", baseline_acc)
        
        return predictions
    
    def _log_metric(self, run_id: str, stage: str, metric: str, value: float):
        """Log metric to CSV"""
        self.metrics_log.append({
            'run_id': run_id,
            'stage': stage,
            'metric': metric,
            'value': value,
            'timestamp': datetime.now().isoformat()
        })
    
    def save_metrics(self):
        """Save metrics log to CSV"""
        metrics_path = os.path.expanduser("~/bda-project/logs/project_metrics_log.csv")
        os.makedirs(os.path.dirname(metrics_path), exist_ok=True)
        
        # Check if file exists to decide on writing header
        file_exists = os.path.exists(metrics_path)
        
        with open(metrics_path, 'a', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=['run_id', 'stage', 'metric', 'value', 'timestamp'])
            if not file_exists:
                writer.writeheader()
            writer.writerows(self.metrics_log)
        
        print(f" Metrics saved to {metrics_path}")
    
    def explain_plan(self, df, output_file: str):
        """Capture Spark execution plan"""
        evidence_dir = os.path.expanduser("~/bda-project/evidence")
        os.makedirs(evidence_dir, exist_ok=True)
        
        output_path = f"{evidence_dir}/{output_file}"
        
        with open(output_path, 'w') as f:
            f.write("="*80 + "\n")
            f.write(f"SPARK EXECUTION PLAN - {datetime.now()}\n")
            f.write("="*80 + "\n\n")
            
            # Get explain output (mode="formatted" gives the best readable output)
            plan = df._jdf.queryExecution().toString()
            f.write(plan)
        
        print(f" Execution plan saved to {output_path}")
    
    def run(self):
        """Run baseline experiment"""
        print("="*60)
        print(" Baseline Model Training & Evaluation")
        print("="*60)
        
        # Load data
        train_df, test_df = self.load_data()
        
        # Define feature columns
        feature_cols = [
            'return_1h',
            'ma_24h',
            'volume_24h',
            'volatility_24h',
            'blocks_per_hour',
            'total_txs',
            'avg_block_size'
        ]
        
        # Drop rows with nulls in features
        train_df = train_df.dropna(subset=feature_cols + ['target_direction'])
        test_df = test_df.dropna(subset=feature_cols + ['target_direction'])
        
        print(f"\n Using features: {feature_cols}")
        print(f" Target: {self.config['prediction']['target']}")
        print(f" Horizon: {self.config['prediction']['horizon_minutes']} minutes\n")
        
        # Train
        model = self.train(train_df, feature_cols)
        
        # Evaluate on train (sanity check)
        train_predictions = self.evaluate(model, train_df, "train")
        
        # Evaluate on test
        test_predictions = self.evaluate(model, test_df, "test")
        
        # Save metrics
        self.save_metrics()
        
        # Capture execution plans
        self.explain_plan(train_predictions, "baseline_train_plan.txt")
        self.explain_plan(test_predictions, "baseline_test_plan.txt")
        
        # Save model
        model_path = os.path.expanduser("~/bda-project/models/baseline_model")
        model.write().overwrite().save(model_path)
        print(f" Model saved to {model_path}")
        
        print("\n Baseline experiment completed!")
        
        self.spark.stop()


if __name__ == "__main__":
    from pyspark.sql.functions import col
    
    baseline = BaselineModel()
    baseline.run()