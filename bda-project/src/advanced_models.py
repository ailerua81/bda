"""
Advanced Models for Bitcoin Price Prediction
Includes Random Forest, Gradient Boosting, and hyperparameter tuning
"""

import os
import yaml
import csv
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer
from pyspark.ml.classification import (
    LogisticRegression,
    RandomForestClassifier,
    GBTClassifier
)
from pyspark.ml.evaluation import (
    BinaryClassificationEvaluator,
    MulticlassClassificationEvaluator
)
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator, TrainValidationSplit


class AdvancedModelExperiments:
    """Advanced model experimentation framework"""
    
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
            .appName(f"{spark_conf['app_name']}-Advanced") \
            .master(spark_conf['master']) \
            .config("spark.executor.memory", spark_conf['executor_memory']) \
            .config("spark.driver.memory", spark_conf['driver_memory']) \
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
            .getOrCreate()
        return spark
    
    def load_data(self, use_advanced: bool = True):
        """Load train and test sets"""
        base_path = os.path.expanduser("~/bda-project/data/processed")
        
        if use_advanced:
            suffix = "_advanced"
        else:
            suffix = ""
        
        train_df = self.spark.read.parquet(f"{base_path}/train_features{suffix}.parquet")
        test_df = self.spark.read.parquet(f"{base_path}/test_features{suffix}.parquet")
        
        # Fix: Convert string columns to double
        string_cols = ['open', 'high', 'low', 'close', 'volume']
        for col_name in string_cols:
            if col_name in train_df.columns:
                train_df = train_df.withColumn(col_name, col(col_name).cast("double"))
                test_df = test_df.withColumn(col_name, col(col_name).cast("double"))
        
        print(f" Train: {train_df.count()} | Test: {test_df.count()}")
        print(f" Features: {len(train_df.columns)} columns")
        
        return train_df, test_df
    
    def get_feature_columns(self, df, exclude_cols: list = None) -> list:
        """Get feature columns (excluding metadata and target)"""
        if exclude_cols is None:
            exclude_cols = [
                'timestamp', 'datetime', 'target_direction', 'target_magnitude',
                'future_close', 'hour', 'hour_ts', 'block_hash', 'row_id'
            ]
        
        feature_cols = [c for c in df.columns if c not in exclude_cols]
        return feature_cols
    
    def build_pipeline(self, model, feature_cols: list):
        """Build ML pipeline with given model"""
        
        # Assemble features
        assembler = VectorAssembler(
            inputCols=feature_cols,
            outputCol="features_raw",
            handleInvalid="skip"
        )
        
        # Scale features
        scaler = StandardScaler(
            inputCol="features_raw",
            outputCol="features",
            withStd=True,
            withMean=True
        )
        
        # Create pipeline
        pipeline = Pipeline(stages=[assembler, scaler, model])
        
        return pipeline
    
    def train_model(self, train_df, model, feature_cols: list, run_id: str):
        """Train a single model"""
        print(f"\n🔧 Training {run_id}...")
        
        # Build pipeline
        pipeline = self.build_pipeline(model, feature_cols)
        
        # Train
        start_time = datetime.now()
        fitted_model = pipeline.fit(train_df)
        train_time = (datetime.now() - start_time).total_seconds()
        
        print(f" Training completed in {train_time:.2f}s")
        
        self._log_metric(run_id, "train", "train_time_sec", train_time)
        
        return fitted_model
    
    def evaluate_model(self, model, test_df, run_id: str, split_name: str = "test"):
        """Evaluate model performance"""
        print(f"\n Evaluating {run_id} on {split_name} set...")
        
        # Predictions
        predictions = model.transform(test_df)
        
        # Binary classification metrics
        evaluator_auc = BinaryClassificationEvaluator(
            labelCol="target_direction",
            metricName="areaUnderROC"
        )
        auc = evaluator_auc.evaluate(predictions)
        
        # Accuracy
        evaluator_acc = MulticlassClassificationEvaluator(
            labelCol="target_direction",
            predictionCol="prediction",
            metricName="accuracy"
        )
        accuracy = evaluator_acc.evaluate(predictions)
        
        # F1 Score
        evaluator_f1 = MulticlassClassificationEvaluator(
            labelCol="target_direction",
            predictionCol="prediction",
            metricName="f1"
        )
        f1 = evaluator_f1.evaluate(predictions)
        
        # Precision
        evaluator_precision = MulticlassClassificationEvaluator(
            labelCol="target_direction",
            predictionCol="prediction",
            metricName="weightedPrecision"
        )
        precision = evaluator_precision.evaluate(predictions)
        
        # Recall
        evaluator_recall = MulticlassClassificationEvaluator(
            labelCol="target_direction",
            predictionCol="prediction",
            metricName="weightedRecall"
        )
        recall = evaluator_recall.evaluate(predictions)
        
        # Print results
        print(f"\n{'='*50}")
        print(f" {run_id.upper()} - {split_name.upper()} RESULTS")
        print(f"{'='*50}")
        print(f"Accuracy:   {accuracy:.4f}")
        print(f"Precision:  {precision:.4f}")
        print(f"Recall:     {recall:.4f}")
        print(f"F1 Score:   {f1:.4f}")
        print(f"AUC-ROC:    {auc:.4f}")
        print(f"{'='*50}\n")
        
        # Log metrics
        self._log_metric(run_id, split_name, "accuracy", accuracy)
        self._log_metric(run_id, split_name, "precision", precision)
        self._log_metric(run_id, split_name, "recall", recall)
        self._log_metric(run_id, split_name, "f1_score", f1)
        self._log_metric(run_id, split_name, "auc_roc", auc)
        
        return predictions, {
            'accuracy': accuracy,
            'precision': precision,
            'recall': recall,
            'f1': f1,
            'auc': auc
        }
    
    def experiment_logistic_regression(self, train_df, test_df, feature_cols: list):
        """Experiment 1: Logistic Regression with advanced features"""
        run_id = "lr_advanced"
        
        lr = LogisticRegression(
            featuresCol="features",
            labelCol="target_direction",
            maxIter=100,
            regParam=0.01,
            elasticNetParam=0.0
        )
        
        model = self.train_model(train_df, lr, feature_cols, run_id)
        _, metrics = self.evaluate_model(model, test_df, run_id, "test")
        
        return model, metrics
    
    def experiment_random_forest(self, train_df, test_df, feature_cols: list):
        """Experiment 2: Random Forest"""
        run_id = "rf_advanced"
        
        rf = RandomForestClassifier(
            featuresCol="features",
            labelCol="target_direction",
            numTrees=100,
            maxDepth=10,
            minInstancesPerNode=5,
            seed=42
        )
        
        model = self.train_model(train_df, rf, feature_cols, run_id)
        _, metrics = self.evaluate_model(model, test_df, run_id, "test")
        
        # Feature importance
        rf_model = model.stages[-1]
        importances = rf_model.featureImportances.toArray()
        
        print("\n Top 10 Feature Importances:")
        feature_importance = list(zip(feature_cols, importances))
        feature_importance.sort(key=lambda x: x[1], reverse=True)
        for feat, imp in feature_importance[:10]:
            print(f"  {feat:30s}: {imp:.4f}")
        
        return model, metrics
    
    def experiment_gradient_boosting(self, train_df, test_df, feature_cols: list):
        """Experiment 3: Gradient Boosting Trees"""
        run_id = "gbt_advanced"
        
        gbt = GBTClassifier(
            featuresCol="features",
            labelCol="target_direction",
            maxIter=50,
            maxDepth=5,
            stepSize=0.1,
            seed=42
        )
        
        model = self.train_model(train_df, gbt, feature_cols, run_id)
        _, metrics = self.evaluate_model(model, test_df, run_id, "test")
        
        # Feature importance
        gbt_model = model.stages[-1]
        importances = gbt_model.featureImportances.toArray()
        
        print("\n Top 10 Feature Importances:")
        feature_importance = list(zip(feature_cols, importances))
        feature_importance.sort(key=lambda x: x[1], reverse=True)
        for feat, imp in feature_importance[:10]:
            print(f"  {feat:30s}: {imp:.4f}")
        
        return model, metrics
    
    def experiment_hyperparameter_tuning(self, train_df, test_df, feature_cols: list):
        """Experiment 4: Hyperparameter tuning for Random Forest"""
        run_id = "rf_tuned"
        
        print(f"\n Hyperparameter tuning for {run_id}...")
        
        rf = RandomForestClassifier(
            featuresCol="features",
            labelCol="target_direction",
            seed=42
        )
        
        pipeline = self.build_pipeline(rf, feature_cols)
        
        # Parameter grid
        paramGrid = ParamGridBuilder() \
            .addGrid(rf.numTrees, [50, 100, 150]) \
            .addGrid(rf.maxDepth, [5, 10, 15]) \
            .addGrid(rf.minInstancesPerNode, [1, 5, 10]) \
            .build()
        
        # Evaluator
        evaluator = BinaryClassificationEvaluator(
            labelCol="target_direction",
            metricName="areaUnderROC"
        )
        
        # Train-validation split (faster than cross-validation)
        tvs = TrainValidationSplit(
            estimator=pipeline,
            estimatorParamMaps=paramGrid,
            evaluator=evaluator,
            trainRatio=0.8,
            seed=42
        )
        
        print(" Running hyperparameter search...")
        start_time = datetime.now()
        model = tvs.fit(train_df)
        tune_time = (datetime.now() - start_time).total_seconds()
        
        print(f" Tuning completed in {tune_time:.2f}s")
        self._log_metric(run_id, "train", "tuning_time_sec", tune_time)
        
        # Best model
        best_model = model.bestModel
        
        # Evaluate
        _, metrics = self.evaluate_model(best_model, test_df, run_id, "test")
        
        return best_model, metrics
    
    def ablation_study(self, train_df, test_df):
        """Ablation study: test importance of feature groups"""
        print("\n" + "="*60)
        print(" Ablation Study: Feature Group Importance")
        print("="*60)
        
        # Define feature groups
        all_features = self.get_feature_columns(train_df)
        
        # Basic features (from baseline)
        basic_features = [
            'return_1h', 'ma_24h', 'volume_24h', 'volatility_24h',
            'blocks_per_hour', 'total_txs', 'avg_block_size'
        ]
        basic_features = [f for f in basic_features if f in all_features]
        
        # Technical indicators
        technical_features = [f for f in all_features if any(x in f for x in 
            ['ma_', 'bb_', 'momentum', 'roc', 'hl_spread', 'price_to_ma'])]
        
        # On-chain features
        onchain_features = [f for f in all_features if any(x in f for x in
            ['block', 'tx_', 'throughput', 'size_trend', 'variance'])]
        
        # Market microstructure
        market_features = [f for f in all_features if any(x in f for x in
            ['volume_', 'vwap', 'illiquidity'])]
        
        # Time features
        time_features = [f for f in all_features if any(x in f for x in
            ['hour_', 'day_', 'weekend', 'session'])]
        
        ablation_results = {}
        
        # Test each group
        feature_groups = {
            'basic_only': basic_features,
            'basic_technical': basic_features + technical_features,
            'basic_onchain': basic_features + onchain_features,
            'basic_market': basic_features + market_features,
            'all_features': all_features
        }
        
        for group_name, features in feature_groups.items():
            if not features:
                continue
                
            print(f"\n Testing: {group_name} ({len(features)} features)")
            
            rf = RandomForestClassifier(
                featuresCol="features",
                labelCol="target_direction",
                numTrees=50,
                maxDepth=10,
                seed=42
            )
            
            model = self.train_model(train_df, rf, features, f"ablation_{group_name}")
            _, metrics = self.evaluate_model(model, test_df, f"ablation_{group_name}", "test")
            
            ablation_results[group_name] = metrics
        
        # Summary
        print("\n" + "="*60)
        print(" Ablation Study Summary")
        print("="*60)
        print(f"{'Group':<20} {'Features':>10} {'AUC-ROC':>10} {'Accuracy':>10}")
        print("-"*60)
        for group_name, metrics in ablation_results.items():
            n_features = len(feature_groups[group_name])
            print(f"{group_name:<20} {n_features:>10} {metrics['auc']:>10.4f} {metrics['accuracy']:>10.4f}")
        print("="*60)
        
        return ablation_results
    
    def _log_metric(self, run_id: str, stage: str, metric: str, value: float):
        """Log metric to list"""
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
        
        # Append to existing file
        file_exists = os.path.exists(metrics_path)
        
        with open(metrics_path, 'a', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=['run_id', 'stage', 'metric', 'value', 'timestamp'])
            if not file_exists:
                writer.writeheader()
            writer.writerows(self.metrics_log)
        
        print(f"\n Metrics saved to {metrics_path}")
    
    def run_all_experiments(self):
        """Run all experiments"""
        print("="*60)
        print(" Advanced Model Experiments - M3")
        print("="*60)
        
        # Load data
        train_df, test_df = self.load_data(use_advanced=True)
        
        # Get feature columns
        feature_cols = self.get_feature_columns(train_df)
        
        # Drop rows with nulls
        train_df = train_df.dropna(subset=feature_cols + ['target_direction'])
        test_df = test_df.dropna(subset=feature_cols + ['target_direction'])
        
        print(f"\n Using {len(feature_cols)} features")
        print(f" Train samples: {train_df.count()}")
        print(f" Test samples: {test_df.count()}")
        
        results = {}
        
        # Experiment 1: Logistic Regression
        model_lr, metrics_lr = self.experiment_logistic_regression(train_df, test_df, feature_cols)
        results['lr_advanced'] = metrics_lr
        
        # Experiment 2: Random Forest
        model_rf, metrics_rf = self.experiment_random_forest(train_df, test_df, feature_cols)
        results['rf_advanced'] = metrics_rf
        
        # Experiment 3: Gradient Boosting
        model_gbt, metrics_gbt = self.experiment_gradient_boosting(train_df, test_df, feature_cols)
        results['gbt_advanced'] = metrics_gbt
        
        # Experiment 4: Hyperparameter Tuning
        model_tuned, metrics_tuned = self.experiment_hyperparameter_tuning(train_df, test_df, feature_cols)
        results['rf_tuned'] = metrics_tuned
        
        # Experiment 5: Ablation Study
        ablation_results = self.ablation_study(train_df, test_df)
        
        # Save metrics
        self.save_metrics()
        
        # Final comparison
        print("\n" + "="*60)
        print(" Final Model Comparison")
        print("="*60)
        print(f"{'Model':<20} {'AUC-ROC':>10} {'Accuracy':>10} {'F1 Score':>10}")
        print("-"*60)
        for model_name, metrics in results.items():
            print(f"{model_name:<20} {metrics['auc']:>10.4f} {metrics['accuracy']:>10.4f} {metrics['f1']:>10.4f}")
        print("="*60)
        
        # Best model
        best_model = max(results.items(), key=lambda x: x[1]['auc'])
        print(f"\n🏆 Best model: {best_model[0]} with AUC-ROC = {best_model[1]['auc']:.4f}")
        
        print("\n All experiments completed!")
        
        self.spark.stop()


if __name__ == "__main__":
    experiments = AdvancedModelExperiments()
    experiments.run_all_experiments()