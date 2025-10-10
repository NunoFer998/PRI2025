#!/usr/bin/env python3
"""
Data Characterization Script for Original Dataset
Analyzes all CSV files in the original folder and generates comprehensive statistics
"""

import pandas as pd
import numpy as np
import os
import json
from pathlib import Path
from collections import Counter
import matplotlib.pyplot as plt
import seaborn as sns


class DataCharacterizer:
    
    def __init__(self, data_path):
        self.data_path = Path(data_path)
        self.results = {}
        self.output_dir = self.data_path.parent / "characterization_results"
        self.output_dir.mkdir(exist_ok=True)
        
    def analyze_csv_file(self, file_path):
        print(f"\nAnalyzing: {file_path.name}")
        print("=" * 60)
        
        try:
            df = pd.read_csv(file_path)
            
            analysis = {
                'filename': file_path.name,
                'basic_info': self._get_basic_info(df),
                'columns_info': self._get_columns_info(df),
                'missing_data': self._get_missing_data(df),
                'unique_values': self._get_unique_values(df),
            }
            
            if 'disease' in df.columns or 'Disease' in df.columns or 'label' in df.columns:
                analysis['disease_analysis'] = self._analyze_diseases(df)
                
            if 'symptoms' in df.columns or 'Symptoms' in df.columns or 'Symptom_List' in df.columns:
                analysis['symptom_analysis'] = self._analyze_symptoms(df)
                
            if 'text' in df.columns:
                analysis['text_analysis'] = self._analyze_text(df)
                
            return analysis
            
        except Exception as e:
            print(f"Error analyzing {file_path.name}: {str(e)}")
            return {'filename': file_path.name, 'error': str(e)}
    
    def _get_basic_info(self, df):
        return {
            'num_rows': len(df),
            'num_columns': len(df.columns),
            'total_cells': df.size,
            'memory_usage_mb': df.memory_usage(deep=True).sum() / (1024 * 1024),
            'column_names': list(df.columns)
        }
    
    def _get_columns_info(self, df):
        columns_info = {}
        for col in df.columns:
            columns_info[col] = {
                'dtype': str(df[col].dtype),
                'non_null_count': df[col].notna().sum(),
                'null_count': df[col].isna().sum(),
                'unique_count': df[col].nunique(),
                'sample_values': df[col].dropna().head(3).tolist()
            }
        return columns_info
    
    def _get_missing_data(self, df):
        """Analyze missing data patterns"""
        missing_counts = df.isnull().sum()
        missing_percentages = (missing_counts / len(df)) * 100
        
        return {
            'total_missing_cells': int(missing_counts.sum()),
            'missing_percentage': float(missing_counts.sum() / df.size * 100),
            'columns_with_missing': {
                col: {
                    'count': int(missing_counts[col]),
                    'percentage': float(missing_percentages[col])
                }
                for col in df.columns if missing_counts[col] > 0
            }
        }
    
    def _get_unique_values(self, df):
        categorical_cols = df.select_dtypes(include=['object']).columns
        unique_info = {}
        
        for col in categorical_cols:
            unique_count = df[col].nunique()
            if unique_count <= 50:
                value_counts = df[col].value_counts().head(10).to_dict()
                unique_info[col] = {
                    'unique_count': int(unique_count),
                    'top_values': {str(k): int(v) for k, v in value_counts.items()}
                }
            else:
                unique_info[col] = {
                    'unique_count': int(unique_count),
                    'note': 'Too many unique values to display'
                }
        
        return unique_info
    
    def _analyze_diseases(self, df):
        disease_col = None
        for col in ['disease', 'Disease', 'Disease_Name', 'label', 'Name']:
            if col in df.columns:
                disease_col = col
                break
        
        if disease_col is None:
            return None
        
        disease_counts = df[disease_col].value_counts()
        
        return {
            'disease_column': disease_col,
            'total_diseases': int(df[disease_col].nunique()),
            'total_records': len(df),
            'most_common_diseases': disease_counts.head(10).to_dict(),
            'least_common_diseases': disease_counts.tail(5).to_dict(),
            'average_records_per_disease': float(len(df) / df[disease_col].nunique()),
        }
    
    def _analyze_symptoms(self, df):
        symptom_col = None
        for col in ['symptoms', 'Symptoms', 'Symptom_List']:
            if col in df.columns:
                symptom_col = col
                break
        
        if symptom_col is None:
            return None
        
        all_symptoms = []
        for symptoms in df[symptom_col].dropna():
            if isinstance(symptoms, str):
                symptom_list = [s.strip() for s in symptoms.replace('|', ',').split(',')]
                all_symptoms.extend([s for s in symptom_list if s])
        
        symptom_counts = Counter(all_symptoms)
        
        symptom_counts_per_record = []
        for symptoms in df[symptom_col].dropna():
            if isinstance(symptoms, str):
                count = len([s.strip() for s in symptoms.replace('|', ',').split(',') if s.strip()])
                symptom_counts_per_record.append(count)
        
        return {
            'symptom_column': symptom_col,
            'total_unique_symptoms': len(symptom_counts),
            'total_symptom_mentions': sum(symptom_counts.values()),
            'most_common_symptoms': dict(symptom_counts.most_common(20)),
            'least_common_symptoms': dict(symptom_counts.most_common()[-10:]),
            'avg_symptoms_per_record': float(np.mean(symptom_counts_per_record)) if symptom_counts_per_record else 0,
            'min_symptoms_per_record': int(min(symptom_counts_per_record)) if symptom_counts_per_record else 0,
            'max_symptoms_per_record': int(max(symptom_counts_per_record)) if symptom_counts_per_record else 0,
        }
    
    def _analyze_text(self, df):
        """Analyze text column"""
        if 'text' not in df.columns:
            return None
        
        text_lengths = df['text'].dropna().apply(lambda x: len(str(x)))
        word_counts = df['text'].dropna().apply(lambda x: len(str(x).split()))
        
        return {
            'total_text_records': int(df['text'].notna().sum()),
            'avg_text_length': float(text_lengths.mean()),
            'min_text_length': int(text_lengths.min()),
            'max_text_length': int(text_lengths.max()),
            'avg_word_count': float(word_counts.mean()),
            'min_word_count': int(word_counts.min()),
            'max_word_count': int(word_counts.max()),
        }
    
    def print_analysis(self, analysis):
        """Print analysis results in a readable format"""
        if 'error' in analysis:
            print(f"Error: {analysis['error']}")
            return
        
        print(f"\n{'='*60}")
        print(f"FILE: {analysis['filename']}")
        print(f"{'='*60}")
        
        print("\n📊 BASIC INFORMATION:")
        print(f"  Rows: {analysis['basic_info']['num_rows']:,}")
        print(f"  Columns: {analysis['basic_info']['num_columns']}")
        print(f"  Total Cells: {analysis['basic_info']['total_cells']:,}")
        print(f"  Memory Usage: {analysis['basic_info']['memory_usage_mb']:.2f} MB")
        print(f"  Column Names: {', '.join(analysis['basic_info']['column_names'])}")

        print("\n🔍 COLUMNS INFO:")
        for col, info in analysis['columns_info'].items():
            print(f"  {col}:")
            print(f"    Type: {info['dtype']}")
            print(f"    Non-Null Count: {info['non_null_count']:,}")
            print(f"    Null Count: {info['null_count']:,}")
            print(f"    Unique Count: {info['unique_count']}")
            print(f"    Sample Values: {info['sample_values']}")
        
        print("\n❌ MISSING DATA:")
        if analysis['missing_data']['total_missing_cells'] > 0:
            print(f"  Total Missing: {analysis['missing_data']['total_missing_cells']:,} "
                  f"({analysis['missing_data']['missing_percentage']:.2f}%)")
            for col, info in analysis['missing_data']['columns_with_missing'].items():
                print(f"    {col}: {info['count']:,} ({info['percentage']:.2f}%)")
        else:
            print("  No missing data ✓")
        
        if 'disease_analysis' in analysis and analysis['disease_analysis']:
            print("\n🏥 DISEASE ANALYSIS:")
            da = analysis['disease_analysis']
            print(f"  Total Unique Diseases: {da['total_diseases']}")
            print(f"  Total Records: {da['total_records']}")
            print(f"  Avg Records per Disease: {da['average_records_per_disease']:.2f}")
            print(f"\n  Top 5 Most Common Diseases:")
            for disease, count in list(da['most_common_diseases'].items())[:5]:
                print(f"    {disease}: {count}")
        
        if 'symptom_analysis' in analysis and analysis['symptom_analysis']:
            print("\n💊 SYMPTOM ANALYSIS:")
            sa = analysis['symptom_analysis']
            print(f"  Total Unique Symptoms: {sa['total_unique_symptoms']}")
            print(f"  Total Symptom Mentions: {sa['total_symptom_mentions']}")
            print(f"  Avg Symptoms per Record: {sa['avg_symptoms_per_record']:.2f}")
            print(f"  Min/Max Symptoms per Record: {sa['min_symptoms_per_record']}/{sa['max_symptoms_per_record']}")
            print(f"\n  Top 10 Most Common Symptoms:")
            for symptom, count in list(sa['most_common_symptoms'].items())[:10]:
                print(f"    {symptom}: {count}")
        
        if 'text_analysis' in analysis and analysis['text_analysis']:
            print("\n📝 TEXT ANALYSIS:")
            ta = analysis['text_analysis']
            print(f"  Total Text Records: {ta['total_text_records']}")
            print(f"  Avg Text Length: {ta['avg_text_length']:.2f} characters")
            print(f"  Text Length Range: {ta['min_text_length']} - {ta['max_text_length']} characters")
            print(f"  Avg Word Count: {ta['avg_word_count']:.2f} words")
            print(f"  Word Count Range: {ta['min_word_count']} - {ta['max_word_count']} words")
    
    def analyze_all_files(self):
        csv_files = list(self.data_path.glob('*.csv'))
        
        if not csv_files:
            print(f"No CSV files found in {self.data_path}")
            return
        
        print(f"\nFound {len(csv_files)} CSV files to analyze")
        print("=" * 60)
        
        for csv_file in csv_files:
            analysis = self.analyze_csv_file(csv_file)
            self.results[csv_file.name] = analysis
            self.print_analysis(analysis)

        final_csv = self.data_path.parent / 'clean' / 'final.csv'
        if final_csv.exists():
            print(f"\nAnalyzing cleaned dataset: {final_csv.name}")
            print("=" * 60)
            analysis = self.analyze_csv_file(final_csv)
            self.results[final_csv.name] = analysis
            self.print_analysis(analysis)
    


def main():
    data_path = Path(__file__).parent.parent / 'data' / 'original'
    
    if not data_path.exists():
        print(f"Error: Data path does not exist: {data_path}")
        return
    
    print("\n" + "=" * 80)
    print("DATA CHARACTERIZATION TOOL")
    print("=" * 80)
    print(f"\nAnalyzing data in: {data_path}")
    
    characterizer = DataCharacterizer(data_path)
    characterizer.analyze_all_files()
    
    print("\n" + "=" * 80)
    print("ANALYSIS COMPLETE!")
    print("=" * 80)


if __name__ == "__main__":
    main()
