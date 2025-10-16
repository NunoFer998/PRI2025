#!/usr/bin/env python3

import pandas as pd
import numpy as np
from pathlib import Path
from collections import Counter

class DataCharacterizer:
    def __init__(self, data_path):
        self.data_path = Path(data_path)
        self.results = {}
        self.output_dir = self.data_path.parent / "characterization_results"
        self.output_dir.mkdir(exist_ok=True)
        self.log_file = self.output_dir / "analysis_report.txt"
        self.log_file.write_text("")

    def log(self, message):
        """Write message to file and print"""
        with open(self.log_file, 'a', encoding='utf-8') as f:
            f.write(message + "\n")
        print(message)

    def analyze_csv_file(self, file_path):
        self.log(f"\nAnalyzing: {file_path.name}")
        try:
            df = pd.read_csv(file_path)
            analysis = {
                'filename': file_path.name,
                'basic_info': self._get_basic_info(df),
                'columns_info': self._get_columns_info(df),
                'missing_data': self._get_missing_data(df),
                'unique_values': self._get_unique_values(df),
            }
            if any(col in df.columns for col in ['disease', 'Disease', 'label']):
                analysis['disease_analysis'] = self._analyze_diseases(df)
            if any(col in df.columns for col in ['symptoms', 'Symptoms', 'Symptom_List']):
                analysis['symptom_analysis'] = self._analyze_symptoms(df)
            if 'text' in df.columns:
                analysis['text_analysis'] = self._analyze_text(df)
            return analysis
        except Exception as e:
            self.log(f"Error analyzing {file_path.name}: {e}")
            return {'filename': file_path.name, 'error': str(e)}

    def _get_basic_info(self, df):
        return {
            'num_rows': len(df),
            'num_columns': len(df.columns),
            'total_cells': df.size,
            'memory_usage_mb': df.memory_usage(deep=True).sum() / (1024*1024),
            'column_names': list(df.columns)
        }

    def _get_columns_info(self, df):
        info = {}
        for col in df.columns:
            info[col] = {
                'dtype': str(df[col].dtype),
                'non_null_count': df[col].notna().sum(),
                'null_count': df[col].isna().sum(),
                'unique_count': df[col].nunique(),
                'sample_values': df[col].dropna().head(3).tolist()
            }
        return info

    def _get_missing_data(self, df):
        missing_counts = df.isnull().sum()
        missing_percent = (missing_counts.sum() / df.size) * 100
        columns_with_missing = {col: {'count': int(missing_counts[col]),
                                       'percentage': float((missing_counts[col]/len(df))*100)}
                                for col in df.columns if missing_counts[col] > 0}
        return {'total_missing_cells': int(missing_counts.sum()),
                'missing_percentage': float(missing_percent),
                'columns_with_missing': columns_with_missing}

    def _get_unique_values(self, df):
        unique_info = {}
        for col in df.select_dtypes(include=['object']).columns:
            n_unique = df[col].nunique()
            if n_unique <= 50:
                top_values = df[col].value_counts().head(10).to_dict()
                unique_info[col] = {'unique_count': n_unique, 'top_values': top_values}
            else:
                unique_info[col] = {'unique_count': n_unique, 'note': 'Too many unique values to display'}
        return unique_info

    def _analyze_diseases(self, df):
        disease_col = next((col for col in ['disease','Disease','label'] if col in df.columns), None)
        if not disease_col: return None
        counts = df[disease_col].value_counts()
        return {
            'disease_column': disease_col,
            'total_diseases': int(df[disease_col].nunique()),
            'total_records': len(df),
            'most_common_diseases': counts.head(10).to_dict(),
            'avg_records_per_disease': float(len(df)/df[disease_col].nunique())
        }

    def _analyze_symptoms(self, df):
        symptom_col = next((col for col in ['symptoms','Symptoms','Symptom_List'] if col in df.columns), None)
        if not symptom_col: return None
        all_symptoms = []
        counts_per_record = []
        for s in df[symptom_col].dropna():
            if isinstance(s, str):
                items = [x.strip() for x in s.replace('|',',').split(',') if x.strip()]
                all_symptoms.extend(items)
                counts_per_record.append(len(items))
        counter = Counter(all_symptoms)
        return {
            'symptom_column': symptom_col,
            'total_unique_symptoms': len(counter),
            'total_symptom_mentions': sum(counter.values()),
            'most_common_symptoms': dict(counter.most_common(10)),
            'avg_symptoms_per_record': float(np.mean(counts_per_record)) if counts_per_record else 0
        }

    def _analyze_text(self, df):
        lengths = df['text'].dropna().apply(lambda x: len(str(x)))
        words = df['text'].dropna().apply(lambda x: len(str(x).split()))
        return {
            'total_text_records': int(df['text'].notna().sum()),
            'avg_text_length': float(lengths.mean()),
            'min_text_length': int(lengths.min()),
            'max_text_length': int(lengths.max()),
            'avg_word_count': float(words.mean()),
            'min_word_count': int(words.min()),
            'max_word_count': int(words.max())
        }

    def print_analysis(self, analysis):
        self.log("\n" + "="*50)
        self.log(f"FILE: {analysis.get('filename','N/A')}")
        self.log("="*50)
        if 'error' in analysis:
            self.log(f"Error: {analysis['error']}")
            return
        bi = analysis['basic_info']
        self.log(f"Rows: {bi['num_rows']:,} | Columns: {bi['num_columns']} | Memory: {bi['memory_usage_mb']:.2f} MB")
        self.log(f"Column Names: {', '.join(bi['column_names'])}")
        self.log("\nMissing Data:")
        md = analysis['missing_data']
        self.log(f"  Total missing cells: {md['total_missing_cells']} ({md['missing_percentage']:.2f}%)")
        for col, info in md['columns_with_missing'].items():
            self.log(f"    {col}: {info['count']} ({info['percentage']:.2f}%)")
        if 'disease_analysis' in analysis and analysis['disease_analysis']:
            da = analysis['disease_analysis']
            self.log("\nDisease Analysis:")
            self.log(f"  Total Diseases: {da['total_diseases']} | Avg records/disease: {da['avg_records_per_disease']:.2f}")
            self.log(f"  Most Common Diseases: {da['most_common_diseases']}")
        if 'symptom_analysis' in analysis and analysis['symptom_analysis']:
            sa = analysis['symptom_analysis']
            self.log("\nSymptom Analysis:")
            self.log(f"  Total unique symptoms: {sa['total_unique_symptoms']} | Total mentions: {sa['total_symptom_mentions']}")
            self.log(f"  Most common symptoms: {sa['most_common_symptoms']}")
        if 'text_analysis' in analysis and analysis['text_analysis']:
            ta = analysis['text_analysis']
            self.log(f"\nText Analysis: {ta}")

    def analyze_all_files(self):
        csv_files = list(self.data_path.glob('*.csv'))
        if not csv_files:
            self.log(f"No CSV files found in {self.data_path}")
            return
        for f in csv_files:
            analysis = self.analyze_csv_file(f)
            self.results[f.name] = analysis
            self.print_analysis(analysis)


def main():
    data_path = Path(__file__).parent.parent / 'data' / 'original'
    if not data_path.exists():
        print(f"Data path does not exist: {data_path}")
        return
    characterizer = DataCharacterizer(data_path)
    characterizer.analyze_all_files()
    print(f"\nAnalysis complete. Results saved to '{characterizer.log_file}'")


if __name__ == "__main__":
    main()
