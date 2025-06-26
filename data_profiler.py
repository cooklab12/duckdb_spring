import pandas as pd
import numpy as np
import re
from collections import Counter
from datetime import datetime
import warnings
warnings.filterwarnings('ignore')

class DataProfiler:
    def generate_html_report(self, output_path="data_profile_report.html"):
        """Generate a beautiful HTML data profiling report with Font Awesome icons."""
        if not self.profile_report:
            print("No report generated yet. Run generate_full_report() first.")
            return

        report = self.profile_report
        # Font Awesome CDN and some modern CSS
        html = f"""
        <!DOCTYPE html>
        <html lang='en'>
        <head>
            <meta charset='UTF-8'>
            <meta name='viewport' content='width=device-width, initial-scale=1.0'>
            <title>CSV Data Profiling Report</title>
            <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.2/css/all.min.css">
            <style>
                body {{ font-family: 'Segoe UI', Arial, sans-serif; background: #f7f9fa; color: #222; margin: 0; }}
                .container {{ max-width: 900px; margin: 40px auto; background: #fff; border-radius: 12px; box-shadow: 0 2px 16px #0001; padding: 32px; }}
                h1, h2, h3 {{ color: #2c3e50; }}
                h1 {{ font-size: 2.2em; margin-bottom: 0.2em; }}
                .section {{ margin-bottom: 2.5em; }}
                .icon {{ color: #007bff; margin-right: 0.5em; }}
                table {{ border-collapse: collapse; width: 100%; margin: 1em 0; }}
                th, td {{ border: 1px solid #e0e0e0; padding: 8px 12px; text-align: left; }}
                th {{ background: #f0f4f8; }}
                tr:nth-child(even) {{ background: #fafbfc; }}
                .tag {{ display: inline-block; background: #eaf6ff; color: #007bff; border-radius: 6px; padding: 2px 8px; font-size: 0.95em; margin-right: 4px; }}
                .score-bar {{ background: #e0e0e0; border-radius: 6px; overflow: hidden; height: 18px; width: 200px; display: inline-block; vertical-align: middle; }}
                .score-bar-inner {{ background: #28a745; height: 100%; transition: width 0.5s; }}
                .recommendation {{ background: #fffbe6; border-left: 4px solid #ffc107; padding: 8px 12px; margin: 8px 0; border-radius: 4px; }}
                .fa-check {{ color: #28a745; }}
                .fa-xmark, .fa-exclamation {{ color: #dc3545; }}
                .fa-circle-info {{ color: #17a2b8; }}
                .fa-table {{ color: #6f42c1; }}
                .fa-hashtag {{ color: #fd7e14; }}
                .fa-calendar {{ color: #20c997; }}
                .fa-font {{ color: #6610f2; }}
                .fa-chart-bar {{ color: #007bff; }}
                .fa-list {{ color: #6c757d; }}
                .fa-warning {{ color: #ffc107; }}
            </style>
        </head>
        <body><div class='container'>
        <h1><i class="fa-solid fa-table icon"></i>CSV Data Profiling Report</h1>
        <div style='color:#888;font-size:1em;'>Generated at: {report['report_generated_at']}</div>
        <div class='section'>
            <h2><i class="fa-solid fa-circle-info icon"></i>Basic Information</h2>
            <table>
                <tr><th>File Name</th><td>{report['basic_info']['file_name']}</td></tr>
                <tr><th>Dimensions</th><td>{report['basic_info']['total_rows']} rows × {report['basic_info']['total_columns']} columns</td></tr>
                <tr><th>Memory Usage</th><td>{report['basic_info']['memory_usage_mb']} MB</td></tr>
                <tr><th>Columns</th><td>{', '.join(report['basic_info']['column_names'])}</td></tr>
            </table>
        </div>

        <div class='section'>
            <h2><i class="fa-solid fa-xmark icon"></i>Missing Data Analysis</h2>
            <table>
                <tr><th>Overall Missing</th><td>{report['missing_data_analysis']['overall']['missing_percentage']}% ({report['missing_data_analysis']['overall']['total_missing_cells']} cells)</td></tr>
                <tr><th>Rows with Missing Data</th><td>{report['missing_data_analysis']['rows_with_missing']['percentage']}% ({report['missing_data_analysis']['rows_with_missing']['count']} rows)</td></tr>
            </table>
            <h3>Missing by Column</h3>
            <table><tr><th>Column</th><th>Missing %</th><th>Missing Count</th></tr>
        """
        for col, info in report['missing_data_analysis']['by_column'].items():
            if info['missing_percentage'] > 0:
                html += f"<tr><td>{col}</td><td>{info['missing_percentage']}%</td><td>{info['missing_count']}</td></tr>"
        html += "</table></div>"

        html += """
        <div class='section'>
            <h2><i class="fa-solid fa-hashtag icon"></i>Data Types Analysis</h2>
            <table><tr><th>Column</th><th>Current Type</th><th>Suggested Type</th><th>Note</th></tr>
        """
        for col, info in report['data_types_analysis'].items():
            note = info.get('note', '')
            html += f"<tr><td>{col}</td><td>{info['current_type']}</td><td>{info['suggested_type']}</td><td>{note}</td></tr>"
        html += "</table></div>"

        if report['numerical_analysis']:
            html += """
            <div class='section'>
                <h2><i class="fa-solid fa-chart-bar icon"></i>Numerical Columns Analysis</h2>
            """
            for col, stats in report['numerical_analysis'].items():
                html += f"<h3>{col}</h3><table>"
                html += f"<tr><th>Count</th><td>{stats['count']}</td><th>Unique</th><td>{stats['unique_values']} ({stats['unique_percentage']}%)</td></tr>"
                html += f"<tr><th>Mean</th><td>{stats['mean']}</td><th>Median</th><td>{stats['median']}</td></tr>"
                html += f"<tr><th>Std</th><td>{stats['std']}</td><th>Range</th><td>{stats['min']} to {stats['max']}</td></tr>"
                html += f"<tr><th>Outliers</th><td colspan='3'>{stats['outliers']['count']} ({stats['outliers']['percentage']}%)</td></tr>"
                if stats['zero_count'] > 0:
                    html += f"<tr><th>Zeros</th><td colspan='3'>{stats['zero_count']}</td></tr>"
                if stats['negative_count'] > 0:
                    html += f"<tr><th>Negative</th><td colspan='3'>{stats['negative_count']}</td></tr>"
                html += "</table>"
            html += "</div>"

        if report['categorical_analysis']:
            html += """
            <div class='section'>
                <h2><i class="fa-solid fa-list icon"></i>Categorical Columns Analysis</h2>
            """
            for col, stats in report['categorical_analysis'].items():
                html += f"<h3>{col}</h3><table>"
                html += f"<tr><th>Count</th><td>{stats['count']}</td><th>Unique</th><td>{stats['unique_values']} ({stats['unique_percentage']}%)</td></tr>"
                html += f"<tr><th>Most frequent</th><td>{stats['most_frequent']} ({stats['most_frequent_count']} times)</td><th>Text length</th><td>{stats['min_length']}-{stats['max_length']} (avg: {stats['average_length']})</td></tr>"
                if stats['patterns']['email_pattern'] > 0:
                    html += f"<tr><th>Emails detected</th><td colspan='3'>{stats['patterns']['email_pattern']}</td></tr>"
                if stats['patterns']['phone_pattern'] > 0:
                    html += f"<tr><th>Phone numbers detected</th><td colspan='3'>{stats['patterns']['phone_pattern']}</td></tr>"
                html += "</table>"
            html += "</div>"

        html += """
        <div class='section'>
            <h2><i class="fa-solid fa-warning icon"></i>Data Quality Issues</h2>
        """
        quality_issues = report['data_quality_checks']
        total_issues = sum(len(issues) for issues in quality_issues.values())
        if total_issues == 0:
            html += "<div><i class='fa-solid fa-check'></i> No major data quality issues detected!</div>"
        else:
            for col, issues in quality_issues.items():
                if issues:
                    html += f"<div><b>{col}:</b><ul>"
                    for issue in issues:
                        html += f"<li><i class='fa-solid fa-xmark'></i> {issue}</li>"
                    html += "</ul></div>"
        html += "</div>"

        summary = report['summary_statistics']
        html += f"""
        <div class='section'>
            <h2><i class=\"fa-solid fa-circle-info icon\"></i>Summary</h2>
            <div><b>Dataset Health Score:</b> {summary['dataset_health_score']}/100
                <span class='score-bar'><span class='score-bar-inner' style='width:{summary['dataset_health_score']}%;'></span></span>
            </div>
        """
        if summary['recommendations']:
            html += "<h3>Recommendations:</h3>"
            for rec in summary['recommendations']:
                html += f"<div class='recommendation'><i class='fa-solid fa-exclamation'></i> {rec}</div>"
        html += "</div>"

        html += "</div></body></html>"

        with open(output_path, "w", encoding="utf-8") as f:
            f.write(html)
        print(f"HTML report saved to {output_path}")
    def __init__(self, file_path):
        """Initialize the profiler with a file path (CSV or Parquet)."""
        self.file_path = file_path
        self.df = None
        self.profile_report = {}

    def load_data(self):
        """Load the CSV or Parquet file with automatic detection."""
        import os
        ext = os.path.splitext(self.file_path)[1].lower()
        if not os.path.exists(self.file_path):
            print(f"Error: File not found at path: {self.file_path}")
            print("Please check if:")
            print("1. The file path is correct")
            print("2. The file exists at that location")
            print("3. You have read permissions for the file")
            return False
        try:
            if ext == '.csv':
                encodings = ['utf-8', 'latin-1', 'cp1252', 'iso-8859-1', 'utf-16']
                for encoding in encodings:
                    try:
                        self.df = pd.read_csv(self.file_path, encoding=encoding)
                        print(f"Successfully loaded CSV with {encoding} encoding")
                        print(f"File size: {os.path.getsize(self.file_path)} bytes")
                        break
                    except UnicodeDecodeError:
                        continue
                    except Exception as e:
                        print(f"Error with {encoding} encoding: {str(e)}")
                        continue
                if self.df is None:
                    raise Exception("Could not decode the CSV file with any common encoding")
            elif ext == '.parquet':
                self.df = pd.read_parquet(self.file_path)
                print(f"Successfully loaded Parquet file")
                print(f"File size: {os.path.getsize(self.file_path)} bytes")
            else:
                print(f"Unsupported file extension: {ext}. Only .csv and .parquet are supported.")
                return False
        except FileNotFoundError:
            print(f"Error: File not found at path: {self.file_path}")
            return False
        except PermissionError:
            print(f"Error: No permission to read file: {self.file_path}")
            return False
        except Exception as e:
            print(f"Error loading file: {str(e)}")
            return False
        return True
    
    def basic_info(self):
        """Get basic information about the dataset."""
        info = {
            'file_name': self.file_path.split('/')[-1],
            'total_rows': len(self.df),
            'total_columns': len(self.df.columns),
            'memory_usage_mb': round(self.df.memory_usage(deep=True).sum() / 1024 / 1024, 2),
            'column_names': list(self.df.columns),
            'data_types': dict(self.df.dtypes.astype(str)),
            'file_size_info': f"{len(self.df)} rows × {len(self.df.columns)} columns"
        }
        return info
    
    def missing_data_analysis(self):
        """Comprehensive missing data analysis."""
        missing_info = {}
        
        # Overall missing statistics
        total_cells = self.df.shape[0] * self.df.shape[1]
        total_missing = self.df.isnull().sum().sum()
        
        missing_info['overall'] = {
            'total_missing_cells': int(total_missing),
            'total_cells': int(total_cells),
            'missing_percentage': round((total_missing / total_cells) * 100, 2)
        }
        
        # Per column missing analysis
        missing_info['by_column'] = {}
        for col in self.df.columns:
            missing_count = self.df[col].isnull().sum()
            missing_info['by_column'][col] = {
                'missing_count': int(missing_count),
                'missing_percentage': round((missing_count / len(self.df)) * 100, 2),
                'non_missing_count': int(len(self.df) - missing_count)
            }
        
        # Rows with missing data
        rows_with_missing = self.df.isnull().any(axis=1).sum()
        missing_info['rows_with_missing'] = {
            'count': int(rows_with_missing),
            'percentage': round((rows_with_missing / len(self.df)) * 100, 2)
        }
        
        return missing_info
    
    def data_types_analysis(self):
        """Analyze data types and suggest improvements."""
        type_analysis = {}
        
        for col in self.df.columns:
            col_data = self.df[col].dropna()
            current_type = str(self.df[col].dtype)
            
            analysis = {
                'current_type': current_type,
                'non_null_count': len(col_data),
                'suggested_type': current_type
            }
            
            if current_type == 'object':
                # Check if it could be numeric
                try:
                    pd.to_numeric(col_data, errors='raise')
                    analysis['suggested_type'] = 'numeric (int/float)'
                    analysis['note'] = 'Could be converted to numeric'
                except:
                    # Check if it could be datetime
                    try:
                        pd.to_datetime(col_data, errors='raise', infer_datetime_format=True)
                        analysis['suggested_type'] = 'datetime'
                        analysis['note'] = 'Could be converted to datetime'
                    except:
                        analysis['suggested_type'] = 'text/categorical'
                        analysis['note'] = 'Text or categorical data'
            
            type_analysis[col] = analysis
        
        return type_analysis
    
    def numerical_analysis(self):
        """Comprehensive analysis for numerical columns."""
        numerical_cols = self.df.select_dtypes(include=[np.number]).columns
        numerical_analysis = {}
        
        for col in numerical_cols:
            data = self.df[col].dropna()
            
            if len(data) == 0:
                continue
                
            analysis = {
                'count': len(data),
                'mean': round(data.mean(), 4),
                'median': round(data.median(), 4),
                'mode': data.mode().iloc[0] if len(data.mode()) > 0 else None,
                'std': round(data.std(), 4),
                'variance': round(data.var(), 4),
                'min': data.min(),
                'max': data.max(),
                'range': data.max() - data.min(),
                'q25': round(data.quantile(0.25), 4),
                'q75': round(data.quantile(0.75), 4),
                'iqr': round(data.quantile(0.75) - data.quantile(0.25), 4),
                'skewness': round(data.skew(), 4),
                'kurtosis': round(data.kurtosis(), 4),
                'unique_values': data.nunique(),
                'unique_percentage': round((data.nunique() / len(data)) * 100, 2)
            }
            
            # Outlier detection using IQR method
            Q1 = data.quantile(0.25)
            Q3 = data.quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            outliers = data[(data < lower_bound) | (data > upper_bound)]
            
            analysis['outliers'] = {
                'count': len(outliers),
                'percentage': round((len(outliers) / len(data)) * 100, 2),
                'values': outliers.tolist()[:20]  # Show first 20 outliers
            }
            
            # Zero and negative values
            analysis['zero_count'] = int((data == 0).sum())
            analysis['negative_count'] = int((data < 0).sum())
            analysis['positive_count'] = int((data > 0).sum())
            
            numerical_analysis[col] = analysis
        
        return numerical_analysis
    
    def categorical_analysis(self):
        """Comprehensive analysis for categorical/text columns."""
        categorical_cols = self.df.select_dtypes(include=['object']).columns
        categorical_analysis = {}
        
        for col in categorical_cols:
            data = self.df[col].dropna().astype(str)
            
            if len(data) == 0:
                continue
            
            # Value counts
            value_counts = data.value_counts()
            
            analysis = {
                'count': len(data),
                'unique_values': data.nunique(),
                'unique_percentage': round((data.nunique() / len(data)) * 100, 2),
                'most_frequent': value_counts.index[0] if len(value_counts) > 0 else None,
                'most_frequent_count': int(value_counts.iloc[0]) if len(value_counts) > 0 else 0,
                'least_frequent': value_counts.index[-1] if len(value_counts) > 0 else None,
                'least_frequent_count': int(value_counts.iloc[-1]) if len(value_counts) > 0 else 0,
                'top_10_values': dict(value_counts.head(10)),
                'average_length': round(data.str.len().mean(), 2),
                'min_length': int(data.str.len().min()),
                'max_length': int(data.str.len().max()),
                'empty_strings': int((data == '').sum()),
                'whitespace_only': int(data.str.strip().eq('').sum())
            }
            
            # Pattern analysis
            patterns = {
                'contains_numbers': int(data.str.contains(r'\d', na=False).sum()),
                'contains_letters': int(data.str.contains(r'[a-zA-Z]', na=False).sum()),
                'contains_special_chars': int(data.str.contains(r'[^a-zA-Z0-9\s]', na=False).sum()),
                'all_uppercase': int(data.str.isupper().sum()),
                'all_lowercase': int(data.str.islower().sum()),
                'email_pattern': int(data.str.contains(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$', na=False).sum()),
                'phone_pattern': int(data.str.contains(r'^\+?1?[-.\s]?\(?[0-9]{3}\)?[-.\s]?[0-9]{3}[-.\s]?[0-9]{4}$', na=False).sum()),
                'url_pattern': int(data.str.contains(r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', na=False).sum())
            }
            
            analysis['patterns'] = patterns
            
            categorical_analysis[col] = analysis
        
        return categorical_analysis
    
    def correlation_analysis(self):
        """Analyze correlations between numerical columns."""
        numerical_df = self.df.select_dtypes(include=[np.number])
        
        if numerical_df.shape[1] < 2:
            return {"note": "Less than 2 numerical columns found for correlation analysis"}
        
        correlation_matrix = numerical_df.corr()
        
        # Find highly correlated pairs
        high_correlations = []
        for i in range(len(correlation_matrix.columns)):
            for j in range(i+1, len(correlation_matrix.columns)):
                corr_value = correlation_matrix.iloc[i, j]
                if abs(corr_value) > 0.7:  # High correlation threshold
                    high_correlations.append({
                        'column1': correlation_matrix.columns[i],
                        'column2': correlation_matrix.columns[j],
                        'correlation': round(corr_value, 4)
                    })
        
        return {
            'correlation_matrix': correlation_matrix.round(4).to_dict(),
            'high_correlations': high_correlations,
            'summary': f"Found {len(high_correlations)} highly correlated pairs (|r| > 0.7)"
        }
    
    def data_quality_checks(self):
        """Comprehensive data quality assessment."""
        quality_issues = {}
        
        for col in self.df.columns:
            issues = []
            data = self.df[col]
            
            # Check for missing values
            missing_pct = (data.isnull().sum() / len(data)) * 100
            if missing_pct > 20:
                issues.append(f"High missing values: {missing_pct:.1f}%")
            
            # Check for duplicates in supposedly unique columns
            if data.nunique() < len(data) * 0.95 and data.nunique() > 1:
                dup_count = len(data) - data.nunique()
                issues.append(f"Potential duplicates: {dup_count} values")
            
            # Check for constant values
            if data.nunique() == 1:
                issues.append("Column has constant values")
            
            # For text columns
            if data.dtype == 'object':
                non_null_data = data.dropna().astype(str)
                if len(non_null_data) > 0:
                    # Check for inconsistent formatting
                    if non_null_data.str.strip().nunique() != non_null_data.nunique():
                        issues.append("Inconsistent whitespace formatting")
                    
                    # Check for mixed case issues
                    if (non_null_data.str.lower().nunique() != non_null_data.nunique() and 
                        non_null_data.nunique() > 1):
                        issues.append("Mixed case formatting")
            
            # For numerical columns
            elif np.issubdtype(data.dtype, np.number):
                non_null_data = data.dropna()
                if len(non_null_data) > 0:
                    # Check for extreme outliers
                    Q1 = non_null_data.quantile(0.25)
                    Q3 = non_null_data.quantile(0.75)
                    IQR = Q3 - Q1
                    extreme_outliers = non_null_data[
                        (non_null_data < Q1 - 3 * IQR) | 
                        (non_null_data > Q3 + 3 * IQR)
                    ]
                    if len(extreme_outliers) > 0:
                        issues.append(f"Extreme outliers detected: {len(extreme_outliers)}")
            
            quality_issues[col] = issues
        
        return quality_issues
    
    def generate_summary_statistics(self):
        """Generate overall summary statistics."""
        summary = {
            'dataset_health_score': 0,
            'total_issues_found': 0,
            'recommendations': []
        }
        
        # Calculate health score based on various factors
        health_factors = []
        
        # Missing data factor
        total_missing_pct = (self.df.isnull().sum().sum() / (self.df.shape[0] * self.df.shape[1])) * 100
        missing_score = max(0, 100 - total_missing_pct * 2)  # Penalize 2 points per % missing
        health_factors.append(missing_score)
        
        # Data type consistency factor
        object_cols = len(self.df.select_dtypes(include=['object']).columns)
        total_cols = len(self.df.columns)
        type_score = 100 - (object_cols / total_cols * 30)  # Penalize for too many object columns
        health_factors.append(type_score)
        
        # Uniqueness factor (avoid too many duplicates)
        uniqueness_scores = []
        for col in self.df.columns:
            col_uniqueness = (self.df[col].nunique() / len(self.df)) * 100
            uniqueness_scores.append(min(col_uniqueness, 100))
        avg_uniqueness = np.mean(uniqueness_scores) if uniqueness_scores else 0
        health_factors.append(avg_uniqueness)
        
        summary['dataset_health_score'] = round(np.mean(health_factors), 1)
        
        # Generate recommendations
        if total_missing_pct > 10:
            summary['recommendations'].append("Consider handling missing values through imputation or removal")
        
        if object_cols / total_cols > 0.7:
            summary['recommendations'].append("Many text columns detected - consider data type optimization")
        
        if avg_uniqueness < 50:
            summary['recommendations'].append("Low data uniqueness detected - check for duplicates")
        
        return summary
    
    def generate_full_report(self):
        """Generate the complete data profiling report."""
        print("Loading CSV file...")
        if not self.load_data():
            return None
        
        print("Generating comprehensive data profile report...")
        
        self.profile_report = {
            'basic_info': self.basic_info(),
            'missing_data_analysis': self.missing_data_analysis(),
            'data_types_analysis': self.data_types_analysis(),
            'numerical_analysis': self.numerical_analysis(),
            'categorical_analysis': self.categorical_analysis(),
            'correlation_analysis': self.correlation_analysis(),
            'data_quality_checks': self.data_quality_checks(),
            'summary_statistics': self.generate_summary_statistics(),
            'report_generated_at': datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        
        return self.profile_report
    
    def print_report(self):
        """Print a formatted version of the report."""
        if not self.profile_report:
            print("No report generated yet. Run generate_full_report() first.")
            return
        
        report = self.profile_report
        
        print("="*80)
        print(f"CSV DATA PROFILING REPORT")
        print("="*80)
        print(f"Generated at: {report['report_generated_at']}")
        print(f"File: {report['basic_info']['file_name']}")
        print()
        
        # Basic Info
        print("📊 BASIC INFORMATION")
        print("-" * 40)
        basic = report['basic_info']
        print(f"Dimensions: {basic['total_rows']} rows × {basic['total_columns']} columns")
        print(f"Memory Usage: {basic['memory_usage_mb']} MB")
        print(f"Columns: {', '.join(basic['column_names'])}")
        print()
        
        # Missing Data
        print("❌ MISSING DATA ANALYSIS")
        print("-" * 40)
        missing = report['missing_data_analysis']
        print(f"Overall Missing: {missing['overall']['missing_percentage']}% ({missing['overall']['total_missing_cells']} cells)")
        print(f"Rows with Missing Data: {missing['rows_with_missing']['percentage']}% ({missing['rows_with_missing']['count']} rows)")
        print("\nMissing by Column:")
        for col, info in missing['by_column'].items():
            if info['missing_percentage'] > 0:
                print(f"  {col}: {info['missing_percentage']}% ({info['missing_count']} values)")
        print()
        
        # Data Types
        print("🔢 DATA TYPES ANALYSIS")
        print("-" * 40)
        for col, info in report['data_types_analysis'].items():
            print(f"{col}: {info['current_type']} → {info['suggested_type']}")
            if 'note' in info:
                print(f"    Note: {info['note']}")
        print()
        
        # Numerical Analysis
        if report['numerical_analysis']:
            print("📈 NUMERICAL COLUMNS ANALYSIS")
            print("-" * 40)
            for col, stats in report['numerical_analysis'].items():
                print(f"\n{col}:")
                print(f"  Count: {stats['count']}, Unique: {stats['unique_values']} ({stats['unique_percentage']}%)")
                print(f"  Mean: {stats['mean']}, Median: {stats['median']}, Std: {stats['std']}")
                print(f"  Range: {stats['min']} to {stats['max']}")
                print(f"  Outliers: {stats['outliers']['count']} ({stats['outliers']['percentage']}%)")
                if stats['zero_count'] > 0:
                    print(f"  Zeros: {stats['zero_count']}")
                if stats['negative_count'] > 0:
                    print(f"  Negative: {stats['negative_count']}")
        
        # Categorical Analysis
        if report['categorical_analysis']:
            print("\n📝 CATEGORICAL COLUMNS ANALYSIS")
            print("-" * 40)
            for col, stats in report['categorical_analysis'].items():
                print(f"\n{col}:")
                print(f"  Count: {stats['count']}, Unique: {stats['unique_values']} ({stats['unique_percentage']}%)")
                print(f"  Most frequent: '{stats['most_frequent']}' ({stats['most_frequent_count']} times)")
                print(f"  Text length: {stats['min_length']}-{stats['max_length']} chars (avg: {stats['average_length']})")
                if stats['patterns']['email_pattern'] > 0:
                    print(f"  Emails detected: {stats['patterns']['email_pattern']}")
                if stats['patterns']['phone_pattern'] > 0:
                    print(f"  Phone numbers detected: {stats['patterns']['phone_pattern']}")
        
        # Data Quality Issues
        print("\n⚠️  DATA QUALITY ISSUES")
        print("-" * 40)
        quality_issues = report['data_quality_checks']
        total_issues = sum(len(issues) for issues in quality_issues.values())
        if total_issues == 0:
            print("No major data quality issues detected!")
        else:
            for col, issues in quality_issues.items():
                if issues:
                    print(f"{col}:")
                    for issue in issues:
                        print(f"  • {issue}")
        
        # Summary
        print(f"\n🎯 SUMMARY")
        print("-" * 40)
        summary = report['summary_statistics']
        print(f"Dataset Health Score: {summary['dataset_health_score']}/100")
        if summary['recommendations']:
            print("Recommendations:")
            for rec in summary['recommendations']:
                print(f"  • {rec}")
        
        print("\n" + "="*80)

# Example usage
def profile_file(file_path):
    """Main function to profile a CSV or Parquet file."""
    profiler = DataProfiler(file_path)
    report = profiler.generate_full_report()
    if report:
        profiler.print_report()
        return profiler
    else:
        print("Failed to generate report.")
        return None

# Usage example with better file path handling:
if __name__ == "__main__":
    import os
    import argparse
    parser = argparse.ArgumentParser(description="Data Profiler: Generate a data profile and HTML report for a CSV or Parquet file.")
    parser.add_argument("file_path", help="Path to the CSV or Parquet file to profile.")
    parser.add_argument("--output", "-o", default="data_profile_report.html", help="Output HTML report file name (default: data_profile_report.html)")
    args = parser.parse_args()

    if not os.path.exists(args.file_path):
        print(f"File not found: {args.file_path}")
        print("Available CSV/Parquet files in the folder:")
        folder = os.path.dirname(args.file_path) or "."
        if os.path.exists(folder):
            files = [f for f in os.listdir(folder) if f.endswith('.csv') or f.endswith('.parquet')]
            for f in files[:10]:
                print(f"  - {f}")
        else:
            print(f"Folder not found: {folder}")
    else:
        profiler = profile_file(args.file_path)
        if profiler:
            profiler.generate_html_report(args.output)