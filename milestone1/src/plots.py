import pandas as pd
from wordcloud import WordCloud
import matplotlib.pyplot as plt
import os
from summary_analysis import create_summary_analysis

# creates a wordcloud for the treatments
def create_treatments_wordcloud(df, plots_path):
    text = " ".join(df["treatments"].dropna().astype(str))
    wordcloud = WordCloud(
        width=600,
        height=600,
        background_color="white",
        colormap="viridis",
        max_words=150
    ).generate(text)
    plt.figure(figsize=(12, 8))
    plt.imshow(wordcloud, interpolation="bilinear")
    plt.axis("off")
    plt.title("Word Cloud of Treatments", fontsize=15, pad=30)
    plots_path = os.path.join(plots_dir, "treatments_wordcloud.png")
    wordcloud.to_file(plots_path)
    print(f"Word cloud saved as 'treatments_wordcloud.png' in {plots_path} directory.")

# creates a wordcloud for the symptoms
def create_symptoms_wordcloud(df, plots_path):
    text = " ".join(df["symptoms"].dropna().astype(str))
    wordcloud = WordCloud(
        width=600,
        height=600,
        background_color="white",
        colormap="prism",
        max_words=150
    ).generate(text)
    plt.figure(figsize=(12, 8))
    plt.imshow(wordcloud, interpolation="bilinear")
    plt.axis("off")
    plt.title("Word Cloud of Symptoms", fontsize=15, pad=30)
    plots_path = os.path.join(plots_dir, "symptoms_wordcloud.png")
    wordcloud.to_file(plots_path)
    print(f"Word cloud saved as 'symptoms_wordcloud.png' in {plots_path} directory.")

 

# creates a plot showing the top 10 most common diseases
def top_10_diseases_plot(var, path):
    plt.figure(figsize=(10, 6))
    pd.Series(var['most_common_diseases']).sort_values(ascending=False).plot(kind='bar', edgecolor='black')
    plt.title("Top 10 Most Common Diseases")
    plt.xlabel("Disease")
    plt.ylabel("Record count")
    plt.xticks(rotation=45, ha='right')
    plt.yscale('log')
    plt.tight_layout()
    path = os.path.join(plots_dir, "disease_counts.png")
    plt.savefig(path)
    plt.close()
    print(f"Saved: {path}")


# creates a plot showing the 20 most common diseases 
def top_20_symptoms_plot(var, path):
    plt.figure(figsize=(10, 8))
    var.head(20).sort_values().plot(kind='barh', edgecolor='black')
    plt.title("Top 20 Most Common Symptoms")
    plt.xlabel("Mentions")
    plt.ylabel("Symptom")
    plt.tight_layout()
    path = os.path.join(plots_dir, "symptom_counts.png")
    plt.savefig(path)
    plt.close()
    print(f"Saved: {path}")


# creates a plot showing the count of unique symptoms per disease
def unique_symptoms_per_disease(var, path):
    plt.figure(figsize=(12, 6))
    # Sort by unique symptom counts 
    sorted_counts = var.sort_values().reset_index(drop=True)
    plt.plot(sorted_counts, marker='o', linestyle='-', color='blue', markersize=4)
    plt.title("Unique Symptom Counts per Disease")
    plt.xlabel("Disease Index (sorted by unique symptom count)")
    plt.ylabel("Number of UNIQUE symptoms")
    plt.grid(True, linestyle='--', alpha=0.5)
    plt.tight_layout()
    path = os.path.join(plots_dir, "unique_symptoms_per_disease_line.png")
    plt.savefig(path)
    plt.close()
    print(f"Saved: {path}")


# creates a plot showing the total symptoms per disease
def symptom_mentions_per_disease(var, path):
    plt.figure(figsize=(10, 6))
    total_mention_counts.plot(kind='hist', bins=20, edgecolor='black')
    plt.title("Distribution of TOTAL Symptom Mentions per Disease")
    plt.xlabel("Total symptom mentions")
    plt.ylabel("Number of diseases")
    plt.tight_layout()
    path = os.path.join(plots_dir, "total_mentions_per_disease_distribution.png")
    plt.savefig(path)
    plt.close()
    print(f"Saved: {path}")


if __name__ == "__main__":

    path = "imgs"
    plots_dir = os.path.join(path, "plots")
    os.makedirs(plots_dir, exist_ok=True)
    df = pd.read_csv("data/final/merged_disease_symptom_list.csv")
    
    symptom_counts_total, summary_data, unique_symptom_counts, total_mention_counts = create_summary_analysis(df, "data/final/analysis.txt", plots_dir)
    create_treatments_wordcloud(df, plots_dir)
    create_symptoms_wordcloud(df, plots_dir)
    top_10_diseases_plot(summary_data, plots_dir)
    top_20_symptoms_plot(symptom_counts_total, plots_dir)
    unique_symptoms_per_disease(unique_symptom_counts, plots_dir)
    symptom_mentions_per_disease(total_mention_counts, plots_dir)